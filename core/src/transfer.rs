//! 多流 TCP 并行传输引擎
//!
//! 设计要点：
//! 1. 文件分块（chunk_size 默认 16MB），并行流数量默认 = min(CPU, 8)
//! 2. 每条数据流负责不同的 chunk，独立 TCP 连接
//! 3. 背压：进度事件走**有界** channel（容量见 `PROGRESS_CHANNEL_CAP`）。
//!    进行中的进度用 `try_send`（满了就丢，不阻塞发送主循环），
//!    终态用 `send().await` 确保送达——否则 UI 会永远停在 99%。
//! 4. 校验：目前只有「整文件 sha256」一道。发送方边传边算，传完后经
//!    `POST /api/verify/:file_id` 补发给接收方，由接收方 finalize 时比对。
//!    TODO: chunk 级校验尚未实现——`DataFrameHeader` 只有 4 个字段，
//!    没有 checksum 位。现状下单块数据损坏只有整文件 sha256 能发现，
//!    而发现之后只能整体重传。阶段 5b 上加密后，认证标签会顺带补上这一层。
//! 5. TODO: 断点续传尚未实现。chunk 完成位图只在内存里，进程重启即丢失，
//!    发送方也没有获知旧 file_id 的渠道。详见方案 N2 / N3。
//! 6. 接收方需先弹窗确认（HTTP offer/accept 握手）才开始 TCP 数据流
//!
//! 跨平台实现：使用 `Seek + Read`，不依赖平台专属零拷贝 API。
//! 后续可按平台用 cfg 切到 sendfile/TransmitFile 等优化。

use crate::protocol::{HttpIncomingResponse, HttpOffer, IncomingEntry, WsEvent, DataFrameHeader};
use crate::storage::StorageManager;
use crate::Result;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot, watch, Mutex as TokioMutex};
use tracing::{error, info, warn};

/// 传输状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Canceled,
}

/// 进度信息（供 UI 订阅）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferProgress {
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub bytes_transferred: u64,
    pub chunks_done: u64,
    pub chunks_total: u64,
    pub speed_bps: u64,
    pub status: TransferStatus,
    pub error: Option<String>,
    /// 标识本条进度是「接收方」视角（true）还是「发送方」视角（false）。
    /// UI 可据此在「接收」与「发出」两个列表分别展示。
    #[serde(default)]
    pub incoming: bool,
    /// 接收完成后文件的最终保存路径（仅接收方 Completed 时填充，供 UI 打开/跳转）
    #[serde(default)]
    pub file_path: Option<String>,
}

/// 传输句柄，可用于取消与订阅进度
/// 单个传输的进度通道容量。
///
/// 用有界而非无界：InProgress 是高频帧（每块一条），UI 若不消费，
/// 无界通道会一路堆到 OOM。
const PROGRESS_CHANNEL_CAP: usize = 256;

/// 终态进度（Failed / Canceled / Completed）的最长等待时间。
const TERMINAL_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);

/// 全量进度快照的条目上限（`GET /api/transfers` 的数据源）。
///
/// daemon 是常驻进程，不设限的话每传一个文件都会在这里留一条终态进度，
/// 传上千个文件就积上千条永不释放的记录。
const PROGRESS_CACHE_CAP: usize = 200;

/// 有上限的进度快照：HashMap + 插入顺序队列。
///
/// 淘汰时优先丢**已终态**的最早条目——老任务的终态进度通常已经没人看了；
/// 全是进行中时退化为丢最早一条。宁可让最老的任务从列表里消失，
/// 也不能让新进度写不进来。
struct ProgressCache {
    cap: usize,
    map: HashMap<String, TransferProgress>,
    /// 插入顺序；只有 file_id 首次出现时才入队
    order: VecDeque<String>,
}

impl ProgressCache {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn insert(&mut self, p: TransferProgress) {
        let file_id = p.file_id.clone();
        // 已存在的 file_id 只是更新进度，不改动它的插入顺序
        if self.map.insert(file_id.clone(), p).is_some() {
            return;
        }
        self.order.push_back(file_id);

        // evict_one 返回 false 表示再也淘汰不掉（理论上不会发生），
        // 此时必须退出，否则是一个死循环。
        while self.map.len() > self.cap && self.evict_one() {}
    }

    /// 淘汰一条，成功返回 true
    fn evict_one(&mut self) -> bool {
        // 优先挑一个终态条目淘汰
        let terminal = self.order.iter().position(|id| {
            self.map
                .get(id)
                .map(|p| is_terminal(&p.status))
                .unwrap_or(false)
        });
        if let Some(idx) = terminal {
            if let Some(id) = self.order.remove(idx) {
                self.map.remove(&id);
                return true;
            }
        }

        // 全是进行中：退化为淘汰最早一条。
        // 这里的 continue（即继续 pop）是刻意的：队列里可能残留已失效的 id，
        // 一旦写成 break，整个淘汰就停摆，快照再也降不下来。
        while let Some(id) = self.order.pop_front() {
            if self.map.remove(&id).is_some() {
                return true;
            }
        }
        false
    }

    fn snapshot(&self) -> Vec<TransferProgress> {
        self.map.values().cloned().collect()
    }
}

/// 终态：不会再发生变化的进度
fn is_terminal(s: &TransferStatus) -> bool {
    matches!(
        s,
        TransferStatus::Completed | TransferStatus::Failed | TransferStatus::Canceled
    )
}

/// 推送一条进度帧。
///
/// 两类帧必须区别对待，这就是本函数存在的全部理由：
/// - **InProgress**：高频（每块一条）。用 `try_send`，通道满就直接丢。
///   绝不能在这里 await——UI 不消费时，无界通道吃光内存、有界通道拖死
///   传输主循环，两种都是事故。丢几帧无所谓：进度是瞬时快照，后到的会覆盖。
/// - **其余状态**：必须送达。丢一帧终态，UI 就永远停在 99%。
///   用带超时的 `send`：超时说明压根没人消费，记一条 warn 后放弃，
///   不能让收尾消息把任务永久挂住。
async fn push_progress(tx: &mpsc::Sender<TransferProgress>, p: TransferProgress) {
    let terminal = !matches!(p.status, TransferStatus::InProgress);
    if terminal {
        let file_id = p.file_id.clone();
        let status = p.status.clone();
        if tokio::time::timeout(TERMINAL_PROGRESS_TIMEOUT, tx.send(p))
            .await
            .is_err()
        {
            warn!(
                file_id = %file_id,
                ?status,
                "终态进度 {}s 未送达，UI 可能停在中间态",
                TERMINAL_PROGRESS_TIMEOUT.as_secs()
            );
        }
    } else if let Err(e) = tx.try_send(p) {
        // 通道满：丢弃这一帧。进度是瞬时快照，下一帧会覆盖它。
        tracing::debug!(error = %e, "进度通道已满，丢弃一帧 InProgress");
    }
}

pub struct TransferHandle {
    pub file_id: String,
    cancel_tx: watch::Sender<bool>,
    progress_rx: mpsc::Receiver<TransferProgress>,
}

impl TransferHandle {
    pub async fn next_progress(&mut self) -> Option<TransferProgress> {
        self.progress_rx.recv().await
    }
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }
}

/// 接收方对每个 incoming offer 维护一个决策槽
struct IncomingSlot {
    entry: IncomingEntry,
    decision_tx: watch::Sender<Option<bool>>, // None=待决定, Some(true)=接受, Some(false)=拒绝
}

/// 接收方 incoming 管理器：登记待决定的传入请求
pub struct IncomingManager {
    slots: Mutex<HashMap<String, IncomingSlot>>,
}

impl IncomingManager {
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// 登记 incoming offer，返回 entry 供 WebSocket 推送给 UI
    pub fn register(&self, offer: HttpOffer) -> IncomingEntry {
        let incoming_id = uuid::Uuid::new_v4().to_string();
        let entry = IncomingEntry {
            incoming_id: incoming_id.clone(),
            file_id: offer.file_id.clone(),
            file_name: offer.file_name,
            file_size: offer.file_size,
            sha256: offer.sha256,
            sha256_deferred: offer.sha256_deferred,
            from_id: offer.from_id,
            from_name: offer.from_name,
            from_ip: offer.from_ip,
            from_gateway_port: offer.from_gateway_port,
            from_transfer_port: offer.from_transfer_port,
            created_at: now_ms(),
            decision: None,
        };
        let (decision_tx, _decision_rx) = watch::channel(None);
        self.slots.lock().insert(incoming_id.clone(), IncomingSlot {
            entry: entry.clone(),
            decision_tx,
        });
        entry
    }

    /// UI 决策。返回 Some(entry) 表示找到该 incoming，None 表示已超时/不存在
    ///
    /// 注意：接受时不能在此移除槽位 —— gateway 的 accept 路由先调本方法设置决议，
    /// 随后后台任务（wait_decision 唤醒后）再调 `TransferEngine::decide_incoming`
    /// 创建接收槽；槽位移除由 decide_incoming 完成（见下）。
    pub async fn decide(&self, incoming_id: &str, accept: bool) -> Option<IncomingEntry> {
        let slot_opt = self.slots.lock().get(incoming_id).map(|s| IncomingSlotRef {
            decision_tx: s.decision_tx.clone(),
            entry: s.entry.clone(),
        });
        let slot = slot_opt?;
        let _ = slot.decision_tx.send(Some(accept));
        if !accept {
            // 拒绝：无后续流程，立即移除
            self.slots.lock().remove(incoming_id);
        }
        Some(slot.entry)
    }

    /// 等待 UI 决策（async，超时自动 reject）
    pub async fn wait_decision(&self, incoming_id: &str) -> Option<bool> {
        let (tx, mut rx) = {
            let slots = self.slots.lock();
            let slot = slots.get(incoming_id)?;
            (slot.decision_tx.clone(), slot.decision_tx.subscribe())
        };
        // 当前值
        if let Some(d) = *rx.borrow() {
            return Some(d);
        }
        // 等待变化，最多 60 秒（给用户足够时间在弹窗上决策）
        match tokio::time::timeout(Duration::from_secs(60), rx.changed()).await {
            Ok(Ok(())) => *rx.borrow(),
            _ => {
                // 超时：自动 reject
                let _ = tx.send(Some(false));
                None
            }
        }
    }

    /// 列出所有 pending incoming（供 UI / GET /api/incoming）
    pub fn list_pending(&self) -> Vec<IncomingEntry> {
        self.slots
            .lock()
            .values()
            .map(|s| s.entry.clone())
            .collect()
    }

    /// 接收完成或失败后清理
    pub fn remove(&self, incoming_id: &str) {
        self.slots.lock().remove(incoming_id);
    }

    /// 按 file_id 清理（传输完成/失败/取消后调用）
    pub fn remove_by_file_id(&self, file_id: &str) {
        let mut slots = self.slots.lock();
        let keys: Vec<String> = slots
            .iter()
            .filter(|(_, s)| s.entry.file_id == file_id)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            slots.remove(&k);
        }
    }
}

/// 辅助：从 Mutex<HashMap> 里安全取出 slot（避免持有锁跨 await）
struct IncomingSlotRef {
    decision_tx: watch::Sender<Option<bool>>,
    entry: IncomingEntry,
}

/// 传输引擎：负责发送与接收
pub struct TransferEngine {
    pub transfer_port: u16,
    pub parallel_streams: usize,
    pub chunk_size: usize,
    pub receive_dir: PathBuf,
    inflight: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
    /// 发送方 daemon：等待接收方回包的 file_id → oneshot
    outgoing_offers: Arc<TokioMutex<HashMap<String, oneshot::Sender<HttpIncomingResponse>>>>,
    /// 接收方 daemon：pending incoming 请求管理
    pub incoming: Arc<IncomingManager>,
    /// 接收方 daemon：文件落盘
    pub storage: Arc<StorageManager>,
    /// 接收方 daemon：file_id_prefix → file_id 映射（Accept 时登记，serve_data_stream 用）
    prefix_to_file_id: TokioMutex<HashMap<u64, String>>,
    /// 进度广播 bus（gateway 注入；None 时 send_file 仍能用，只是不广播）
    progress_bus: TokioMutex<Option<Arc<broadcast::Sender<TransferProgress>>>>,
    /// WebSocket 事件 bus（gateway 注入）
    ws_event_bus: TokioMutex<Option<Arc<broadcast::Sender<WsEvent>>>>,
    /// 全量进度快照：file_id → 最近一条进度（GET /api/transfers 用）。
    /// 有条目上限，超出后按插入顺序淘汰，见 [`ProgressCache`]。
    progress_cache: Arc<Mutex<ProgressCache>>,
    /// 接收方速率统计：file_id → (首 chunk 时间, 上次已收字节数)
    recv_speed_state: TokioMutex<HashMap<String, (Instant, u64)>>,
    /// 已接受的 incoming：file_id → (发送方 IP, 发送方 gateway 端口)
    /// 接收方取消时用于通知发送方联动取消
    incoming_endpoints: TokioMutex<HashMap<String, (String, u16)>>,
}

impl TransferEngine {
    pub fn new(
        transfer_port: u16,
        parallel_streams: usize,
        chunk_size: usize,
        receive_dir: PathBuf,
    ) -> Self {
        Self {
            transfer_port,
            parallel_streams,
            chunk_size,
            receive_dir: receive_dir.clone(),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            outgoing_offers: Arc::new(TokioMutex::new(HashMap::new())),
            incoming: Arc::new(IncomingManager::new()),
            storage: Arc::new(StorageManager::new(receive_dir)),
            prefix_to_file_id: TokioMutex::new(HashMap::new()),
            progress_bus: TokioMutex::new(None),
            ws_event_bus: TokioMutex::new(None),
            progress_cache: Arc::new(Mutex::new(ProgressCache::new(PROGRESS_CACHE_CAP))),
            recv_speed_state: TokioMutex::new(HashMap::new()),
            incoming_endpoints: TokioMutex::new(HashMap::new()),
        }
    }

    /// gateway 启动后注入 progress 广播
    pub async fn set_progress_bus(&self, bus: Arc<broadcast::Sender<TransferProgress>>) {
        *self.progress_bus.lock().await = Some(bus);
    }

    /// gateway 启动后注入 WsEvent 广播
    pub async fn set_ws_event_bus(&self, bus: Arc<broadcast::Sender<WsEvent>>) {
        *self.ws_event_bus.lock().await = Some(bus);
    }

    async fn broadcast_progress(&self, p: TransferProgress) {
        if let Some(bus) = self.progress_bus.lock().await.clone() {
            let _ = bus.send(p);
        }
    }

    /// 发布一条进度：写入全量快照 + 广播给订阅方
    pub async fn publish_progress(&self, p: TransferProgress) {
        self.progress_cache.lock().insert(p.clone());
        self.broadcast_progress(p).await;
    }

    /// 当前所有传输的最近进度（GET /api/transfers 数据源）
    pub fn list_transfers(&self) -> Vec<TransferProgress> {
        self.progress_cache.lock().snapshot()
    }

    /// 取消一个传输（file_id 可能是本机作为发送方或接收方的任务）
    ///
    /// 返回是否找到对应任务。
    pub async fn cancel(&self, file_id: &str) -> bool {
        let mut found = false;

        // ---- 发送方视角：通知 do_send 停止 + 让 offer 等待立即返回 ----
        if let Some(tx) = self.inflight.lock().get(file_id).cloned() {
            let _ = tx.send(true);
            found = true;
        }
        // 若发送方正卡在等回包，直接注入一条“已取消”回包
        if let Some(tx) = self.outgoing_offers.lock().await.remove(file_id) {
            let _ = tx.send(HttpIncomingResponse {
                file_id: file_id.to_string(),
                accepted: false,
                reason: Some("canceled by sender".into()),
                transfer_port: self.transfer_port,
            });
            found = true;
        }

        // ---- 接收方视角：移除接收槽位、清理映射、推 Canceled ----
        let slot = self
            .storage
            .list_in_progress()
            .await
            .into_iter()
            .find(|s| s.file_id == file_id);
        if let Some(slot) = slot {
            self.storage.abort(file_id).await;
            let prefix = file_id_prefix_u64(file_id);
            self.prefix_to_file_id.lock().await.remove(&prefix);
            self.incoming.remove_by_file_id(file_id);
            self.recv_speed_state.lock().await.remove(file_id);
            // 通知发送方联动取消（发送方收到 POST /api/cancel 后停止发送）
            if let Some((from_ip, from_gateway_port)) =
                self.incoming_endpoints.lock().await.remove(file_id)
            {
                let file_id_owned = file_id.to_string();
                tokio::spawn(async move {
                    let path = format!("/api/cancel/{}", file_id_owned);
                    if let Err(e) = http_post_json(&from_ip, from_gateway_port, &path, "{}").await
                    {
                        warn!(error = %e, "notify sender cancel failed");
                    }
                });
            }
            self.publish_progress(TransferProgress {
                file_id: file_id.to_string(),
                file_name: slot.file_name.clone(),
                file_size: slot.file_size,
                bytes_transferred: 0,
                chunks_done: 0,
                chunks_total: slot.chunk_count,
                speed_bps: 0,
                status: TransferStatus::Canceled,
                error: Some("canceled by receiver".into()),
                incoming: true,
                file_path: None,
            })
            .await;
            found = true;
        }

        if found {
            info!(%file_id, "transfer canceled");
        }
        found
    }

    async fn broadcast_ws_event(&self, ev: WsEvent) {
        if let Some(bus) = self.ws_event_bus.lock().await.clone() {
            let _ = bus.send(ev);
        }
    }

    /// 发送任务结束后清理注册表（inflight / 等回包 oneshot）
    async fn cleanup_send_state(&self, file_id: &str) {
        self.inflight.lock().remove(file_id);
        self.outgoing_offers.lock().await.remove(file_id);
    }

    /// 启动接收端 TCP listener，等待对端发起的数据流连接
    pub async fn spawn_receiver(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(("0.0.0.0", self.transfer_port))
            .await
            .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
        info!(port = self.transfer_port, "transfer listener bound");
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let engine = self.clone();
                        tokio::spawn(async move {
                            if let Err(e) = engine.serve_data_stream(stream).await {
                                warn!(?peer, error = %e, "data stream error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "accept failed");
                    }
                }
            }
        });
        Ok(())
    }

    /// 服务一个数据流连接：读 header + chunk → 写入 storage → 推进度
    async fn serve_data_stream(self: Arc<Self>, mut stream: TcpStream) -> Result<()> {
        let mut header_buf = [0u8; DataFrameHeader::SIZE];
        stream
            .read_exact(&mut header_buf)
            .await
            .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
        let header = DataFrameHeader::from_bytes(&header_buf)
            .map_err(|e| crate::CoreError::Transfer(format!("invalid header: {}", e)))?;

        let mut buf = vec![0u8; header.data_len as usize];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;

        // 通过 file_id_prefix 反查 file_id（接收方在 Accept 时建立映射）
        let file_id = {
            let prefix = header.file_id_prefix;
            let map = self.prefix_to_file_id.lock().await;
            map.get(&prefix).cloned()
        };

        if let Some(file_id) = file_id {
            // 写入对应 chunk
            if let Err(e) = self.storage.write_chunk(&file_id, header.chunk_id, &buf).await {
                // 写盘失败：推 Failed 并返回，不再 ACK
                error!(%file_id, chunk = header.chunk_id, error = %e, "write chunk failed");
                let slot = self.storage.list_in_progress().await.into_iter().find(|s| s.file_id == file_id);
                let (name, size, chunk_count) = match &slot {
                    Some(s) => (s.file_name.clone(), s.file_size, s.chunk_count),
                    None => (String::new(), 0, 0),
                };
                self.publish_progress(TransferProgress {
                    file_id: file_id.clone(),
                    file_name: name,
                    file_size: size,
                    bytes_transferred: 0,
                    chunks_done: 0,
                    chunks_total: chunk_count,
                    speed_bps: 0,
                    status: TransferStatus::Failed,
                    error: Some(format!("write chunk: {}", e)),
                    incoming: true,
                file_path: None,
                })
                .await;
                return Err(e);
            }

            // 更新接收进度并广播（速率 = 累计字节 / 首chunk以来的耗时）
            let slot = self.storage.list_in_progress().await.into_iter().find(|s| s.file_id == file_id);
            if let Some(slot) = slot {
                let chunks_done = slot.received_chunks.iter().filter(|ok| **ok).count() as u64;
                let bytes_done = chunks_done * slot.chunk_size as u64;
                let speed_bps = {
                    let mut state = self.recv_speed_state.lock().await;
                    let (start, _) = state
                        .entry(file_id.clone())
                        .or_insert_with(|| (Instant::now(), 0));
                    let elapsed = start.elapsed().as_secs_f64();
                    if elapsed >= 0.5 {
                        (bytes_done as f64 / elapsed) as u64
                    } else {
                        0 // 首个 chunk 窗口太短，避免虚高
                    }
                };
                let progress = TransferProgress {
                    file_id: file_id.clone(),
                    file_name: slot.file_name.clone(),
                    file_size: slot.file_size,
                    bytes_transferred: bytes_done.min(slot.file_size),
                    chunks_done,
                    chunks_total: slot.chunk_count,
                    speed_bps,
                    status: TransferStatus::InProgress,
                    error: None,
                    incoming: true,
                file_path: None,
                };
                self.publish_progress(progress).await;

                // 全部 chunk 收齐后的收尾
                if slot.is_complete() {
                    if slot.await_sha256 && slot.sha256.is_none() {
                        // 发送方声明会补发 sha256（大文件边传边算）：先不 finalize，
                        // 等 POST /api/verify。保险丝：120 秒未收到（发送方失联）
                        // 则跳过校验直接完成，避免接收方永久卡在 InProgress。
                        let engine = self.clone();
                        let fid = file_id.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_secs(120)).await;
                            // None 不覆盖已到的真哈希；slot 已 finalize 则为幂等 no-op
                            let _ = engine.apply_final_sha256(&fid, None).await;
                        });
                    } else {
                        let _ = self.finish_receive(&file_id, &slot).await;
                    }
                }
            }
        } else {
            // 不回 ACK：发送方等不到确认会按失败处理。
            // 若这里回 ok=true，会掩盖丢块（发送方显示完成、接收方缺 chunk）。
            warn!(prefix = header.file_id_prefix, "unknown file_id_prefix (no receive slot)");
            return Err(crate::CoreError::Transfer(format!(
                "unknown file_id_prefix {} (no receive slot)",
                header.file_id_prefix
            )));
        }

        // ACK 给发送方（控制消息，JSON 行）
        let ack = crate::protocol::ControlMessage::ChunkAck {
            file_id: String::new(),
            chunk_id: header.chunk_id,
            ok: true,
        };
        let line = ack.to_line().unwrap_or_default();
        stream
            .write_all(line.as_bytes())
            .await
            .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
        Ok(())
    }

    /// 接收方收尾：校验 sha256 + 原子重命名 + 清理注册表 + 推最终进度
    ///
    /// （原 serve_data_stream 完成分支的逻辑，抽出供「哈希随到随 finalize」复用）
    async fn finish_receive(
        &self,
        file_id: &str,
        slot: &crate::storage::ReceiveSlot,
    ) -> crate::Result<()> {
        let result = self.storage.finalize(file_id, slot.sha256.as_deref()).await;
        // 清理 prefix 映射、incoming 条目、发送方地址登记与速率统计
        let prefix = file_id_prefix_u64(file_id);
        self.prefix_to_file_id.lock().await.remove(&prefix);
        self.incoming.remove_by_file_id(file_id);
        self.incoming_endpoints.lock().await.remove(file_id);
        self.recv_speed_state.lock().await.remove(file_id);

        let (status, error) = match &result {
            Ok(()) => (TransferStatus::Completed, None),
            Err(e) => (TransferStatus::Failed, Some(e.to_string())),
        };
        // 完成时携带最终保存路径，UI 据此提供“打开文件 / 打开所在文件夹”
        let file_path = if matches!(status, TransferStatus::Completed) {
            Some(slot.final_path.to_string_lossy().into_owned())
        } else {
            None
        };
        self.publish_progress(TransferProgress {
            file_id: file_id.to_string(),
            file_name: slot.file_name.clone(),
            file_size: slot.file_size,
            bytes_transferred: if matches!(status, TransferStatus::Completed) {
                slot.file_size
            } else {
                0
            },
            chunks_done: slot.chunk_count,
            chunks_total: slot.chunk_count,
            speed_bps: 0,
            status,
            error,
            incoming: true,
            file_path,
        })
        .await;
        result
    }

    /// 接收方收到发送方补发的最终 sha256（POST /api/verify/:file_id）。
    ///
    /// - 哈希先到（传输未完）：暂存到槽位，等最后一个 chunk 收齐时一并 finalize
    /// - chunk 已收齐（常见时序）：立即校验 + finalize
    /// - 槽位不存在（已 finalize / 重复通知）：幂等成功
    pub async fn apply_final_sha256(
        &self,
        file_id: &str,
        sha256: Option<String>,
    ) -> crate::Result<()> {
        let Some(slot) = self.storage.set_final_sha256(file_id, sha256).await else {
            return Ok(());
        };
        if slot.is_complete() {
            return self.finish_receive(file_id, &slot).await;
        }
        Ok(())
    }

    /// 接收方 daemon 收到发送方 POST /api/incoming 时调用
    pub async fn handle_incoming_offer(&self, offer: HttpOffer) -> IncomingEntry {
        let entry = self.incoming.register(offer);
        // 推 WebSocket 事件给 UI
        self.broadcast_ws_event(WsEvent::Incoming {
            entry: entry.clone(),
        })
        .await;

        // 同步等待 UI 决策（30 秒超时自动 reject）
        let incoming_id = entry.incoming_id.clone();
        let accepted = self.incoming.wait_decision(&incoming_id).await;

        // 推 IncomingResolved 让 UI 关闭弹窗
        self.broadcast_ws_event(WsEvent::IncomingResolved {
            incoming_id: incoming_id.clone(),
            accepted: accepted.unwrap_or(false),
        })
        .await;

        entry
    }

    /// UI 决策后调用
    pub async fn decide_incoming(&self, incoming_id: &str, accept: bool) -> Option<IncomingEntry> {
        let entry = self.incoming.decide(incoming_id, accept).await?;
        if accept {
            // 接受：在 storage 中创建接收槽，登记 file_id_prefix 映射
            let _ = self
                .storage
                .create_slot(
                    entry.file_id.clone(),
                    entry.file_name.clone(),
                    entry.file_size,
                    self.chunk_size,
                    entry.sha256.clone(),
                    entry.sha256_deferred,
                )
                .await;
            let prefix = file_id_prefix_u64(&entry.file_id);
            self.prefix_to_file_id.lock().await.insert(prefix, entry.file_id.clone());
            // 登记发送方地址（接收方取消时通知其联动取消）
            self.incoming_endpoints.lock().await.insert(
                entry.file_id.clone(),
                (entry.from_ip.clone(), entry.from_gateway_port),
            );
            info!(file_id = %entry.file_id, "incoming accepted, receive slot ready");
            // 接收槽已创建、prefix 映射已登记 → 从 pending 表移除
            self.incoming.remove(incoming_id);
        } else {
            info!(file_id = %entry.file_id, "incoming rejected by user");
        }
        Some(entry)
    }

    /// 发送方 daemon 收到接收方 POST /api/incoming-resp 时调用
    pub async fn handle_incoming_response(&self, file_id: &str, resp: HttpIncomingResponse) {
        let tx_opt = self.outgoing_offers.lock().await.remove(file_id);
        if let Some(tx) = tx_opt {
            let _ = tx.send(resp);
        }
    }

    /// 发送文件到对端，返回可订阅进度的句柄
    ///
    /// 流程：
    /// 1. 计算 file_id, file_size, file_name
    /// 2. POST /api/incoming 到对端 gateway（offer，立即发出不等哈希）
    /// 3. 等对端回 /api/incoming-resp（Accept/Reject）
    /// 4. Accept → 启动多流 TCP 传输；Reject → 推 Canceled 进度
    /// 5. 全部 chunk ACK 后，POST /api/verify/:file_id 补发 sha256（接收方校验）
    ///
    /// `file_name_override`：SAF 等场景传入的路径无法推断出原始文件名
    /// （如 /proc/self/fd/N）时，由调用方显式指定
    pub async fn send_file(
        self: Arc<Self>,
        target_ip: String,
        target_transfer_port: u16,
        target_gateway_port: u16,
        file_path: PathBuf,
        file_name_override: Option<String>,
        self_id: String,
        self_name: String,
        self_ip: String,
        self_gateway_port: u16,
    ) -> Result<TransferHandle> {
        // 源 IP（回包地址）自动选择：用 UDP connect 让 OS 按目标做路由决策，
        // 多网卡环境（如 Android WiFi+蜂窝）下必选对通往目标的网卡。
        // 调用方传入的 self_ip 仅作回退（UDP 路由不可用时）。
        let self_ip = local_source_ip(&target_ip).unwrap_or(self_ip);

        let file_id = uuid::Uuid::new_v4().to_string();
        let file_name = file_name_override.unwrap_or_else(|| {
            file_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string()
        });
        let file_size = tokio::fs::metadata(&file_path)
            .await
            .map_err(|e| crate::CoreError::Transfer(e.to_string()))?
            .len();

        let chunk_size = self.chunk_size;
        let chunk_count = (file_size + chunk_size as u64 - 1) / chunk_size as u64;
        let parallel = self.parallel_streams;

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (progress_tx, progress_rx) = mpsc::channel(PROGRESS_CHANNEL_CAP);
        // 保留一份 cancel 订阅，供发送任务结束时判断最终状态是 Canceled 还是 Completed
        let cancel_check = cancel_rx.clone();

        self.inflight
            .lock()
            .insert(file_id.clone(), cancel_tx.clone());

        // 登记等待回包的 oneshot
        let (resp_tx, resp_rx) = oneshot::channel::<HttpIncomingResponse>();
        self.outgoing_offers
            .lock()
            .await
            .insert(file_id.clone(), resp_tx);

        let initial = TransferProgress {
            file_id: file_id.clone(),
            file_name: file_name.clone(),
            file_size,
            bytes_transferred: 0,
            chunks_done: 0,
            chunks_total: chunk_count,
            speed_bps: 0,
            status: TransferStatus::Pending,
            error: None,
            incoming: false,
                file_path: None,
        };
        push_progress(&progress_tx, initial).await;

        let engine = self.clone();
        let file_id_for_spawn = file_id.clone();
        let file_name_for_err = file_name.clone();
        tokio::spawn(async move {
            // 0. 后台并行计算整文件 sha256：与传输同时进行，offer 不再等它。
            //    大文件哈希要读完整文件（2GB 十几秒），之前放在 offer 前面会
            //    导致对端弹窗迟迟不出现；现在哈希随 /api/verify 在传完后补发。
            let hash_task = {
                let path_clone = file_path.clone();
                tokio::task::spawn_blocking(move || -> std::io::Result<String> {
                    use sha2::{Digest, Sha256};
                    let mut f = std::fs::File::open(&path_clone)?;
                    let mut hasher = Sha256::new();
                    std::io::copy(&mut f, &mut hasher)?;
                    Ok(format!("{:x}", hasher.finalize()))
                })
            };

            // 1. POST offer 到对端 gateway（立即发出）
            let offer = HttpOffer {
                file_id: file_id_for_spawn.clone(),
                file_name: file_name_for_err.clone(),
                file_size,
                sha256: None,
                sha256_deferred: true,
                from_id: self_id.clone(),
                from_name: self_name.clone(),
                from_ip: self_ip.clone(),
                from_gateway_port: self_gateway_port,
                from_transfer_port: engine.transfer_port,
            };
            let offer_json = match serde_json::to_string(&offer) {
                Ok(s) => s,
                Err(e) => {
                    push_progress(&progress_tx, TransferProgress {
                        file_id: file_id_for_spawn.clone(),
                        file_name: file_name_for_err.clone(),
                        file_size,
                        bytes_transferred: 0,
                        chunks_done: 0,
                        chunks_total: chunk_count,
                        speed_bps: 0,
                        status: TransferStatus::Failed,
                        error: Some(format!("serialize offer: {}", e)),
                        incoming: false,
                file_path: None,
                    }).await;
                    return;
                }
            };

            // 用纯 TCP 发 HTTP POST
            let post_result = http_post_json(
                &target_ip,
                target_gateway_port,
                "/api/incoming",
                &offer_json,
            )
            .await;

            if let Err(e) = post_result {
                push_progress(&progress_tx, TransferProgress {
                    file_id: file_id_for_spawn.clone(),
                    file_name: file_name_for_err.clone(),
                    file_size,
                    bytes_transferred: 0,
                    chunks_done: 0,
                    chunks_total: chunk_count,
                    speed_bps: 0,
                    status: TransferStatus::Failed,
                    error: Some(format!("post offer: {}", e)),
                    incoming: false,
                file_path: None,
                }).await;
                engine.cleanup_send_state(&file_id_for_spawn).await;
                return;
            }

            // 2. 等对端 /api/incoming-resp 回包（70 秒，长于接收方 60 秒
            //    的决策超时：正常必收到明确回包，自身超时=对端 daemon 失联）
            let resp = match tokio::time::timeout(Duration::from_secs(70), resp_rx).await {
                Ok(Ok(r)) => r,
                _ => HttpIncomingResponse {
                    file_id: file_id_for_spawn.clone(),
                    accepted: false,
                    reason: Some("timeout waiting decision".into()),
                    transfer_port: target_transfer_port,
                },
            };

            if !resp.accepted {
                push_progress(&progress_tx, TransferProgress {
                    file_id: file_id_for_spawn.clone(),
                    file_name: file_name_for_err.clone(),
                    file_size,
                    bytes_transferred: 0,
                    chunks_done: 0,
                    chunks_total: chunk_count,
                    speed_bps: 0,
                    status: TransferStatus::Canceled,
                    error: resp.reason.clone(),
                    incoming: false,
                file_path: None,
                }).await;
                engine.cleanup_send_state(&file_id_for_spawn).await;
                return;
            }

            // 3. Accept → 启动多流 TCP 传输
            let engine_for_cleanup = engine.clone();
            let mut result = engine
                .do_send(
                    target_ip.clone(),
                    target_transfer_port,
                    file_path,
                    file_id_for_spawn.clone(),
                    file_name_for_err.clone(),
                    file_size,
                    chunk_count,
                    chunk_size,
                    parallel,
                    cancel_rx,
                    progress_tx.clone(),
                )
                .await;

            // 清理发送方注册表（inflight / 等回包 oneshot）
            engine_for_cleanup.cleanup_send_state(&file_id_for_spawn).await;

            // 被取消时最终状态是 Canceled，不是 Completed/Failed
            let canceled = *cancel_check.borrow();

            // 4. 数据全部 ACK 且未取消 → 取后台哈希结果，通知接收方最终校验。
            //    哈希与传输并行进行，这里通常零等待；
            //    哈希失败则发 null（接收方跳过校验直接完成），网络失败则整体 Failed。
            if !canceled && result.is_ok() {
                let sha256 = match hash_task.await {
                    Ok(Ok(s)) => Some(s),
                    Ok(Err(e)) => {
                        warn!(file_id = %file_id_for_spawn, error = %e, "sha256 compute failed, skip verify");
                        None
                    }
                    Err(e) => {
                        warn!(file_id = %file_id_for_spawn, error = %e, "sha256 task panicked, skip verify");
                        None
                    }
                };
                let body = serde_json::json!({ "sha256": sha256 }).to_string();
                if let Err(e) = http_post_json(
                    &target_ip,
                    target_gateway_port,
                    &format!("/api/verify/{}", file_id_for_spawn),
                    &body,
                )
                .await
                {
                    result = Err(crate::CoreError::Transfer(format!(
                        "notify verify: {}",
                        e
                    )));
                }
            }
            let final_progress = if canceled {
                TransferProgress {
                    file_id: file_id_for_spawn.clone(),
                    file_name: file_name_for_err.clone(),
                    file_size,
                    bytes_transferred: 0,
                    chunks_done: 0,
                    chunks_total: chunk_count,
                    speed_bps: 0,
                    status: TransferStatus::Canceled,
                    error: Some("canceled".into()),
                    incoming: false,
                file_path: None,
                }
            } else {
                match result {
                    Ok(()) => TransferProgress {
                        file_id: file_id_for_spawn.clone(),
                        file_name: file_name_for_err.clone(),
                        file_size,
                        bytes_transferred: file_size,
                        chunks_done: chunk_count,
                        chunks_total: chunk_count,
                        speed_bps: 0,
                        status: TransferStatus::Completed,
                        error: None,
                        incoming: false,
                file_path: None,
                    },
                    Err(e) => TransferProgress {
                        file_id: file_id_for_spawn.clone(),
                        file_name: file_name_for_err.clone(),
                        file_size,
                        bytes_transferred: 0,
                        chunks_done: 0,
                        chunks_total: chunk_count,
                        speed_bps: 0,
                        status: TransferStatus::Failed,
                        error: Some(e.to_string()),
                        incoming: false,
                file_path: None,
                    },
                }
            };
            push_progress(&progress_tx, final_progress).await;
        });

        Ok(TransferHandle {
            file_id,
            cancel_tx,
            progress_rx,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn do_send(
        self: Arc<Self>,
        target_ip: String,
        target_port: u16,
        file_path: PathBuf,
        file_id: String,
        file_name: String,
        file_size: u64,
        chunk_count: u64,
        chunk_size: usize,
        parallel: usize,
        cancel_rx: watch::Receiver<bool>,
        progress_tx: mpsc::Sender<TransferProgress>,
    ) -> Result<()> {
        let file_id_prefix = file_id_prefix_u64(&file_id);
        let bytes_done = Arc::new(AtomicU64::new(0));
        let chunks_done = Arc::new(AtomicU64::new(0));
        let start = Instant::now();

        let mut handles = Vec::new();
        for stream_idx in 0..parallel {
            let target_ip = target_ip.clone();
            let file_id_prefix = file_id_prefix;
            let progress_tx = progress_tx.clone();
            let bytes_done = bytes_done.clone();
            let chunks_done = chunks_done.clone();
            let file_path = file_path.clone();
            let cancel_rx = cancel_rx.clone();
            let file_id_for_err = file_id.clone();
            let file_name_for_err = file_name.clone();
            let file_size_for_err = file_size;

            let handle = tokio::spawn(async move {
                let mut chunk_id = stream_idx as u64;
                while chunk_id < chunk_count {
                    if *cancel_rx.borrow() {
                        return Ok::<(), crate::CoreError>(());
                    }
                    let offset = chunk_id * chunk_size as u64;
                    let read_len = std::cmp::min(
                        chunk_size as u64,
                        file_size_for_err.saturating_sub(offset),
                    ) as usize;
                    if read_len == 0 {
                        break;
                    }

                    // 每个 chunk 独立打开文件读取。
                    // 不能共享同一个 File 句柄：seek 与 read 是两个独立系统调用，
                    // 多流并发时 seek 会互相覆盖，导致读到错误偏移的数据（内容错乱 → sha256 不匹配）。
                    let path_for_read = file_path.clone();
                    let buf = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
                        use std::io::{Read, Seek, SeekFrom};
                        let mut f = std::fs::File::open(&path_for_read)?;
                        f.seek(SeekFrom::Start(offset))?;
                        let mut buf = vec![0u8; read_len];
                        f.read_exact(&mut buf)?;
                        Ok(buf)
                    })
                    .await
                    .map_err(|e| crate::CoreError::Transfer(e.to_string()))?
                    .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;

                    let stream = TcpStream::connect((target_ip.as_str(), target_port))
                        .await
                        .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
                    let header = DataFrameHeader {
                        file_id_prefix,
                        chunk_id,
                        data_len: buf.len() as u32,
                        _reserved: 0,
                    };
                    send_chunk(stream, &header, &buf).await?;

                    bytes_done.fetch_add(buf.len() as u64, Ordering::Relaxed);
                    chunks_done.fetch_add(1, Ordering::Relaxed);

                    let bd = bytes_done.load(Ordering::Relaxed);
                    let cd = chunks_done.load(Ordering::Relaxed);
                    let elapsed = start.elapsed().as_secs_f64().max(0.001);
                    push_progress(&progress_tx, TransferProgress {
                        file_id: file_id_for_err.clone(),
                        file_name: file_name_for_err.clone(),
                        file_size: file_size_for_err,
                        bytes_transferred: bd,
                        chunks_done: cd,
                        chunks_total: chunk_count,
                        speed_bps: (bd as f64 / elapsed) as u64,
                        status: TransferStatus::InProgress,
                        error: None,
                        incoming: false,
                file_path: None,
                    }).await;

                    chunk_id += parallel as u64;
                }
                Ok(())
            });
            handles.push(handle);
        }

        // 任何一条并行流失败都视为整体失败：
        // “写进 socket”不代表对端收到并落盘，错误不能吞掉，否则发送方会误报 Completed
        let mut first_err: Option<crate::CoreError> = None;
        for h in handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    first_err.get_or_insert(e);
                }
                Err(e) => {
                    first_err.get_or_insert(crate::CoreError::Transfer(format!(
                        "send worker crashed: {}",
                        e
                    )));
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// 发送单个 chunk：写 header + data → 等待接收方落盘 ACK。
///
/// 数据写入 TCP 缓冲 ≠ 对端已收到并写入磁盘。
/// 只有读到接收方的 ChunkAck（chunk_id 匹配且 ok=true）才算发送成功，
/// 否则（连接断开 / 超时 / ACK 异常）视为失败，由上层把整次传输标记为 Failed。
async fn send_chunk(mut stream: TcpStream, header: &DataFrameHeader, data: &[u8]) -> Result<()> {
    stream
        .write_all(&header.to_bytes())
        .await
        .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
    stream
        .write_all(data)
        .await
        .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;

    // 等待接收方 ACK（写盘完成才回 ACK；出错时对端不发 ACK、直接断开）
    // read_until 属于 AsyncBufReadExt，需要包一层 BufReader
    let mut reader = tokio::io::BufReader::new(stream);
    let mut ack_line = Vec::new();
    let n = tokio::time::timeout(
        Duration::from_secs(60),
        reader.read_until(b'\n', &mut ack_line),
    )
    .await
    .map_err(|_| crate::CoreError::Transfer("timeout waiting chunk ack".into()))?
    .map_err(|e| crate::CoreError::Transfer(format!("read chunk ack: {}", e)))?;
    if n == 0 {
        return Err(crate::CoreError::Transfer(
            "connection closed before chunk ack".into(),
        ));
    }
    let ack = crate::protocol::ControlMessage::from_line(&String::from_utf8_lossy(&ack_line))
        .map_err(|e| crate::CoreError::Transfer(format!("parse chunk ack: {}", e)))?;
    match ack {
        crate::protocol::ControlMessage::ChunkAck { chunk_id, ok, .. }
            if chunk_id == header.chunk_id && ok =>
        {
            Ok(())
        }
        crate::protocol::ControlMessage::ChunkAck { chunk_id, ok, .. } => Err(
            crate::CoreError::Transfer(format!("chunk {} rejected by receiver (ok={})", chunk_id, ok)),
        ),
        _ => Err(crate::CoreError::Transfer(
            "unexpected control message as chunk ack".into(),
        )),
    }
}

/// 通过 UDP connect 让 OS 路由决策选出「通往 target 的本机源 IP」。
/// UDP connect 不发包，只建立路由状态，零开销且跨平台。
fn local_source_ip(target_ip: &str) -> Option<String> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    // 端口任意（只求路由，不实际通信）
    sock.connect((target_ip, 7878)).ok()?;
    let addr = sock.local_addr().ok()?;
    let ip = addr.ip().to_string();
    if ip.is_empty() || ip == "0.0.0.0" {
        None
    } else {
        Some(ip)
    }
}

fn file_id_prefix_u64(file_id: &str) -> u64 {
    let bytes = file_id.as_bytes();
    let mut buf = [0u8; 8];
    let len = bytes.len().min(8);
    buf[..len].copy_from_slice(&bytes[..len]);
    u64::from_le_bytes(buf)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 纯 TCP 实现 HTTP POST JSON
///
/// 不引入 reqwest/hyper 直接依赖，几行代码手写 HTTP/1.1 请求
async fn http_post_json(host: &str, port: u16, path: &str, body: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect((host, port)).await?;
    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path, host, port, body.len(), body
    );
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;

    let response_str = String::from_utf8_lossy(&response);
    let body_start = response_str
        .find("\r\n\r\n")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "no header/body sep"))?;
    Ok(response_str[body_start + 4..].to_string())
}

#[cfg(test)]
mod tests {
    //! [`ProgressCache`] 的淘汰策略走单元测试：不碰网络、不碰 tokio，
    //! 而这些边界（无终态可淘汰时的退化、重复 file_id 不入队）用集成测试
    //! 很难构造出来。
    use super::*;

    fn prog(id: &str, status: TransferStatus) -> TransferProgress {
        TransferProgress {
            file_id: id.to_string(),
            file_name: format!("{id}.bin"),
            file_size: 100,
            bytes_transferred: 0,
            chunks_done: 0,
            chunks_total: 1,
            speed_bps: 0,
            status,
            error: None,
            incoming: false,
            file_path: None,
        }
    }

    #[test]
    fn cache_keeps_everything_below_cap() {
        let mut c = ProgressCache::new(3);
        c.insert(prog("a", TransferStatus::InProgress));
        c.insert(prog("b", TransferStatus::InProgress));
        assert_eq!(c.snapshot().len(), 2);
    }

    /// 超上限后总数压回 cap，且最新一条必然还在
    #[test]
    fn cache_trims_to_cap_and_keeps_newest() {
        let mut c = ProgressCache::new(3);
        for id in ["a", "b", "c", "d", "e"] {
            c.insert(prog(id, TransferStatus::InProgress));
        }
        assert_eq!(c.snapshot().len(), 3);
        assert!(c.map.contains_key("e"), "刚插入的不该被淘汰");
        assert!(!c.map.contains_key("a"), "最早的应被淘汰");
    }

    /// 优先淘汰终态：终态条目即使比进行中的条目新，也先被丢掉
    #[test]
    fn cache_prefers_evicting_terminal_entries() {
        let mut c = ProgressCache::new(2);
        c.insert(prog("old-running", TransferStatus::InProgress));
        c.insert(prog("done", TransferStatus::Completed));
        c.insert(prog("new-running", TransferStatus::InProgress));

        assert!(c.map.contains_key("old-running"), "进行中的老条目应保留");
        assert!(!c.map.contains_key("done"), "终态应优先被淘汰");
        assert!(c.map.contains_key("new-running"));
    }

    /// 全是进行中时必须仍能降回上限，不能卡死在超限状态
    #[test]
    fn cache_falls_back_to_oldest_when_nothing_is_terminal() {
        let mut c = ProgressCache::new(2);
        for id in ["a", "b", "c", "d"] {
            c.insert(prog(id, TransferStatus::Pending));
        }
        assert_eq!(c.snapshot().len(), 2, "无终态可淘汰时也必须降回上限");
        assert!(c.map.contains_key("d"));
        assert!(!c.map.contains_key("a"));
    }

    /// 同一 file_id 反复更新只入队一次，否则队列膨胀且淘汰顺序错乱
    #[test]
    fn repeated_file_id_is_not_requeued() {
        let mut c = ProgressCache::new(10);
        for _ in 0..5 {
            c.insert(prog("same", TransferStatus::InProgress));
        }
        assert_eq!(c.order.len(), 1, "同一 file_id 只应入队一次");
        assert_eq!(c.snapshot().len(), 1);
    }

    #[test]
    fn is_terminal_matches_only_final_states() {
        assert!(is_terminal(&TransferStatus::Completed));
        assert!(is_terminal(&TransferStatus::Failed));
        assert!(is_terminal(&TransferStatus::Canceled));
        assert!(!is_terminal(&TransferStatus::Pending));
        assert!(!is_terminal(&TransferStatus::InProgress));
    }
}
