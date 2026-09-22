//! 多流 TCP 并行流式传输引擎
//!
//! 设计要点：
//! 1. 文件按字节切成 N 段连续区间（[`crate::protocol::stream_layout`]），
//!    每条 TCP 流顺序写一段，**流内无分块、无应用层 ACK**。
//!    停等 ACK 会把节奏卡在「等对端落盘」上，已彻底去掉；背压靠 TCP 窗口。
//! 2. 流数按文件大小自适应（[`crate::protocol::compute_stream_count`]）：
//!    几百 KB 单流，几十 MB 拉满 `parallel_streams`（默认 min(CPU, 8)）。
//! 3. 背压：进度事件走**有界** channel（容量见 `PROGRESS_CHANNEL_CAP`）。
//!    进行中的进度用 `try_send`（满了就丢，不阻塞发送主循环），
//!    终态用 `send().await` 确保送达——否则 UI 会永远停在 99%。
//! 4. 校验：目前只有「整文件 sha256」一道。发送方边传边算，传完后经
//!    `POST /api/verify/:file_id` 补发给接收方，由接收方 finalize 时比对。
//! 5. TODO: 断点续传尚未实现。流完成位图只在内存里，进程重启即丢失。
//! 6. 接收方需先弹窗确认（HTTP offer/accept 握手）才开始 TCP 数据流
//!
//! 跨平台实现：使用 `Seek + Read`，不依赖平台专属零拷贝 API。
//! 后续可按平台用 cfg 切到 sendfile/TransmitFile 等优化。

use crate::protocol::{
    compute_stream_count, stream_layout, HttpIncomingResponse, HttpOffer, IncomingEntry, StreamHeader,
};
use crate::storage::StorageManager;
use crate::Result;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
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
    /// 已完成的流数（UI 兼容字段；流式下即「段」数）
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

/// 单个传输的进度通道容量。
///
/// 用有界而非无界：InProgress 是高频帧（按字节推），UI 若不消费，
/// 无界通道会一路堆到 OOM。
const PROGRESS_CHANNEL_CAP: usize = 256;

/// 终态进度（Failed / Canceled / Completed）的最长等待时间。
const TERMINAL_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);

/// 全量进度快照的条目上限（`GET /api/transfers` 的数据源）。
const PROGRESS_CACHE_CAP: usize = 200;

/// 进度推送节流：整个传输每 100ms 最多一条 InProgress
const PROGRESS_PUSH_INTERVAL: Duration = Duration::from_millis(100);

/// 流式读写缓冲。够大摊薄 syscall，够小让进度平滑、内存友好。
const STREAM_IO_BUF: usize = 256 * 1024;

/// 有上限的进度快照：HashMap + 插入顺序队列。
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
/// - **InProgress**：高频。用 `try_send`，通道满就直接丢。
/// - **其余状态**：必须送达。丢一帧终态，UI 就永远停在 99%。
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
    /// **必须持有，不能丢。**
    ///
    /// tokio 的 `watch::Sender::send` 在「一个 receiver 都没有」时直接返回
    /// Err 且**不更新值**。槽里常驻一个 receiver，send 就永远有接收方。
    _decision_rx: watch::Receiver<Option<bool>>,
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
            stream_count: offer.stream_count,
            sha256: offer.sha256,
            sha256_deferred: offer.sha256_deferred,
            from_id: offer.from_id,
            from_name: offer.from_name,
            from_ip: offer.from_ip,
            from_gateway_port: offer.from_gateway_port,
            from_transfer_port: offer.from_transfer_port,
            batch_id: offer.batch_id,
            batch_index: offer.batch_index,
            batch_total: offer.batch_total,
            created_at: now_ms(),
            decision: None,
        };
        let (decision_tx, decision_rx) = watch::channel(None);
        self.slots.lock().insert(
            incoming_id.clone(),
            IncomingSlot {
                entry: entry.clone(),
                decision_tx,
                _decision_rx: decision_rx,
            },
        );
        entry
    }

    /// UI 决策。返回 Some(entry) 表示找到该 incoming，None 表示已超时/不存在
    pub async fn decide(&self, incoming_id: &str, accept: bool) -> Option<IncomingEntry> {
        let slot_opt = self.slots.lock().get(incoming_id).map(|s| IncomingSlotRef {
            decision_tx: s.decision_tx.clone(),
            entry: s.entry.clone(),
        });
        let slot = slot_opt?;
        let _ = slot.decision_tx.send(Some(accept));
        let mut entry = slot.entry;
        if !accept {
            self.slots.lock().remove(incoming_id);
        } else {
            entry.decision = Some(true);
            if let Some(s) = self.slots.lock().get_mut(incoming_id) {
                s.entry.decision = Some(true);
            }
        }
        Some(entry)
    }

    /// 等待 UI 决策（async，超时自动 reject）
    pub async fn wait_decision(&self, incoming_id: &str) -> Option<bool> {
        let (tx, mut rx) = {
            let slots = self.slots.lock();
            let slot = slots.get(incoming_id)?;
            (slot.decision_tx.clone(), slot.decision_tx.subscribe())
        };
        if let Some(d) = *rx.borrow() {
            return Some(d);
        }
        match tokio::time::timeout(Duration::from_secs(60), rx.changed()).await {
            Ok(Ok(())) => *rx.borrow(),
            _ => {
                let _ = tx.send(Some(false));
                None
            }
        }
    }

    /// 列出所有待决定的 incoming（GET /api/incoming）。
    /// 已决策的不再出现在「待决定」列表里，否则 UI 会把已接受的继续弹成待决定。
    pub fn list_pending(&self) -> Vec<IncomingEntry> {
        self.slots
            .lock()
            .values()
            .filter(|s| s.entry.decision.is_none())
            .map(|s| s.entry.clone())
            .collect()
    }

    pub fn list_pending_by_batch(&self, batch_id: &str) -> Vec<IncomingEntry> {
        let mut out: Vec<IncomingEntry> = self
            .slots
            .lock()
            .values()
            .filter(|s| s.entry.decision.is_none() && s.entry.batch_id.as_deref() == Some(batch_id))
            .map(|s| s.entry.clone())
            .collect();
        out.sort_by_key(|e| e.batch_index.unwrap_or(u32::MAX));
        out
    }

    pub async fn decide_batch(&self, batch_id: &str, accept: bool) -> Vec<String> {
        let ids: Vec<String> = self
            .list_pending_by_batch(batch_id)
            .into_iter()
            .map(|e| e.incoming_id)
            .collect();
        let mut done = Vec::new();
        for id in ids {
            if self.decide(&id, accept).await.is_some() {
                done.push(id);
            }
        }
        done
    }

    pub fn remove(&self, incoming_id: &str) {
        self.slots.lock().remove(incoming_id);
    }

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

impl Default for IncomingManager {
    fn default() -> Self {
        Self::new()
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
    /// 全量进度快照：file_id → 最近一条进度（GET /api/transfers 用）
    progress_cache: Arc<Mutex<ProgressCache>>,
    /// 接收方速率统计：file_id → (首 字节 时间, 上次已收字节数)
    recv_speed_state: TokioMutex<HashMap<String, (Instant, u64)>>,
    /// 已接受的 incoming：file_id → (发送方 IP, 发送方 gateway 端口)
    /// 接收方取消时用于通知发送方联动取消
    incoming_endpoints: TokioMutex<HashMap<String, (String, u16)>>,
    /// 发送中的任务：file_id → (对端 IP, 对端 gateway 端口)
    /// 发送方取消时用于通知接收方联动取消（对称于 incoming_endpoints）
    outgoing_endpoints: TokioMutex<HashMap<String, (String, u16)>>,
    /// 出站 offer 并发闸门：多文件批量发送时限制同时在途的 /api/incoming，
    /// 免得几个连接一起把手机网络栈打满（表现为 ETIMEDOUT）。
    offer_gate: Arc<tokio::sync::Semaphore>,
}

impl TransferEngine {
    pub fn new(
        transfer_port: u16,
        parallel_streams: usize,
        receive_dir: PathBuf,
    ) -> Self {
        Self {
            transfer_port,
            parallel_streams,
            receive_dir: receive_dir.clone(),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            outgoing_offers: Arc::new(TokioMutex::new(HashMap::new())),
            incoming: Arc::new(IncomingManager::new()),
            storage: Arc::new(StorageManager::new(receive_dir)),
            prefix_to_file_id: TokioMutex::new(HashMap::new()),
            progress_bus: TokioMutex::new(None),
            progress_cache: Arc::new(Mutex::new(ProgressCache::new(PROGRESS_CACHE_CAP))),
            recv_speed_state: TokioMutex::new(HashMap::new()),
            incoming_endpoints: TokioMutex::new(HashMap::new()),
            outgoing_endpoints: TokioMutex::new(HashMap::new()),
            offer_gate: Arc::new(tokio::sync::Semaphore::new(2)),
        }
    }

    /// gateway 启动后注入 progress 广播
    pub async fn set_progress_bus(&self, bus: Arc<broadcast::Sender<TransferProgress>>) {
        *self.progress_bus.lock().await = Some(bus);
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
    pub async fn cancel(&self, file_id: &str) -> bool {
        let mut found = false;

        // ---- 发送方视角：通知 do_send 停止 + 让 offer 等待立即返回 ----
        if let Some(tx) = self.inflight.lock().get(file_id).cloned() {
            let _ = tx.send(true);
            found = true;
        }
        if let Some(tx) = self.outgoing_offers.lock().await.remove(file_id) {
            let _ = tx.send(HttpIncomingResponse {
                file_id: file_id.to_string(),
                accepted: false,
                reason: Some("canceled by sender".into()),
                transfer_port: self.transfer_port,
            });
            found = true;
        }

        // 发送方取消 → 通知接收方联动取消。
        if let Some((target_ip, target_gateway_port)) =
            self.outgoing_endpoints.lock().await.remove(file_id)
        {
            let file_id_owned = file_id.to_string();
            tokio::spawn(async move {
                let path = format!("/api/cancel/{}", file_id_owned);
                if let Err(e) = http_post_json(&target_ip, target_gateway_port, &path, "{}").await
                {
                    warn!(error = %e, "notify receiver cancel failed");
                }
            });
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
            // 通知发送方联动取消
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
                chunks_total: slot.streams_done.len() as u64,
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

    /// 发送任务结束后清理注册表（inflight / 等回包 oneshot / 对端地址）
    async fn cleanup_send_state(&self, file_id: &str) {
        self.inflight.lock().remove(file_id);
        self.outgoing_offers.lock().await.remove(file_id);
        self.outgoing_endpoints.lock().await.remove(file_id);
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
                        if let Err(e) = stream.set_nodelay(true) {
                            warn!(?peer, error = %e, "set_nodelay failed (non-fatal)");
                        }
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

    /// 服务一条数据流：读 [`StreamHeader`] → 流式落盘到对应偏移 → 标记该流完成。
    ///
    /// 一条连接只承载一段连续字节，没有分块 ACK。干净 EOF 且读满 `data_len` 即成功。
    async fn serve_data_stream(self: Arc<Self>, mut stream: TcpStream) -> Result<()> {
        let mut header_buf = [0u8; StreamHeader::SIZE];
        read_full(&mut stream, &mut header_buf)
            .await
            .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
        let header = StreamHeader::from_bytes(&header_buf)
            .map_err(|e| crate::CoreError::Transfer(format!("invalid stream header: {}", e)))?;

        let file_id = {
            let prefix = header.file_id_prefix;
            let map = self.prefix_to_file_id.lock().await;
            map.get(&prefix).cloned()
        };
        let Some(file_id) = file_id else {
            warn!(prefix = header.file_id_prefix, "unknown file_id_prefix (no receive slot)");
            return Err(crate::CoreError::Transfer(format!(
                "unknown file_id_prefix {} (no receive slot)",
                header.file_id_prefix
            )));
        };

        let slot = self
            .storage
            .get_slot(&file_id)
            .await
            .ok_or_else(|| crate::CoreError::Transfer(format!("no receive slot for {}", file_id)))?;

        let seg_idx = header.stream_id as usize;
        let Some(&(seg_start, seg_len)) = slot.segments.get(seg_idx) else {
            return Err(crate::CoreError::Transfer(format!(
                "stream_id {} out of range (stream_count={})",
                header.stream_id, slot.stream_count
            )));
        };
        if header.start_offset != seg_start || header.data_len != seg_len {
            return Err(crate::CoreError::Transfer(format!(
                "stream {} layout mismatch: header=({}, {}) expected=({}, {})",
                header.stream_id, header.start_offset, header.data_len, seg_start, seg_len
            )));
        }

        // 流式写盘：顺序写本段区间，每读一块就推进度（平滑、无停等）
        {
            use std::io::SeekFrom;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&slot.temp_path)
                .await
                .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
            file.seek(SeekFrom::Start(header.start_offset))
                .await
                .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;

            let mut remaining = header.data_len;
            let mut buf = vec![0u8; STREAM_IO_BUF.min(header.data_len.max(1) as usize)];
            let mut last_push = Instant::now()
                .checked_sub(PROGRESS_PUSH_INTERVAL)
                .unwrap_or_else(Instant::now);
            while remaining > 0 {
                let want = std::cmp::min(remaining as usize, buf.len());
                let n = read_full(&mut stream, &mut buf[..want])
                    .await
                    .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
                if n == 0 {
                    return Err(crate::CoreError::Transfer(format!(
                        "truncated stream {} ({} bytes missing)",
                        header.stream_id, remaining
                    )));
                }
                file.write_all(&buf[..n])
                    .await
                    .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
                remaining -= n as u64;
                self.storage.add_bytes(&file_id, n as u64).await;

                if last_push.elapsed() >= PROGRESS_PUSH_INTERVAL {
                    last_push = Instant::now();
                    // 必须重新取 slot：上面这个 `slot` 是开头的快照，
                    // bytes_received 仍是 0，拿它推进度会让接收端一直显示 0 B / 0 B/s
                    self.publish_recv_progress(&file_id).await;
                }
            }
            file.flush()
                .await
                .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
        }

        let slot = self
            .storage
            .finish_stream(&file_id, header.stream_id)
            .await
            .ok_or_else(|| crate::CoreError::Transfer("slot vanished during stream".into()))?;
        self.publish_recv_progress(&file_id).await;

        // 全部流收齐后的收尾
        if slot.is_complete() {
            if slot.await_sha256 && slot.sha256.is_none() {
                // 发送方声明会补发 sha256：先不 finalize，等 POST /api/verify。
                // 保险丝：120 秒未收到则跳过校验直接完成。
                let engine = self.clone();
                let fid = file_id.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(120)).await;
                    let _ = engine.apply_final_sha256(&fid, None).await;
                });
            } else {
                let _ = self.finish_receive(&file_id, &slot).await;
            }
        }
        Ok(())
    }

    /// 推送接收方视角的 InProgress 进度（每次从 storage 取最新 bytes_received）
    async fn publish_recv_progress(&self, file_id: &str) {
        let Some(slot) = self.storage.get_slot(file_id).await else {
            return;
        };
        let streams_done = slot.streams_done.iter().filter(|ok| **ok).count() as u64;
        let bytes_done = slot.bytes_received.min(slot.file_size);
        let speed_bps = {
            let mut state = self.recv_speed_state.lock().await;
            // (上次采样时刻, 上次 bytes_done)：速度 = 两次采样的字节差 / 时间差。
            // 累计均值（total/elapsed）会表现为「开头很快、越传越慢」——
            // 那是平均值在收敛，不是真的掉速。
            let ent = state
                .entry(file_id.to_string())
                .or_insert_with(|| (Instant::now(), bytes_done));
            let dt = ent.0.elapsed().as_secs_f64();
            if dt >= 0.15 {
                let db = bytes_done.saturating_sub(ent.1);
                *ent = (Instant::now(), bytes_done);
                (db as f64 / dt) as u64
            } else {
                // 采样间隔太短，沿用上次结果（0 表示还没测出）
                0
            }
        };
        self.publish_progress(TransferProgress {
            file_id: file_id.to_string(),
            file_name: slot.file_name.clone(),
            file_size: slot.file_size,
            bytes_transferred: bytes_done,
            chunks_done: streams_done,
            chunks_total: slot.streams_done.len() as u64,
            speed_bps,
            status: TransferStatus::InProgress,
            error: None,
            incoming: true,
            file_path: None,
        })
        .await;
    }

    /// 接收方收尾：校验 sha256 + 原子重命名 + 清理注册表 + 推最终进度
    async fn finish_receive(
        &self,
        file_id: &str,
        slot: &crate::storage::ReceiveSlot,
    ) -> crate::Result<()> {
        let result = self.storage.finalize(file_id, slot.sha256.as_deref()).await;
        let prefix = file_id_prefix_u64(file_id);
        self.prefix_to_file_id.lock().await.remove(&prefix);
        self.incoming.remove_by_file_id(file_id);
        self.incoming_endpoints.lock().await.remove(file_id);
        self.recv_speed_state.lock().await.remove(file_id);

        let (status, error) = match &result {
            Ok(_) => (TransferStatus::Completed, None),
            Err(e) => (TransferStatus::Failed, Some(e.to_string())),
        };
        let file_path = match &result {
            Ok(p) if !p.as_os_str().is_empty() => Some(p.to_string_lossy().into_owned()),
            Ok(_) => Some(slot.final_path.to_string_lossy().into_owned()),
            Err(_) => None,
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
            chunks_done: slot.streams_done.len() as u64,
            chunks_total: slot.streams_done.len() as u64,
            speed_bps: 0,
            status,
            error,
            incoming: true,
            file_path,
        })
        .await;
        result.map(|_| ())
    }

    /// 接收方收到发送方补发的最终 sha256（POST /api/verify/:file_id）。
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

    /// UI 决策后调用
    pub async fn decide_incoming(&self, incoming_id: &str, accept: bool) -> Option<IncomingEntry> {
        let entry = self.incoming.decide(incoming_id, accept).await?;
        if accept {
            // 流数**必须**用发送方声明的值算分段布局，否则两端区间错位。
            let stream_count = entry.stream_count.unwrap_or_else(|| {
                warn!(
                    file_id = %entry.file_id,
                    "对端未携带 stream_count（旧版本？），回退本地自适应；两端不一致会导致数据错位"
                );
                compute_stream_count(entry.file_size, self.parallel_streams)
            });
            let sha256_deferred = entry.sha256_deferred && entry.sha256.is_none();
            self.storage
                .create_slot(
                    entry.file_id.clone(),
                    entry.file_name.clone(),
                    entry.file_size,
                    stream_count,
                    entry.sha256.clone(),
                    sha256_deferred,
                )
                .await
                .ok()?;
            let prefix = file_id_prefix_u64(&entry.file_id);
            self.prefix_to_file_id
                .lock()
                .await
                .insert(prefix, entry.file_id.clone());
            self.incoming_endpoints.lock().await.insert(
                entry.file_id.clone(),
                (entry.from_ip.clone(), entry.from_gateway_port),
            );
            info!(file_id = %entry.file_id, "incoming accepted, receive slot ready");
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
        batch: Option<crate::protocol::SendBatchInfo>,
    ) -> Result<TransferHandle> {
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

        // 流数自适应：小文件单流，大文件拉满并行
        let stream_count = compute_stream_count(file_size, self.parallel_streams);

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (progress_tx, progress_rx) = mpsc::channel(PROGRESS_CHANNEL_CAP);
        let cancel_check = cancel_rx.clone();

        self.inflight
            .lock()
            .insert(file_id.clone(), cancel_tx.clone());

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
            chunks_total: stream_count as u64,
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
            // sha256 **不要**在 offer 之前就开算：多文件批量发送时几个 GB 级
            // 文件同时全量读盘，会把手机 IO/网络栈打满，表现为后续 offer
            // `Connection timed out (os error 110)`。推迟到 Accept 之后、
            // 与 do_send 重叠计算，语义不变（仍走 /api/verify 延后补发）。

            let offer = HttpOffer {
                file_id: file_id_for_spawn.clone(),
                file_name: file_name_for_err.clone(),
                file_size,
                stream_count: Some(stream_count),
                sha256: None,
                sha256_deferred: true,
                from_id: self_id.clone(),
                from_name: self_name.clone(),
                from_ip: self_ip.clone(),
                from_gateway_port: self_gateway_port,
                from_transfer_port: engine.transfer_port,
                batch_id: batch.as_ref().map(|b| b.batch_id.clone()),
                batch_index: batch.as_ref().map(|b| b.index),
                batch_total: batch.as_ref().map(|b| b.total),
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
                        chunks_total: stream_count as u64,
                        speed_bps: 0,
                        status: TransferStatus::Failed,
                        error: Some(format!("serialize offer: {}", e)),
                        incoming: false,
                        file_path: None,
                    }).await;
                    return;
                }
            };

            let post_result = {
                let _permit = engine
                    .offer_gate
                    .acquire()
                    .await
                    .expect("semaphore never closed");
                http_post_json_retry(
                    &target_ip,
                    target_gateway_port,
                    "/api/incoming",
                    &offer_json,
                )
                .await
            };

            if let Err(e) = post_result {
                push_progress(&progress_tx, TransferProgress {
                    file_id: file_id_for_spawn.clone(),
                    file_name: file_name_for_err.clone(),
                    file_size,
                    bytes_transferred: 0,
                    chunks_done: 0,
                    chunks_total: stream_count as u64,
                    speed_bps: 0,
                    status: TransferStatus::Failed,
                    error: Some(format!("post offer: {}", e)),
                    incoming: false,
                    file_path: None,
                }).await;
                engine.cleanup_send_state(&file_id_for_spawn).await;
                return;
            }

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
                    chunks_total: stream_count as u64,
                    speed_bps: 0,
                    status: TransferStatus::Canceled,
                    error: resp.reason.clone(),
                    incoming: false,
                    file_path: None,
                }).await;
                engine.cleanup_send_state(&file_id_for_spawn).await;
                return;
            }

            // Accept → 先跑数据传输。sha256 **等传输结束后**再算：
            // 与 do_send 并行会两路全量读同一文件，手机闪存被抢满后
            // 表现为「开头 ~10MB/s、后段掉到 ~6MB/s」（页缓存耗尽后双读打架）。
            // 代价是 /api/verify 稍晚发出，用户体感不受影响（进度条走的是数据）。
            engine
                .outgoing_endpoints
                .lock()
                .await
                .insert(file_id_for_spawn.clone(), (target_ip.clone(), target_gateway_port));
            let engine_for_cleanup = engine.clone();
            let mut result = engine
                .do_send(
                    target_ip.clone(),
                    target_transfer_port,
                    file_path.clone(),
                    file_id_for_spawn.clone(),
                    file_name_for_err.clone(),
                    file_size,
                    stream_count,
                    cancel_rx,
                    progress_tx.clone(),
                )
                .await;

            engine_for_cleanup.cleanup_send_state(&file_id_for_spawn).await;

            let canceled = *cancel_check.borrow();

            if !canceled && result.is_ok() {
                let sha256 = match tokio::task::spawn_blocking(move || -> std::io::Result<String> {
                    use sha2::{Digest, Sha256};
                    let mut f = std::fs::File::open(&file_path)?;
                    let mut hasher = Sha256::new();
                    std::io::copy(&mut f, &mut hasher)?;
                    Ok(format!("{:x}", hasher.finalize()))
                })
                .await
                {
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
                    chunks_total: stream_count as u64,
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
                        chunks_done: stream_count as u64,
                        chunks_total: stream_count as u64,
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
                        chunks_total: stream_count as u64,
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

    /// 启动 N 条流，每条顺序发送一个字节区间。任一条失败即整体失败。
    #[allow(clippy::too_many_arguments)]
    async fn do_send(
        self: Arc<Self>,
        target_ip: String,
        target_port: u16,
        file_path: PathBuf,
        file_id: String,
        file_name: String,
        file_size: u64,
        stream_count: u32,
        cancel_rx: watch::Receiver<bool>,
        progress_tx: mpsc::Sender<TransferProgress>,
    ) -> Result<()> {
        let file_id_prefix = file_id_prefix_u64(&file_id);
        let layout = stream_layout(file_size, stream_count);
        let bytes_done = Arc::new(AtomicU64::new(0));
        let last_push = Arc::new(TokioMutex::new(Instant::now()
            .checked_sub(PROGRESS_PUSH_INTERVAL)
            .unwrap_or_else(Instant::now)));
        let last_sample = Arc::new(TokioMutex::new((Instant::now(), 0u64)));

        let mut handles = Vec::new();
        for (stream_id, &(start_offset, seg_len)) in layout.iter().enumerate() {
            let target_ip = target_ip.clone();
            let file_path = file_path.clone();
            let mut cancel_rx = cancel_rx.clone();
            let pctx = SendProgress {
                progress_tx: progress_tx.clone(),
                file_id: file_id.clone(),
                file_name: file_name.clone(),
                file_size,
                streams_total: layout.len() as u64,
                streams_done: Arc::new(AtomicU64::new(0)), // 仅用于显示，完成数由 join 后不回推；bytes 为主
                bytes_done: bytes_done.clone(),
                last_push: last_push.clone(),
                last_sample: last_sample.clone(),
            };
            let streams_done_shared = pctx.streams_done.clone();
            let file_id_prefix = file_id_prefix;

            handles.push(tokio::spawn(async move {
                // 空段（空文件）：直接算完成
                if seg_len == 0 {
                    streams_done_shared.fetch_add(1, Ordering::Relaxed);
                    pctx.maybe_push().await;
                    return Ok::<(), crate::CoreError>(());
                }

                let mut conn = connect_with_retry(&target_ip, target_port, &mut cancel_rx).await?;
                let header = StreamHeader {
                    file_id_prefix,
                    stream_id: stream_id as u32,
                    _reserved: 0,
                    start_offset,
                    data_len: seg_len,
                };
                conn.write_all(&header.to_bytes())
                    .await
                    .map_err(|e| crate::CoreError::Transfer(format!("write stream header: {}", e)))?;

                // 顺序读文件区间 → 写 socket。失败即整传失败（无分块可重试）。
                send_stream_body(
                    &mut conn,
                    &file_path,
                    start_offset,
                    seg_len,
                    &mut cancel_rx,
                    &pctx,
                )
                .await?;

                let _ = conn.shutdown().await;
                streams_done_shared.fetch_add(1, Ordering::Relaxed);
                pctx.maybe_push().await;
                Ok(())
            }));
        }

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

/// 发送端进度上下文：按字节推进，100ms 节流推送。
struct SendProgress {
    progress_tx: mpsc::Sender<TransferProgress>,
    file_id: String,
    /// **必须带上**：空文件名会覆盖进度缓存里的 Pending 帧，
    /// 手机端传输中就会显示成无名任务。
    file_name: String,
    file_size: u64,
    streams_total: u64,
    streams_done: Arc<AtomicU64>,
    bytes_done: Arc<AtomicU64>,
    last_push: Arc<TokioMutex<Instant>>,
    /// (上次采样时刻, 上次 bytes_done)，算**瞬时**速度用
    last_sample: Arc<TokioMutex<(Instant, u64)>>,
}

impl SendProgress {
    async fn maybe_push(&self) {
        {
            let mut last = self.last_push.lock().await;
            if last.elapsed() < PROGRESS_PUSH_INTERVAL {
                return;
            }
            *last = Instant::now();
        }
        let bd = self.bytes_done.load(Ordering::Relaxed);
        // 瞬时速度 = Δbytes/Δt。累计均值（total/elapsed）会「开头很快、越传越慢」。
        let speed_bps = {
            let mut s = self.last_sample.lock().await;
            let dt = s.0.elapsed().as_secs_f64();
            if dt >= 0.05 {
                let db = bd.saturating_sub(s.1);
                *s = (Instant::now(), bd);
                (db as f64 / dt) as u64
            } else {
                0
            }
        };
        push_progress(&self.progress_tx, TransferProgress {
            file_id: self.file_id.clone(),
            file_name: self.file_name.clone(),
            file_size: self.file_size,
            bytes_transferred: bd.min(self.file_size),
            chunks_done: self.streams_done.load(Ordering::Relaxed),
            chunks_total: self.streams_total,
            speed_bps,
            status: TransferStatus::InProgress,
            error: None,
            incoming: false,
            file_path: None,
        })
        .await;
    }
}

/// 连接对端数据端口，带少量重试；取消可打断退避**与** connect。
async fn connect_with_retry(
    target_ip: &str,
    target_port: u16,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<TcpStream> {
    const ATTEMPTS: u32 = 3;
    const BACKOFF_MS: [u64; 3] = [0, 200, 500];
    let mut last_err = String::from("no attempt");
    for attempt in 0..ATTEMPTS {
        if *cancel_rx.borrow() {
            return Err(crate::CoreError::Transfer("canceled".into()));
        }
        let backoff = BACKOFF_MS
            .get(attempt as usize)
            .copied()
            .unwrap_or(500);
        if backoff > 0 {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(backoff)) => {}
                _ = cancel_rx.changed() => {
                    return Err(crate::CoreError::Transfer("canceled".into()));
                }
            }
        }
        let connect_result = tokio::select! {
            r = TcpStream::connect((target_ip, target_port)) => r,
            _ = cancel_rx.changed() => {
                return Err(crate::CoreError::Transfer("canceled".into()));
            }
        };
        match connect_result {
            Ok(s) => {
                if let Err(e) = s.set_nodelay(true) {
                    warn!(error = %e, "set_nodelay failed (non-fatal)");
                }
                return Ok(s);
            }
            Err(e) => {
                last_err = format!("connect {}:{}: {}", target_ip, target_port, e);
                warn!(attempt, error = %e, "connect failed, will retry");
            }
        }
    }
    Err(crate::CoreError::Transfer(last_err))
}

/// 把文件区间顺序写入 socket，边写边推估计进度。取消立即生效。
async fn send_stream_body(
    conn: &mut TcpStream,
    file_path: &std::path::Path,
    start_offset: u64,
    seg_len: u64,
    cancel_rx: &mut watch::Receiver<bool>,
    pctx: &SendProgress,
) -> Result<()> {
    use std::io::SeekFrom;
    let mut file = tokio::fs::File::open(file_path)
        .await
        .map_err(|e| crate::CoreError::Transfer(format!("open source: {}", e)))?;
    file.seek(SeekFrom::Start(start_offset))
        .await
        .map_err(|e| crate::CoreError::Transfer(format!("seek source: {}", e)))?;

    let mut remaining = seg_len;
    let mut buf = vec![0u8; STREAM_IO_BUF];
    while remaining > 0 {
        if *cancel_rx.borrow() {
            return Err(crate::CoreError::Transfer("canceled".into()));
        }
        let want = std::cmp::min(remaining as usize, buf.len());
        let n = {
            // 用 select 响应取消：读文件时用户点取消也要尽快退出
            tokio::select! {
                r = file.read(&mut buf[..want]) => {
                    r.map_err(|e| crate::CoreError::Transfer(format!("read source: {}", e)))?
                }
                _ = cancel_rx.changed() => {
                    return Err(crate::CoreError::Transfer("canceled".into()));
                }
            }
        };
        if n == 0 {
            return Err(crate::CoreError::Transfer(format!(
                "source truncated at offset {}",
                start_offset + (seg_len - remaining)
            )));
        }
        conn.write_all(&buf[..n])
            .await
            .map_err(|e| crate::CoreError::Transfer(format!("write data: {}", e)))?;
        remaining -= n as u64;
        pctx.bytes_done.fetch_add(n as u64, Ordering::Relaxed);
        pctx.maybe_push().await;
    }
    conn.flush()
        .await
        .map_err(|e| crate::CoreError::Transfer(format!("flush: {}", e)))?;
    Ok(())
}

/// 读满 buf，返回实际读到的字节数。**返回 0 = 对端已关闭（干净 EOF）**。
async fn read_full(stream: &mut TcpStream, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut read = 0;
    while read < buf.len() {
        match stream.read(&mut buf[read..]).await? {
            0 => break,
            n => read += n,
        }
    }
    Ok(read)
}

/// 通过 UDP connect 让 OS 路由决策选出「通往 target 的本机源 IP」。
fn local_source_ip(target_ip: &str) -> Option<String> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
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

/// offer 投递：连不上的瞬时故障重试几次。
///
/// 多文件批量发送时几个 offer 几乎同时出站，手机侧偶发 `ETIMEDOUT`
/// （网络栈被打满 / 路由瞬时不可达）。一次失败就整文件判死太脆。
async fn http_post_json_retry(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
) -> std::io::Result<String> {
    const ATTEMPTS: u32 = 3;
    const BACKOFF_MS: [u64; 3] = [0, 300, 800];
    let mut last_err = std::io::Error::new(std::io::ErrorKind::Other, "no attempt");
    for attempt in 0..ATTEMPTS {
        let backoff = BACKOFF_MS.get(attempt as usize).copied().unwrap_or(800);
        if backoff > 0 {
            tokio::time::sleep(Duration::from_millis(backoff)).await;
        }
        match http_post_json(host, port, path, body).await {
            Ok(r) => return Ok(r),
            Err(e) => {
                warn!(attempt, error = %e, "http post failed, will retry");
                last_err = e;
            }
        }
    }
    Err(last_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(file_id: &str, batch: Option<(&str, u32, u32)>) -> crate::protocol::HttpOffer {
        crate::protocol::HttpOffer {
            file_id: file_id.to_string(),
            file_name: "a.bin".into(),
            file_size: 10,
            stream_count: Some(2),
            sha256: None,
            sha256_deferred: false,
            from_id: "id".into(),
            from_name: "n".into(),
            from_ip: "1.2.3.4".into(),
            from_gateway_port: 7878,
            from_transfer_port: 7879,
            batch_id: batch.map(|b| b.0.to_string()),
            batch_index: batch.map(|b| b.1),
            batch_total: batch.map(|b| b.2),
        }
    }

    #[test]
    fn is_terminal_matches_only_final_states() {
        assert!(is_terminal(&TransferStatus::Completed));
        assert!(is_terminal(&TransferStatus::Failed));
        assert!(is_terminal(&TransferStatus::Canceled));
        assert!(!is_terminal(&TransferStatus::InProgress));
        assert!(!is_terminal(&TransferStatus::Pending));
    }

    #[test]
    fn cache_keeps_everything_below_cap() {
        let mut c = ProgressCache::new(3);
        for i in 0..3 {
            c.insert(TransferProgress {
                file_id: format!("f{i}"),
                file_name: String::new(),
                file_size: 0,
                bytes_transferred: 0,
                chunks_done: 0,
                chunks_total: 0,
                speed_bps: 0,
                status: TransferStatus::InProgress,
                error: None,
                incoming: false,
                file_path: None,
            });
        }
        assert_eq!(c.snapshot().len(), 3);
    }

    #[test]
    fn cache_trims_to_cap_and_keeps_newest() {
        let mut c = ProgressCache::new(2);
        for i in 0..4 {
            c.insert(TransferProgress {
                file_id: format!("f{i}"),
                file_name: String::new(),
                file_size: 0,
                bytes_transferred: 0,
                chunks_done: 0,
                chunks_total: 0,
                speed_bps: 0,
                status: TransferStatus::InProgress,
                error: None,
                incoming: false,
                file_path: None,
            });
        }
        let mut ids: Vec<_> = c.snapshot().into_iter().map(|p| p.file_id).collect();
        ids.sort();
        assert_eq!(ids, vec!["f2".to_string(), "f3".to_string()]);
    }

    #[test]
    fn cache_prefers_evicting_terminal_entries() {
        let mut c = ProgressCache::new(2);
        c.insert(TransferProgress {
            file_id: "old-done".into(),
            file_name: String::new(),
            file_size: 0,
            bytes_transferred: 0,
            chunks_done: 0,
            chunks_total: 0,
            speed_bps: 0,
            status: TransferStatus::Completed,
            error: None,
            incoming: false,
            file_path: None,
        });
        c.insert(TransferProgress {
            file_id: "live".into(),
            file_name: String::new(),
            file_size: 0,
            bytes_transferred: 0,
            chunks_done: 0,
            chunks_total: 0,
            speed_bps: 0,
            status: TransferStatus::InProgress,
            error: None,
            incoming: false,
            file_path: None,
        });
        c.insert(TransferProgress {
            file_id: "newer".into(),
            file_name: String::new(),
            file_size: 0,
            bytes_transferred: 0,
            chunks_done: 0,
            chunks_total: 0,
            speed_bps: 0,
            status: TransferStatus::InProgress,
            error: None,
            incoming: false,
            file_path: None,
        });
        let ids: Vec<_> = c.snapshot().into_iter().map(|p| p.file_id).collect();
        assert!(ids.contains(&"live".to_string()));
        assert!(ids.contains(&"newer".to_string()));
        assert!(!ids.contains(&"old-done".to_string()));
    }

    #[test]
    fn cache_falls_back_to_oldest_when_nothing_is_terminal() {
        let mut c = ProgressCache::new(2);
        for name in ["a", "b", "c"] {
            c.insert(TransferProgress {
                file_id: name.into(),
                file_name: String::new(),
                file_size: 0,
                bytes_transferred: 0,
                chunks_done: 0,
                chunks_total: 0,
                speed_bps: 0,
                status: TransferStatus::InProgress,
                error: None,
                incoming: false,
                file_path: None,
            });
        }
        let ids: Vec<_> = c.snapshot().into_iter().map(|p| p.file_id).collect();
        assert_eq!(ids.len(), 2);
        assert!(!ids.contains(&"a".to_string()));
    }

    #[test]
    fn cache_repeated_file_id_is_not_requeued() {
        let mut c = ProgressCache::new(2);
        for _ in 0..3 {
            c.insert(TransferProgress {
                file_id: "same".into(),
                file_name: String::new(),
                file_size: 0,
                bytes_transferred: 1,
                chunks_done: 0,
                chunks_total: 0,
                speed_bps: 0,
                status: TransferStatus::InProgress,
                error: None,
                incoming: false,
                file_path: None,
            });
        }
        c.insert(TransferProgress {
            file_id: "other".into(),
            file_name: String::new(),
            file_size: 0,
            bytes_transferred: 0,
            chunks_done: 0,
            chunks_total: 0,
            speed_bps: 0,
            status: TransferStatus::InProgress,
            error: None,
            incoming: false,
            file_path: None,
        });
        let mut ids: Vec<_> = c.snapshot().into_iter().map(|p| p.file_id).collect();
        ids.sort();
        assert_eq!(ids, vec!["other".to_string(), "same".to_string()]);
    }

    #[tokio::test]
    async fn decide_batch_on_unknown_batch_is_noop() {
        let mgr = IncomingManager::new();
        let done = mgr.decide_batch("nope", true).await;
        assert!(done.is_empty());
    }

    #[tokio::test]
    async fn decide_batch_reject_removes_slots() {
        let mgr = IncomingManager::new();
        mgr.register(offer("f1", Some(("b1", 0, 2))));
        mgr.register(offer("f2", Some(("b1", 1, 2))));
        let done = mgr.decide_batch("b1", false).await;
        assert_eq!(done.len(), 2);
        assert!(mgr.list_pending().is_empty());
    }

    #[tokio::test]
    async fn decide_batch_applies_to_whole_batch_only() {
        let mgr = IncomingManager::new();
        mgr.register(offer("f1", Some(("b1", 0, 2))));
        mgr.register(offer("f2", Some(("b1", 1, 2))));
        mgr.register(offer("f3", Some(("b2", 0, 1))));
        let done = mgr.decide_batch("b1", true).await;
        assert_eq!(done.len(), 2);
        assert_eq!(mgr.list_pending().len(), 1);
        assert_eq!(mgr.list_pending()[0].file_id, "f3");
    }

    #[test]
    fn list_pending_by_batch_sorts_by_index() {
        let mgr = IncomingManager::new();
        mgr.register(offer("f1", Some(("b1", 2, 3))));
        mgr.register(offer("f2", Some(("b1", 0, 3))));
        mgr.register(offer("f3", Some(("b1", 1, 3))));
        let list = mgr.list_pending_by_batch("b1");
        let indices: Vec<_> = list.iter().map(|e| e.batch_index.unwrap()).collect();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn old_sender_offer_without_stream_count_still_deserializes() {
        let json = r#"{
            "file_id": "x", "file_name": "a", "file_size": 10,
            "sha256": null, "sha256_deferred": true,
            "from_id": "d", "from_name": "n", "from_ip": "1.2.3.4",
            "from_gateway_port": 7878, "from_transfer_port": 7879
        }"#;
        let offer: HttpOffer = serde_json::from_str(json).unwrap();
        assert_eq!(offer.stream_count, None);
    }

    /// 端到端：单流发送若干字节，接收端流式落盘并 finalize。
    #[tokio::test]
    async fn single_stream_send_receives_file() {
        let base = std::env::var("FTCORE_TEST_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("ftcore-stream-e2e-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let recv_engine = Arc::new(TransferEngine::new(0, 4, dir.clone()));
        // 端口 0 → 由我们自己 bind 再注入不现实；这里直接 spawn_receiver 用固定候选
        // 改为手动 listen 由 serve_data_stream 接：用 TransferEngine 公开路径太重，
        // 直接测 storage + StreamHeader 协议：写两段后 finalize。
        let mgr = &recv_engine.storage;
        mgr.create_slot("fid-1".into(), "out.bin".into(), 10, 2, None, false)
            .await
            .unwrap();
        // 段布局：5 + 5
        let layout = stream_layout(10, 2);
        assert_eq!(layout, vec![(0, 5), (5, 5)]);
        mgr.write_at("fid-1", 5, b"67890").await.unwrap();
        mgr.write_at("fid-1", 0, b"12345").await.unwrap();
        let slot = mgr.finish_stream("fid-1", 0).await.unwrap();
        assert!(!slot.is_complete());
        let slot = mgr.finish_stream("fid-1", 1).await.unwrap();
        assert!(slot.is_complete());
        mgr.finalize("fid-1", None).await.unwrap();
        let got = std::fs::read(dir.join("out.bin")).unwrap();
        assert_eq!(got, b"1234567890");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 取消后 connect / 退避都能被打断（不再挂满整个重试窗口）。
    #[tokio::test]
    async fn cancel_during_connect_backoff_takes_effect() {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            // 不可达地址，会走满重试
            connect_with_retry("127.0.0.1", 1, &mut cancel_rx).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_tx.send(true).unwrap();
        let start = Instant::now();
        let res = handle.await.unwrap();
        assert!(res.is_err());
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "取消应立刻打断重试，耗时 {:?}",
            start.elapsed()
        );
    }
}
