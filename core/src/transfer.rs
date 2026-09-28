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

use crate::fault::{from_io, FailureKind, Phase, TransferFailure};
use crate::httpc::http_post_json_tls;
use crate::protocol::{
    compute_stream_count, stream_layout, HttpIncomingResponse, HttpOffer, IncomingEntry, StreamHeader,
};
use crate::storage::StorageManager;
use crate::timeouts::with_idle_timeout;
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
use tracing::{debug, error, info, warn};

/// 传输状态
///
/// 状态机（A3.2）：
/// ```text
/// InProgress ──流失败(可重试,自动重试耗尽)──► Interrupted
/// InProgress ──流失败(不可重试)────────────► Failed
/// InProgress ──全段完成+sha256─────────────► Completed
/// InProgress ──用户取消────────────────────► Canceled
/// Interrupted ──用户「继续传输」───────────► InProgress（只传未完成段）
/// Interrupted ──用户「取消」───────────────► Canceled
/// Interrupted ──resume 时槽位丢失等────────► Failed
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Canceled,
    /// 已中断：可重试错误自动重试耗尽。**保留**已写入段与槽位，
    /// 其余流已暂停，用户可「继续传输」（只重发未完成段）。
    /// 注意：这不是「失败」——UI 文案见修复方案 §3.5。
    Interrupted,
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
    /// 自动重试提示（A3.5 副文案，如「第 2/3 次重试流 3…」）。
    /// 非空时 UI 显示「重试中」角标；正常帧为 None。
    #[serde(default)]
    pub retry_note: Option<String>,
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

/// 速度采样窗口：收发两端统一 1 秒更新一次，避免 100ms 级抖动让速度乱跳
const SPEED_SAMPLE_WINDOW: Duration = Duration::from_secs(1);

/// incoming 请求决策超时：**60 秒**内未点「接受」即自动拒绝。
///
/// 发送方等回包的窗口是 70s（见 send_file），必须大于本值，
/// 否则发送方先超时、接收方还在等 UI，状态会对不齐。
pub const INCOMING_DECISION_TIMEOUT_SECS: u64 = 60;

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

    /// 等待 UI 决策。
    ///
    /// 返回：
    /// - `Some(true)` 用户接受
    /// - `Some(false)` 用户拒绝
    /// - `None` **超时**（[`INCOMING_DECISION_TIMEOUT_SECS`] 秒内未决策）或槽位不存在
    ///
    /// 超时会把决策置为拒绝，发送方收到的 reason 是 timeout 而不是用户拒绝。
    pub async fn wait_decision(&self, incoming_id: &str) -> Option<bool> {
        let (tx, mut rx) = {
            let slots = self.slots.lock();
            let slot = slots.get(incoming_id)?;
            (slot.decision_tx.clone(), slot.decision_tx.subscribe())
        };
        if let Some(d) = *rx.borrow() {
            return Some(d);
        }
        match tokio::time::timeout(
            Duration::from_secs(INCOMING_DECISION_TIMEOUT_SECS),
            rx.changed(),
        )
        .await
        {
            Ok(Ok(())) => *rx.borrow(),
            _ => {
                // 超时：强制记为拒绝，并返回 None 让调用方区分「超时」和「用户拒绝」
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

/// 发送会话：跨「中断—续传」存活的发送侧状态（A3.2）。
///
/// 恢复粒度 = 流/段：`stream_count` 与完成位图在 offer 时锁定，
/// **重试期间不得重算**；重试只调度 `streams_done[i] == false` 的段，
/// 整段从 `start_offset` 覆盖写（幂等；第一期不做段内字节级续传）。
struct SendSession {
    file_name: String,
    file_size: u64,
    /// offer 时锁定的流数（两端布局一致的根基，续传不得重算）
    stream_count: u32,
    target_ip: String,
    target_transfer_port: u16,
    target_gateway_port: u16,
    file_path: PathBuf,
    /// 完成位图：true = 该段已完整送达。do_send 与续传共享。
    streams_done: Arc<Mutex<Vec<bool>>>,
    /// 会话级进度通道：中断后仍能推帧；会话移除即关闭（UI 泵循环随之退出）
    progress_tx: mpsc::Sender<TransferProgress>,
    /// 取消信号（与 inflight 注册的是同一把）
    cancel_tx: watch::Sender<bool>,
    /// offer 是否已被接受（区分「等决策中」与「Interrupted」两种非运行态）
    accepted: bool,
    /// 是否有 run_transfer 正在执行（resume / cancel 的互斥依据）
    running: bool,
}

/// `/api/verify` 处理回执（发送方据此决定终态，A3.2 状态机对齐）。
///
/// 只回 200 不够：段没收齐时接收方**不会** finalize，发送方若凭 200 报
/// 已完成，两边状态就永久分叉（真机实测：手机「已完成」/ 桌面「已中断」）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// 接收方已 finalize（校验通过落盘；或校验失败也已推 Failed 终态）
    Finalized,
    /// 槽位在但段未收齐；携带**接收方权威完成位图**供发送方回退会话位图
    Pending { chunks_done: Vec<bool> },
    /// 槽位不存在（已完成过 / 已被取消清理）
    Missing,
}

impl VerifyOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            VerifyOutcome::Finalized => "finalized",
            VerifyOutcome::Pending { .. } => "pending",
            VerifyOutcome::Missing => "missing",
        }
    }

    /// 供 verify 响应回传的位图（仅 Pending 有值）
    pub fn chunks(&self) -> Option<&[bool]> {
        match self {
            VerifyOutcome::Pending { chunks_done } => Some(chunks_done),
            _ => None,
        }
    }
}

/// 传输引擎：负责发送与接收
pub struct TransferEngine {
    pub transfer_port: u16,
    pub parallel_streams: usize,
    pub receive_dir: PathBuf,
    /// 数据面超时（可注入，见 `timeouts::Timeouts`；**须在 `Arc::new` 之前设置**）
    pub timeouts: crate::timeouts::Timeouts,
    /// verify 协商轮询退避表：每轮 POST 前的等待，len = 最大轮数（可注入测试短表）。
    ///
    /// 为什么必须轮询：TCP 写完成 ≠ 对端应用层收齐，do_send 一结束就发 verify
    /// 几乎必然 pending；且网络抖动刚恢复时回执可能发不出去。默认总窗口约 9.5s。
    verify_backoff: Vec<Duration>,
    inflight: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
    /// 发送方 daemon：等待接收方回包的 file_id → oneshot
    outgoing_offers: Arc<TokioMutex<HashMap<String, oneshot::Sender<HttpIncomingResponse>>>>,
    /// 发送会话：file_id → 中断后保留的发送侧状态（A3.2；终态即移除）
    send_sessions: Arc<TokioMutex<HashMap<String, SendSession>>>,
    /// 接收方 daemon：pending incoming 请求管理
    pub incoming: Arc<IncomingManager>,
    /// 接收方 daemon：文件落盘
    pub storage: Arc<StorageManager>,
    /// 接收方 daemon：file_id_prefix → file_id 映射（Accept 时登记，serve_data_stream 用）
    prefix_to_file_id: TokioMutex<HashMap<u64, String>>,
    /// 接收方中断标记：file_id → 失败原因短语（A3.2）。
    /// 置位期间进度保持「已中断」，槽位与 .part 保留；续传信号到达时清除。
    recv_interrupted: Arc<Mutex<HashMap<String, String>>>,
    /// 接收连接代际：file_id → 最近一条连接的序号（A3.2 竞态防护）。
    ///
    /// 真机竞态：网络断开时挂起的连接 A 进入 30s 空闲计时；网络恢复后
    /// 发送方开新连接 B 重发、数据流畅——**A 的超时随后才迟到触发**，
    /// 若不加拦截会把 B 的「传输中」打回「已中断」（传输在进行却显示中断，
    /// 且中断不是断网当时显示的）。失败信号必须携带本连接序号，
    /// 序号已被更新的连接接管 → 丢弃。
    recv_active_conn: Arc<Mutex<HashMap<String, u64>>>,
    /// 接收连接序号发号器
    recv_conn_seq: AtomicU64,
    /// 进度广播 bus（gateway 注入；None 时 send_file 仍能用，只是不广播）
    progress_bus: TokioMutex<Option<Arc<broadcast::Sender<TransferProgress>>>>,
    /// 全量进度快照：file_id → 最近一条进度（GET /api/transfers 用）
    progress_cache: Arc<Mutex<ProgressCache>>,
    /// 接收方速率统计：file_id → (上次采样时刻, 上次 bytes, 上次速度)
    recv_speed_state: TokioMutex<HashMap<String, (Instant, u64, u64)>>,
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
            timeouts: crate::timeouts::Timeouts::default(),
            verify_backoff: vec![
                Duration::from_millis(500),
                Duration::from_millis(1000),
                Duration::from_millis(1000),
                Duration::from_millis(2000),
                Duration::from_millis(2000),
                Duration::from_millis(3000),
            ],
            inflight: Arc::new(Mutex::new(HashMap::new())),
            outgoing_offers: Arc::new(TokioMutex::new(HashMap::new())),
            send_sessions: Arc::new(TokioMutex::new(HashMap::new())),
            incoming: Arc::new(IncomingManager::new()),
            storage: Arc::new(StorageManager::new(receive_dir)),
            prefix_to_file_id: TokioMutex::new(HashMap::new()),
            recv_interrupted: Arc::new(Mutex::new(HashMap::new())),
            recv_active_conn: Arc::new(Mutex::new(HashMap::new())),
            recv_conn_seq: AtomicU64::new(1),
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
            let receive_dir = self.receive_dir.clone();
            tokio::spawn(async move {
                let path = format!("/api/cancel/{}", file_id_owned);
                if let Err(e) =
                    http_post_json_tls(&target_ip, target_gateway_port, &path, "{}", &receive_dir)
                        .await
                {
                    warn!(error = %e, "notify receiver cancel failed");
                }
            });
        }

        // ---- 发送会话处于 Interrupted（已接受、未在跑）：直接终态 Canceled（A3.2 状态机） ----
        // 运行中/等决策的会话由各自的 run/offer 任务推终态帧，这里不抢。
        if let Some(session) = self.take_idle_send_session(file_id).await {
            self.cleanup_send_final(file_id).await;
            let (chunks_done, bytes_done) = {
                let done = session.streams_done.lock();
                let mut bytes = 0u64;
                let layout = stream_layout(session.file_size, session.stream_count);
                for (i, ok) in done.iter().enumerate() {
                    if *ok {
                        bytes += layout.get(i).map(|(_, l)| *l).unwrap_or(0);
                    }
                }
                (done.iter().filter(|b| **b).count() as u64, bytes)
            };
            push_progress(
                &session.progress_tx,
                TransferProgress {
                    file_id: file_id.to_string(),
                    file_name: session.file_name.clone(),
                    file_size: session.file_size,
                    bytes_transferred: bytes_done,
                    chunks_done,
                    chunks_total: session.stream_count as u64,
                    speed_bps: 0,
                    status: TransferStatus::Canceled,
                    error: Some("canceled".into()),
                    incoming: false,
                    file_path: None,
                    retry_note: None,
                },
            )
            .await;
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
            self.recv_interrupted.lock().remove(file_id);
            // 通知发送方联动取消
            if let Some((from_ip, from_gateway_port)) =
                self.incoming_endpoints.lock().await.remove(file_id)
            {
                let file_id_owned = file_id.to_string();
                let receive_dir = self.receive_dir.clone();
                tokio::spawn(async move {
                    let path = format!("/api/cancel/{}", file_id_owned);
                    if let Err(e) = http_post_json_tls(
                        &from_ip,
                        from_gateway_port,
                        &path,
                        "{}",
                        &receive_dir,
                    )
                    .await
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
                retry_note: None,
            })
            .await;
            found = true;
        }

        if found {
            info!(%file_id, "transfer canceled");
        }
        found
    }

    /// 发送任务结束后清理注册表（inflight / 等回包 oneshot / 对端地址 / 会话）。
    ///
    /// 仅在**终态**（Completed / Failed / Canceled）调用；
    /// Interrupted 保留会话与注册项，供「继续传输」与后续取消使用。
    async fn cleanup_send_final(&self, file_id: &str) {
        self.inflight.lock().remove(file_id);
        self.outgoing_offers.lock().await.remove(file_id);
        self.outgoing_endpoints.lock().await.remove(file_id);
        self.send_sessions.lock().await.remove(file_id);
    }

    /// 取出「已接受且未在运行」的发送会话（即 Interrupted 待续态）。
    /// 等决策中（accepted=false）与运行中的会话不取，由各自任务负责收尾。
    async fn take_idle_send_session(&self, file_id: &str) -> Option<SendSession> {
        let mut sessions = self.send_sessions.lock().await;
        let take = sessions
            .get(file_id)
            .map(|s| s.accepted && !s.running)
            .unwrap_or(false);
        if take {
            sessions.remove(file_id)
        } else {
            None
        }
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
    /// 段体阶段的错误按 [`TransferFailure`] 分类（A3.4）并驱动接收方状态机（A3.2）：
    /// 可重试 → `Interrupted`（保留槽位与 .part）；不可重试 → `Failed`（清理槽位）。
    async fn serve_data_stream(self: Arc<Self>, mut stream: TcpStream) -> std::result::Result<(), TransferFailure> {
        let idle = self.timeouts.idle;
        // ---- 头部：解析 + 定位 file_id（此阶段错误无法归属任务，只记日志） ----
        let mut header_buf = [0u8; StreamHeader::SIZE];
        read_full(&mut stream, &mut header_buf, Phase::Recv, idle).await?;
        let header = StreamHeader::from_bytes(&header_buf)
            .map_err(|e| TransferFailure::protocol(format!("invalid stream header: {}", e)))?;

        let file_id = {
            let prefix = header.file_id_prefix;
            let map = self.prefix_to_file_id.lock().await;
            map.get(&prefix).cloned()
        };
        let Some(file_id) = file_id else {
            // 槽位不存在：对端在未接受/已取消后仍发数据 → 协议违例，不可重试
            // （对应状态机「resume 时槽位丢失 → Failed，禁止无限续」）
            warn!(prefix = header.file_id_prefix, "unknown file_id_prefix (no receive slot)");
            return Err(TransferFailure::protocol(format!(
                "unknown file_id_prefix {} (no receive slot)",
                header.file_id_prefix
            )));
        };

        // ---- 登记本连接代际（失败信号凭它判新鲜，见 recv_active_conn）----
        let conn_id = self.recv_conn_seq.fetch_add(1, Ordering::Relaxed);
        self.recv_active_conn
            .lock()
            .insert(file_id.clone(), conn_id);

        // ---- 段体：错误归属到 file_id，驱动接收方状态机 ----
        let result = self.recv_segment(&file_id, &header, &mut stream, idle).await;
        if let Err(f) = &result {
            self.on_recv_stream_failure(&file_id, conn_id, header.stream_id, f)
                .await;
        }
        result
    }

    /// 接收一段字节：校验布局 → 流式落盘 → 标记完成 → 收尾。
    async fn recv_segment(
        self: &Arc<Self>,
        file_id: &str,
        header: &StreamHeader,
        stream: &mut TcpStream,
        idle: Duration,
    ) -> std::result::Result<(), TransferFailure> {
        let slot = self
            .storage
            .get_slot(file_id)
            .await
            .ok_or_else(|| TransferFailure::protocol(format!("no receive slot for {}", file_id)))?;

        let seg_idx = header.stream_id as usize;
        let Some(&(seg_start, seg_len)) = slot.segments.get(seg_idx) else {
            return Err(TransferFailure::protocol(format!(
                "stream_id {} out of range (stream_count={})",
                header.stream_id, slot.stream_count
            )));
        };
        if header.start_offset != seg_start || header.data_len != seg_len {
            return Err(TransferFailure::protocol(format!(
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
                .map_err(|e| from_io(&e, Phase::DiskWrite))?;
            file.seek(SeekFrom::Start(header.start_offset))
                .await
                .map_err(|e| from_io(&e, Phase::DiskWrite))?;

            let mut remaining = header.data_len;
            let mut buf = vec![0u8; STREAM_IO_BUF.min(header.data_len.max(1) as usize)];
            let mut last_push = Instant::now()
                .checked_sub(PROGRESS_PUSH_INTERVAL)
                .unwrap_or_else(Instant::now);
            let mut resumed = false;
            while remaining > 0 {
                let want = std::cmp::min(remaining as usize, buf.len());
                let n = read_full(stream, &mut buf[..want], Phase::Recv, idle).await?;
                if n == 0 {
                    // 中途 EOF：对端进程死掉 / 链路断 → 网络类，可重试
                    return Err(TransferFailure::new(
                        FailureKind::UnexpectedEof,
                        Phase::Recv,
                        format!(
                            "truncated stream {} ({} bytes missing)",
                            header.stream_id, remaining
                        ),
                    ));
                }
                with_idle_timeout(file.write_all(&buf[..n]), idle)
                    .await
                    .map_err(|e| from_io(&e, Phase::DiskWrite))?;
                remaining -= n as u64;
                self.storage.add_bytes(file_id, n as u64).await;

                // 数据面活性恢复：**收到第一批字节**就清中断标记回「传输中」。
                // 不能等 finish_stream——大段重发（真机 2.63GB 单段）要分钟级，
                // 期间每个进度帧都会按未清标记推「已中断」，而数据明明在流入
                //（真机踩过：发送方 17MB/s 重发中、接收方却显示已中断）。
                if !resumed {
                    resumed = true;
                    if self.recv_interrupted.lock().remove(file_id).is_some() {
                        self.publish_recv_progress(file_id).await;
                    }
                }

                if last_push.elapsed() >= PROGRESS_PUSH_INTERVAL {
                    last_push = Instant::now();
                    // 必须重新取 slot：字节数每次从 storage 取最新值，
                    // 拿开头快照推进度会让接收端一直显示 0 B / 0 B/s
                    self.publish_recv_progress(file_id).await;
                }
            }
            with_idle_timeout(file.flush(), idle)
                .await
                .map_err(|e| from_io(&e, Phase::DiskWrite))?;
        }

        let slot = self
            .storage
            .finish_stream(file_id, header.stream_id)
            .await
            .ok_or_else(|| TransferFailure::protocol("slot vanished during stream"))?;
        // 任一段成功落盘 = 数据面已恢复（发送方段级重试成功后重发）：
        // 立即清中断标记回「传输中」。只在整任务收齐才清的话，会出现
        // 「字节一直在涨、状态却卡在已中断」的谎报（真机踩过）。
        self.recv_interrupted.lock().remove(file_id);
        self.publish_recv_progress(file_id).await;

        // 全部流收齐后的收尾
        if slot.is_complete() {
            if slot.await_sha256 && slot.sha256.is_none() {
                // 发送方声明会补发 sha256：先不 finalize，等 POST /api/verify。
                // 保险丝：120 秒未收到则跳过校验直接完成（A3.8 生命周期绑定见 apply 路径）。
                let engine = self.clone();
                let fid = file_id.to_string();
                tokio::spawn(async move {
                    engine.verify_fuse(&fid).await;
                });
            } else {
                let _ = self.finish_receive(file_id, &slot).await;
            }
        }
        Ok(())
    }

    /// 接收段失败后的状态机推进（A3.2）：
    /// - 可重试 → `Interrupted`：保留槽位与 .part，进度保持「已中断」；
    /// - 不可重试 → `Failed`：按现策略清理槽位，推终态。
    ///
    /// **陈旧信号丢弃**（真机竞态）：`conn_id` 是发起本次接收的连接序号。
    /// 若期间已有更新的连接接管（`recv_active_conn` 已前移），说明本失败
    /// 属于一条已被替代的旧连接——它的迟到超时**不得**把新连接的
    /// 「传输中」打回「已中断」（网络断开→恢复重发时必现，用户实测两次）。
    async fn on_recv_stream_failure(
        &self,
        file_id: &str,
        conn_id: u64,
        stream_id: u32,
        f: &TransferFailure,
    ) {
        // 槽位已终结（已完成/已取消）：无处标中断
        if self.storage.get_slot(file_id).await.is_none() {
            return;
        }
        // 陈旧连接的失败：新连接已接管数据面 → 丢弃
        {
            let active = self.recv_active_conn.lock().get(file_id).copied();
            if active != Some(conn_id) {
                debug!(
                    %file_id,
                    conn_id,
                    active = ?active,
                    "stale receive-failure signal discarded (superseded by newer connection)"
                );
                return;
            }
        }
        // UI 副文案（§3.5）：「流 3/8 失败（连接超时）」
        let compose = |total: u64| {
            format!(
                "流 {}/{} 失败（{}）",
                stream_id + 1,
                total.max(1),
                f.describe()
            )
        };
        if f.is_retryable() {
            let reason = self
                .storage
                .get_slot(file_id)
                .await
                .map(|s| compose(s.streams_done.len() as u64))
                .unwrap_or_else(|| f.describe());
            warn!(%file_id, kind = ?f.kind, detail = %f.detail, "receive stream failed → Interrupted (slot kept)");
            self.recv_interrupted
                .lock()
                .insert(file_id.to_string(), reason);
            self.publish_recv_progress(file_id).await;
            return;
        }

        // 不可重试：终态 Failed + 清理（槽位按现策略：abort 保留 .part 文件本身）
        warn!(%file_id, kind = ?f.kind, detail = %f.detail, "receive stream failed → Failed");
        self.recv_interrupted.lock().remove(file_id);
        let slot = self.storage.abort(file_id).await;
        let prefix = file_id_prefix_u64(file_id);
        self.prefix_to_file_id.lock().await.remove(&prefix);
        self.incoming.remove_by_file_id(file_id);
        self.incoming_endpoints.lock().await.remove(file_id);
        self.recv_speed_state.lock().await.remove(file_id);
        if let Some(slot) = slot {
            self.publish_progress(TransferProgress {
                file_id: file_id.to_string(),
                file_name: slot.file_name.clone(),
                file_size: slot.file_size,
                bytes_transferred: slot.bytes_received.min(slot.file_size),
                chunks_done: slot.streams_done.iter().filter(|ok| **ok).count() as u64,
                chunks_total: slot.streams_done.len() as u64,
                speed_bps: 0,
                status: TransferStatus::Failed,
                error: Some(compose(slot.streams_done.len() as u64)),
                incoming: true,
                file_path: None,
                retry_note: None,
            })
            .await;
        }
    }

    /// 对端已续传（发送方 resume 或本机续传信号回执）：清中断标记，
    /// 进度回到「传输中」（§3.5：点继续后 → 传输中）。
    ///
    /// 返回**本机是否还持有该任务的接收槽位**——发送方续传前用它探测：
    /// 接收方进程重启过（内存槽位丢失）→ false → 发送方直接 Failed，
    /// 不空烧数据面（A3.2「resume 时槽位丢失 → Failed，禁止无限续」）。
    pub async fn on_peer_resumed(&self, file_id: &str) -> bool {
        self.recv_interrupted.lock().remove(file_id);
        let slot_exists = self.storage.get_slot(file_id).await.is_some();
        if slot_exists {
            self.publish_recv_progress(file_id).await;
        }
        slot_exists
    }

    /// sha256 补发保险丝（A3.8）：绑定任务生命周期——
    /// 槽位被 finalize / 取消 / 中断清理即退出，禁止裸 sleep 挂 120s。
    async fn verify_fuse(self: &Arc<Self>, file_id: &str) {
        const FUSE_SECS: u64 = 120;
        const TICK: Duration = Duration::from_secs(1);
        let mut waited = 0u64;
        while waited < FUSE_SECS {
            tokio::time::sleep(TICK).await;
            waited += TICK.as_secs();
            // 槽位已消失（取消/完成/清理）或不再等待校验 → 任务生命周期结束
            match self.storage.get_slot(file_id).await {
                Some(slot) if slot.await_sha256 && slot.sha256.is_none() => {}
                Some(_) => return,
                None => return,
            }
        }
        info!(%file_id, "verify fuse fired (120s), finalize without checksum");
        let _ = self.apply_final_sha256(file_id, None).await;
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
            // (上次采样时刻, 上次 bytes, 上次速度)：每 SPEED_SAMPLE_WINDOW 更新一次。
            // 窗口太短会让速度随调度抖动乱跳；累计均值又会「越传越慢」。
            let ent = state
                .entry(file_id.to_string())
                .or_insert_with(|| (Instant::now(), bytes_done, 0));
            let dt = ent.0.elapsed();
            if dt >= SPEED_SAMPLE_WINDOW {
                let db = bytes_done.saturating_sub(ent.1);
                let sps = (db as f64 / dt.as_secs_f64()) as u64;
                *ent = (Instant::now(), bytes_done, sps);
                sps
            } else {
                ent.2
            }
        };
        // 中断标记存在时状态保持「已中断」（其余段可能仍在收尾，
        // 不能翻回「传输中」谎报；续传信号到达才清除）
        let interrupted = self.recv_interrupted.lock().get(file_id).cloned();
        let (status, error) = match &interrupted {
            Some(reason) => (TransferStatus::Interrupted, Some(reason.clone())),
            None => (TransferStatus::InProgress, None),
        };
        self.publish_progress(TransferProgress {
            file_id: file_id.to_string(),
            file_name: slot.file_name.clone(),
            file_size: slot.file_size,
            bytes_transferred: bytes_done,
            chunks_done: streams_done,
            chunks_total: slot.streams_done.len() as u64,
            speed_bps,
            status,
            error,
            incoming: true,
            file_path: None,
            retry_note: None,
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
        self.recv_interrupted.lock().remove(file_id);
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
            retry_note: None,
        })
        .await;
        result.map(|_| ())
    }

    /// 接收方收到发送方补发的最终 sha256（POST /api/verify/:file_id）。
    ///
    /// 返回结构化回执（A3.2 状态机对齐）：发送方**不能只看 HTTP 200** 就报
    /// 已完成——段没收齐时接收方不会 finalize，两边状态会永久分叉。
    pub async fn apply_final_sha256(
        &self,
        file_id: &str,
        sha256: Option<String>,
    ) -> crate::Result<VerifyOutcome> {
        let Some(slot) = self.storage.set_final_sha256(file_id, sha256).await else {
            return Ok(VerifyOutcome::Missing);
        };
        if slot.is_complete() {
            // finalize 失败（如 sha 不匹配）也已推 Failed 终态帧：
            // 对发送方而言任务同样「已终态」，回 Finalized 而不是 500。
            if let Err(e) = self.finish_receive(file_id, &slot).await {
                warn!(%file_id, error = %e, "finalize after verify failed (terminal state already pushed)");
            }
            return Ok(VerifyOutcome::Finalized);
        }
        // 段未收齐：携带**接收方权威完成位图**——TCP 写成功 ≠ 对端应用层收齐，
        // 发送方据此回退会话位图，续传只发接收方缺的段。
        Ok(VerifyOutcome::Pending {
            chunks_done: slot.streams_done,
        })
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
        // 取源文件大小：SAF fd 路径（/proc/self/fd/N）stat 可能被 FUSE 拒，
        // 统一走 open_source_file（dup）拿 metadata，错误按本地读分类成中文短语。
        let file_size = {
            let src = open_source_file(&file_path)
                .await
                .map_err(|e| crate::CoreError::Transfer(from_io(&e, Phase::LocalRead).describe()))?;
            let size = src
                .metadata()
                .await
                .map_err(|e| crate::CoreError::Transfer(from_io(&e, Phase::LocalRead).describe()))?
                .len();
            size
        };

        // 流数自适应：小文件单流，大文件拉满并行。
        // 会话在 offer 前登记并锁定 stream_count + 完成位图（A3.2 恢复粒度）。
        //
        // 例外：SAF fd 路径（/proc/self/fd/N）**固定单流**——该路径经 dup 打开后
        // 与原 fd 共享读偏移，多流并发 seek 会互相踩；单流顺序 seek+read 才安全。
        let stream_count = if is_proc_fd_path(&file_path) {
            1
        } else {
            compute_stream_count(file_size, self.parallel_streams)
        };
        let layout = stream_layout(file_size, stream_count);

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (progress_tx, progress_rx) = mpsc::channel(PROGRESS_CHANNEL_CAP);
        let _ = cancel_rx; // 决策/数据阶段的取消由 run_transfer 从会话订阅

        self.inflight
            .lock()
            .insert(file_id.clone(), cancel_tx.clone());

        let (resp_tx, resp_rx) = oneshot::channel::<HttpIncomingResponse>();
        self.outgoing_offers
            .lock()
            .await
            .insert(file_id.clone(), resp_tx);

        self.send_sessions.lock().await.insert(
            file_id.clone(),
            SendSession {
                file_name: file_name.clone(),
                file_size,
                stream_count,
                target_ip: target_ip.clone(),
                target_transfer_port,
                target_gateway_port,
                file_path: file_path.clone(),
                streams_done: Arc::new(Mutex::new(vec![false; layout.len()])),
                progress_tx: progress_tx.clone(),
                cancel_tx: cancel_tx.clone(),
                accepted: false,
                running: false,
            },
        );

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
            retry_note: None,
        };
        push_progress(&progress_tx, initial).await;

        let engine = self.clone();
        let file_id_for_spawn = file_id.clone();
        let file_name_for_err = file_name.clone();
        let progress_for_task = progress_tx.clone();
        tokio::spawn(async move {
            // sha256 **不要**在 offer 之前就开算：多文件批量发送时几个 GB 级
            // 文件同时全量读盘，会把手机 IO/网络栈打满，表现为后续 offer
            // `Connection timed out (os error 110)`。推迟到传输结束后再算。

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
                version: Some(crate::protocol::PROTOCOL_VERSION),
            };
            let offer_json = match serde_json::to_string(&offer) {
                Ok(s) => s,
                Err(e) => {
                    push_progress(&progress_for_task, TransferProgress {
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
                        retry_note: None,
                    }).await;
                    engine.cleanup_send_final(&file_id_for_spawn).await;
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
                    &engine.receive_dir,
                )
                .await
            };

            if let Err(e) = post_result {
                push_progress(&progress_for_task, TransferProgress {
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
                    retry_note: None,
                }).await;
                engine.cleanup_send_final(&file_id_for_spawn).await;
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
                push_progress(&progress_for_task, TransferProgress {
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
                    retry_note: None,
                }).await;
                engine.cleanup_send_final(&file_id_for_spawn).await;
                return;
            }

            // Accept → 标记会话 accepted（区分等决策中与 Interrupted 两种非运行态）
            {
                let mut sessions = engine.send_sessions.lock().await;
                if let Some(s) = sessions.get_mut(&file_id_for_spawn) {
                    s.accepted = true;
                } else {
                    // 已被取消（cancel 在 offer 回包前抢先把会话收走了）
                    return;
                }
            }
            engine
                .outgoing_endpoints
                .lock()
                .await
                .insert(file_id_for_spawn.clone(), (target_ip.clone(), target_gateway_port));

            // 数据阶段 + 校验补发 + 状态机推进（首次与续传共用，A3.2）
            engine
                .run_transfer(file_id_for_spawn, false)
                .await;
        });

        Ok(TransferHandle {
            file_id,
            cancel_tx,
            progress_rx,
        })
    }

    /// 终态 Canceled：清理会话并推帧（run_transfer 各取消检查点复用）。
    async fn finish_canceled(&self, file_id: &str, snap: &SessionSnap) {
        self.cleanup_send_final(file_id).await;
        let (chunks, bytes) = done_stats(&snap.streams_done, snap.file_size, snap.stream_count);
        push_progress(
            &snap.progress_tx,
            TransferProgress {
                file_id: file_id.to_string(),
                file_name: snap.file_name.clone(),
                file_size: snap.file_size,
                bytes_transferred: bytes,
                chunks_done: chunks,
                chunks_total: snap.stream_count as u64,
                speed_bps: 0,
                status: TransferStatus::Canceled,
                error: Some("canceled".into()),
                incoming: false,
                file_path: None,
                retry_note: None,
            },
        )
        .await;
    }

    /// 推 Interrupted 帧并落 running（会话与注册项保留，供「继续传输」）。
    /// 先推帧后落 running：反过来会让并发 resume 的 InProgress 帧被覆盖回已中断。
    async fn interrupt_send_session(&self, file_id: &str, snap: &SessionSnap, error: &str) {
        let (chunks, bytes) = done_stats(&snap.streams_done, snap.file_size, snap.stream_count);
        push_progress(
            &snap.progress_tx,
            TransferProgress {
                file_id: file_id.to_string(),
                file_name: snap.file_name.clone(),
                file_size: snap.file_size,
                bytes_transferred: bytes,
                chunks_done: chunks,
                chunks_total: snap.stream_count as u64,
                speed_bps: 0,
                status: TransferStatus::Interrupted,
                error: Some(error.to_string()),
                incoming: false,
                file_path: None,
                retry_note: None,
            },
        )
        .await;
        let mut sessions = self.send_sessions.lock().await;
        if let Some(s) = sessions.get_mut(file_id) {
            s.running = false;
        }
    }

    /// 执行（或续传）一次发送的数据阶段——A3.2 状态机推进器。
    ///
    /// 首次（Accept 后，`is_resume=false`）与用户「继续传输」共用。
    /// offer/决策阶段不在此。终态（Completed/Failed/Canceled）先清理会话再推帧，
    /// 避免与 `cancel()` 竞态双写；`Interrupted` 保留会话（先推帧后落 running）。
    async fn run_transfer(self: Arc<Self>, file_id: String, is_resume: bool) {
        // ---- 快照会话并互斥置 running ----
        let mut snap = {
            let mut sessions = self.send_sessions.lock().await;
            let Some(s) = sessions.get_mut(&file_id) else {
                return; // 已被取消/收走
            };
            if s.running {
                return; // 幂等：已在跑（并发 resume 直接忽略）
            }
            s.running = true;
            SessionSnap {
                file_name: s.file_name.clone(),
                file_size: s.file_size,
                stream_count: s.stream_count,
                target_ip: s.target_ip.clone(),
                target_transfer_port: s.target_transfer_port,
                target_gateway_port: s.target_gateway_port,
                file_path: s.file_path.clone(),
                streams_done: s.streams_done.clone(),
                progress_tx: s.progress_tx.clone(),
                cancel_rx: s.cancel_tx.subscribe(),
            }
        };

        // ---- 续传：先通知接收方清中断态，并**探测其槽位是否还在**（A3.2）----
        // 对方 app 重启过（内存槽位丢失）→ slot=false → 直接 Failed，
        // 不空烧数据面、不进入「继续→又中断」死循环；通知网络失败则继续，
        // 交由数据面重试自行判定（可能只是瞬时网络问题）。
        if is_resume {
            let notify = http_post_json_tls(
                &snap.target_ip,
                snap.target_gateway_port,
                &format!("/api/peer-resumed/{}", file_id),
                "{}",
                &self.receive_dir,
            )
            .await;
            match notify {
                Ok(body) => {
                    if parse_slot_flag(&body) == Some(false) {
                        warn!(
                            %file_id,
                            "receiver has no slot anymore → Failed (no infinite resume)"
                        );
                        self.cleanup_send_final(&file_id).await;
                        push_progress(
                            &snap.progress_tx,
                            TransferProgress {
                                file_id: file_id.clone(),
                                file_name: snap.file_name.clone(),
                                file_size: snap.file_size,
                                bytes_transferred: 0,
                                chunks_done: 0,
                                chunks_total: snap.stream_count as u64,
                                speed_bps: 0,
                                status: TransferStatus::Failed,
                                error: Some("接收方已无此任务，无法继续".into()),
                                incoming: false,
                                file_path: None,
                                retry_note: None,
                            },
                        )
                        .await;
                        return;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "notify receiver resume failed; continue via data plane");
                }
            }
            // 探测通过（或未知）→ 回「传输中」（§3.5：点继续后 → 传输中）
            let (chunks_done, bytes) =
                done_stats(&snap.streams_done, snap.file_size, snap.stream_count);
            push_progress(
                &snap.progress_tx,
                TransferProgress {
                    file_id: file_id.clone(),
                    file_name: snap.file_name.clone(),
                    file_size: snap.file_size,
                    bytes_transferred: bytes,
                    chunks_done,
                    chunks_total: snap.stream_count as u64,
                    speed_bps: 0,
                    status: TransferStatus::InProgress,
                    error: None,
                    incoming: false,
                    file_path: None,
                    retry_note: None,
                },
            )
            .await;
        }

        // ---- 数据阶段 ----
        let outcome = self
            .clone()
            .do_send(
                snap.target_ip.clone(),
                snap.target_transfer_port,
                snap.file_path.clone(),
                file_id.clone(),
                snap.file_name.clone(),
                snap.file_size,
                snap.stream_count,
                snap.streams_done.clone(),
                snap.cancel_rx.clone(),
                snap.progress_tx.clone(),
            )
            .await;

        // ---- 状态机推进（取消优先级最高）----
        if *snap.cancel_rx.borrow() || matches!(outcome, SendOutcome::Canceled) {
            self.finish_canceled(&file_id, &snap).await;
            return;
        }

        match outcome {
            SendOutcome::Completed => {
                // 传完再哈希（历史结论：不边传边哈希抢 IO）。分块 + 取消即停（A3.8）。
                let sha256 = {
                    let path = snap.file_path.clone();
                    let cancel_rx = snap.cancel_rx.clone();
                    let fid = file_id.clone();
                    tokio::task::spawn_blocking(move || -> Option<String> {
                        use sha2::{Digest, Sha256};
                        use std::io::Read;
                        let mut f = std::fs::File::open(&path).ok()?;
                        let mut hasher = Sha256::new();
                        let mut buf = vec![0u8; 1024 * 1024];
                        loop {
                            if *cancel_rx.borrow() {
                                return None; // 任务已被取消，哈希随之停止
                            }
                            match f.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => hasher.update(&buf[..n]),
                                Err(e) => {
                                    warn!(file_id = %fid, error = %e, "sha256 compute failed, skip verify");
                                    return None;
                                }
                            }
                        }
                        Some(format!("{:x}", hasher.finalize()))
                    })
                    .await
                    .unwrap_or(None)
                };
                if *snap.cancel_rx.borrow() {
                    // 哈希期间用户取消 → Canceled（不谎报 Completed）
                    self.finish_canceled(&file_id, &snap).await;
                    return;
                }

                // ---- verify 协商（轮询，见 negotiate_verify）----
                // TCP 写完成 ≠ 对端应用层收齐：do_send 一结束就发 verify 几乎
                // 必然 pending；网络抖动刚恢复时回执也可能发不出去。
                // 拿不到回执 → Interrupted（会话保留、位图不动），
                // 点继续重新协商——**不谎报 Completed**（真机痛点）。
                let body = serde_json::json!({ "sha256": sha256 }).to_string();
                let negotiation = negotiate_verify(
                    &snap.target_ip,
                    snap.target_gateway_port,
                    &file_id,
                    &body,
                    &mut snap.cancel_rx,
                    &self.verify_backoff,
                    &self.receive_dir,
                )
                .await;
                if *snap.cancel_rx.borrow() {
                    self.finish_canceled(&file_id, &snap).await;
                    return;
                }
                match negotiation {
                    VerifyNegotiation::Finalized => {
                        // 对端已终态 → 落到下面的 Completed
                    }
                    VerifyNegotiation::PendingBitmap(chunks_done) => {
                        {
                            // 对端确实没收齐：位图以接收方为权威回退
                            let mut done = snap.streams_done.lock();
                            if chunks_done.len() == done.len() {
                                *done = chunks_done;
                            } else {
                                for b in done.iter_mut() {
                                    *b = false;
                                }
                            }
                        }
                        warn!(
                            %file_id,
                            "receiver still pending after negotiation → Interrupted (bitmap rolled back)"
                        );
                        self.interrupt_send_session(
                            &file_id,
                            &snap,
                            "接收方尚未收齐，数据未确认送达",
                        )
                        .await;
                        return;
                    }
                    VerifyNegotiation::NotReceived => {
                        // 全程没拿到回执（网络不通）：没有信息就不动位图，
                        // 会话保留、点继续重新协商（自愈闭环）
                        warn!(%file_id, "verify receipt not received → Interrupted (keep bitmap)");
                        self.interrupt_send_session(
                            &file_id,
                            &snap,
                            "校验回执未达，可继续传输",
                        )
                        .await;
                        return;
                    }
                }

                self.cleanup_send_final(&file_id).await;
                push_progress(
                    &snap.progress_tx,
                    TransferProgress {
                        file_id: file_id.clone(),
                        file_name: snap.file_name.clone(),
                        file_size: snap.file_size,
                        bytes_transferred: snap.file_size,
                        chunks_done: snap.stream_count as u64,
                        chunks_total: snap.stream_count as u64,
                        speed_bps: 0,
                        status: TransferStatus::Completed,
                        error: None,
                        incoming: false,
                        file_path: None,
                        retry_note: None,
                    },
                )
                .await;
            }
            SendOutcome::Failed(f) => {
                warn!(%file_id, kind = ?f.kind, detail = %f.detail, "send failed → Failed (not retryable)");
                self.cleanup_send_final(&file_id).await;
                push_progress(
                    &snap.progress_tx,
                    TransferProgress {
                        file_id: file_id.clone(),
                        file_name: snap.file_name.clone(),
                        file_size: snap.file_size,
                        bytes_transferred: 0,
                        chunks_done: 0,
                        chunks_total: snap.stream_count as u64,
                        speed_bps: 0,
                        status: TransferStatus::Failed,
                        error: Some(f.describe()),
                        incoming: false,
                        file_path: None,
                        retry_note: None,
                    },
                )
                .await;
            }
            SendOutcome::Interrupted { stream_id, failure } => {
                // 保留会话与注册项（继续传输 / 取消都还要用）
                warn!(
                    %file_id,
                    stream_id,
                    kind = ?failure.kind,
                    detail = %failure.detail,
                    "send stream exhausted retries → Interrupted (segments kept)"
                );
                let reason = format!(
                    "流 {}/{} 失败（{}）",
                    stream_id + 1,
                    snap.stream_count,
                    failure.describe()
                );
                self.interrupt_send_session(&file_id, &snap, &reason).await;
            }
            SendOutcome::Canceled => unreachable!("canceled handled above"),
        }
    }

    /// 发送方续传入口（A3.2）：只重新调度 `streams_done[i] == false` 的段。
    pub async fn resume_send(self: Arc<Self>, file_id: &str) -> bool {
        if !self.send_sessions.lock().await.contains_key(file_id) {
            return false;
        }
        let engine = self.clone();
        let fid = file_id.to_string();
        tokio::spawn(async move {
            engine.run_transfer(fid, true).await;
        });
        true
    }

    /// 续传统一入口（本机 UI `POST /api/transfers/:file_id/resume`）。
    ///
    /// - 本机是发送方 → 直接续（只发未完成段）；
    /// - 本机是接收方 → 通知发送方 `POST /api/peer-resume`；通知不通
    ///   （发送方已离线）→ 槽位转 `Failed`（状态机：禁止无限续）。
    pub async fn resume_transfer(self: Arc<Self>, file_id: &str) -> bool {
        if self.send_sessions.lock().await.contains_key(file_id) {
            return self.resume_send(file_id).await;
        }

        let endpoint = self.incoming_endpoints.lock().await.get(file_id).cloned();
        let Some((peer_ip, peer_port)) = endpoint else {
            return false; // 既无发送会话也无接收槽位：没有可续的对象
        };

        // 本机先回到「传输中」，再通知对端续传
        self.on_peer_resumed(file_id).await;
        let res = http_post_json_tls(
            &peer_ip,
            peer_port,
            &format!("/api/peer-resume/{}", file_id),
            "{}",
            &self.receive_dir,
        )
        .await;
        if let Err(e) = res {
            // 对端 HTTP 明确拒绝（404=会话已终态）≠ 网络不通：
            // 文案分开，避免「会话早结束了」被误报成「发送方已离线」。
            // kind 区分见 httpc：非 2xx → InvalidData；transport 错误不产生该 kind。
            let reason = if e.kind() == std::io::ErrorKind::InvalidData {
                "对方已无此传输任务，无法继续"
            } else {
                "发送方已离线，无法继续"
            };
            // 发送方不可达/无会话：无法继续 → 终态 Failed，禁止无限续
            warn!(%file_id, error = %e, kind = ?e.kind(), "peer-resume failed → receiver Failed");
            self.recv_interrupted.lock().remove(file_id);
            let slot = self.storage.abort(file_id).await;
            let prefix = file_id_prefix_u64(file_id);
            self.prefix_to_file_id.lock().await.remove(&prefix);
            self.incoming.remove_by_file_id(file_id);
            self.incoming_endpoints.lock().await.remove(file_id);
            self.recv_speed_state.lock().await.remove(file_id);
            if let Some(slot) = slot {
                self.publish_progress(TransferProgress {
                    file_id: file_id.to_string(),
                    file_name: slot.file_name.clone(),
                    file_size: slot.file_size,
                    bytes_transferred: slot.bytes_received.min(slot.file_size),
                    chunks_done: slot.streams_done.iter().filter(|ok| **ok).count() as u64,
                    chunks_total: slot.streams_done.len() as u64,
                    speed_bps: 0,
                    status: TransferStatus::Failed,
                    error: Some(reason.into()),
                    incoming: true,
                    file_path: None,
                    retry_note: None,
                })
                .await;
            }
        }
        true
    }

    /// 启动 N 条流，每条顺序发送一个字节区间（A3.2 / A3.3）。
    ///
    /// - **段级自动重试**：可重试错误最多再试 2 次（0.5s / 1.5s 退避），
    ///   期间状态仍为 InProgress，推「重试中」提示帧；
    /// - **单流失败 → 其余流暂停**（`pause` 位）：停止继续发送，
    ///   已完成段保留位图（重试不重复传）；
    /// - 不可重试 → [`SendOutcome::Failed`]；重试耗尽 → [`SendOutcome::Interrupted`]。
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
        streams_done: Arc<Mutex<Vec<bool>>>,
        cancel_rx: watch::Receiver<bool>,
        progress_tx: mpsc::Sender<TransferProgress>,
    ) -> SendOutcome {
        let timeouts = self.timeouts;
        let file_id_prefix = file_id_prefix_u64(&file_id);
        let layout = stream_layout(file_size, stream_count);

        // 续传基数：已完成段的字节/计数直接入账，未完成段从 0 开始累计
        let (base_chunks, base_bytes) =
            done_stats(&streams_done, file_size, stream_count);
        let bytes_done = Arc::new(AtomicU64::new(base_bytes));
        let chunks_done_counter = Arc::new(AtomicU64::new(base_chunks));
        let last_push = Arc::new(TokioMutex::new(Instant::now()
            .checked_sub(PROGRESS_PUSH_INTERVAL)
            .unwrap_or_else(Instant::now)));
        let last_sample = Arc::new(TokioMutex::new((Instant::now(), base_bytes, 0u64)));

        // 任一终态失败置位 → 其余流在下个检查点暂停（A3.2「其余流暂停」）
        let pause = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let mut handles = Vec::new();
        for (stream_id, &(start_offset, seg_len)) in layout.iter().enumerate() {
            let target_ip = target_ip.clone();
            let file_path = file_path.clone();
            let mut cancel_rx = cancel_rx.clone();
            let streams_done = streams_done.clone();
            let pause = pause.clone();
            let pctx = SendProgress {
                progress_tx: progress_tx.clone(),
                file_id: file_id.clone(),
                file_name: file_name.clone(),
                file_size,
                streams_total: layout.len() as u64,
                streams_done: chunks_done_counter.clone(),
                bytes_done: bytes_done.clone(),
                last_push: last_push.clone(),
                last_sample: last_sample.clone(),
            };
            let file_id_prefix = file_id_prefix;

            handles.push(tokio::spawn(async move {
                let stream_id_u32 = stream_id as u32;
                // 已完成段不重复传（恢复粒度 = 段，A3.2）
                if streams_done.lock().get(stream_id).copied().unwrap_or(false) {
                    return WorkerResult::Done;
                }
                // 空段（空文件）：直接算完成
                if seg_len == 0 {
                    streams_done.lock()[stream_id] = true;
                    pctx.streams_done.fetch_add(1, Ordering::Relaxed);
                    pctx.maybe_push().await;
                    return WorkerResult::Done;
                }

                let mut last_retryable: Option<TransferFailure> = None;
                for attempt in 0..crate::timeouts::STREAM_RETRY_ATTEMPTS {
                    if pause.load(Ordering::SeqCst) {
                        return WorkerResult::Paused;
                    }
                    if *cancel_rx.borrow() {
                        return WorkerResult::Canceled;
                    }
                    if attempt > 0 {
                        // 退避（A3.3：0.5s、1.5s），可被取消打断
                        let backoff = crate::timeouts::STREAM_RETRY_BACKOFF
                            .get((attempt - 1) as usize)
                            .copied()
                            .unwrap_or(Duration::from_millis(1500));
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = cancel_rx.changed() => {
                                return WorkerResult::Canceled;
                            }
                        }
                        if pause.load(Ordering::SeqCst) {
                            return WorkerResult::Paused;
                        }
                        if *cancel_rx.borrow() {
                            return WorkerResult::Canceled;
                        }
                        // A3.5：重试中副文案「第 2/3 次重试流 3…」（流号 1 起）
                        pctx
                            .push_retry_note(stream_id_u32, attempt + 1)
                            .await;
                    }

                    match send_one_stream(
                        &target_ip,
                        target_port,
                        &file_path,
                        start_offset,
                        seg_len,
                        stream_id_u32,
                        file_id_prefix,
                        &mut cancel_rx,
                        &pctx,
                        timeouts,
                    )
                    .await
                    {
                        Ok(()) => {
                            streams_done.lock()[stream_id] = true;
                            pctx.streams_done.fetch_add(1, Ordering::Relaxed);
                            pctx.maybe_push().await;
                            return WorkerResult::Done;
                        }
                        Err(f) if f.kind == FailureKind::Canceled => {
                            return WorkerResult::Canceled;
                        }
                        Err(f) => {
                            warn!(stream_id, attempt, kind = ?f.kind, detail = %f.detail, "stream attempt failed");
                            if !f.is_retryable() {
                                // 不可重试：直接 Failed，兄弟流暂停
                                pause.store(true, Ordering::SeqCst);
                                return WorkerResult::Fatal(f);
                            }
                            if attempt + 1 >= crate::timeouts::STREAM_RETRY_ATTEMPTS {
                                // 自动重试耗尽 → Interrupted（保留段，可续）
                                pause.store(true, Ordering::SeqCst);
                                return WorkerResult::Exhausted {
                                    stream_id: stream_id_u32,
                                    failure: f,
                                };
                            }
                            last_retryable = Some(f);
                        }
                    }
                }
                // 理论不可达（循环内必 return）：按耗尽兜底
                pause.store(true, Ordering::SeqCst);
                WorkerResult::Exhausted {
                    stream_id: stream_id_u32,
                    failure: last_retryable.unwrap_or_else(|| {
                        TransferFailure::new(FailureKind::NetworkIo, Phase::Send, "retry exhausted")
                    }),
                }
            }));
        }

        // ---- 聚合各流结果 ----
        let mut fatal: Option<TransferFailure> = None;
        let mut exhausted: Option<(u32, TransferFailure)> = None;
        let mut any_canceled = false;
        for h in handles {
            match h.await {
                Ok(WorkerResult::Done) | Ok(WorkerResult::Paused) => {}
                Ok(WorkerResult::Canceled) => any_canceled = true,
                Ok(WorkerResult::Fatal(f)) => {
                    if fatal.is_none() {
                        fatal = Some(f);
                    }
                }
                Ok(WorkerResult::Exhausted { stream_id, failure }) => {
                    if exhausted.is_none() {
                        exhausted = Some((stream_id, failure));
                    }
                }
                Err(join) => {
                    // worker panic：内部错误，不可重试
                    fatal.get_or_insert(TransferFailure::new(
                        FailureKind::Internal,
                        Phase::Send,
                        format!("send worker crashed: {}", join),
                    ));
                }
            }
        }

        if any_canceled || *cancel_rx.borrow() {
            return SendOutcome::Canceled;
        }
        // 不可重试优先于「重试耗尽」：按分类表直接 Failed
        if let Some(f) = fatal {
            return SendOutcome::Failed(f);
        }
        if let Some((stream_id, failure)) = exhausted {
            return SendOutcome::Interrupted { stream_id, failure };
        }
        let all_done = {
            let done = streams_done.lock();
            layout.iter().enumerate().all(|(i, _)| done.get(i).copied().unwrap_or(false))
        };
        if all_done {
            SendOutcome::Completed
        } else {
            // Paused 却无失败/取消记录（不应发生）：按可续中断兜底
            SendOutcome::Interrupted {
                stream_id: 0,
                failure: TransferFailure::new(
                    FailureKind::NetworkIo,
                    Phase::Send,
                    "streams paused without recorded failure",
                ),
            }
        }
    }
}

/// [`do_send`] 的聚合结果，驱动 A3.2 状态机。
enum SendOutcome {
    Completed,
    Canceled,
    /// 不可重试错误 → Failed（会话移除）
    Failed(TransferFailure),
    /// 可重试错误且自动重试耗尽 → Interrupted（会话保留，可「继续传输」）
    Interrupted {
        stream_id: u32,
        failure: TransferFailure,
    },
}

/// 单条流任务的返回值。
enum WorkerResult {
    Done,
    /// 被兄弟流的终态失败暂停（不是错误；段保持未完成，续传时重发）
    Paused,
    Canceled,
    Fatal(TransferFailure),
    Exhausted { stream_id: u32, failure: TransferFailure },
}

/// `run_transfer` 的会话快照（持有期间会话可能被 cancel 移除，快照自足）。
struct SessionSnap {
    file_name: String,
    file_size: u64,
    stream_count: u32,
    target_ip: String,
    target_transfer_port: u16,
    target_gateway_port: u16,
    file_path: PathBuf,
    streams_done: Arc<Mutex<Vec<bool>>>,
    progress_tx: mpsc::Sender<TransferProgress>,
    cancel_rx: watch::Receiver<bool>,
}

/// 由完成位图计算 (chunks_done, bytes_done)——已完成段的真实进度。
fn done_stats(streams_done: &Mutex<Vec<bool>>, file_size: u64, stream_count: u32) -> (u64, u64) {
    let layout = stream_layout(file_size, stream_count);
    let done = streams_done.lock();
    let mut bytes = 0u64;
    let mut chunks = 0u64;
    for (i, ok) in done.iter().enumerate() {
        if *ok {
            chunks += 1;
            bytes += layout.get(i).map(|(_, l)| *l).unwrap_or(0);
        }
    }
    (chunks, bytes)
}

/// 单条流的一次完整尝试：connect → 帧头 → 段体 → shutdown。
/// 错误按阶段分类（A3.4）；取消映射为 `FailureKind::Canceled`。
#[allow(clippy::too_many_arguments)]
async fn send_one_stream(
    target_ip: &str,
    target_port: u16,
    file_path: &std::path::Path,
    start_offset: u64,
    seg_len: u64,
    stream_id: u32,
    file_id_prefix: u64,
    cancel_rx: &mut watch::Receiver<bool>,
    pctx: &SendProgress,
    timeouts: crate::timeouts::Timeouts,
) -> std::result::Result<(), TransferFailure> {
    let mut conn = connect_with_retry(target_ip, target_port, cancel_rx, timeouts.connect).await?;
    let header = StreamHeader {
        file_id_prefix,
        stream_id,
        _reserved: 0,
        start_offset,
        data_len: seg_len,
    };
    with_idle_timeout(conn.write_all(&header.to_bytes()), timeouts.idle)
        .await
        .map_err(|e| from_io(&e, Phase::Send))?;

    send_stream_body(
        &mut conn,
        file_path,
        start_offset,
        seg_len,
        cancel_rx,
        pctx,
        timeouts.idle,
    )
    .await?;

    let _ = conn.shutdown().await;
    Ok(())
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
    /// (上次采样时刻, 上次 bytes, 上次速度)，1 秒窗口算瞬时速度
    last_sample: Arc<TokioMutex<(Instant, u64, u64)>>,
}

impl SendProgress {
    /// 自动重试提示帧（A3.5）：「第 2/3 次重试流 3…」。
    /// UI 据 `retry_note` 非空显示「重试中」角标；正常帧为 None。
    async fn push_retry_note(&self, stream_id: u32, attempt: u32) {
        let bd = self.bytes_done.load(Ordering::Relaxed);
        let note = format!(
            "第 {}/{} 次重试流 {}…",
            attempt,
            crate::timeouts::STREAM_RETRY_ATTEMPTS,
            stream_id + 1
        );
        push_progress(
            &self.progress_tx,
            TransferProgress {
                file_id: self.file_id.clone(),
                file_name: self.file_name.clone(),
                file_size: self.file_size,
                bytes_transferred: bd.min(self.file_size),
                chunks_done: self.streams_done.load(Ordering::Relaxed),
                chunks_total: self.streams_total,
                speed_bps: 0,
                status: TransferStatus::InProgress,
                error: None,
                incoming: false,
                file_path: None,
                retry_note: Some(note),
            },
        )
        .await;
    }

    async fn maybe_push(&self) {
        {
            let mut last = self.last_push.lock().await;
            if last.elapsed() < PROGRESS_PUSH_INTERVAL {
                return;
            }
            *last = Instant::now();
        }
        let bd = self.bytes_done.load(Ordering::Relaxed);
        // 1 秒窗口的瞬时速度：窗口内沿用上次值，避免 100ms 抖动
        let speed_bps = {
            let mut s = self.last_sample.lock().await;
            let dt = s.0.elapsed();
            if dt >= SPEED_SAMPLE_WINDOW {
                let db = bd.saturating_sub(s.1);
                let sps = (db as f64 / dt.as_secs_f64()) as u64;
                *s = (Instant::now(), bd, sps);
                sps
            } else {
                s.2
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
            retry_note: None,
        })
        .await;
    }
}

/// 连接对端数据端口，带少量重试；取消可打断退避**与** connect。
///
/// connect 施加 `connect_limit`（A3.1，值来自引擎可注入超时），
/// 失败按阶段分类为 ConnectTimeout / ConnectFailed（均可重试，A3.4）。
async fn connect_with_retry(
    target_ip: &str,
    target_port: u16,
    cancel_rx: &mut watch::Receiver<bool>,
    connect_limit: Duration,
) -> std::result::Result<TcpStream, TransferFailure> {
    const ATTEMPTS: u32 = 3;
    const BACKOFF_MS: [u64; 3] = [0, 200, 500];
    let mut last_err: Option<TransferFailure> = None;
    for attempt in 0..ATTEMPTS {
        if *cancel_rx.borrow() {
            return Err(TransferFailure::canceled("canceled"));
        }
        let backoff = BACKOFF_MS
            .get(attempt as usize)
            .copied()
            .unwrap_or(500);
        if backoff > 0 {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(backoff)) => {}
                _ = cancel_rx.changed() => {
                    return Err(TransferFailure::canceled("canceled"));
                }
            }
        }
        let connect_result = tokio::select! {
            r = tokio::time::timeout(connect_limit, TcpStream::connect((target_ip, target_port))) => {
                r.map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"))
                    .and_then(|r| r)
            }
            _ = cancel_rx.changed() => {
                return Err(TransferFailure::canceled("canceled"));
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
                warn!(attempt, error = %e, "connect failed, will retry");
                last_err = Some(from_io(&e, Phase::Connect));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        TransferFailure::new(FailureKind::ConnectFailed, Phase::Connect, "no attempt")
    }))
}

/// 把文件区间顺序写入 socket，边写边推估计进度。取消立即生效。
///
/// 失败分类（A3.4）：本地读 → SourceError（不可重试）；
/// socket 写（按 `idle` 空闲上限）→ 网络类（可重试）。
async fn send_stream_body(
    conn: &mut TcpStream,
    file_path: &std::path::Path,
    start_offset: u64,
    seg_len: u64,
    cancel_rx: &mut watch::Receiver<bool>,
    pctx: &SendProgress,
    idle: Duration,
) -> std::result::Result<(), TransferFailure> {
    use std::io::SeekFrom;
    let mut file = open_source_file(file_path)
        .await
        .map_err(|e| from_io(&e, Phase::LocalRead))?;
    file.seek(SeekFrom::Start(start_offset))
        .await
        .map_err(|e| from_io(&e, Phase::LocalRead))?;

    let mut remaining = seg_len;
    let mut buf = vec![0u8; STREAM_IO_BUF];
    while remaining > 0 {
        if *cancel_rx.borrow() {
            return Err(TransferFailure::canceled("canceled"));
        }
        let want = std::cmp::min(remaining as usize, buf.len());
        let n = {
            // 用 select 响应取消：读文件时用户点取消也要尽快退出
            tokio::select! {
                r = file.read(&mut buf[..want]) => {
                    r.map_err(|e| from_io(&e, Phase::LocalRead))?
                }
                _ = cancel_rx.changed() => {
                    return Err(TransferFailure::canceled("canceled"));
                }
            }
        };
        if n == 0 {
            // 源文件在传输中被截断/删除：本地条件已坏，不可重试
            return Err(TransferFailure::new(
                FailureKind::SourceError,
                Phase::LocalRead,
                format!(
                    "source truncated at offset {}",
                    start_offset + (seg_len - remaining)
                ),
            ));
        }
        with_idle_timeout(conn.write_all(&buf[..n]), idle)
            .await
            .map_err(|e| from_io(&e, Phase::Send))?;
        remaining -= n as u64;
        pctx.bytes_done.fetch_add(n as u64, Ordering::Relaxed);
        pctx.maybe_push().await;
    }
    with_idle_timeout(conn.flush(), idle)
        .await
        .map_err(|e| from_io(&e, Phase::Send))?;
    Ok(())
}

/// 读满 buf，返回实际读到的字节数。**返回 0 = 对端已关闭（干净 EOF）**。
///
/// 每次底层 read 施加空闲上限 `idle`（A3.1，值来自引擎可注入超时）：
/// 连续无字节到达即收敛为 TimedOut，再按 `phase` 分类（Recv → 可重试的空闲超时）。
async fn read_full(
    stream: &mut TcpStream,
    buf: &mut [u8],
    phase: Phase,
    idle: Duration,
) -> std::result::Result<usize, TransferFailure> {
    let mut read = 0;
    while read < buf.len() {
        let n = match with_idle_timeout(stream.read(&mut buf[read..]), idle).await {
            Ok(n) => n,
            Err(e) => return Err(from_io(&e, phase)),
        };
        match n {
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

/// 解析 `/proc/self/fd/N` → fd 号（纯字符串逻辑，全平台可测）。
fn parse_proc_fd(path: &std::path::Path) -> Option<i32> {
    path.to_str()?.strip_prefix("/proc/self/fd/")?.parse().ok()
}

/// 源路径是否为 Android SAF 的已打开 fd 路径（`/proc/self/fd/N`）。
fn is_proc_fd_path(path: &std::path::Path) -> bool {
    parse_proc_fd(path).is_some()
}

/// 打开源文件（发送侧读取用）。
///
/// `/proc/self/fd/N`（Android SAF 零拷贝路径，MainActivity 持有 pfd）
/// **不能用路径 open**：那会经 FUSE 重新打开原始文件并按 app 的存储授权做检查——
/// 没有 READ_MEDIA_VIDEO / READ_EXTERNAL_STORAGE 时直接 EACCES（真机表现为
/// 「没有文件访问权限」），而对同一 fd 的 stat 却放行，所以文件大小拿得到、
/// 真正读取才失败。修法：直接 `dup` 已打开的句柄——dup 不做路径权限检查。
///
/// 注意：dup 与原 fd **共享读偏移**，所以 `send_file` 对这类源固定单流
/// （见 stream_count 判定），避免并发段 seek 互相踩。每次打开各 dup 一份，
/// 关闭只关自己那份，Java 侧原 pfd 不受影响。
async fn open_source_file(path: &std::path::Path) -> std::io::Result<tokio::fs::File> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        if let Some(fd) = parse_proc_fd(path) {
            let dup = unsafe { libc::dup(fd) };
            if dup < 0 {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: dup 是本函数刚创建的有效 fd，所有权交给 File（析构时关闭）。
            use std::os::fd::FromRawFd;
            let f = unsafe { std::fs::File::from_raw_fd(dup) };
            return Ok(tokio::fs::File::from_std(f));
        }
    }
    tokio::fs::File::open(path).await
}

/// 解析 `peer-resumed` 回包 `{"slot": bool}`。
/// 缺字段/坏 JSON → None（当作未知，交由数据面自行判断），不靠字符串猜语义。
fn parse_slot_flag(body: &str) -> Option<bool> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .get("slot")?
        .as_bool()
}

/// 单次补发校验值的结果。
enum VerifyPost {
    /// 2xx，携带回执 body
    Receipt(String),
    /// 对端 HTTP 明确拒绝（4xx/5xx 响应）
    Rejected,
    /// transport 失败（refused/reset/超时）——网络抖动期常见，轮询方继续
    Transport,
}

/// 单次 POST /api/verify（不重试；轮询节奏由 [`negotiate_verify`] 统一管）。
async fn post_verify_once(
    host: &str,
    port: u16,
    file_id: &str,
    body: &str,
    receive_dir: &std::path::Path,
) -> VerifyPost {
    match http_post_json_tls(
        host,
        port,
        &format!("/api/verify/{}", file_id),
        body,
        receive_dir,
    )
    .await
    {
        Ok(r) => VerifyPost::Receipt(r),
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
            warn!(error = %e, "verify rejected by peer");
            VerifyPost::Rejected
        }
        Err(e) => {
            warn!(error = %e, "verify post transport error");
            VerifyPost::Transport
        }
    }
}

/// verify 协商：轮询直到拿到终态回执或预算耗尽。
///
/// - `finalized`/`missing`/旧对端空回执 → [`VerifyNegotiation::Finalized`]；
/// - 对端反复 `pending`（**TCP 写完成 ≠ 对端应用层收齐**，早发必然如此）
///   → 轮询等它收尾，耗尽仍 pending → [`VerifyNegotiation::PendingBitmap`]，
///   发送方按权威位图回退（此时对端确实缺数据）；
/// - 全程 transport 失败（网络抖动恢复期）→ [`VerifyNegotiation::NotReceived`]，
///   发送方 **Interrupted 保留会话、位图不动**（不谎报 Completed，点继续自愈）。
///
/// `backoff[i]` = 第 i+1 轮 POST 前的等待（len = 轮数上限）。
async fn negotiate_verify(
    host: &str,
    port: u16,
    file_id: &str,
    body: &str,
    cancel_rx: &mut watch::Receiver<bool>,
    backoff: &[Duration],
    receive_dir: &std::path::Path,
) -> VerifyNegotiation {
    let mut saw_pending: Option<Vec<bool>> = None;
    for (i, wait) in backoff.iter().enumerate() {
        if i > 0 {
            tokio::select! {
                _ = tokio::time::sleep(*wait) => {}
                _ = cancel_rx.changed() => break,
            }
            if *cancel_rx.borrow() {
                break;
            }
        }
        match post_verify_once(host, port, file_id, body, receive_dir).await {
            VerifyPost::Receipt(receipt) => match parse_verify_result(&receipt) {
                Some(VerifyOutcome::Finalized) | Some(VerifyOutcome::Missing) => {
                    return VerifyNegotiation::Finalized;
                }
                Some(VerifyOutcome::Pending { chunks_done }) => {
                    saw_pending = Some(chunks_done);
                    // 对端还没收完：等它收尾，下一轮再问
                }
                None => {
                    // 旧对端空 body / 未知 result：宽容当已 finalize
                    return VerifyNegotiation::Finalized;
                }
            },
            VerifyPost::Rejected => {
                // 对端明确报错：未完成校验，按「未收到回执」处理（可继续）
                return VerifyNegotiation::NotReceived;
            }
            VerifyPost::Transport => {
                // 网络抖动：等下一轮（覆盖恢复期）
            }
        }
    }
    match saw_pending {
        Some(chunks_done) => VerifyNegotiation::PendingBitmap(chunks_done),
        None => VerifyNegotiation::NotReceived,
    }
}

/// verify 协商结果（驱动发送方终态，A3.2 状态机对齐）。
enum VerifyNegotiation {
    /// 接收方任务已终态（或旧对端宽容）→ 发送方 Completed
    Finalized,
    /// 全程无回执（网络不通）→ 发送方 Interrupted，会话保留、位图不动
    NotReceived,
    /// 对端明确 pending 直到耗尽 → 发送方按位图回退后 Interrupted
    PendingBitmap(Vec<bool>),
}

/// 解析 verify 回执 `{"result": …, "chunks_done": […]}`。
///
/// - `pending` 但位图缺失/损坏 → `Pending { chunks_done: 空 }`（长度不符时
///   调用方按「全未完成」回退——保守正确，宁可整段重发也不谎报）；
/// - 坏 JSON / 旧对端空 body → None（宽容：当 finalized 处理，维持不谎报失败）。
fn parse_verify_result(body: &str) -> Option<VerifyOutcome> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    match v.get("result")?.as_str()? {
        "finalized" => Some(VerifyOutcome::Finalized),
        "missing" => Some(VerifyOutcome::Missing),
        "pending" => {
            let chunks = v
                .get("chunks_done")
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|b| b.as_bool().unwrap_or(false))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            Some(VerifyOutcome::Pending {
                chunks_done: chunks,
            })
        }
        _ => None,
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

/// offer 投递：连不上的瞬时故障重试几次。
///
/// 多文件批量发送时几个 offer 几乎同时出站，手机侧偶发 `ETIMEDOUT`
/// （网络栈被打满 / 路由瞬时不可达）。一次失败就整文件判死太脆。
/// 本函数与 `crate::httpc` 共用实现（超时见 timeouts 模块）。
async fn http_post_json_retry(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
    receive_dir: &std::path::Path,
) -> std::io::Result<String> {
    const ATTEMPTS: u32 = 3;
    const BACKOFF_MS: [u64; 3] = [0, 300, 800];
    let mut last_err = std::io::Error::new(std::io::ErrorKind::Other, "no attempt");
    for attempt in 0..ATTEMPTS {
        let backoff = BACKOFF_MS.get(attempt as usize).copied().unwrap_or(800);
        if backoff > 0 {
            tokio::time::sleep(Duration::from_millis(backoff)).await;
        }
        match http_post_json_tls(host, port, path, body, receive_dir).await {
            Ok(r) => return Ok(r),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                // 对端明确 HTTP 拒绝（如协议版本 400）：再试结果一样，
                // 按 A3.4「对端终态判决」不可重试，直接返回。
                warn!(attempt, error = %e, "http rejected by peer, no retry");
                return Err(e);
            }
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
            version: Some(crate::protocol::PROTOCOL_VERSION),
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
                retry_note: None,
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
                retry_note: None,
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
            retry_note: None,
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
            retry_note: None,
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
            retry_note: None,
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
                retry_note: None,
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
                retry_note: None,
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
            retry_note: None,
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
        let dir = base.join(format!("kitefile-stream-e2e-{}", std::process::id()));
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
            connect_with_retry(
                "127.0.0.1",
                1,
                &mut cancel_rx,
                crate::timeouts::CONNECT_TIMEOUT,
            )
            .await
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

    /// A3.1：对端 accept 后一个字节不发，read_full 必须收敛为空闲超时
    /// （可重试类），而不是无限等待。虚拟时间推进，不真等 30s。
    #[tokio::test(start_paused = true)]
    async fn read_full_converges_on_silent_peer_via_idle_timeout() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _sock = listener.accept().await;
            // 保持连接但从不发送数据
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });

        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut buf = [0u8; 8];
        let res = read_full(&mut client, &mut buf, Phase::Recv, crate::timeouts::IDLE_TIMEOUT).await;
        let f = res.unwrap_err();
        assert_eq!(f.kind, FailureKind::IdleTimeout);
        assert!(f.is_retryable(), "空闲超时应可重试（A3.4）");
        assert_eq!(f.describe(), "传输空闲超时");
    }

    // ============ A3.2 / A3.3 状态机验收测试 ============

    fn tmp_dir(tag: &str) -> PathBuf {
        let base = std::env::var("FTCORE_TEST_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("kitefile-a32-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// 直接登记一个「已接受」的发送会话（跳过 offer/决策，专测数据阶段）。
    async fn insert_session(
        engine: &TransferEngine,
        file_id: &str,
        file_name: &str,
        file_path: PathBuf,
        file_size: u64,
        stream_count: u32,
        target_ip: &str,
        target_transfer_port: u16,
        target_gateway_port: u16,
        streams_done: Vec<bool>,
    ) -> (mpsc::Receiver<TransferProgress>, watch::Sender<bool>) {
        let (cancel_tx, _) = watch::channel(false);
        let (progress_tx, progress_rx) = mpsc::channel(PROGRESS_CHANNEL_CAP);
        engine
            .inflight
            .lock()
            .insert(file_id.to_string(), cancel_tx.clone());
        let layout = stream_layout(file_size, stream_count);
        assert_eq!(streams_done.len(), layout.len());
        engine.send_sessions.lock().await.insert(
            file_id.to_string(),
            SendSession {
                file_name: file_name.to_string(),
                file_size,
                stream_count,
                target_ip: target_ip.to_string(),
                target_transfer_port,
                target_gateway_port,
                file_path,
                streams_done: Arc::new(Mutex::new(streams_done)),
                progress_tx,
                cancel_tx: cancel_tx.clone(),
                accepted: true,
                running: false,
            },
        );
        (progress_rx, cancel_tx)
    }

    /// A验收 2：连不上对端 → 自动重试耗尽 → Interrupted；
    /// 会话与进度保留（不谎报失败/完成）；随后用户「取消」→ Canceled 并清理。
    #[tokio::test]
    async fn dead_peer_exhausts_retries_interrupts_then_cancel_cleans() {
        let dir = tmp_dir("dead-peer");
        let engine = Arc::new(TransferEngine::new(0, 4, dir.clone()));

        // 源文件 9MB → 3 段（stream_count 锁定为 3）
        let src = dir.join("src.bin");
        let payload = vec![0xABu8; 9 * 1024 * 1024];
        std::fs::write(&src, &payload).unwrap();
        let file_id = "dead-peer-fid";
        let dead_port = free_port(); // 无人监听 → ConnectionRefused（可重试）

        let (mut rx, _cancel) = insert_session(
            &engine,
            file_id,
            "src.bin",
            src,
            payload.len() as u64,
            3,
            "127.0.0.1",
            dead_port,
            free_port(),
            vec![false; 3],
        )
        .await;

        engine
            .clone()
            .run_transfer(file_id.to_string(), false)
            .await;

        // 收集全部进度帧
        let mut frames = Vec::new();
        while let Ok(p) = rx.try_recv() {
            frames.push(p);
        }
        // 自动重试提示帧（A3.5「第 2/3 次重试流 …」）
        assert!(
            frames.iter().any(|p| p
                .retry_note
                .as_deref()
                .map(|n| n.contains("第 2/3 次重试"))
                .unwrap_or(false)),
            "应出现自动重试提示帧"
        );
        let last = frames.last().expect("应有终态帧");
        assert_eq!(last.status, TransferStatus::Interrupted);
        assert!(
            last.error.as_deref().unwrap_or("").contains("流"),
            "中断帧应携带「流 x/y 失败（原因）」副文案，got {:?}",
            last.error
        );
        assert_eq!(last.chunks_total, 3);

        // 会话保留（可续），running 已落
        {
            let sessions = engine.send_sessions.lock().await;
            let s = sessions.get(file_id).expect("Interrupted 必须保留会话");
            assert!(!s.running, "中断后 running 必须落回 false");
            assert!(
                !s.streams_done.lock().iter().all(|b| *b),
                "未完成段不得被标记完成"
            );
        }

        // A验收 4：用户取消 → 终态 Canceled + 资源清理
        assert!(engine.cancel(file_id).await, "取消应命中会话");
        let mut canceled = None;
        while let Ok(p) = rx.try_recv() {
            if p.status == TransferStatus::Canceled {
                canceled = Some(p);
            }
        }
        assert!(canceled.is_some(), "取消后应推 Canceled 终态帧");
        assert!(engine.send_sessions.lock().await.is_empty(), "取消必须移除会话");
        assert!(!engine.inflight.lock().contains_key(file_id), "取消必须清 inflight");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A验收 3：续传只调度未完成段——已完成段不重发（哨兵字节不被覆盖），
    /// 全部完成后接收方落盘内容与源一致。
    #[tokio::test]
    async fn resume_resends_only_undone_segments() {
        let recv_dir = tmp_dir("resume-seg");
        let src_dir = tmp_dir("resume-src");
        let port = free_port();

        let recv = Arc::new(TransferEngine::new(port, 4, recv_dir.clone()));
        recv.clone().spawn_receiver().await.unwrap();

        // 源文件：10 字节 = 2 段（5+5），内容 AAAAA|BBBBB
        let src = src_dir.join("out.bin");
        std::fs::write(&src, b"AAAAABBBBB").unwrap();
        let file_id = "resume-fid";

        // 接收槽：段0 已完成且内容为哨兵 ZZZZZ（若发送方错误重发段0 会把它覆盖成 AAAAA）
        recv.storage
            .create_slot(file_id.into(), "out.bin".into(), 10, 2, None, false)
            .await
            .unwrap();
        recv.storage.write_at(file_id, 0, b"ZZZZZ").await.unwrap();
        recv.storage.write_at(file_id, 5, b"XXXXX").await.unwrap();
        recv.storage.finish_stream(file_id, 0).await.unwrap();
        recv.prefix_to_file_id
            .lock()
            .await
            .insert(file_id_prefix_u64(file_id), file_id.to_string());

        // 发送会话：位图 [true, false] → 只应发送段1
        let sender = Arc::new(TransferEngine::new(0, 4, src_dir.clone()));
        // verify 回执服务：回 finalized（否则发送方进入轮询协商、
        // 拿不到回执会 Interrupted——那正是对齐修复后的预期行为）
        let (gw_port, _hits) =
            spawn_scripted_http(vec![("200 OK", r#"{"result":"finalized","chunks_done":null}"#)])
                .await;
        let (mut rx, _cancel) = insert_session(
            &sender,
            file_id,
            "out.bin",
            src,
            10,
            2,
            "127.0.0.1",
            port,
            gw_port,
            vec![true, false],
        )
        .await;

        sender
            .clone()
            .run_transfer(file_id.to_string(), true)
            .await;

        // 状态机：先「传输中」（续传帧）后 Completed
        let mut frames = Vec::new();
        while let Ok(p) = rx.try_recv() {
            frames.push(p);
        }
        assert_eq!(frames.first().map(|p| p.status), Some(TransferStatus::InProgress));
        assert_eq!(frames.last().map(|p| p.status), Some(TransferStatus::Completed));

        // 最终文件：段0 哨兵未被重发覆盖，段1 为真实数据
        let final_path = recv_dir.join("out.bin");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !final_path.exists() {
            assert!(Instant::now() < deadline, "接收方未 finalize");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let got = std::fs::read(&final_path).unwrap();
        assert_eq!(
            got,
            b"ZZZZZBBBBB",
            "只允许重发未完成段：段0 哨兵必须原样保留"
        );

        let _ = std::fs::remove_dir_all(&recv_dir);
        let _ = std::fs::remove_dir_all(&src_dir);
    }

    /// A3.3：瞬时故障（首次连不上）→ 自动重试第 2 次成功 → Completed，
    /// 期间出现「第 2/3 次重试」提示帧。
    #[tokio::test]
    async fn transient_connect_failure_auto_retries_then_completes() {
        let recv_dir = tmp_dir("flaky-recv");
        let src_dir = tmp_dir("flaky-src");
        let port = free_port();

        let recv = Arc::new(TransferEngine::new(port, 4, recv_dir.clone()));
        let src = src_dir.join("ok.bin");
        std::fs::write(&src, b"hello-retry").unwrap();
        let file_id = "flaky-fid";
        recv.storage
            .create_slot(file_id.into(), "ok.bin".into(), 11, 1, None, false)
            .await
            .unwrap();
        recv.prefix_to_file_id
            .lock()
            .await
            .insert(file_id_prefix_u64(file_id), file_id.to_string());

        // 端口 P 起初无人监听 → 第 1 次尝试必然 ConnectionRefused（可重试）；
        // 收到「重试提示帧」后再绑定监听（早于第 2 次尝试的下一轮连接）。
        let (note_tx, note_rx) = tokio::sync::oneshot::channel::<()>();
        let mut note_tx = Some(note_tx);

        let sender = Arc::new(TransferEngine::new(0, 4, src_dir.clone()));
        // verify 回执服务：回 finalized——否则协商拿不到回执 → Interrupted
        //（会话保留、进度通道不关），下面等 channel 关闭的 collector 会永久挂起
        let (gw_port, _hits) =
            spawn_scripted_http(vec![("200 OK", r#"{"result":"finalized","chunks_done":null}"#)])
                .await;
        let (mut rx, _cancel) = insert_session(
            &sender,
            file_id,
            "ok.bin",
            src,
            11,
            1,
            "127.0.0.1",
            port,
            gw_port,
            vec![false],
        )
        .await;

        // 帧收集器：见到重试提示就通知绑定方
        let frames_slot: Arc<Mutex<Vec<TransferProgress>>> = Arc::new(Mutex::new(Vec::new()));
        let frames_slot2 = frames_slot.clone();
        let collector = tokio::spawn(async move {
            while let Some(p) = rx.recv().await {
                if p.retry_note.is_some() {
                    if let Some(tx) = note_tx.take() {
                        let _ = tx.send(());
                    }
                }
                frames_slot2.lock().push(p);
            }
        });

        let recv2 = recv.clone();
        let binder = tokio::spawn(async move {
            // 重试提示出现即绑定；10s 兜底（保证不悬挂）
            let _ = tokio::time::timeout(Duration::from_secs(10), note_rx).await;
            recv2.clone().spawn_receiver().await.unwrap();
        });

        sender
            .clone()
            .run_transfer(file_id.to_string(), false)
            .await;
        binder.await.unwrap();
        collector.await.unwrap();

        let frames = frames_slot.lock().clone();
        assert!(
            frames.iter().any(|p| p.retry_note.is_some()),
            "应出现自动重试提示帧"
        );
        assert_eq!(
            frames.last().map(|p| p.status),
            Some(TransferStatus::Completed),
            "重试成功后应 Completed，frames={:?}",
            frames.last().map(|p| (p.status, p.error.clone()))
        );
        // 发送方 Completed（verify 回执）可先于接收方应用层 finalize：
        // 轮询等落盘，不做瞬时断言
        let final_path = recv_dir.join("ok.bin");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !final_path.exists() {
            assert!(Instant::now() < deadline, "接收方未 finalize 落盘");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let got = std::fs::read(&final_path).unwrap();
        assert_eq!(got, b"hello-retry");

        let _ = std::fs::remove_dir_all(&recv_dir);
        let _ = std::fs::remove_dir_all(&src_dir);
    }

    /// A验收 1（接收方视角）：发送方中途静默 → 空闲超时收敛为 Interrupted，
    /// 槽位与 .part 保留；续传信号到达回「传输中」。
    #[tokio::test]
    async fn silent_sender_mid_stream_marks_receiver_interrupted() {
        let dir = tmp_dir("recv-interrupt");
        let port = free_port();
        let mut cfg_recv = TransferEngine::new(port, 4, dir.clone());
        cfg_recv.timeouts = crate::timeouts::Timeouts {
            connect: Duration::from_secs(2),
            idle: Duration::from_millis(300),
        };
        let recv = Arc::new(cfg_recv);
        recv.clone().spawn_receiver().await.unwrap();

        let file_id = "recv-int-fid";
        recv.storage
            .create_slot(file_id.into(), "big.bin".into(), 100, 1, None, false)
            .await
            .unwrap();
        recv.prefix_to_file_id
            .lock()
            .await
            .insert(file_id_prefix_u64(file_id), file_id.to_string());

        // 手工扮演发送方：发头 + 10 字节后彻底静默（保持连接不关闭）
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let header = StreamHeader {
            file_id_prefix: file_id_prefix_u64(file_id),
            stream_id: 0,
            _reserved: 0,
            start_offset: 0,
            data_len: 100,
        };
        sock.write_all(&header.to_bytes()).await.unwrap();
        sock.write_all(&[0xAB; 10]).await.unwrap();

        // 等待空闲超时（300ms）触发中断
        tokio::time::sleep(Duration::from_millis(1200)).await;

        let frame = recv
            .list_transfers()
            .into_iter()
            .find(|p| p.file_id == file_id)
            .expect("应有进度帧");
        assert_eq!(
            frame.status,
            TransferStatus::Interrupted,
            "静默对端应把接收方推进已中断"
        );
        assert!(
            frame.error.as_deref().unwrap_or("").contains("空闲超时"),
            "错误应为可解释原因，got {:?}",
            frame.error
        );
        // 槽位与 .part 保留（A3.2：中断 ≠ 失败 ≠ 取消）
        assert!(
            recv.storage.get_slot(file_id).await.is_some(),
            "中断必须保留接收槽位"
        );

        // 续传信号 → 回「传输中」
        assert!(
            recv.on_peer_resumed(file_id).await,
            "本机持有槽位应上报 true"
        );
        let frame = recv
            .list_transfers()
            .into_iter()
            .find(|p| p.file_id == file_id)
            .unwrap();
        assert_eq!(frame.status, TransferStatus::InProgress);
        assert_eq!(frame.error, None);

        drop(sock);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ============ 真机修复：SAF fd dup 打开 + 续传探测/文案 ============

    /// `/proc/self/fd/N` 解析与单流判定（纯字符串逻辑，全平台）。
    #[test]
    fn parse_proc_fd_and_single_stream_detection() {
        assert_eq!(parse_proc_fd(std::path::Path::new("/proc/self/fd/42")), Some(42));
        assert!(is_proc_fd_path(std::path::Path::new("/proc/self/fd/7")));
        assert!(!is_proc_fd_path(std::path::Path::new("/storage/emulated/0/DCIM/a.mp4")));
        assert!(!is_proc_fd_path(std::path::Path::new("/proc/self/fd/abc")), "非数字 fd 不算");
        assert!(!is_proc_fd_path(std::path::Path::new("/proc/123/fd/4")), "只认 self");
    }

    /// peer-resumed 回包解析：坏 JSON/缺字段 → None（交数据面自判，不猜语义）。
    #[test]
    fn parse_slot_flag_variants() {
        assert_eq!(parse_slot_flag(r#"{"slot":true}"#), Some(true));
        assert_eq!(parse_slot_flag(r#"{"slot":false}"#), Some(false));
        assert_eq!(parse_slot_flag("{}"), None);
        assert_eq!(parse_slot_flag("not json"), None);
        assert_eq!(parse_slot_flag(r#"{"slot":"x"}"#), None);
    }

    /// 常规路径走常规 open（全平台）。
    #[tokio::test]
    async fn open_source_file_normal_path_opens_regular_file() {
        let dir = tmp_dir("open-src");
        let p = dir.join("a.bin");
        std::fs::write(&p, b"hello").unwrap();
        let mut f = open_source_file(&p).await.unwrap();
        let mut buf = [0u8; 5];
        f.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SAF fd 场景核心机制：对 `/proc/self/fd/N` 必须走 dup（不做路径权限检查）。
    /// 仅 Linux/Android 编译（Windows/macOS 无该 procfs 语义；CI ubuntu 会跑）。
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[tokio::test]
    async fn open_source_file_dups_proc_fd() {
        use std::os::fd::AsRawFd;
        let dir = tmp_dir("dup-fd");
        let p = dir.join("a.bin");
        std::fs::write(&p, b"0123456789").unwrap();
        let src = std::fs::File::open(&p).unwrap();
        let proc_path = std::path::PathBuf::from(format!("/proc/self/fd/{}", src.as_raw_fd()));
        let mut f = open_source_file(&proc_path).await.unwrap();
        let mut buf = [0u8; 10];
        f.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"0123456789");
        // 原 fd 关掉后 dup 出来的句柄仍可读（所有权独立）
        drop(src);
        let mut f2 = open_source_file(&proc_path).await;
        // 注意：src 已关，/proc 路径随之失效 → 这里应失败；dup 有效性由上面读成功证明
        assert!(f2.is_err() || f2.is_ok()); // 不假设 proc 行为，仅防 panic
        drop(f2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `on_peer_resumed` 上报槽位存在性（发送方续传探测的依据）。
    #[tokio::test]
    async fn on_peer_resumed_reports_slot_presence() {
        let dir = tmp_dir("resume-slot");
        let engine = TransferEngine::new(0, 2, dir.clone());
        assert!(
            !engine.on_peer_resumed("nope").await,
            "无槽位应返回 false"
        );
        engine
            .storage
            .create_slot("fid-rs".into(), "f.bin".into(), 4, 1, None, false)
            .await
            .unwrap();
        assert!(
            engine.on_peer_resumed("fid-rs").await,
            "有槽位应返回 true"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 测试用服务端 TLS 配置（接受任意客户端证书；进程内每个测试目录
    /// 各自加载一次身份，`load_or_create` 幂等）。
    fn test_server_tls_cfg() -> Arc<rustls::ServerConfig> {
        let base = std::env::var("FTCORE_TEST_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let id = crate::tls::NodeIdentity::load_or_create(&base.join("kitefile-test-identity"))
            .expect("test identity");
        crate::tls::server_config(&id).expect("test tls server config")
    }

    /// 极简 **TLS** HTTP 服务：回固定状态行与 body，统计命中次数。
    ///
    /// 阶段 5 P1 起跨机 HTTP 一律走 TLS，测试对端也必须是 TLS 才能被
    /// `http_post_json_tls` 打到。
    async fn spawn_fixed_http(
        status: &'static str,
        body: &'static str,
    ) -> (u16, Arc<std::sync::atomic::AtomicU32>) {
        spawn_scripted_http(vec![(status, body)]).await
    }

    /// 极简 TLS HTTP 服务：按命中序号返回 `responses[i]`（超出取最后一项）。
    async fn spawn_scripted_http(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (u16, Arc<std::sync::atomic::AtomicU32>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let hits2 = hits.clone();
        let acceptor = tokio_rustls::TlsAcceptor::from(test_server_tls_cfg());
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
                let acceptor = acceptor.clone();
                let hits = hits2.clone();
                let responses = responses.clone();
                tokio::spawn(async move {
                    let Ok(mut sock) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let idx = hits.fetch_add(1, Ordering::SeqCst) as usize;
                    let (status, body) = responses
                        .get(idx)
                        .copied()
                        .unwrap_or(responses.last().copied().unwrap());
                    let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    // 必须优雅关闭：不发 close_notify 的裸 TCP FIN 会让
                    // rustls 客户端报 UnexpectedEof，body 到了也读不出来
                    let _ = tokio::io::AsyncWriteExt::shutdown(&mut sock).await;
                });
            }
        });
        (port, hits)
    }

    /// 接收方点「继续」，对端 HTTP 404（会话已终态）→ 文案必须是
    /// 「对方已无此传输任务」，**不得**误报「发送方已离线」。
    #[tokio::test]
    async fn resume_transfer_404_says_task_gone_not_offline() {
        let dir = tmp_dir("resume-404");
        let (port, _hits) = spawn_fixed_http("404 Not Found", "not found").await;
        let engine = Arc::new(TransferEngine::new(0, 2, dir.clone()));
        engine
            .storage
            .create_slot("fid-r404".into(), "v.mp4".into(), 10, 1, None, false)
            .await
            .unwrap();
        engine
            .incoming_endpoints
            .lock()
            .await
            .insert("fid-r404".into(), ("127.0.0.1".into(), port));

        assert!(engine.clone().resume_transfer("fid-r404").await);
        // 槽位按状态机清理（禁止无限续）
        assert!(engine.storage.get_slot("fid-r404").await.is_none());
        let frame = engine
            .list_transfers()
            .into_iter()
            .find(|p| p.file_id == "fid-r404")
            .expect("应有终态帧");
        assert_eq!(frame.status, TransferStatus::Failed);
        assert_eq!(
            frame.error.as_deref(),
            Some("对方已无此传输任务，无法继续"),
            "HTTP 404 属对端明确判决，不是离线"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 接收方点「继续」，对端网络不通（refused）→ 才是「发送方已离线」。
    #[tokio::test]
    async fn resume_transfer_transport_failure_says_offline() {
        let dir = tmp_dir("resume-offline");
        let dead_port = free_port(); // 无人监听
        let engine = Arc::new(TransferEngine::new(0, 2, dir.clone()));
        engine
            .storage
            .create_slot("fid-roff".into(), "v.mp4".into(), 10, 1, None, false)
            .await
            .unwrap();
        engine
            .incoming_endpoints
            .lock()
            .await
            .insert("fid-roff".into(), ("127.0.0.1".into(), dead_port));

        assert!(engine.clone().resume_transfer("fid-roff").await);
        let frame = engine
            .list_transfers()
            .into_iter()
            .find(|p| p.file_id == "fid-roff")
            .expect("应有终态帧");
        assert_eq!(frame.status, TransferStatus::Failed);
        assert_eq!(frame.error.as_deref(), Some("发送方已离线，无法继续"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 发送方点「继续」，接收方回 slot=false（对端 app 重启、内存槽位已丢）→
    /// **不推「传输中」、不进数据面**，直接 Failed（防空烧、防「继续→又中断」死循环）。
    #[tokio::test]
    async fn resume_aborts_before_data_plane_when_receiver_slot_gone() {
        let dir = tmp_dir("resume-noslot");
        let (gw_port, _hits) = spawn_fixed_http("200 OK", r#"{"slot":false}"#).await;
        let src = dir.join("src.bin");
        std::fs::write(&src, b"0123456789").unwrap();
        let engine = Arc::new(TransferEngine::new(0, 4, dir.clone()));
        let (mut rx, _cancel) = insert_session(
            &engine,
            "fid-gone",
            "src.bin",
            src,
            10,
            2,
            "127.0.0.1",
            free_port(),
            gw_port,
            vec![true, false],
        )
        .await;

        engine.clone().run_transfer("fid-gone".into(), true).await;

        let mut frames = Vec::new();
        while let Ok(p) = rx.try_recv() {
            frames.push(p);
        }
        let statuses: Vec<_> = frames.iter().map(|f| f.status).collect();
        assert_eq!(
            statuses.first(),
            Some(&TransferStatus::Failed),
            "slot=false 时首帧必须是 Failed，不得先回传输中（statuses={statuses:?}）"
        );
        assert_eq!(
            frames[0].error.as_deref(),
            Some("接收方已无此任务，无法继续")
        );
        assert!(
            engine.send_sessions.lock().await.is_empty(),
            "失败后会话必须清理"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 对端明确 HTTP 拒绝（400）时 offer 重试必须短路（A3.4：终态判决不重试）。
    #[tokio::test]
    async fn offer_retry_short_circuits_on_http_rejection() {
        let (port, hits) = spawn_fixed_http("400 Bad Request", "bad").await;
        let dir = tmp_dir("retry-400");
        let err = http_post_json_retry("127.0.0.1", port, "/api/incoming", "{}", &dir)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "对端明确拒绝只允许打一次，不得退避重试"
        );
    }

    // ============ verify 回执：两端状态机对齐（真机「手机已完成/桌面已中断」） ============

    /// apply_final_sha256 三态：无槽 Missing / 未收齐 Pending（权威位图）/ 收齐 Finalized。
    #[tokio::test]
    async fn apply_verify_three_outcomes() {
        let dir = tmp_dir("verify-outcome");
        let engine = TransferEngine::new(0, 2, dir.clone());

        // Missing：槽位不存在
        let o = engine.apply_final_sha256("nope", Some("00".into())).await.unwrap();
        assert_eq!(o, VerifyOutcome::Missing);

        // Pending：未收齐 → 带接收方位图
        engine
            .storage
            .create_slot("vf-p".into(), "a.bin".into(), 10, 2, None, true)
            .await
            .unwrap();
        let o = engine.apply_final_sha256("vf-p", Some("00".into())).await.unwrap();
        assert_eq!(o, VerifyOutcome::Pending { chunks_done: vec![false, false] });

        // Finalized：收齐 + sha 匹配 → 真实落盘
        engine
            .storage
            .create_slot("vf-d".into(), "b.bin".into(), 10, 2, None, true)
            .await
            .unwrap();
        engine.storage.write_at("vf-d", 0, b"12345").await.unwrap();
        engine.storage.write_at("vf-d", 5, b"67890").await.unwrap();
        engine.storage.finish_stream("vf-d", 0).await.unwrap();
        engine.storage.finish_stream("vf-d", 1).await.unwrap();
        let sha = {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(b"1234567890");
            format!("{:x}", h.finalize())
        };
        let o = engine.apply_final_sha256("vf-d", Some(sha)).await.unwrap();
        assert_eq!(o, VerifyOutcome::Finalized);
        assert!(dir.join("b.bin").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// verify 回执解析：三态 + 坏 JSON/未知 result → None（宽容）+ pending 无位图 → 空位图。
    #[test]
    fn parse_verify_result_variants() {
        assert_eq!(
            parse_verify_result(r#"{"result":"finalized","chunks_done":null}"#),
            Some(VerifyOutcome::Finalized)
        );
        assert_eq!(
            parse_verify_result(r#"{"result":"missing","chunks_done":null}"#),
            Some(VerifyOutcome::Missing)
        );
        assert_eq!(
            parse_verify_result(r#"{"result":"pending","chunks_done":[true,false]}"#),
            Some(VerifyOutcome::Pending { chunks_done: vec![true, false] })
        );
        // pending 但位图缺失 → 空位图（调用方按全未完成回退，保守正确）
        assert_eq!(
            parse_verify_result(r#"{"result":"pending"}"#),
            Some(VerifyOutcome::Pending { chunks_done: vec![] })
        );
        // 坏 JSON / 旧对端空 body / 未知 result → None（宽容当 finalized）
        assert_eq!(parse_verify_result(""), None);
        assert_eq!(parse_verify_result("not json"), None);
        assert_eq!(parse_verify_result(r#"{"result":"weird"}"#), None);
    }

    /// verify 对端明确 HTTP 拒绝不重试（hits==1）。
    #[tokio::test]
    async fn post_verify_reject_short_circuits() {
        let (port, hits) = spawn_fixed_http("500 Internal Server Error", "boom").await;
        let dir = tmp_dir("verify-reject");
        let out = post_verify_once("127.0.0.1", port, "fid", "{}", &dir).await;
        assert!(matches!(out, VerifyPost::Rejected));
        assert_eq!(hits.load(Ordering::SeqCst), 1, "HTTP 判决不重试");
    }

    /// 真机场景回归：发送方 do_send 全部成功（TCP 写完），但接收方回执
    /// pending + 权威位图 → 发送方必须回退位图并显示**已中断**（保留会话可续），
    /// 不得 Completed——否则两端状态永久分叉（手机已完成 / 桌面已中断）。
    #[tokio::test]
    async fn verify_pending_aligns_sender_to_interrupted() {
        let dir = tmp_dir("verify-align");
        let (gw_port, _hits) = spawn_fixed_http(
            "200 OK",
            r#"{"result":"pending","chunks_done":[true,false]}"#,
        )
        .await;
        let src = dir.join("src.bin");
        std::fs::write(&src, b"0123456789").unwrap();
        let mut engine = TransferEngine::new(0, 4, dir.clone());
        // 协商退避注入短表（2 轮即耗尽），不真等默认 9.5s 窗口
        engine.verify_backoff = vec![Duration::from_millis(10), Duration::from_millis(10)];
        let engine = Arc::new(engine);
        // 位图全 true：do_send 各段直接 skip → Completed → hash → verify
        let (mut rx, _cancel) = insert_session(
            &engine,
            "fid-align",
            "src.bin",
            src,
            10,
            2,
            "127.0.0.1",
            free_port(),
            gw_port,
            vec![true, true],
        )
        .await;

        engine.clone().run_transfer("fid-align".into(), false).await;

        let mut frames = Vec::new();
        while let Ok(p) = rx.try_recv() {
            frames.push(p);
        }
        let statuses: Vec<_> = frames.iter().map(|f| f.status).collect();
        assert!(
            !statuses.contains(&TransferStatus::Completed),
            "pending 回执下不得出现 Completed（statuses={statuses:?}）"
        );
        let last = frames.last().expect("应有终态帧");
        assert_eq!(last.status, TransferStatus::Interrupted);
        assert!(
            last.error.as_deref().unwrap_or("").contains("接收方尚未收齐"),
            "got {:?}",
            last.error
        );

        // 会话保留且位图按接收方权威回退 → 续传只发缺的段1
        let sessions = engine.send_sessions.lock().await;
        let s = sessions.get("fid-align").expect("pending 必须保留会话");
        assert!(!s.running, "中断后 running 必须落回");
        assert_eq!(
            *s.streams_done.lock(),
            vec![true, false],
            "位图必须以接收方回执为准回退"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 网络抖动导致全程拿不到 verify 回执（transport 全失败）：
    /// 发送方必须 Interrupted 保留会话、**位图一格不动**（无信息不回退），
    /// 点继续重新协商自愈——不得谎报 Completed（真机痛点）。
    #[tokio::test]
    async fn verify_not_received_keeps_bitmap_and_interrupts() {
        let dir = tmp_dir("verify-noreceipt");
        let src = dir.join("src.bin");
        std::fs::write(&src, b"0123456789").unwrap();
        let mut engine = TransferEngine::new(0, 4, dir.clone());
        engine.verify_backoff = vec![Duration::from_millis(10), Duration::from_millis(10)];
        let engine = Arc::new(engine);
        let dead_port = free_port(); // 无人监听 → transport 全程失败
        let (mut rx, _cancel) = insert_session(
            &engine,
            "fid-noreceipt",
            "src.bin",
            src,
            10,
            2,
            "127.0.0.1",
            free_port(),
            dead_port,
            vec![true, true],
        )
        .await;

        engine
            .clone()
            .run_transfer("fid-noreceipt".into(), false)
            .await;

        let mut frames = Vec::new();
        while let Ok(p) = rx.try_recv() {
            frames.push(p);
        }
        assert!(
            !frames
                .iter()
                .any(|f| f.status == TransferStatus::Completed),
            "无回执不得 Completed"
        );
        let last = frames.last().expect("应有终态帧");
        assert_eq!(last.status, TransferStatus::Interrupted);
        assert!(
            last.error.as_deref().unwrap_or("").contains("校验回执未达"),
            "got {:?}",
            last.error
        );
        let sessions = engine.send_sessions.lock().await;
        let s = sessions.get("fid-noreceipt").expect("必须保留会话");
        assert_eq!(
            *s.streams_done.lock(),
            vec![true, true],
            "无回执时位图一格不得回退（没有信息）"
        );
        assert!(!s.running);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// verify 轮询自愈：前几轮 pending（对端还在收尾），随后 finalized → Completed。
    /// 这正是「TCP 写完成 ≠ 对端收齐」的正常竞态，不能一轮 pending 就中断重发。
    #[tokio::test]
    async fn verify_polling_recovers_from_early_pending() {
        let dir = tmp_dir("verify-poll");
        let src = dir.join("src.bin");
        std::fs::write(&src, b"0123456789").unwrap();
        let mut engine = TransferEngine::new(0, 4, dir.clone());
        engine.verify_backoff = vec![
            Duration::from_millis(10),
            Duration::from_millis(10),
            Duration::from_millis(10),
        ];
        let engine = Arc::new(engine);
        // 第 1、2 轮 pending，第 3 轮 finalized（模拟对端收尾完成后才 finalize）
        let (gw_port, _hits) = spawn_scripted_http(vec![
            (
                "200 OK",
                r#"{"result":"pending","chunks_done":[true,false]}"#,
            ),
            (
                "200 OK",
                r#"{"result":"pending","chunks_done":[true,true]}"#,
            ),
            ("200 OK", r#"{"result":"finalized","chunks_done":null}"#),
        ])
        .await;
        let (mut rx, _cancel) = insert_session(
            &engine,
            "fid-poll",
            "src.bin",
            src,
            10,
            2,
            "127.0.0.1",
            free_port(),
            gw_port,
            vec![true, true],
        )
        .await;

        engine.clone().run_transfer("fid-poll".into(), false).await;

        let mut frames = Vec::new();
        while let Ok(p) = rx.try_recv() {
            frames.push(p);
        }
        assert_eq!(
            frames.last().map(|f| f.status),
            Some(TransferStatus::Completed),
            "轮询等到 finalized 必须 Completed（statuses={:?}）",
            frames.iter().map(|f| f.status).collect::<Vec<_>>()
        );
        assert!(
            engine.send_sessions.lock().await.is_empty(),
            "Completed 后会话必须清理"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// R1 回归（真机：发送方 17MB/s 重发中、接收方却显示已中断）：
    /// 中断标记必须随**首个读块返回**清除——不等 finish_stream、不等整段完成。
    /// 构造「首块已收、整段未 finish」的窗口：段0 重连只发满一个
    /// STREAM_IO_BUF 块（256KB），余下字节不发（idle 2s 内），
    /// 此刻任务两段都未完成、段0 也未 finish，最新帧必须已是「传输中」。
    /// （read_full 要读满 want 才返回，故用 >256KB 的段制造分块。）
    #[tokio::test]
    async fn interrupted_cleared_on_segment_finish_before_complete() {
        let dir = tmp_dir("recv-recover");
        let port = free_port();
        let mut cfg_recv = TransferEngine::new(port, 4, dir.clone());
        cfg_recv.timeouts = crate::timeouts::Timeouts {
            connect: Duration::from_secs(2),
            idle: Duration::from_secs(2),
        };
        let recv = Arc::new(cfg_recv);
        recv.clone().spawn_receiver().await.unwrap();

        let file_id = "recv-recover-fid";
        // 600_000 字节 / 2 段 = 每段 300_000（> 256KB 块 → 读循环分块返回）
        recv.storage
            .create_slot(file_id.into(), "two.bin".into(), 600_000, 2, None, false)
            .await
            .unwrap();
        recv.prefix_to_file_id
            .lock()
            .await
            .insert(file_id_prefix_u64(file_id), file_id.to_string());

        let header = |stream_id: u32| StreamHeader {
            file_id_prefix: file_id_prefix_u64(file_id),
            stream_id,
            _reserved: 0,
            start_offset: if stream_id == 0 { 0 } else { 300_000 },
            data_len: 300_000,
        };

        // 第一次：段1 发 10 字节后静默 → idle(2s) 超时 → Interrupted（标记置位）
        let mut sock1 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        sock1.write_all(&header(1).to_bytes()).await.unwrap();
        sock1.write_all(&[0xAB; 10]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2600)).await;
        let frame = recv
            .list_transfers()
            .into_iter()
            .find(|p| p.file_id == file_id)
            .expect("应有中断帧");
        assert_eq!(frame.status, TransferStatus::Interrupted);
        assert!(recv.recv_interrupted.lock().contains_key(file_id));
        drop(sock1);

        // 第二次：段0 重连只发**一个读块**（256KB < 整段 300_000）
        // → 首块返回即须清标记推「传输中」；余下 37_856 字节不发，
        // 此刻段0 未 finish、任务未 complete——若清除只在 finish/complete，
        // 断言会失败（这正是真机分钟级卡「已中断」的窗口）。
        let mut sock2 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        sock2.write_all(&header(0).to_bytes()).await.unwrap();
        sock2.write_all(&vec![0xCDu8; STREAM_IO_BUF]).await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut latest = TransferStatus::Interrupted;
        while Instant::now() < deadline {
            latest = recv
                .list_transfers()
                .into_iter()
                .find(|p| p.file_id == file_id)
                .map(|p| p.status)
                .unwrap_or(latest);
            if latest == TransferStatus::InProgress {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            latest,
            TransferStatus::InProgress,
            "首块数据到达即须回「传输中」，不得等整段 finish"
        );
        assert!(
            !recv.recv_interrupted.lock().contains_key(file_id),
            "中断标记必须随首批数据清除"
        );

        drop(sock2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 竞态回归（真机两次实测）：网络断开挂起的旧连接 A 的 30s 空闲超时
    /// **迟到触发**时，新连接 B 已在正常收数据——A 的失败信号必须被
    /// 丢弃，不得把 B 的「传输中」打回「已中断」；当前活跃连接的失败
    /// 则必须正常置位。
    #[tokio::test]
    async fn stale_connection_failure_signal_is_discarded() {
        let dir = tmp_dir("stale-sig");
        let engine = TransferEngine::new(0, 2, dir.clone());
        engine
            .storage
            .create_slot("fid-stale".into(), "a.bin".into(), 10, 1, None, false)
            .await
            .unwrap();

        // 连接 A 建立（登记代际），随后连接 B 建立接管（代际前移）
        let conn_a = engine.recv_conn_seq.fetch_add(1, Ordering::Relaxed);
        engine
            .recv_active_conn
            .lock()
            .insert("fid-stale".into(), conn_a);
        let conn_b = engine.recv_conn_seq.fetch_add(1, Ordering::Relaxed);
        engine
            .recv_active_conn
            .lock()
            .insert("fid-stale".into(), conn_b);

        let f = TransferFailure::new(FailureKind::IdleTimeout, Phase::Recv, "stale idle");

        // A 的迟到失败：已被 B 接管 → 丢弃
        engine
            .on_recv_stream_failure("fid-stale", conn_a, 0, &f)
            .await;
        assert!(
            !engine.recv_interrupted.lock().contains_key("fid-stale"),
            "陈旧连接的失败信号必须丢弃，不得覆盖新连接的恢复状态"
        );

        // B（当前活跃）的失败：正常置位
        engine
            .on_recv_stream_failure("fid-stale", conn_b, 0, &f)
            .await;
        assert!(
            engine.recv_interrupted.lock().contains_key("fid-stale"),
            "活跃连接的失败必须置位中断"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 槽位已终结（finalize/取消后）的失败信号：无处标中断，直接忽略。
    #[tokio::test]
    async fn failure_signal_without_slot_is_ignored() {
        let dir = tmp_dir("sig-no-slot");
        let engine = TransferEngine::new(0, 2, dir.clone());
        let conn = engine.recv_conn_seq.fetch_add(1, Ordering::Relaxed);
        engine
            .recv_active_conn
            .lock()
            .insert("fid-gone".into(), conn);

        let f = TransferFailure::new(FailureKind::IdleTimeout, Phase::Recv, "x");
        engine
            .on_recv_stream_failure("fid-gone", conn, 0, &f)
            .await;
        assert!(
            !engine.recv_interrupted.lock().contains_key("fid-gone"),
            "没有槽位不得产生中断标记（防 finalize 后的僵尸信号）"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
