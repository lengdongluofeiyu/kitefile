//! HTTP 网关：供 Web 前端 / 其他 daemon 调用
//!
//! 路由：
//! - GET  /api/whoami           → 本机信息
//! - GET  /api/devices          → 已发现的设备列表
//! - POST /api/send             → 发起发送：body = { target_ip, target_port?, target_gateway_port?, file_path }
//! - GET  /api/transfers        → 当前所有传输进度
//! - POST /api/cancel/:file_id → 取消传输
//! - GET  /api/files            → 接收目录文件列表
//!
//! 收发握手（双向）：
//! - POST /api/incoming                  → 接收方 daemon 接收发送方 daemon 的 Offer
//! - GET  /api/incoming                  → 接收方 UI 列出待决定的传入请求
//! - POST /api/incoming/:id/accept       → 接收方 UI 决定接受
//! - POST /api/incoming/:id/reject       → 接收方 UI 决定拒绝
//! - POST /api/incoming-resp             → 发送方 daemon 接收接收方 daemon 的回包
//! - POST /api/verify/:file_id           → 发送方补发整文件 sha256（边传边算，传完后校验）
//!
//! - WS   /ws/progress                   → 实时推送 WsEvent（进度 / 传入请求 / 决议完成）
//! - GET  /                              → 静态资源（web build 产物）
//!
//! ## 访问分级
//!
//! 网关监听 0.0.0.0（对端 daemon 必须能回调本机，不能只听 127.0.0.1），
//! 因此用一层 middleware 按 **(Method, Path)** 把接口分成两档，见 [`classify`]：
//!
//! - `Remote`：P2P 协商主流程 + 只读信息，局域网可访问
//! - `LocalOnly`：控制本机的敏感操作，仅接受回环地址
//! - 表中查不到的（method, path）组合：远程一律 403，本机放行
//!   （fail-closed 对外、fail-open 对内：宁可远程调不通，也不能让本机 UI 挂掉）
//!
//! 注意维度是二元的：`/api/incoming` 上 POST（对端发 offer）属 Remote，
//! GET（本机 UI 拉收件箱）属 LocalOnly，只按路径分级必然二选一出错。
//!
//! 开发期可从其他设备遥控本机：`--remote-admin` 开关会放开 LocalOnly 那一档。
//! 默认是关的。
//!
//! 本机判定依赖 `ConnectInfo<SocketAddr>`，因此 serve 时必须用
//! `into_make_service_with_connect_info`，否则 extensions 里取不到 peer 地址
//! （取不到时按非本机处理，会让本机 UI 全部 403）。

use crate::discovery::DiscoveryService;
use crate::httpc::http_post_json;
use crate::protocol::{HttpIncomingResponse, HttpOffer, IncomingEntry, WsEvent};
use crate::transfer::{TransferEngine, TransferProgress};
use crate::{EngineConfig, Result};
use axum::{
    body::Body,
    extract::{
        connect_info::ConnectInfo,
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Request, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Clone)]
pub struct AppState {
    pub discovery: Arc<DiscoveryService>,
    pub transfer: Arc<TransferEngine>,
    pub config: Arc<EngineConfig>,
    pub progress_bus: Arc<tokio::sync::broadcast::Sender<TransferProgress>>,
    pub ws_event_bus: Arc<tokio::sync::broadcast::Sender<WsEvent>>,
}

#[derive(Debug, Deserialize)]
pub struct SendRequest {
    pub target_ip: String,
    pub target_port: Option<u16>,
    #[serde(default)]
    pub target_gateway_port: Option<u16>,
    pub file_path: String,
    /// 文件名覆盖：SAF（Android）场景传入 /proc/self/fd/N 这类
    /// 无法从路径推断出原始文件名的路径时使用
    #[serde(default)]
    pub file_name: Option<String>,
    /// 批量发送：一次多选的多个文件共享同一个 batch_id，
    /// 接收端据此合成一张卡片、一次确认整批。三个字段必须同时给，
    /// 缺任何一个都按单文件处理（不会因为部分缺失导致批次信息自相矛盾）。
    #[serde(default)]
    pub batch_id: Option<String>,
    #[serde(default)]
    pub batch_index: Option<u32>,
    #[serde(default)]
    pub batch_total: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct SendResponse {
    pub file_id: String,
}

#[derive(Debug, Serialize)]
pub struct WhoAmI {
    pub id: String,
    pub name: String,
    pub platform: String,
    pub gateway_port: u16,
    pub transfer_port: u16,
}

pub struct HttpGateway {
    state: AppState,
}

impl HttpGateway {
    pub fn new(
        discovery: Arc<DiscoveryService>,
        transfer: Arc<TransferEngine>,
        config: Arc<EngineConfig>,
    ) -> Self {
        let (progress_bus, _) = tokio::sync::broadcast::channel(256);
        let (ws_event_bus, _) = tokio::sync::broadcast::channel(256);
        Self {
            state: AppState {
                discovery,
                transfer,
                config,
                progress_bus: Arc::new(progress_bus),
                ws_event_bus: Arc::new(ws_event_bus),
            },
        }
    }

    /// gateway 启动前把 progress 广播注入 transfer engine
    ///
    /// WsEvent 广播**不注入**——它由 gateway 自己持有并直接发送
    /// （见 `incoming_offer` / `decide_incoming`），engine 侧不需要第二份 bus。
    pub async fn bind_buses(&self) {
        self.state
            .transfer
            .set_progress_bus(self.state.progress_bus.clone())
            .await;
    }

    pub async fn run(self, port: u16) -> Result<()> {
        self.bind_buses().await;

        let allow_remote_admin = self.state.config.allow_remote_admin;

        let app = Router::new()
            .route("/api/whoami", get(whoami))
            .route("/api/devices", get(list_devices))
            .route("/api/send", post(send_file))
            .route("/api/transfers", get(list_transfers))
            .route("/api/cancel/:file_id", post(cancel_transfer))
            .route("/api/files", get(list_files))
            .route("/api/files/:name", get(download_file))
            .route("/api/incoming", post(incoming_offer).get(list_incoming))
            .route("/api/incoming/:id/accept", post(accept_incoming))
            .route("/api/incoming/:id/reject", post(reject_incoming))
            .route("/api/incoming/batch-decide", post(batch_decide_incoming))
            .route("/api/incoming-resp", post(incoming_resp))
            .route("/api/verify/:file_id", post(verify_file))
            .route("/api/config", get(get_config))
            .route("/api/config/receive-dir", post(set_receive_dir))
            .route("/api/config/device-name", post(set_device_name))
            .route("/ws/progress", get(ws_progress))
            .route("/", get(root_handler))
            // route_layer 只对「已注册的路由」生效，404 / 405 的请求根本不会
            // 走到 middleware——所以不存在"未分级路径被误放行"的口子。
            //（换成 .layer() 则会对所有请求生效，包括 404，反而多一个面。）
            .route_layer(middleware::from_fn(move |req: Request, next: Next| async move {
                access_guard(req, next, allow_remote_admin).await
            }))
            .with_state(self.state);

        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
            .await
            .map_err(|e| crate::CoreError::Gateway(e.to_string()))?;
        if allow_remote_admin {
            warn!(
                "remote admin ON: 局域网内任何设备都可调用本机的管理接口 \
                 （发文件 / 读接收目录 / 改配置）"
            );
        }
        info!(port, "http gateway listening");
        // 必须走 into_make_service_with_connect_info：否则 extensions 里取不到
        // peer 地址，access_guard 会把所有请求都当成远程处理。
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .map_err(|e| crate::CoreError::Gateway(e.to_string()))?;
        Ok(())
    }
}

/// 接口的访问档位
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessPolicy {
    /// 仅本机（回环地址）可调用
    LocalOnly,
    /// 局域网内可调用（对端 daemon / 只读信息）
    Remote,
}

/// 路径模板匹配：`:name` 段匹配任意单段，其余逐段按字面相等。
/// 段数不同直接不匹配（`/api/files` 不会被误配成 `/api/files/:name`）。
pub fn path_matches(pattern: &str, path: &str) -> bool {
    let pat: Vec<&str> = pattern.trim_matches('/').split('/').collect();
    let act: Vec<&str> = path.trim_matches('/').split('/').collect();
    pat.len() == act.len()
        && pat
            .iter()
            .zip(act.iter())
            .all(|(p, a)| p.starts_with(':') || p == a)
}

/// 按 **(Method, Path)** 二元组分级。返回 `None` 表示表中未列出。
///
/// 维度必须是二元组而非只看路径：`/api/incoming` 的 POST（对端发 offer）
/// 与 GET（本机 UI 拉收件箱）分属两档，只按路径分级必然二选一出错。
pub fn classify(method: &Method, path: &str) -> Option<AccessPolicy> {
    use AccessPolicy::{LocalOnly, Remote};

    // 跨机调用点已逐一核实，只有四处，全部落在 Remote 这一档：
    //   transfer.rs:768 /api/incoming（发 offer）
    //   transfer.rs:864 /api/verify/:file_id（补发 sha256）
    //   transfer.rs:317 /api/cancel/:file_id（接收方取消时通知发送方）
    //   gateway.rs:437  /api/incoming-resp（发送方回包）
    // 新增路由时若不在此登记，远程调用一律 403（fail-closed）。
    const TABLE: &[(Method, &str, AccessPolicy)] = &[
        // ---- 允许远程：P2P 协商主流程 + 只读信息 ----
        (Method::POST, "/api/incoming", Remote),
        (Method::POST, "/api/incoming-resp", Remote),
        (Method::POST, "/api/verify/:file_id", Remote),
        (Method::POST, "/api/cancel/:file_id", Remote),
        (Method::GET, "/api/whoami", Remote),
        // ---- 仅本机：会控制本机的操作 ----
        (Method::POST, "/api/send", LocalOnly),
        (Method::GET, "/api/transfers", LocalOnly),
        (Method::GET, "/api/files", LocalOnly),
        (Method::GET, "/api/files/:name", LocalOnly),
        (Method::GET, "/api/incoming", LocalOnly),
        (Method::POST, "/api/incoming/:id/accept", LocalOnly),
        (Method::POST, "/api/incoming/:id/reject", LocalOnly),
        (Method::POST, "/api/incoming/batch-decide", LocalOnly),
        (Method::GET, "/api/config", LocalOnly),
        (Method::POST, "/api/config/receive-dir", LocalOnly),
        (Method::POST, "/api/config/device-name", LocalOnly),
        // 没有远程调用方，收归本机（N4）；遥控场景由 --remote-admin 整体放开
        (Method::GET, "/api/devices", LocalOnly),
        (Method::GET, "/ws/progress", LocalOnly),
        // 首页（当前只是一行提示文本）。显式登记是为了让分级表保持闭合：
        // 表里缺一条，远程访问就会落到"未列入"分支，日志里的措辞会误导排查。
        // 将来若在这里挂上 Web 前端的静态资源，需要改判为 Remote。
        (Method::GET, "/", LocalOnly),
    ];

    TABLE
        .iter()
        .find(|(m, p, _)| m == method && path_matches(p, path))
        .map(|(_, _, policy)| *policy)
}

/// 访问分级 middleware
///
/// 两条放行路径：请求来自回环地址，或开了 `--remote-admin`。
/// 其余按 [`classify`] 判档：Remote 放行，LocalOnly 与"表中未列出"一律 403。
async fn access_guard(req: Request, next: Next, allow_remote_admin: bool) -> Response {
    // 取不到 peer 地址时按「非本机」处理：宁可误拒，也不能让分级静默失效。
    // 若哪天忘了注入 ConnectInfo，症状是本机 UI 全部 403——动静很大，藏不住。
    let is_local = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip().is_loopback())
        .unwrap_or(false);

    if is_local || allow_remote_admin {
        return next.run(req).await;
    }

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    // 把「要不要拒、为什么拒」先算出来，避免在 match 里重复判定、
    // 也避免为了穷尽 Option 而写出不可达分支。
    let deny_reason = match classify(&method, &path) {
        Some(AccessPolicy::Remote) => None,
        Some(AccessPolicy::LocalOnly) => Some("该接口仅限本机访问"),
        None => Some("该接口未列入访问分级表"),
    };
    match deny_reason {
        None => next.run(req).await,
        Some(why) => {
            warn!(%method, %path, "拒绝非本机请求：{why}");
            (
                StatusCode::FORBIDDEN,
                format!("{why}。若需从其他设备控制本机，请用 --remote-admin 启动守护进程。"),
            )
                .into_response()
        }
    }
}

async fn root_handler() -> impl IntoResponse {
    "kitefile gateway is running. See /api/* for endpoints."
}

async fn whoami(State(state): State<AppState>) -> Json<WhoAmI> {
    Json(WhoAmI {
        id: state.discovery.self_id().to_string(),
        name: state.discovery.self_name().to_string(),
        platform: crate::platform::platform_name().to_string(),
        gateway_port: state.config.gateway_port,
        transfer_port: state.config.transfer_port,
    })
}

async fn list_devices(State(state): State<AppState>) -> Json<Vec<crate::discovery::Device>> {
    Json(state.discovery.list_devices())
}

async fn send_file(
    State(state): State<AppState>,
    Json(req): Json<SendRequest>,
) -> std::result::Result<(StatusCode, Json<SendResponse>), (StatusCode, String)> {
    let target_transfer_port = req.target_port.unwrap_or(state.config.transfer_port);
    let target_gateway_port = req.target_gateway_port.unwrap_or(state.config.gateway_port);
    let self_id = state.discovery.self_id().to_string();
    let self_name = state.discovery.self_name().to_string();
    let self_ip = state
        .discovery
        .self_ip()
        .unwrap_or_else(|| "127.0.0.1".into());

    let path = std::path::PathBuf::from(&req.file_path);
    let mut handle = state
        .transfer
        .clone()
        .send_file(
            req.target_ip,
            target_transfer_port,
            target_gateway_port,
            path,
            req.file_name,
            self_id,
            self_name,
            self_ip,
            state.config.gateway_port,
            match (req.batch_id, req.batch_index, req.batch_total) {
                (Some(batch_id), Some(index), Some(total)) => {
                    Some(crate::protocol::SendBatchInfo { batch_id, index, total })
                }
                _ => None,
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let file_id = handle.file_id.clone();
    let transfer = state.transfer.clone();
    tokio::spawn(async move {
        // 进度统一走 engine.publish_progress：写入全量快照 + 广播
        while let Some(p) = handle.next_progress().await {
            transfer.publish_progress(p).await;
        }
    });

    Ok((
        StatusCode::OK,
        Json(SendResponse { file_id }),
    ))
}

#[derive(Serialize)]
struct TransfersList {
    transfers: Vec<TransferProgress>,
}

async fn list_transfers(State(state): State<AppState>) -> Json<TransfersList> {
    // 全量快照由 engine 维护（每次 publish_progress 更新）
    Json(TransfersList {
        transfers: state.transfer.list_transfers(),
    })
}

async fn cancel_transfer(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
) -> impl IntoResponse {
    info!(%file_id, "cancel requested");
    match state.transfer.cancel(&file_id).await {
        true => StatusCode::OK,
        false => StatusCode::NOT_FOUND,
    }
}

/// POST /api/verify/:file_id —— 发送方在全部 chunk ACK 后补发整文件 sha256。
///
/// 接收方据此做最终校验并 finalize（sha256 边传边算，offer 不再携带哈希，
/// 大文件弹窗即时出现）。body: {"sha256": "<hex>" 或 null}
#[derive(Debug, Deserialize)]
struct VerifyRequest {
    sha256: Option<String>,
}

async fn verify_file(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
    Json(req): Json<VerifyRequest>,
) -> impl IntoResponse {
    match state.transfer.apply_final_sha256(&file_id, req.sha256).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_files(State(state): State<AppState>) -> Json<Vec<String>> {
    let dir = state.transfer.storage.receive_dir();
    let mut names = Vec::new();
    if let Ok(mut rd) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            if let Some(name) = entry.file_name().to_str() {
                // 过滤：隐藏标记文件（.kitefile-save-dir 等）与传输中的临时文件
                if name.starts_with('.') || name.ends_with(".part") {
                    continue;
                }
                names.push(name.to_string());
            }
        }
    }
    Json(names)
}

// ============ 配置（接收目录等） ============

#[derive(Debug, Serialize)]
struct AppConfig {
    /// 当前接收目录（设置页展示 / 修改）
    receive_dir: String,
    /// 当前设备显示名（设置页展示 / 修改）
    device_name: String,
}

async fn get_config(State(state): State<AppState>) -> Json<AppConfig> {
    Json(AppConfig {
        receive_dir: state.transfer.storage.receive_dir().to_string_lossy().into_owned(),
        device_name: state.discovery.self_name(),
    })
}

#[derive(Debug, Deserialize)]
struct DeviceNameRequest {
    device_name: String,
}

/// 修改设备显示名：更新内存 + 持久化 + 重新广播 mDNS（对端立刻看到新名字）
async fn set_device_name(
    State(state): State<AppState>,
    Json(req): Json<DeviceNameRequest>,
) -> std::result::Result<Json<AppConfig>, (StatusCode, String)> {
    state
        .discovery
        .set_name(&req.device_name)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    info!(device_name = %req.device_name, "device name updated");
    Ok(Json(AppConfig {
        receive_dir: state.transfer.storage.receive_dir().to_string_lossy().into_owned(),
        device_name: state.discovery.self_name(),
    }))
}

#[derive(Debug, Deserialize)]
struct ReceiveDirRequest {
    receive_dir: String,
}

/// 设置接收目录：尝试创建目录，成功后立即生效（后续新接收的文件存入新目录）
async fn set_receive_dir(
    State(state): State<AppState>,
    Json(req): Json<ReceiveDirRequest>,
) -> std::result::Result<Json<AppConfig>, (StatusCode, String)> {
    let dir = std::path::PathBuf::from(&req.receive_dir);
    state
        .transfer
        .storage
        .set_receive_dir(dir)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    info!(receive_dir = %req.receive_dir, "receive dir updated");
    Ok(Json(AppConfig {
        receive_dir: req.receive_dir,
        device_name: state.discovery.self_name(),
    }))
}

/// GET /api/files/:name → 下载接收目录中的文件（流式）
async fn download_file(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    // 路径安全：拒绝目录分隔符与 ..
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return (StatusCode::BAD_REQUEST, "invalid file name").into_response();
    }
    let path = state.transfer.storage.receive_dir().join(&name);
    match tokio::fs::File::open(&path).await {
        Ok(file) => {
            // 显式 Content-Length：避免流式 body 走 chunked 编码，
            // 简单 HTTP 客户端 / 下载工具可直接读取
            let len = match file.metadata().await {
                Ok(m) => m.len(),
                Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "metadata failed").into_response(),
            };
            let stream = tokio_util::io::ReaderStream::new(file);
            let body = Body::from_stream(stream);
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
            if let Ok(cl) = HeaderValue::from_str(&len.to_string()) {
                headers.insert(header::CONTENT_LENGTH, cl);
            }
            if let Ok(cd) = HeaderValue::from_str(&format!("attachment; filename=\"{}\"", name.replace('"', ""))) {
                headers.insert(header::CONTENT_DISPOSITION, cd);
            }
            (headers, body).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "file not found").into_response(),
    }
}

// ============ 接收方 daemon：处理 incoming offer ============

/// 接收方 daemon 收到发送方 daemon POST /api/incoming
///
/// 立即返回 200，异步等待 UI 决策。
/// 决策完成后，daemon 调用发送方 daemon 的 /api/incoming-resp 回包。
#[derive(Debug, Serialize)]
struct IncomingAck {
    incoming_id: String,
    pending: bool,
    expires_in_seconds: u32,
}

async fn incoming_offer(
    State(state): State<AppState>,
    Json(offer): Json<HttpOffer>,
) -> Json<IncomingAck> {
    let entry = state.transfer.incoming.register(offer);
    info!(incoming_id = %entry.incoming_id, file_id = %entry.file_id, "incoming offer registered");

    let _ = state
        .ws_event_bus
        .send(WsEvent::Incoming { entry: entry.clone() });

    let transfer = state.transfer.clone();
    let ws_bus = state.ws_event_bus.clone();
    let config = state.config.clone();
    let incoming_id = entry.incoming_id.clone();
    let entry_for_spawn = entry.clone();

    tokio::spawn(async move {
        // Some(true)=接受，Some(false)=用户拒绝，None=超时
        let decision = transfer.incoming.wait_decision(&incoming_id).await;
        let accepted = decision.unwrap_or(false);

        let _ = ws_bus.send(WsEvent::IncomingResolved {
            incoming_id: incoming_id.clone(),
            accepted,
        });

        if accepted {
            // 接收槽由 decide_incoming 创建（含 pending 表清理）
            let _ = transfer.decide_incoming(&incoming_id, true).await;
        } else {
            // 拒绝 / 超时：确保 pending 表清理（decide 已移除则为幂等 no-op）
            transfer.incoming.remove(&incoming_id);
        }

        let resp = HttpIncomingResponse {
            file_id: entry_for_spawn.file_id.clone(),
            accepted,
            reason: match decision {
                Some(true) => None,
                Some(false) => Some("rejected by user".into()),
                None => Some(format!(
                    "timeout: not accepted within {}s",
                    crate::transfer::INCOMING_DECISION_TIMEOUT_SECS
                )),
            },
            transfer_port: config.transfer_port,
        };
        let resp_json = serde_json::to_string(&resp).unwrap_or_default();
        if let Err(e) = http_post_json(
            &entry_for_spawn.from_ip,
            entry_for_spawn.from_gateway_port,
            "/api/incoming-resp",
            &resp_json,
        )
        .await
        {
            warn!(error = %e, "回包给发送方 daemon 失败");
        }
    });

    Json(IncomingAck {
        incoming_id: entry.incoming_id,
        pending: true,
        expires_in_seconds: crate::transfer::INCOMING_DECISION_TIMEOUT_SECS as u32,
    })
}

async fn list_incoming(State(state): State<AppState>) -> Json<Vec<IncomingEntry>> {
    Json(state.transfer.incoming.list_pending())
}

async fn accept_incoming(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.transfer.incoming.decide(&id, true).await {
        Some(_) => StatusCode::OK,
        None => StatusCode::NOT_FOUND,
    }
}

/// 批量决策请求体：对一个批次内的所有 pending 条目下同一个决定
#[derive(Debug, serde::Deserialize)]
struct BatchDecideRequest {
    batch_id: String,
    accept: bool,
}

/// 一次接受/拒绝整批（多文件发送时接收端只需确认一次）
async fn batch_decide_incoming(
    State(state): State<AppState>,
    Json(req): Json<BatchDecideRequest>,
) -> impl IntoResponse {
    let done = state
        .transfer
        .incoming
        .decide_batch(&req.batch_id, req.accept)
        .await;
    info!(
        batch_id = %req.batch_id,
        accepted = req.accept,
        count = done.len(),
        "batch incoming decided"
    );
    if done.is_empty() {
        // 批次不存在或已全部超时：和单个决策一样回 404，UI 据此提示"已过期"
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "decided": 0 })),
        );
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "decided": done.len(),
            "incoming_ids": done,
        })),
    )
}

async fn reject_incoming(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.transfer.incoming.decide(&id, false).await {
        Some(_) => StatusCode::OK,
        None => StatusCode::NOT_FOUND,
    }
}

// ============ 发送方 daemon：处理接收方回包 ============

async fn incoming_resp(
    State(state): State<AppState>,
    Json(resp): Json<HttpIncomingResponse>,
) -> impl IntoResponse {
    info!(file_id = %resp.file_id, accepted = resp.accepted, "incoming response received");
    let file_id = resp.file_id.clone();
    state
        .transfer
        .handle_incoming_response(&file_id, resp)
        .await;
    StatusCode::OK
}

// ============ WebSocket：推送 WsEvent（进度 + 传入请求 + 决议完成） ============

async fn ws_progress(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    let progress_rx = state.progress_bus.subscribe();
    let ws_event_rx = state.ws_event_bus.subscribe();
    ws.on_upgrade(move |socket| ws_handler(socket, progress_rx, ws_event_rx))
}

async fn ws_handler(
    mut socket: WebSocket,
    mut progress_rx: tokio::sync::broadcast::Receiver<TransferProgress>,
    mut ws_event_rx: tokio::sync::broadcast::Receiver<WsEvent>,
) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        tokio::select! {
            recv = progress_rx.recv() => match recv {
                Ok(p) => {
                    let ev = WsEvent::Progress { progress: p };
                    let msg = serde_json::to_string(&ev).unwrap_or_default();
                    if socket.send(Message::Text(msg)).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            },
            recv = ws_event_rx.recv() => match recv {
                Ok(ev) => {
                    let msg = serde_json::to_string(&ev).unwrap_or_default();
                    if socket.send(Message::Text(msg)).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            },
        }
    }
}
