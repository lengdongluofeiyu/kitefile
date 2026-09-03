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
//! CORS 已开启，方便 Web 开发期跨端口调用。

use crate::discovery::DiscoveryService;
use crate::protocol::{HttpIncomingResponse, HttpOffer, IncomingEntry, WsEvent};
use crate::transfer::{TransferEngine, TransferProgress};
use crate::{EngineConfig, Result};
use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tower_http::cors::{Any, CorsLayer};
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

    /// gateway 启动前注入 progress + ws_event 广播到 transfer engine
    pub async fn bind_buses(&self) {
        self.state
            .transfer
            .set_progress_bus(self.state.progress_bus.clone())
            .await;
        self.state
            .transfer
            .set_ws_event_bus(self.state.ws_event_bus.clone())
            .await;
    }

    pub async fn run(self, port: u16) -> Result<()> {
        self.bind_buses().await;

        let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);

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
            .route("/api/incoming-resp", post(incoming_resp))
            .route("/api/verify/:file_id", post(verify_file))
            .route("/api/config", get(get_config))
            .route("/api/config/receive-dir", post(set_receive_dir))
            .route("/api/config/device-name", post(set_device_name))
            .route("/ws/progress", get(ws_progress))
            .route("/", get(root_handler))
            .layer(cors)
            .with_state(self.state);

        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
            .await
            .map_err(|e| crate::CoreError::Gateway(e.to_string()))?;
        info!(port, "http gateway listening");
        axum::serve(listener, app)
            .await
            .map_err(|e| crate::CoreError::Gateway(e.to_string()))?;
        Ok(())
    }
}

async fn root_handler() -> impl IntoResponse {
    "ftcore gateway is running. See /api/* for endpoints."
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
                // 过滤：隐藏标记文件（.ftcore-save-dir 等）与传输中的临时文件
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
        let accepted = transfer
            .incoming
            .wait_decision(&incoming_id)
            .await
            .unwrap_or(false);

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
            reason: if accepted {
                None
            } else {
                Some("rejected by user or timeout".into())
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
        expires_in_seconds: 60,
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

/// 纯 TCP 实现 HTTP POST JSON
///
/// 不引入 reqwest/hyper，复用 tokio TcpStream
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
