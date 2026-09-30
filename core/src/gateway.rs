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
use crate::protocol::{HttpIncomingResponse, HttpOffer, IncomingEntry, WsEvent};
use crate::transfer::{TransferEngine, TransferProgress};
use crate::{EngineConfig, Result};
use axum::{
    body::Body,
    extract::{
        connect_info::ConnectInfo,
        ws::{Message, WebSocket, WebSocketUpgrade},
        Extension, Path, Request, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use tower::Service;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct AppState {
    pub discovery: Arc<DiscoveryService>,
    pub transfer: Arc<TransferEngine>,
    pub config: Arc<EngineConfig>,
    pub progress_bus: Arc<tokio::sync::broadcast::Sender<TransferProgress>>,
    pub ws_event_bus: Arc<tokio::sync::broadcast::Sender<WsEvent>>,
    /// 已配对设备表（P3 鉴权 / 配对去重）
    pub peers: Arc<crate::pairing::PeersStore>,
    /// 本机 TLS 身份；`run()` 启动时预填，handler 兜底惰性加载
    pub identity: std::sync::OnceLock<Arc<crate::tls::NodeIdentity>>,
}

impl AppState {
    /// 本机身份（指纹用于确认码 / 自证）
    fn identity(&self) -> Arc<crate::tls::NodeIdentity> {
        self.identity
            .get_or_init(|| {
                crate::tls::NodeIdentity::load_or_create(&self.config.receive_dir)
                    .expect("TLS identity 加载失败")
            })
            .clone()
    }
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
    /// 本机是否处于配对模式（P2 修复：对端经 60s 探活/切换触发的 whoami
    /// 主动拉取同步此标志，不再单靠 mDNS TXT 公告一条路）
    pub pairing_enabled: bool,
}

/// POST /api/pair/mode 请求体（阶段 5 P2）
#[derive(Debug, Deserialize)]
pub struct PairModeRequest {
    pub enabled: bool,
}

/// 配对模式状态
#[derive(Debug, Serialize)]
pub struct PairModeStatus {
    pub enabled: bool,
    pub seconds_left: u32,
    pub ttl_seconds: u32,
}

fn pair_mode_status(state: &AppState) -> PairModeStatus {
    let (enabled, seconds_left) = state.discovery.pairing_status();
    PairModeStatus {
        enabled,
        seconds_left,
        ttl_seconds: crate::pairing::PAIRING_TTL.as_secs() as u32,
    }
}

async fn get_pair_mode(State(state): State<AppState>) -> Json<PairModeStatus> {
    Json(pair_mode_status(&state))
}

async fn set_pair_mode(
    State(state): State<AppState>,
    Json(req): Json<PairModeRequest>,
) -> std::result::Result<Json<PairModeStatus>, (StatusCode, String)> {
    state
        .discovery
        .clone()
        .set_pairing_mode(req.enabled)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(pair_mode_status(&state)))
}

/// 立即刷新已知设备的 pair 标志（whoami 通道）。**不改变**本机配对模式
/// 状态与倒计时——供添加设备页在搜索期间定期调用，兜住「对端比本机晚
/// 开启配对模式」的同步窗口。
async fn refresh_pair_flags(State(state): State<AppState>) -> Json<serde_json::Value> {
    state.discovery.refresh_pair_flags();
    Json(serde_json::json!({ "ok": true }))
}

// ---------------------------------------------------------------------------
// 配对协议（P3，设计 §6）
//
// 方向：A（发起方）→ B（接收方）
//   1. A: POST /api/pair/start（本机 UI 入口）→ TLS POST B /api/pair/hello
//   2. B: 配对模式门 + 限频 + 单 pending → WS 推 PairRequest（确认码弹窗）
//   3. B: POST /api/pair/decide {accept} → TLS POST A /api/pair/confirm
//   4. A: 校验 session + B 的客户端证书 → 落库 peers[B] → 200
//   5. B: 收到 200 → 落库 peers[A]（两阶段写，200 丢失可重发起自愈）
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct PairHelloRequest {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    /// 发起方（A）的 LAN TLS 端口：B 确认后把 confirm 推回这里
    pub gateway_port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PairHelloResponse {
    pub pairing_session: String,
}

#[derive(Debug, Deserialize)]
pub struct PairStartRequest {
    /// 对端（B）的 device_id：peers 表索引键（5d 手动添加时 UI 自行传入）
    pub device_id: String,
    /// 对端展示名（落库 name_hint）
    pub name: String,
    pub platform: String,
    pub ip: String,
    /// 对端 LAN TLS 端口（设备表 gateway_port）
    pub gateway_port: u16,
}

#[derive(Debug, Serialize)]
pub struct PairStartResponse {
    pub pairing_session: String,
    /// 本机侧确认码（对方屏上应显示同一串）
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct PairDecideRequest {
    pub session: String,
    pub accept: bool,
}

/// POST /api/peers/:device_id/rename：仅改本机显示名
#[derive(Debug, Deserialize)]
pub struct PeerRenameRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct PairDecideResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PairPendingResponse {
    pub pending: Option<PairPendingView>,
}

#[derive(Debug, Serialize)]
pub struct PairPendingView {
    pub session: String,
    pub name: String,
    pub ip: String,
    pub platform: String,
    pub code: String,
    /// 发起方是否已提交正确确认码；false 时接收方 UI 必须禁用「确认配对」
    pub code_verified: bool,
}

/// POST /api/pair/submit-code：发起方输入对方屏幕上的确认码后提交
#[derive(Debug, Deserialize)]
pub struct PairSubmitCodeRequest {
    pub session: String,
    pub code: String,
}

/// POST /api/pair/cancel：发起方中止本机会话（并尽力通知对端清理 pending）
#[derive(Debug, Deserialize)]
pub struct PairCancelRequest {
    pub session: String,
}

#[derive(Debug, Serialize)]
pub struct PeerView {
    pub device_id: String,
    pub name_hint: String,
    pub platform: String,
    pub fp_sha256: String,
    pub paired_at: u64,
    /// 设备表里还活着（mDNS 发现 + 探活未移除）
    pub online: bool,
}

/// B 侧：接收配对 hello（Remote；鉴权层对本端点放行，合法性在此校验）。
async fn pair_hello(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    peer_cert: Extension<crate::tls::PeerFingerprint>,
    Json(req): Json<PairHelloRequest>,
) -> std::result::Result<Json<PairHelloResponse>, (StatusCode, String)> {
    let (mode_on, _) = state.discovery.pairing_status();
    if !mode_on {
        return Err((StatusCode::FORBIDDEN, "对方未开启配对模式".into()));
    }
    let pairing = state.discovery.pairing();
    let ip = peer.ip().to_string();
    if !pairing.allow_hello(&ip) {
        return Err((StatusCode::TOO_MANY_REQUESTS, "请求过于频繁，请稍后再试".into()));
    }
    // mTLS：客户端证书必须在（TLS 层已验私钥持有，应用层不用再签名）
    let Extension(fp_opt) = peer_cert;
    let fp = fp_opt
        .0
        .ok_or((StatusCode::UNAUTHORIZED, "配对需要出示客户端证书".into()))?;
    // 已配对：配对模式开启时允许**重新配对**（应对单方删除设备后重建信任；
    // 确认码 + 用户点确认仍是门禁）。模式关闭时仍拒绝覆盖（Q3）。
    if state.peers.contains_fp(&fp) || state.peers.get(&req.device_id).is_some() {
        info!(
            %ip,
            name = %req.name,
            "re-pair requested for already-paired device (pairing mode on)"
        );
    }
    let session = uuid::Uuid::new_v4().simple().to_string();
    // 确认码：指纹 + 会话 id 联合哈希。session 每次 hello 都是新 UUID，
    // 同一对设备不同配对轮次必须出不同码（避免「永远 409072」）。
    let code = crate::pairing::confirm_code(&fp, state.identity().fp(), &session);
    pairing
        .begin_in_pending(crate::pairing::InPending {
            session: session.clone(),
            peer_fp: fp.clone(),
            peer_device_id: req.device_id.clone(),
            peer_name: req.name.clone(),
            peer_platform: req.platform.clone(),
            peer_ip: ip.clone(),
            peer_gateway_port: req.gateway_port,
            created: tokio::time::Instant::now(),
            code: code.clone(),
            // 门禁：发起方尚未输入确认码前，B 的「确认配对」必须禁用
            code_verified: false,
        })
        .map_err(|_| (StatusCode::CONFLICT, "已有配对请求待处理".into()))?;

    let _ = state.ws_event_bus.send(WsEvent::PairRequest {
        session: session.clone(),
        name: req.name.clone(),
        ip: ip.clone(),
        platform: req.platform.clone(),
        code: code.clone(),
        code_verified: false,
    });
    info!(%ip, name = %req.name, %session, code = %code, "pairing hello accepted, awaiting confirm");
    Ok(Json(PairHelloResponse {
        pairing_session: session,
    }))
}

/// A 侧：本机 UI 发起配对——对 B 发 hello，拿到会话与**自己屏上的**确认码。
async fn pair_start(
    State(state): State<AppState>,
    Json(req): Json<PairStartRequest>,
) -> std::result::Result<Json<PairStartResponse>, (StatusCode, String)> {
    let hello_body = serde_json::json!({
        "device_id": state.discovery.self_id(),
        "name": state.discovery.self_name(),
        "platform": crate::platform::platform_name(),
        "gateway_port": state.config.lan_tls_port,
    })
    .to_string();
    let (resp_body, server_fp) = crate::httpc::http_post_json_tls_peer_fp(
        &req.ip,
        req.gateway_port,
        "/api/pair/hello",
        &hello_body,
        &state.config.receive_dir,
    )
    .await
    .map_err(|e| {
        let hint = if e.kind() == std::io::ErrorKind::InvalidData {
            format!("对方拒绝了配对请求：{e}")
        } else {
            format!("连接对方失败：{e}")
        };
        (StatusCode::BAD_GATEWAY, hint)
    })?;
    let hello: PairHelloResponse = serde_json::from_str(&resp_body)
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("对端响应异常: {e}")))?;
    // 确认码必须用**握手亲眼所见**的对端证书算——不能信任何响应体字段
    let fp_b = server_fp.ok_or((
        StatusCode::BAD_GATEWAY,
        "对方未出示服务端证书".into(),
    ))?;
    let code = crate::pairing::confirm_code(
        state.identity().fp(),
        &fp_b,
        &hello.pairing_session,
    );

    state.discovery.pairing().set_out_pending(
        crate::pairing::OutPending {
            session: hello.pairing_session.clone(),
            peer_fp: fp_b,
            peer_device_id: req.device_id.clone(),
            peer_name: req.name.clone(),
            peer_platform: req.platform.clone(),
            created: tokio::time::Instant::now(),
            peer_gateway_port: req.gateway_port,
            peer_ip: req.ip.clone(),
        },
    );
    Ok(Json(PairStartResponse {
        pairing_session: hello.pairing_session,
        code,
    }))
}

/// B 侧：本机用户点了「确认 / 拒绝」。确认 → 向 A 推 confirm，成功后落库。
async fn pair_decide(
    State(state): State<AppState>,
    Json(req): Json<PairDecideRequest>,
) -> std::result::Result<Json<PairDecideResponse>, (StatusCode, String)> {
    let pairing = state.discovery.pairing();
    let Some(pending) = pairing.in_pending() else {
        return Err((
            StatusCode::NOT_FOUND,
            "配对会话不存在或已过期".into(),
        ));
    };
    if pending.session != req.session {
        return Err((StatusCode::NOT_FOUND, "配对会话不匹配".into()));
    }

    if !req.accept {
        pairing.clear_in_pending();
        return Ok(Json(PairDecideResponse {
            ok: true,
            error: None,
        }));
    }
    // 门禁：发起方必须已提交正确确认码，接收方才能点「确认配对」
    if !pending.code_verified {
        return Err((
            StatusCode::CONFLICT,
            "对方尚未输入确认码，请等待对方在本机输入正确后再确认".into(),
        ));
    }

    // 两阶段写：先让 A 落库（200），B 收到 200 后才落库——
    // 200 丢失时只有 A 单边已配对，重新发起即可自愈（设计 §6）
    let confirm_body = serde_json::json!({ "pairing_session": pending.session }).to_string();
    crate::httpc::http_post_json_tls(
        &pending.peer_ip,
        pending.peer_gateway_port,
        "/api/pair/confirm",
        &confirm_body,
        &state.config.receive_dir,
    )
    .await
    .map_err(|e| {
        pairing.clear_in_pending();
        (
            StatusCode::BAD_GATEWAY,
            format!("向对方确认失败（对方会话可能已过期）：{e}"),
        )
    })?;
    // 确认成功后再取走 pending 落库
    pairing.take_in_pending(&req.session);
    state.peers.insert(
        &pending.peer_device_id,
        crate::pairing::PeerRecord {
            name_hint: pending.peer_name,
            platform: pending.peer_platform,
            fp_sha256: pending.peer_fp,
            paired_at: now_unix(),
        },
    );
    info!(peer = %pending.peer_device_id, "pairing completed (B side)");
    Ok(Json(PairDecideResponse {
        ok: true,
        error: None,
    }))
}

/// A 侧：接收 B 的 confirm（Remote）。校验会话 + **对方客户端证书与
/// hello 时同一把**，通过则 A 先落库（两阶段写第一步）。
async fn pair_confirm(
    State(state): State<AppState>,
    peer_cert: Extension<crate::tls::PeerFingerprint>,
    Json(req): Json<PairHelloResponse>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, String)> {
    let pairing = state.discovery.pairing();
    let pending = pairing.check_out_session(&req.pairing_session).ok_or((
        StatusCode::FORBIDDEN,
        "无此配对会话或已过期（请重新发起配对）".into(),
    ))?;
    let Extension(fp_opt) = peer_cert;
    let fp = fp_opt
        .0
        .ok_or((StatusCode::UNAUTHORIZED, "需要客户端证书".into()))?;
    if fp != pending.peer_fp {
        return Err((
            StatusCode::FORBIDDEN,
            "证书与配对会话不一致，已拒绝".into(),
        ));
    }
    state.peers.insert(
        &pending.peer_device_id,
        crate::pairing::PeerRecord {
            name_hint: pending.peer_name.clone(),
            platform: pending.peer_platform.clone(),
            fp_sha256: fp,
            paired_at: now_unix(),
        },
    );
    pairing.clear_out_pending();
    info!(peer = %pending.peer_device_id, "pairing completed (A side)");
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// 本机 UI 轮询：当前待确认的配对请求（含确认码与门禁状态）
async fn get_pair_pending(State(state): State<AppState>) -> Json<PairPendingResponse> {
    let Some(p) = state.discovery.pairing().in_pending() else {
        return Json(PairPendingResponse { pending: None });
    };
    Json(PairPendingResponse {
        pending: Some(PairPendingView {
            session: p.session,
            name: p.peer_name,
            ip: p.peer_ip,
            platform: p.peer_platform,
            // hello 时已写入 pending 的码，展示与校验同一份
            code: p.code,
            code_verified: p.code_verified,
        }),
    })
}

/// 发起方提交确认码（Remote，免配对鉴权；合法性由 session + 码比对保证）
async fn pair_submit_code(
    State(state): State<AppState>,
    Json(req): Json<PairSubmitCodeRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, String)> {
    let pairing = state.discovery.pairing();
    let Some(p) = pairing.in_pending() else {
        return Err((StatusCode::NOT_FOUND, "配对会话不存在或已过期".into()));
    };
    if p.session != req.session {
        return Err((StatusCode::NOT_FOUND, "配对会话不匹配".into()));
    }
    let got = req.code.trim();
    if got != p.code {
        return Err((
            StatusCode::BAD_REQUEST,
            "确认码不一致：请核对对方屏幕上的数字".into(),
        ));
    }
    if !pairing.mark_code_verified(&req.session) {
        return Err((StatusCode::NOT_FOUND, "配对会话不存在或已过期".into()));
    }
    // 立即推给接收方 UI：不能只靠 1s 轮询，否则弹窗会长时间停在「等待对方输入」
    let _ = state.ws_event_bus.send(WsEvent::PairCodeVerified {
        session: req.session.clone(),
    });
    info!(session = %req.session, "pairing code submitted and verified");
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// 发起方中止本机会话；尽力通知对端清理 pending
async fn pair_cancel(
    State(state): State<AppState>,
    Json(req): Json<PairCancelRequest>,
) -> Json<serde_json::Value> {
    let pairing = state.discovery.pairing();
    let Some(p) = pairing.check_out_session(&req.session) else {
        return Json(serde_json::json!({ "ok": true, "had_pending": false }));
    };
    pairing.clear_out_pending();
    // 通知 B 清理 in_pending（失败靠 60s TTL 兜底）
    let receive_dir = state.config.receive_dir.clone();
    let ip = {
        let map = state.discovery.devices_handle();
        let guard = map.read();
        guard
            .values()
            .find(|d| d.id == p.peer_device_id)
            .map(|d| d.ip.clone())
    };
    if let Some(ip) = ip {
        let peer_port = p.peer_gateway_port;
        let session = req.session.clone();
        tokio::spawn(async move {
            let body = serde_json::json!({ "session": session }).to_string();
            if let Err(e) = crate::httpc::http_post_json_tls(
                &ip,
                peer_port,
                "/api/pair/cancel-in",
                &body,
                &receive_dir,
            )
            .await
            {
                tracing::warn!(error = %e, "notify peer cancel failed");
            }
        });
    }
    Json(serde_json::json!({ "ok": true, "had_pending": true }))
}

/// 发起方中止后，接收方清理 pending（Remote，免配对鉴权）
async fn pair_cancel_in(
    State(state): State<AppState>,
    Json(req): Json<PairCancelRequest>,
) -> Json<serde_json::Value> {
    let pairing = state.discovery.pairing();
    let cleared = pairing
        .in_pending()
        .map(|p| p.session == req.session)
        .unwrap_or(false);
    if cleared {
        pairing.clear_in_pending();
    }
    Json(serde_json::json!({ "ok": true }))
}

/// 发起方轮询：本机 out_pending 是否仍在（会话过期检测）
async fn get_out_pending(State(state): State<AppState>) -> Json<serde_json::Value> {
    let session = state.discovery.pairing().out_pending_active();
    Json(serde_json::json!({ "active": session.is_some(), "session": session }))
}

/// 发起方本机校验确认码并转发给对端（Dart 只打本机回环口）
#[derive(Debug, Deserialize)]
pub struct PairVerifyForwardRequest {
    pub session: String,
    pub code: String,
}

async fn pair_verify_forward(
    State(state): State<AppState>,
    Json(req): Json<PairVerifyForwardRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, String)> {
    let pairing = state.discovery.pairing();
    let Some(p) = pairing.check_out_session(&req.session) else {
        return Err((
            StatusCode::FORBIDDEN,
            "无此配对会话或已过期（请重新发起配对）".into(),
        ));
    };
    let expect = crate::pairing::confirm_code(state.identity().fp(), &p.peer_fp, &p.session);
    let got = req.code.trim();
    if got != expect {
        return Err((
            StatusCode::BAD_REQUEST,
            "确认码不一致：请核对对方屏幕上的数字，可能有人在中间拦截".into(),
        ));
    }
    let body = serde_json::json!({ "session": req.session, "code": got }).to_string();
    crate::httpc::http_post_json_tls(
        &p.peer_ip,
        p.peer_gateway_port,
        "/api/pair/submit-code",
        &body,
        &state.config.receive_dir,
    )
    .await
    .map_err(|e| {
        let hint = if e.kind() == std::io::ErrorKind::InvalidData {
            format!("对方拒绝了确认码提交：{e}")
        } else {
            format!("提交确认码失败：{e}")
        };
        (StatusCode::BAD_GATEWAY, hint)
    })?;
    info!(session = %req.session, "pairing code forwarded to peer");
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// 已配对设备列表（含在线状态，拼设备表）
async fn list_peers(State(state): State<AppState>) -> Json<Vec<PeerView>> {
    let devices = state.discovery.list_devices();
    let rows = state
        .peers
        .list()
        .into_iter()
        .map(|(device_id, rec)| {
            let online = devices.iter().any(|d| d.id == device_id);
            PeerView {
                device_id,
                name_hint: rec.name_hint,
                platform: rec.platform,
                fp_sha256: rec.fp_sha256,
                paired_at: rec.paired_at,
                online,
            }
        })
        .collect();
    Json(rows)
}

/// 解除配对（撤销：该证书的双向信任立即失效）
async fn delete_peer(
    State(state): State<AppState>,
    axum::extract::Path(device_id): axum::extract::Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, String)> {
    if state.peers.remove(&device_id) {
        info!(%device_id, "peer removed (revoked)");
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err((StatusCode::NOT_FOUND, "该设备不在配对列表中".into()))
    }
}

/// 仅改本机显示名（不通知对端、不改指纹/信任）。
async fn rename_peer(
    State(state): State<AppState>,
    axum::extract::Path(device_id): axum::extract::Path<String>,
    Json(req): Json<PeerRenameRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, String)> {
    let name = req.name.trim();
    if name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "名称不能为空".into()));
    }
    if state.peers.rename(&device_id, name) {
        info!(%device_id, name, "peer renamed (local only)");
        Ok(Json(serde_json::json!({ "ok": true, "name_hint": name })))
    } else {
        Err((StatusCode::NOT_FOUND, "该设备不在配对列表中".into()))
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        let peers =
            crate::pairing::PeersStore::load(&config.receive_dir.join("peers.json"));
        Self {
            state: AppState {
                discovery,
                transfer,
                config,
                progress_bus: Arc::new(progress_bus),
                ws_event_bus: Arc::new(ws_event_bus),
                peers,
                identity: std::sync::OnceLock::new(),
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

    /// 启动双监听（阶段 5 P1）：
    ///
    /// - `127.0.0.1:gateway_port` 明文——本机 UI / WS，语义与旧版完全一致
    /// - `0.0.0.0:lan_tls_port` HTTPS——局域网跨机控制面；加载本机证书，
    ///   请求客户端证书但暂不鉴权（P1 承诺：不改任何鉴权行为，只注入
    ///   [`crate::tls::PeerFingerprint`] 供 P3 的 auth_guard 读取）
    ///
    /// whoami 按监听侧广告各自端口：本机扫回环口拿到回环端口，对端探 TLS 口
    /// 拿到 TLS 端口——同一字段按连接来源自洽，Dart 侧 `daemonPort = p`
    /// （记录的是"哪个候选口应答"）不受影响。
    pub async fn run(self) -> Result<()> {
        self.bind_buses().await;
        let config = self.state.config.clone();

        // 身份先加载（tls 层写失败已降级为内存身份，此处失败≈不可能发生；
        // handler 的 identity() 兜底也依赖它，保留硬失败避免半初始化）
        let identity = crate::tls::NodeIdentity::load_or_create(&config.receive_dir)?;
        let server_cfg = crate::tls::server_config(&identity)?;
        // 预填 AppState：handler 侧 identity() 免二次读盘
        let _ = self.state.identity.set(identity.clone());

        // 回环口：优先配置值，被占则退避其它候选——Dart 的 whoami 扫描覆盖
        // 全部候选，退避后依然能发现（真机反馈过「守护进程未就绪」，
        // 每一次绑定失败都要留余地，而不是整体拒绝启动）
        let lb_listener = {
            let mut ports = vec![config.gateway_port];
            ports.extend(crate::GATEWAY_PORT_CANDIDATES.iter().copied());
            ports.dedup();
            bind_first("127.0.0.1", &ports).await.map_err(|e| {
                crate::CoreError::Gateway(format!("回环口全部候选绑定失败: {e}"))
            })?
        };
        let lb_port = lb_listener
            .local_addr()
            .map_err(|e| crate::CoreError::Gateway(e.to_string()))?
            .port();

        let allow_remote_admin = config.allow_remote_admin;
        if allow_remote_admin {
            warn!(
                "remote admin ON: 局域网内任何设备都可调用本机的管理接口 \
                 （发文件 / 读接收目录 / 改配置）"
            );
        }

        // LAN TLS 侧：失败**只降级、不拖死网关**——本机 UI 必须活着，
        // 否则手机端表现成「内嵌守护进程未就绪」，降级原因只能去日志里找
        //（阶段 5 真机反馈：daemon 整体起不来却无从诊断）。
        let mut lan_bound: Option<u16> = None;
        match tokio::net::TcpListener::bind(("0.0.0.0", config.lan_tls_port)).await {
            Ok(lan_listener) => {
                let lan_app = build_app(self.state.clone(), config.lan_tls_port);
                tokio::spawn(serve_tls_loop(lan_listener, server_cfg, lan_app));
                lan_bound = Some(config.lan_tls_port);
                ensure_windows_lan_firewall(config.lan_tls_port);
            }
            Err(e) => {
                error!(
                    error = %e,
                    port = config.lan_tls_port,
                    "LAN TLS 口绑定失败，降级为仅本机回环可用（跨机传输暂不可用）"
                );
            }
        }

        let lb_app = build_app(self.state.clone(), lb_port);
        info!(
            gateway_port = lb_port,
            lan_tls_port = lan_bound.unwrap_or(0),
            identity_fp = %identity.fp(),
            "http gateway listening"
        );
        // 回环口走原生 serve：ConnectInfo 由 into_make_service_with_connect_info 注入
        axum::serve(
            lb_listener,
            lb_app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .map_err(|e| crate::CoreError::Gateway(e.to_string()))?;
        Ok(())
    }
}

/// 按优先级依次尝试绑定，返回首个成功者（全失败返回最后一个错误）。
async fn bind_first(
    ip: &str,
    ports: &[u16],
) -> std::result::Result<tokio::net::TcpListener, std::io::Error> {
    let mut last: Option<std::io::Error> = None;
    for p in ports {
        match tokio::net::TcpListener::bind((ip, *p)).await {
            Ok(l) => return Ok(l),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "no port candidates")
    }))
}

/// Windows 防火墙：局域网对端要能连上 LAN TLS 口，否则配对/传输全挂
///（真实反馈：mDNS 可能仍通，但 whoami/pair/hello 全部超时）。
/// 无管理员权限时 netsh 会失败——只记 warn，不阻断本机回环 UI。
fn ensure_windows_lan_firewall(port: u16) {
    #[cfg(windows)]
    {
        use std::process::Command;
        let name = format!("KiteFile-LAN-TLS-{port}");
        let exists = Command::new("netsh")
            .args([
                "advfirewall",
                "firewall",
                "show",
                "rule",
                &format!("name={name}"),
            ])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if exists {
            return;
        }
        let status = Command::new("netsh")
            .args([
                "advfirewall",
                "firewall",
                "add",
                "rule",
                &format!("name={name}"),
                "dir=in",
                "action=allow",
                "protocol=TCP",
                &format!("localport={port}"),
            ])
            .status();
        match status {
            Ok(s) if s.success() => info!(port, "windows firewall rule ensured"),
            Ok(s) => warn!(
                port,
                code = ?s.code(),
                "windows firewall rule add failed（需管理员权限时请手动放行）"
            ),
            Err(e) => warn!(port, error = %e, "windows firewall netsh spawn failed"),
        }
    }
    #[cfg(not(windows))]
    {
        let _ = port;
    }
}

/// 构建路由表。`advertised_gateway_port` 是**本 listener 应答给外界的
/// gateway 端口**（回环口报回环端口、TLS 口报 TLS 端口），经 Extension
/// 注入，只有 whoami 用它。
fn build_app(state: AppState, advertised_gateway_port: u16) -> Router {
    let allow_remote_admin = state.config.allow_remote_admin;
    Router::new()
        .route("/api/whoami", get(whoami))
        .route("/api/devices", get(list_devices))
        .route("/api/send", post(send_file))
        .route("/api/transfers", get(list_transfers))
        .route("/api/cancel/:file_id", post(cancel_transfer))
        // 续传（A3.2）：本机 UI 入口 / 对端 daemon 互相触发
        .route("/api/transfers/:file_id/resume", post(resume_transfer))
        .route("/api/peer-resume/:file_id", post(peer_resume))
        .route("/api/peer-resumed/:file_id", post(peer_resumed))
        .route("/api/files", get(list_files))
        .route("/api/files/:name", get(download_file))
        .route("/api/incoming", post(incoming_offer).get(list_incoming))
        .route("/api/incoming/:id/accept", post(accept_incoming))
        .route("/api/incoming/:id/reject", post(reject_incoming))
        .route("/api/incoming/batch-decide", post(batch_decide_incoming))
        .route("/api/incoming-resp", post(incoming_resp))
        .route("/api/verify/:file_id", post(verify_file))
        .route("/api/pair/mode", get(get_pair_mode).post(set_pair_mode))
        // 即时刷新已知设备的 pair 标志（不动本机倒计时）——添加设备页搜索期调用
        .route("/api/pair/refresh", post(refresh_pair_flags))
        // 配对协议（P3）：hello/confirm 走局域网 TLS（Remote），
        // start/decide/pending 是本机 UI 入口（LocalOnly）
        .route("/api/pair/hello", post(pair_hello))
        .route("/api/pair/confirm", post(pair_confirm))
        .route("/api/pair/start", post(pair_start))
        .route("/api/pair/decide", post(pair_decide))
        .route("/api/pair/pending", get(get_pair_pending))
        .route("/api/pair/out-pending", get(get_out_pending))
        .route("/api/pair/submit-code", post(pair_submit_code))
        .route("/api/pair/verify-forward", post(pair_verify_forward))
        .route("/api/pair/cancel", post(pair_cancel))
        .route("/api/pair/cancel-in", post(pair_cancel_in))
        .route("/api/peers", get(list_peers))
        .route(
            "/api/peers/:device_id",
            axum::routing::delete(delete_peer),
        )
        .route("/api/peers/:device_id/rename", post(rename_peer))
        .route("/api/config", get(get_config))
        .route("/api/config/receive-dir", post(set_receive_dir))
        .route("/api/config/device-name", post(set_device_name))
        .route("/ws/progress", get(ws_progress))
        .route("/", get(root_handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_guard,
        ))
        // route_layer 只对「已注册的路由」生效，404 / 405 的请求根本不会
        // 走到 middleware——所以不存在"未分级路径被误放行"的口子。
        //（换成 .layer() 则会对所有请求生效，包括 404，反而多一个面。）
        // 注意：**后加的 route_layer 在外层**——先分级（access）再鉴权（auth），
        // 远程访问 LocalOnly 时先得到 403（分级语义），而不是 401。
        .route_layer(middleware::from_fn(move |req: Request, next: Next| async move {
            access_guard(req, next, allow_remote_admin).await
        }))
        .layer(axum::Extension(advertised_gateway_port))
        .with_state(state)
}

/// TLS accept 循环：逐连接握手 → 注入 `ConnectInfo` + `PeerFingerprint`
/// → 用 hyper 的 HTTP/1 连接执行同一个 axum Router。
///
/// （axum 0.7 的 `serve()` 只吃 `tokio::net::TcpListener`，不接受自定义
/// 流类型，所以 TLS 侧必须自己接。）
async fn serve_tls_loop(
    listener: tokio::net::TcpListener,
    server_cfg: Arc<rustls::ServerConfig>,
    app: Router,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(server_cfg);
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "lan tls accept failed");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            // 握手超时：防慢速客户端长期占坑
            let tls =
                match tokio::time::timeout(std::time::Duration::from_secs(10), acceptor.accept(tcp))
                    .await
                {
                    Ok(Ok(s)) => s,
                    _ => return,
                };
            // 对端证书指纹 → 每请求扩展。P1 只注入不拦截；P3 auth_guard 查表。
            let peer_fp = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|c| c.first())
                .map(|c| crate::tls::fingerprint(c.as_ref()));
            let io = hyper_util::rt::TokioIo::new(tls);
            // 注意用 hyper 的 service_fn（hyper 1.x 有自己的 Service trait，
            // tower 的 ServiceFn 不满足 serve_connection 的约束）
            let svc = hyper::service::service_fn(
                move |mut req: hyper::Request<hyper::body::Incoming>| {
                    let mut app = app.clone();
                    req.extensions_mut().insert(ConnectInfo(peer));
                    req.extensions_mut()
                        .insert(crate::tls::PeerFingerprint(peer_fp.clone()));
                    async move {
                        // Router::call 返回 Result<Response, Infallible>——
                        // hyper 的 service 要求未来输出恰为 Result<Response, E>，
                        // 这里剥掉内层 Result（Infallible 不可能失败）
                        let resp = match app.call(req).await {
                            Ok(r) => r,
                            Err(never) => match never {},
                        };
                        Ok::<_, std::convert::Infallible>(resp)
                    }
                },
            );
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await;
        });
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
        // ---- 允许远程：P2P 协商主流程 + 只读信息 + 配对协议 ----
        (Method::POST, "/api/incoming", Remote),
        (Method::POST, "/api/incoming-resp", Remote),
        (Method::POST, "/api/verify/:file_id", Remote),
        (Method::POST, "/api/cancel/:file_id", Remote),
        // 对端触发的续传信号（A3.2）：接收方请求发送方继续 / 发送方通知接收方恢复
        (Method::POST, "/api/peer-resume/:file_id", Remote),
        (Method::POST, "/api/peer-resumed/:file_id", Remote),
        (Method::GET, "/api/whoami", Remote),
        // 配对协议（P3）：鉴权在 auth_guard + handler 会话层（不要求已配对）
        (Method::POST, "/api/pair/hello", Remote),
        (Method::POST, "/api/pair/confirm", Remote),
        // ---- 仅本机：会控制本机的操作 ----
        (Method::POST, "/api/pair/mode", LocalOnly),
        (Method::GET, "/api/pair/mode", LocalOnly),
        (Method::POST, "/api/pair/refresh", LocalOnly),
        (Method::POST, "/api/pair/start", LocalOnly),
        (Method::POST, "/api/pair/decide", LocalOnly),
        (Method::GET, "/api/pair/pending", LocalOnly),
        (Method::GET, "/api/pair/out-pending", LocalOnly),
        (Method::POST, "/api/pair/submit-code", Remote),
        (Method::POST, "/api/pair/verify-forward", LocalOnly),
        (Method::POST, "/api/pair/cancel", LocalOnly),
        (Method::POST, "/api/pair/cancel-in", Remote),
        (Method::GET, "/api/peers", LocalOnly),
        (Method::POST, "/api/peers/:device_id/rename", LocalOnly),
        (Method::DELETE, "/api/peers/:device_id", LocalOnly),
        (Method::POST, "/api/send", LocalOnly),
        (Method::GET, "/api/transfers", LocalOnly),
        (Method::POST, "/api/transfers/:file_id/resume", LocalOnly),
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

/// 请求是否来自回环地址（取不到 ConnectInfo 按非本机处理，fail-closed）
fn req_is_local(req: &Request) -> bool {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip().is_loopback())
        .unwrap_or(false)
}

/// 鉴权 middleware（P3，mTLS 定案）：跑在分级**之内**、handler 之外。
///
/// - 本机（回环）：放行（回环口明文 + 本机信任域）
/// - 免鉴权远程接口：`whoami`（信息与 mDNS TXT 重合）、配对协议两件套
///   （hello / confirm 的合法性由 handler 按配对模式 + 会话 + 证书校验，
///   那时对端还没进 peers 表，正是这里要放行的原因）
/// - 其余远程：连接必须出示**已配对设备的客户端证书**，否则 401
async fn auth_guard(State(state): State<AppState>, req: Request, next: Next) -> Response {
    if req_is_local(&req) {
        return next.run(req).await;
    }
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let exempt = (method == Method::GET && path == "/api/whoami")
        || (method == Method::POST
            && (path == "/api/pair/hello"
                || path == "/api/pair/confirm"
                || path == "/api/pair/submit-code"
                || path == "/api/pair/cancel-in"));
    if exempt {
        return next.run(req).await;
    }

    let fp = req
        .extensions()
        .get::<crate::tls::PeerFingerprint>()
        .and_then(|p| p.0.clone());
    match fp {
        Some(f) if state.peers.contains_fp(&f) => next.run(req).await,
        Some(_) => (
            StatusCode::UNAUTHORIZED,
            "设备未配对：请先与本机完成配对后再试",
        )
            .into_response(),
        None => (
            StatusCode::UNAUTHORIZED,
            "对方未出示客户端证书，无法验证设备身份",
        )
            .into_response(),
    }
}

/// 访问分级 middleware
///
/// 两条放行路径：请求来自回环地址，或开了 `--remote-admin`。
/// 其余按 [`classify`] 判档：Remote 放行，LocalOnly 与"表中未列出"一律 403。
async fn access_guard(req: Request, next: Next, allow_remote_admin: bool) -> Response {
    // 取不到 peer 地址时按「非本机」处理：宁可误拒，也不能让分级静默失效。
    // 若哪天忘了注入 ConnectInfo，症状是本机 UI 全部 403——动静很大，藏不住。
    let is_local = req_is_local(&req);

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

/// 广告端口来自 Extension（回环口报 7878、TLS 口报 7880，见 [`build_app`]）
async fn whoami(
    State(state): State<AppState>,
    Extension(advertised_gateway_port): Extension<u16>,
) -> Json<WhoAmI> {
    let (pairing_enabled, _) = state.discovery.pairing_status();
    Json(WhoAmI {
        id: state.discovery.self_id().to_string(),
        name: state.discovery.self_name().to_string(),
        platform: crate::platform::platform_name().to_string(),
        gateway_port: advertised_gateway_port,
        transfer_port: state.config.transfer_port,
        pairing_enabled,
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
    // 目标是跨机设备：缺省按对端 LAN TLS 端口（UI 正常带设备表里的
    // gateway_port——mDNS 广告的就是 LAN TLS 口——这里只是兜底）
    let target_gateway_port = req.target_gateway_port.unwrap_or(state.config.lan_tls_port);
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
            // 对端回包走本机 LAN TLS 口（回环口对端不可达）
            state.config.lan_tls_port,
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

/// POST /api/transfers/:file_id/resume —— 本机 UI「继续传输」入口（A3.2）。
/// 发送会话存在 → 只续未完成段；本机是接收方 → 通知发送方 peer-resume。
async fn resume_transfer(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
) -> impl IntoResponse {
    info!(%file_id, "resume requested");
    match state.transfer.clone().resume_transfer(&file_id).await {
        true => StatusCode::OK,
        false => StatusCode::NOT_FOUND,
    }
}

/// POST /api/peer-resume/:file_id —— 接收方请求发送方继续传输（Remote）。
async fn peer_resume(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
) -> impl IntoResponse {
    info!(%file_id, "peer-resume received");
    match state.transfer.clone().resume_send(&file_id).await {
        true => StatusCode::OK,
        false => StatusCode::NOT_FOUND,
    }
}

/// POST /api/peer-resumed/:file_id —— 发送方已续传，接收方清中断态回「传输中」（Remote）。
///
/// 响应体 `{"slot": bool}`：本机是否还持有接收槽位。发送方据此在续传前
/// 探测（slot=false → 对端已无此任务 → 直接 Failed，防空烧，A3.2）。
async fn peer_resumed(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
) -> Json<serde_json::Value> {
    let slot = state.transfer.on_peer_resumed(&file_id).await;
    Json(serde_json::json!({ "slot": slot }))
}

/// POST /api/verify/:file_id —— 发送方在**全部数据流发送完成后**补发整文件 sha256。
///
/// 接收方据此做最终校验并 finalize（sha256 传完再算，offer 不携带哈希，
/// 大文件弹窗即时出现）。body: {"sha256": "<hex>" 或 null}
///
/// 响应（A3.2 状态机对齐）：`{"result": "finalized"|"pending"|"missing",
/// "chunks_done": [bool…] | null}`——pending 时发送方必须按位图回退并显示
/// 已中断，不得凭 200 报已完成（否则两边状态永久分叉）。
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
        Ok(outcome) => Json(serde_json::json!({
            "result": outcome.as_str(),
            "chunks_done": outcome.chunks(),
        }))
        .into_response(),
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
) -> Response {
    // 协议版本门槛（工作流 B）：不识别的版本直接拒绝，不注册 incoming。
    // 不携带版本的旧对端按 legacy 放行（Option 语义，见 HttpOffer::version）。
    if let Some(v) = offer.version {
        if v != crate::protocol::PROTOCOL_VERSION {
            warn!(
                file_id = %offer.file_id,
                peer_version = v,
                local_version = crate::protocol::PROTOCOL_VERSION,
                "拒绝 offer：协议版本不匹配"
            );
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "protocol version mismatch: peer={}, local={}",
                    v,
                    crate::protocol::PROTOCOL_VERSION
                ),
            )
                .into_response();
        }
    }

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
    // 出站回包的对端 pin（P4：目标已配对时校验其服务端证书）
    let state_transfer_pin = state.transfer.out_pin(&entry.from_ip);

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
        if let Err(e) = crate::httpc::http_post_json_tls_pinned(
            &entry_for_spawn.from_ip,
            entry_for_spawn.from_gateway_port,
            "/api/incoming-resp",
            &resp_json,
            &config.receive_dir,
            state_transfer_pin.as_deref(),
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
    .into_response()
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
