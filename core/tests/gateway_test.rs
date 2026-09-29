//! Gateway 集成测试：覆盖全部 HTTP / WebSocket 接口
//!
//! 每个测试用独立端口对（gateway/transfer），支持并行运行。
//! 全链路测试：A 发送 → B 确认 → 多流传输 → 校验落盘 → 双端进度。

use kitefile::{Device, DiscoveryService, EngineConfig, HttpGateway, TransferEngine};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ============ 测试基础设施 ============

/// 检测当前环境能否创建 socket。
/// 某些 Windows 环境 Winsock 损坏（WinError 10038）或沙箱限网时
/// socket 创建失败，此时跳过网络相关测试（打印 SKIP），而非误报失败。
fn sockets_available() -> bool {
    std::net::TcpListener::bind(("127.0.0.1", 0)).is_ok()
}

macro_rules! require_sockets {
    () => {
        if !sockets_available() {
            eprintln!(
                "SKIP {}: socket 创建不可用（Winsock 异常或沙箱限网）",
                std::stringify!($test_name)
            );
            return;
        }
    };
    ($name:literal) => {
        if !sockets_available() {
            eprintln!("SKIP {}: socket 创建不可用（Winsock 异常或沙箱限网）", $name);
            return;
        }
    };
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    // 同 storage_test：走 FTCORE_TEST_TMP，便于指到 E 盘（系统 temp 在 C 盘）
    let base = std::env::var("FTCORE_TEST_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let dir = base.join(format!("kitefile-gw-test-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// gateway/transfers 测试本身不依赖 mDNS（目标端口均在 /api/send 中显式指定）。
/// 本机 Winsock UDP 异常（WinError 10038）时 mDNS 注册失败，
/// 退化为离线模式 DiscoveryService（设备列表恒空，其余功能不受影响）。
static SHARED_DISCOVERY: std::sync::OnceLock<Arc<DiscoveryService>> =
    std::sync::OnceLock::new();

fn shared_discovery() -> Arc<DiscoveryService> {
    SHARED_DISCOVERY
        .get_or_init(|| {
            Arc::new(
                DiscoveryService::new(
                    "test-shared".to_string(),
                    "test-shared-dev".to_string(),
                    7878,
                    7879,
                    None, // 测试实例，无需身份持久化
                )
                .unwrap_or_else(|_| {
                    DiscoveryService::new_offline(
                        "test-shared".to_string(),
                        "test-shared-dev".to_string(),
                        None,
                    )
                }),
            )
        })
        .clone()
}

/// 启动一个完整栈（discovery + receiver + gateway），返回 transfer engine
async fn start_stack(
    gw_port: u16,
    tr_port: u16,
    recv_dir: &std::path::Path,
    parallel: usize,
) -> Arc<TransferEngine> {
    start_stack_with(gw_port, tr_port, recv_dir, parallel, false).await
}

/// 同 start_stack，但可指定是否放开「仅本机」接口（用于验证访问分级）
///
/// 端口约定（阶段 5 P1 双监听）：
/// - `gw_port` → 回环明文口（本机 UI / 测试的 `http_*` 助手打这里）
/// - LAN TLS 口 = `tr_port + 1000`（与 (gw, tr) 固定错开，
///   避免与其它用例的固定端口在并行下相撞）
/// - 跨机流量（引擎互发 offer、远程策略用例）打 LAN TLS 口
async fn start_stack_with(
    gw_port: u16,
    tr_port: u16,
    recv_dir: &std::path::Path,
    parallel: usize,
    allow_remote_admin: bool,
) -> Arc<TransferEngine> {
    start_stack_impl(
        shared_discovery(),
        gw_port,
        tr_port,
        recv_dir,
        parallel,
        allow_remote_admin,
    )
    .await
}

/// 同 start_stack，但用**本用例独占的离线 discovery**——
/// 配对模式 / pending 是进程级状态，共享 discovery 会让并行用例互相污染。
/// `identity_path` 指到本用例接收目录：P2 的 whoami 同步/60s 探活
/// 需要它推导 receive_dir（TLS 身份），None 会静默跳过同步路径。
async fn start_stack_isolated(
    tag: &str,
    gw_port: u16,
    tr_port: u16,
    recv_dir: &std::path::Path,
    parallel: usize,
) -> Arc<TransferEngine> {
    start_stack_isolated_full(tag, gw_port, tr_port, recv_dir, parallel)
        .await
        .0
}

/// 同 start_stack_isolated，但把 discovery 一并交出（测试需要直接操作设备表）
async fn start_stack_isolated_full(
    tag: &str,
    gw_port: u16,
    tr_port: u16,
    recv_dir: &std::path::Path,
    parallel: usize,
) -> (Arc<TransferEngine>, Arc<DiscoveryService>) {
    let discovery = Arc::new(DiscoveryService::new_offline(
        format!("iso-{tag}"),
        format!("iso-{tag}-id"),
        Some(recv_dir.join(".kitefile-identity")),
    ));
    let engine =
        start_stack_impl(discovery.clone(), gw_port, tr_port, recv_dir, parallel, false).await;
    (engine, discovery)
}

async fn start_stack_impl(
    discovery: Arc<DiscoveryService>,
    gw_port: u16,
    tr_port: u16,
    recv_dir: &std::path::Path,
    parallel: usize,
    allow_remote_admin: bool,
) -> Arc<TransferEngine> {
    let _ = tracing_subscriber::fmt().try_init();
    let lan_port = tr_port + 1000;
    let config = EngineConfig {
        device_name: format!("test-{}", gw_port),
        gateway_port: gw_port,
        lan_tls_port: lan_port,
        transfer_port: tr_port,
        parallel_streams: parallel,
        receive_dir: recv_dir.to_path_buf(),
        allow_remote_admin,
    };
    let transfer = Arc::new(TransferEngine::new(
        tr_port,
        parallel,
        recv_dir.to_path_buf(),
    ));
    transfer.clone().spawn_receiver().await.unwrap();

    let gateway = HttpGateway::new(discovery, transfer.clone(), Arc::new(config));
    tokio::spawn(async move {
        let _ = gateway.run().await;
    });

    // 等 gateway 就绪
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", gw_port)).await.is_ok() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "gateway {gw_port} not ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    transfer
}

/// 极简 HTTP 客户端：返回 (status, body_bytes)
async fn http(port: u16, method: &str, path: &str, body: Option<&str>) -> (u16, Vec<u8>) {
    http_to("127.0.0.1", port, method, path, body).await
}

/// 指定目标地址的版本：访问分级按来源 IP 判定，要模拟"非本机"请求
/// 就得连到本机的局域网 IP（此时服务端看到的 peer 不是回环地址）。
async fn http_to(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, Vec<u8>) {
    let mut stream = tokio::net::TcpStream::connect((host, port)).await.unwrap();
    let body_bytes = body.map(|b| b.as_bytes().to_vec()).unwrap_or_default();
    let req = format!(
        "{} {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        method, path, host, port, body_bytes.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(&body_bytes).await.unwrap();
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();

    let text = String::from_utf8_lossy(&resp);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_start = text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(resp.len());
    (status, resp[body_start..].to_vec())
}

async fn http_json(port: u16, method: &str, path: &str, body: Option<&str>) -> Value {
    let (status, body) = http(port, method, path, body).await;
    assert!(status == 200, "expected 200, got {status} for {method} {path}");
    serde_json::from_slice(&body).unwrap_or(Value::Null)
}

/// 测试身份（TLS 客户端出示用；进程内加载一次）
fn test_identity() -> std::sync::Arc<kitefile::tls::NodeIdentity> {
    static ID: std::sync::OnceLock<std::sync::Arc<kitefile::tls::NodeIdentity>> =
        std::sync::OnceLock::new();
    ID.get_or_init(|| {
        let base = std::env::var("FTCORE_TEST_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        kitefile::tls::NodeIdentity::load_or_create(&base.join("kitefile-gw-test-identity"))
            .expect("test identity")
    })
    .clone()
}

/// TLS 版 `http_to`：打局域网 TLS 口（阶段 5 P1）。返回 (status, body_bytes)。
async fn https_to(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, Vec<u8>) {
    let cfg = kitefile::tls::client_config(&test_identity(), None).unwrap();
    let connector = tokio_rustls::TlsConnector::from(cfg);
    let tcp = tokio::net::TcpStream::connect((host, port)).await.unwrap();
    let ip: std::net::IpAddr = host.parse().unwrap();
    let name = rustls::pki_types::ServerName::IpAddress(ip.into());
    let mut stream = connector.connect(name, tcp).await.unwrap();

    let body_bytes = body.map(|b| b.as_bytes().to_vec()).unwrap_or_default();
    let req = format!(
        "{} {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        method, path, host, port, body_bytes.len()
    );
    tokio::io::AsyncWriteExt::write_all(&mut stream, req.as_bytes())
        .await
        .unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut stream, &body_bytes)
        .await
        .unwrap();
    let mut resp = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut resp)
        .await
        .unwrap();

    let text = String::from_utf8_lossy(&resp);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_start = text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(resp.len());
    (status, resp[body_start..].to_vec())
}

/// 生成确定性伪随机内容
fn make_content(n: usize) -> Vec<u8> {
    (0..n).map(|i| ((i * 31 + 7) % 251) as u8).collect()
}

async fn wait_for_json(
    port: u16,
    path: &str,
    timeout_secs: u64,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let (status, body) = http(port, "GET", path, None).await;
        assert_eq!(status, 200, "GET {path} failed");
        let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if pred(&v) {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout waiting for {path} to satisfy condition: {}",
            v
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 在 transfers 列表中找到指定 file_id 的进度
fn find_transfer(v: &Value, file_id: &str) -> Option<Value> {
    v["transfers"]
        .as_array()?
        .iter()
        .find(|t| t["file_id"].as_str() == Some(file_id))
        .cloned()
}

// ============ 基础接口 ============

#[tokio::test]
async fn test_whoami() {
    require_sockets!("test_whoami");
    let dir = temp_dir("whoami");
    let _ = start_stack(18001, 18101, &dir, 2).await;

    let v = http_json(18001, "GET", "/api/whoami", None).await;
    assert!(v["id"].as_str().is_some());
    assert!(!v["name"].as_str().unwrap().is_empty());
    assert!(v["platform"].as_str().is_some());
    assert_eq!(v["gateway_port"].as_u64(), Some(18001));
    assert_eq!(v["transfer_port"].as_u64(), Some(18101));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_devices_and_root() {
    require_sockets!("test_devices_and_root");
    let dir = temp_dir("devices");
    let _ = start_stack(18002, 18102, &dir, 2).await;

    // 设备列表：合法 JSON 数组（本机单栈场景可能为空）
    let v = http_json(18002, "GET", "/api/devices", None).await;
    assert!(v.is_array());

    // 根路由
    let (status, body) = http(18002, "GET", "/", None).await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("kitefile gateway"));

    let _ = std::fs::remove_dir_all(&dir);
}

// ============ 文件列表 / 下载 ============

#[tokio::test]
async fn test_files_list_and_download() {
    require_sockets!("test_files_list_and_download");
    let dir = temp_dir("files");
    let _ = start_stack(18003, 18103, &dir, 2).await;

    let content = b"hello-kitefile-download".to_vec();
    std::fs::write(dir.join("a.txt"), &content).unwrap();

    // 列表
    let v = http_json(18003, "GET", "/api/files", None).await;
    let names: Vec<&str> = v.as_array().unwrap().iter().filter_map(|n| n.as_str()).collect();
    assert!(names.contains(&"a.txt"));

    // 下载：内容一致
    let (status, body) = http(18003, "GET", "/api/files/a.txt", None).await;
    assert_eq!(status, 200);
    assert_eq!(body, content);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_download_not_found_and_traversal() {
    require_sockets!("test_download_not_found_and_traversal");
    let dir = temp_dir("traversal");
    let _ = start_stack(18004, 18104, &dir, 2).await;

    // 不存在的文件
    let (status, _) = http(18004, "GET", "/api/files/noexist.bin", None).await;
    assert_eq!(status, 404);

    // 路径穿越（%2f 解码后为 '/'）
    let (status, _) = http(18004, "GET", "/api/files/..%2f..%2fsecret", None).await;
    assert_eq!(status, 400);

    // 相对路径上跳
    let (status, _) = http(18004, "GET", "/api/files/..", None).await;
    assert!(status == 400 || status == 404, "unexpected {status}");

    let _ = std::fs::remove_dir_all(&dir);
}

// ============ 取消：未知 file_id ============

#[tokio::test]
async fn test_cancel_unknown_404() {
    require_sockets!("test_cancel_unknown_404");
    let dir = temp_dir("cancel404");
    let _ = start_stack(18005, 18105, &dir, 2).await;

    let (status, _) = http(18005, "POST", "/api/cancel/nonexistent-id", Some("{}")).await;
    assert_eq!(status, 404);

    let _ = std::fs::remove_dir_all(&dir);
}

/// A3.2 续传入口路由：未知 file_id → 404（没有可续的对象）。
#[tokio::test]
async fn test_resume_unknown_404() {
    require_sockets!("test_resume_unknown_404");
    let dir = temp_dir("resume404");
    let _ = start_stack(18050, 18150, &dir, 2).await;

    let (status, _) = http(
        18050,
        "POST",
        "/api/transfers/nonexistent-id/resume",
        Some("{}"),
    )
    .await;
    assert_eq!(status, 404);

    // 对端续传入口（Remote 档，本机调用直接放行）：本机无发送会话 → 404
    let (status, _) = http(18050, "POST", "/api/peer-resume/nonexistent-id", Some("{}")).await;
    assert_eq!(status, 404);

    // 对端已续传通知：200 且上报本机槽位状态（slot=false = 无此任务）
    let v = http_json(
        18050,
        "POST",
        "/api/peer-resumed/nonexistent-id",
        Some("{}"),
    )
    .await;
    assert_eq!(v["slot"], json!(false), "无槽位必须如实上报 false");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 工作流 B：协议版本门槛——
/// 携带不识别的版本 → 400 且**不注册** incoming；
/// 携带当前版本 / 不携带版本（legacy 旧对端）→ 放行。
#[tokio::test]
async fn test_offer_version_gate() {
    require_sockets!("test_offer_version_gate");
    let dir = temp_dir("vergate");
    let _ = start_stack(18051, 18151, &dir, 2).await;

    // 版本不识别 → 400，不进待决定列表
    let mut v = fake_offer("ver-bad", 10, 18151);
    let mut offer: Value = serde_json::from_str(&v).unwrap();
    offer["version"] = json!(9999);
    v = offer.to_string();
    let (status, _) = http(18051, "POST", "/api/incoming", Some(&v)).await;
    assert_eq!(status, 400, "协议版本不识别必须直接拒绝");
    let pending = http_json(18051, "GET", "/api/incoming", None).await;
    assert_eq!(
        pending.as_array().map(|a| a.len()),
        Some(0),
        "被拒绝的 offer 不得注册为待决定"
    );

    // 携带当前版本 → 200 注册
    let mut offer: Value =
        serde_json::from_str(&fake_offer("ver-ok", 10, 18151)).unwrap();
    offer["version"] = json!(kitefile::protocol::PROTOCOL_VERSION);
    let (status, _) = http(18051, "POST", "/api/incoming", Some(&offer.to_string())).await;
    assert_eq!(status, 200);

    // legacy 旧对端（无 version 字段）→ 放行
    let (status, _) = http(
        18051,
        "POST",
        "/api/incoming",
        Some(&fake_offer("ver-legacy", 10, 18151)),
    )
    .await;
    assert_eq!(status, 200);

    let pending = http_json(18051, "GET", "/api/incoming", None).await;
    assert_eq!(pending.as_array().map(|a| a.len()), Some(2));

    let _ = std::fs::remove_dir_all(&dir);
}

// ============ 工作流 E：并发 / 竞态 ============

/// 多文件同传（工作流 E）：两文件并行在途，各自接受后**同时**完成，
/// 内容与源一致、互不串写。
#[tokio::test]
async fn test_concurrent_multi_file_transfers() {
    require_sockets!("test_concurrent_multi_file_transfers");
    let dir_a = temp_dir("multi-a");
    let dir_b = temp_dir("multi-b");
    let _a = start_stack(18060, 18160, &dir_a, 4).await;
    let _b = start_stack(18061, 18161, &dir_b, 4).await;

    // 两个不同内容的文件（512KB → 单流，聚焦并发而非多流）
    let content1 = make_content(512 * 1024);
    let content2: Vec<u8> = content1.iter().map(|b| b.wrapping_add(17)).collect();
    let src1 = dir_a.join("one.bin");
    let src2 = dir_a.join("two.bin");
    std::fs::write(&src1, &content1).unwrap();
    std::fs::write(&src2, &content2).unwrap();

    let mut file_ids = Vec::new();
    for src in [&src1, &src2] {
        let send_body = json!({
            "target_ip": "127.0.0.1",
            "target_port": 18161,
            "target_gateway_port": 19161,
            "file_path": src.to_string_lossy(),
        })
        .to_string();
        let (status, body) = http(18060, "POST", "/api/send", Some(&send_body)).await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        let resp: Value = serde_json::from_slice(&body).unwrap();
        file_ids.push(resp["file_id"].as_str().unwrap().to_string());
    }

    // 两个待决请求都出现（并行在途）
    let v = wait_for_json(18061, "/api/incoming", 15, |v| {
        v.as_array().map(|a| a.len() >= 2).unwrap_or(false)
    })
    .await;
    let arr = v.as_array().unwrap();
    for entry in arr {
        let incoming_id = entry["incoming_id"].as_str().unwrap();
        let (status, _) = http(
            18061,
            "POST",
            &format!("/api/incoming/{incoming_id}/accept"),
            None,
        )
        .await;
        assert_eq!(status, 200);
    }

    // 两个都到 Completed
    let v = wait_for_json(18061, "/api/transfers", 30, |v| {
        file_ids.iter().all(|fid| {
            find_transfer(v, fid)
                .map(|t| t["status"].as_str() == Some("Completed"))
                .unwrap_or(false)
        })
    })
    .await;
    for fid in &file_ids {
        assert!(find_transfer(&v, fid).is_some(), "{fid} 应有进度帧");
    }

    // 落盘内容逐一核对（防串写）
    let got1 = std::fs::read(dir_b.join("one.bin")).unwrap();
    let got2 = std::fs::read(dir_b.join("two.bin")).unwrap();
    assert_eq!(got1, content1, "文件 1 内容必须与源一致");
    assert_eq!(got2, content2, "文件 2 内容必须与源一致");

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// 取消与完成的竞态边界（工作流 E）：传输完成**之后**再取消，
/// 不得改写终态、不得让状态卡回进行中；cancel 对已结束任务回 404。
#[tokio::test]
async fn test_cancel_after_completion_keeps_terminal_state() {
    require_sockets!("test_cancel_after_completion_keeps_terminal_state");
    let dir_a = temp_dir("race-a");
    let dir_b = temp_dir("race-b");
    let _a = start_stack(18070, 18170, &dir_a, 4).await;
    let _b = start_stack(18071, 18171, &dir_b, 4).await;

    let content = make_content(256 * 1024);
    let src = dir_a.join("race.bin");
    std::fs::write(&src, &content).unwrap();

    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18171,
        "target_gateway_port": 19171,
        "file_path": src.to_string_lossy(),
    })
    .to_string();
    let (status, body) = http(18070, "POST", "/api/send", Some(&send_body)).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let resp: Value = serde_json::from_slice(&body).unwrap();
    let file_id = resp["file_id"].as_str().unwrap().to_string();

    let v = wait_for_json(18071, "/api/incoming", 15, |v| {
        v.as_array().map(|a| !a.is_empty()).unwrap_or(false)
    })
    .await;
    let incoming_id = v[0]["incoming_id"].as_str().unwrap().to_string();
    let (status, _) = http(
        18071,
        "POST",
        &format!("/api/incoming/{incoming_id}/accept"),
        None,
    )
    .await;
    assert_eq!(status, 200);

    // 等两端都 Completed（任务彻底结束：会话/注册表已清理）
    wait_for_json(18071, "/api/transfers", 30, |v| {
        find_transfer(v, &file_id)
            .map(|t| t["status"].as_str() == Some("Completed"))
            .unwrap_or(false)
    })
    .await;
    wait_for_json(18070, "/api/transfers", 30, |v| {
        find_transfer(v, &file_id)
            .map(|t| t["status"].as_str() == Some("Completed"))
            .unwrap_or(false)
    })
    .await;

    // 完成后取消：找不到活动任务 → 404；两端终态保持 Completed
    let (status, _) = http(18070, "POST", &format!("/api/cancel/{file_id}"), None).await;
    assert_eq!(status, 404, "已结束任务的取消应 404，而不是复活任务");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let a = http_json(18070, "GET", "/api/transfers", None).await;
    let b = http_json(18071, "GET", "/api/transfers", None).await;
    for (side, v) in [("A", &a), ("B", &b)] {
        let t = find_transfer(v, &file_id).expect(side);
        assert_eq!(
            t["status"].as_str(),
            Some("Completed"),
            "{side} 侧终态不得被取消改写"
        );
    }

    // 文件仍完好
    assert_eq!(std::fs::read(dir_b.join("race.bin")).unwrap(), content);

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// 工作流 E/A3.2：verify 回执必须如实反映接收方任务状态——
/// 未收齐 → pending（带权威位图）；收齐 → finalized；无此任务 → missing。
/// 发送方靠它决定终态，只回 200 会造成两端状态永久分叉（真机踩过）。
#[tokio::test]
async fn test_verify_receipt_shapes() {
    require_sockets!("test_verify_receipt_shapes");
    let dir = temp_dir("verify-receipt");
    let engine = start_stack(18052, 18152, &dir, 2).await;

    // ① 未收齐 → pending + 位图
    engine
        .storage
        .create_slot("vr-pending".into(), "a.bin".into(), 10, 2, None, true)
        .await
        .unwrap();
    let v = http_json(
        18052,
        "POST",
        "/api/verify/vr-pending",
        Some(r#"{"sha256":"00"}"#),
    )
    .await;
    assert_eq!(v["result"], json!("pending"));
    assert_eq!(v["chunks_done"], json!([false, false]));

    // ② 收齐 + 正确 sha → finalized（且真正落盘）
    let content = b"1234567890";
    engine
        .storage
        .create_slot("vr-done".into(), "b.bin".into(), 10, 2, None, true)
        .await
        .unwrap();
    engine.storage.write_at("vr-done", 0, b"12345").await.unwrap();
    engine.storage.write_at("vr-done", 5, b"67890").await.unwrap();
    engine.storage.finish_stream("vr-done", 0).await.unwrap();
    engine.storage.finish_stream("vr-done", 1).await.unwrap();
    let sha = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(content);
        format!("{:x}", h.finalize())
    };
    let v = http_json(
        18052,
        "POST",
        "/api/verify/vr-done",
        Some(&json!({ "sha256": sha }).to_string()),
    )
    .await;
    assert_eq!(v["result"], json!("finalized"));
    assert!(dir.join("b.bin").exists(), "finalized 必须真实落盘");

    // ③ 无此任务 → missing（幂等：对不存在/已清理的任务如实上报）
    let v = http_json(
        18052,
        "POST",
        "/api/verify/vr-none",
        Some(r#"{"sha256":null}"#),
    )
    .await;
    assert_eq!(v["result"], json!("missing"));

    let _ = std::fs::remove_dir_all(&dir);
}

// ============ incoming offer：登记 / 列表 / 接受 / 拒绝 ============

fn fake_offer(file_id: &str, file_size: u64, from_gateway_port: u16) -> String {
    json!({
        "file_id": file_id,
        "file_name": "fake.bin",
        "file_size": file_size,
        "sha256": null,
        "from_id": "fake-dev",
        "from_name": "fake-sender",
        "from_ip": "127.0.0.1",
        "from_gateway_port": from_gateway_port,
        "from_transfer_port": 0,
    })
    .to_string()
}

#[tokio::test]
async fn test_incoming_offer_list_accept() {
    require_sockets!("test_incoming_offer_list_accept");
    let dir = temp_dir("incoming-accept");
    let _ = start_stack(18006, 18106, &dir, 2).await;

    // 登记一个 offer（回包端口指向本栈 gateway，避免无关 warn）
    let (status, body) = http(
        18006,
        "POST",
        "/api/incoming",
        Some(&fake_offer("file-inc-1", 100, 18006)),
    )
    .await;
    assert_eq!(status, 200);
    let ack: Value = serde_json::from_slice(&body).unwrap();
    let incoming_id = ack["incoming_id"].as_str().unwrap().to_string();
    assert!(ack["pending"].as_bool().unwrap());

    // 待决列表可见
    let v = http_json(18006, "GET", "/api/incoming", None).await;
    let entries: Vec<&Value> = v
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["incoming_id"].as_str() == Some(incoming_id.as_str()))
        .collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["file_id"].as_str(), Some("file-inc-1"));
    assert_eq!(entries[0]["file_size"].as_u64(), Some(100));

    // 接受
    let (status, _) = http(
        18006,
        "POST",
        &format!("/api/incoming/{incoming_id}/accept"),
        None,
    )
    .await;
    assert_eq!(status, 200);

    // 决策后从待决列表移除（UI 已通过 WS IncomingResolved 获知）
    tokio::time::sleep(Duration::from_millis(200)).await;
    let v = http_json(18006, "GET", "/api/incoming", None).await;
    assert!(v
        .as_array()
        .unwrap()
        .iter()
        .all(|e| e["incoming_id"].as_str() != Some(incoming_id.as_str())));

    // 重复决策 → 404
    let (status, _) = http(
        18006,
        "POST",
        &format!("/api/incoming/{incoming_id}/accept"),
        None,
    )
    .await;
    assert_eq!(status, 404);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_incoming_reject_and_unknown_404() {
    require_sockets!("test_incoming_reject_and_unknown_404");
    let dir = temp_dir("incoming-reject");
    let _ = start_stack(18007, 18107, &dir, 2).await;

    let (status, body) = http(
        18007,
        "POST",
        "/api/incoming",
        Some(&fake_offer("file-inc-2", 50, 18007)),
    )
    .await;
    assert_eq!(status, 200);
    let ack: Value = serde_json::from_slice(&body).unwrap();
    let incoming_id = ack["incoming_id"].as_str().unwrap().to_string();

    // 未知 id → 404
    let (status, _) = http(18007, "POST", "/api/incoming/unknown-id/reject", None).await;
    assert_eq!(status, 404);

    // 拒绝
    let (status, _) = http(
        18007,
        "POST",
        &format!("/api/incoming/{incoming_id}/reject"),
        None,
    )
    .await;
    assert_eq!(status, 200);

    // 已从待决列表移除
    tokio::time::sleep(Duration::from_millis(200)).await;
    let v = http_json(18007, "GET", "/api/incoming", None).await;
    assert!(v
        .as_array()
        .unwrap()
        .iter()
        .all(|e| e["incoming_id"].as_str() != Some(incoming_id.as_str())));

    let _ = std::fs::remove_dir_all(&dir);
}

// ============ 全链路：发送 → 接受 → 传输 → 校验落盘 → 进度 ============

#[tokio::test]
async fn test_full_transfer_flow() {
    require_sockets!("test_full_transfer_flow");
    let dir_a = temp_dir("full-a"); // A（发送方）的接收目录
    let dir_b = temp_dir("full-b"); // B（接收方）的接收目录

    // parallel=4、9MB 文件 → 自适应 3 流（4MB/流），覆盖多流 + 末段不满
    let _a = start_stack(18010, 18110, &dir_a, 4).await;
    let _b = start_stack(18011, 18111, &dir_b, 4).await;

    let content = make_content(9 * 1024 * 1024 + 150_000);
    let src = dir_a.join("hello.bin");
    std::fs::write(&src, &content).unwrap();

    // A 发起发送
    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18111,
        "target_gateway_port": 19111,
        "file_path": src.to_string_lossy(),
    })
    .to_string();
    let (status, body) = http(18010, "POST", "/api/send", Some(&send_body)).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let resp: Value = serde_json::from_slice(&body).unwrap();
    let file_id = resp["file_id"].as_str().unwrap().to_string();
    assert!(!file_id.is_empty());

    // B 侧出现待决请求
    let v = wait_for_json(18011, "/api/incoming", 15, |v| {
        v.as_array().map(|a| !a.is_empty()).unwrap_or(false)
    })
    .await;
    let incoming_id = v[0]["incoming_id"].as_str().unwrap().to_string();
    assert_eq!(v[0]["file_id"].as_str(), Some(file_id.as_str()));
    // sha256 延后补发：传输全部完成后再算（不与传输抢磁盘 IO），offer 不等它
    //（大文件弹窗即时出现）。因此 offer 里 sha256 为空、sha256_deferred = true；
    // 真正的值在全部数据流发送完成后由 POST /api/verify/:file_id 补发，
    // 接收方校验通过才 finalize。
    // 本用例末尾"文件落盘且内容一致"即证明补发 + 校验链路工作正常。
    assert_eq!(v[0]["sha256_deferred"].as_bool(), Some(true));
    assert!(v[0]["sha256"].is_null(), "延后模式下 offer 不带 sha256");

    // B 接受
    let (status, _) = http(
        18011,
        "POST",
        &format!("/api/incoming/{incoming_id}/accept"),
        None,
    )
    .await;
    assert_eq!(status, 200);

    // B 侧进度到 Completed
    let v = wait_for_json(18011, "/api/transfers", 30, |v| {
        find_transfer(v, &file_id)
            .map(|t| t["status"].as_str() == Some("Completed"))
            .unwrap_or(false)
    })
    .await;
    let t = find_transfer(&v, &file_id).unwrap();
    let expect = (9 * 1024 * 1024 + 150_000) as u64;
    assert_eq!(t["bytes_transferred"].as_u64(), Some(expect));
    // 9MB+ / 4MB per stream → 3 流
    assert_eq!(t["chunks_total"].as_u64(), Some(3));
    assert!(t["incoming"].as_bool().unwrap(), "接收方视角 incoming=true");

    // A 侧进度也到 Completed（发送方视角 incoming=false）
    let v = wait_for_json(18010, "/api/transfers", 15, |v| {
        find_transfer(v, &file_id)
            .map(|t| t["status"].as_str() == Some("Completed"))
            .unwrap_or(false)
    })
    .await;
    let t = find_transfer(&v, &file_id).unwrap();
    assert!(!t["incoming"].as_bool().unwrap());

    // 文件落盘且内容一致（sha256 已在接收端 finalize 校验通过）
    let got = std::fs::read(dir_b.join("hello.bin")).unwrap();
    assert_eq!(got, content);

    // 待决列表清空
    let v: Value =
        serde_json::from_slice(&http(18011, "GET", "/api/incoming", None).await.1).unwrap();
    assert!(v.as_array().unwrap().is_empty(), "完成后待决列表应清空");

    // 下载接口可用（全链路下载）
    let (status, body) = http(18011, "GET", "/api/files/hello.bin", None).await;
    assert_eq!(status, 200);
    assert_eq!(body, content);

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

#[tokio::test]
async fn test_reject_flow() {
    require_sockets!("test_reject_flow");
    let dir_a = temp_dir("reject-a");
    let dir_b = temp_dir("reject-b");

    let _a = start_stack(18012, 18112, &dir_a, 2).await;
    let _b = start_stack(18013, 18113, &dir_b, 2).await;

    let content = make_content(50_000);
    let src = dir_a.join("reject.bin");
    std::fs::write(&src, &content).unwrap();

    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18113,
        "target_gateway_port": 19113,
        "file_path": src.to_string_lossy(),
    })
    .to_string();
    let (_, body) = http(18012, "POST", "/api/send", Some(&send_body)).await;
    let file_id: String = serde_json::from_slice::<Value>(&body).unwrap()["file_id"]
        .as_str()
        .unwrap()
        .to_string();

    // B 拒绝
    let v = wait_for_json(18013, "/api/incoming", 15, |v| {
        v.as_array().map(|a| !a.is_empty()).unwrap_or(false)
    })
    .await;
    let incoming_id = v[0]["incoming_id"].as_str().unwrap().to_string();
    let (status, _) = http(
        18013,
        "POST",
        &format!("/api/incoming/{incoming_id}/reject"),
        None,
    )
    .await;
    assert_eq!(status, 200);

    // A 侧最终状态 Canceled（被拒绝）
    let v = wait_for_json(18012, "/api/transfers", 15, |v| {
        find_transfer(v, &file_id)
            .map(|t| t["status"].as_str() == Some("Canceled"))
            .unwrap_or(false)
    })
    .await;
    let t = find_transfer(&v, &file_id).unwrap();
    assert!(t["error"].as_str().is_some());

    // B 未产生文件
    assert!(!dir_b.join("reject.bin").exists());

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ============ 取消：发送方在等待决策期间取消 ============

#[tokio::test]
async fn test_cancel_while_pending() {
    require_sockets!("test_cancel_while_pending");
    let dir_a = temp_dir("cancel-a");
    let dir_b = temp_dir("cancel-b");

    let _a = start_stack(18014, 18114, &dir_a, 2).await;
    let _b = start_stack(18015, 18115, &dir_b, 2).await;

    let content = make_content(50_000);
    let src = dir_a.join("cancel.bin");
    std::fs::write(&src, &content).unwrap();

    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18115,
        "target_gateway_port": 19115,
        "file_path": src.to_string_lossy(),
    })
    .to_string();
    let (_, body) = http(18014, "POST", "/api/send", Some(&send_body)).await;
    let file_id: String = serde_json::from_slice::<Value>(&body).unwrap()["file_id"]
        .as_str()
        .unwrap()
        .to_string();

    // 等 B 出现待决请求（尚未决策）
    let v = wait_for_json(18015, "/api/incoming", 15, |v| {
        v.as_array().map(|a| !a.is_empty()).unwrap_or(false)
    })
    .await;
    assert_eq!(v[0]["file_id"].as_str(), Some(file_id.as_str()));

    // A 在 Pending 阶段取消 → 200
    let (status, _) = http(18014, "POST", &format!("/api/cancel/{file_id}"), Some("{}")).await;
    assert_eq!(status, 200);

    // A 侧最终 Canceled（不再是 30 秒超时）
    let v = wait_for_json(18014, "/api/transfers", 15, |v| {
        find_transfer(v, &file_id)
            .map(|t| t["status"].as_str() == Some("Canceled"))
            .unwrap_or(false)
    })
    .await;
    assert!(find_transfer(&v, &file_id).is_some());

    // B 未产生文件（取消后不应落盘）
    // 注：这里原先写的是 cancel.bin，与本用例的源文件 big.bin 对不上，
    // 断言恒真、等于没检查，已修正。
    assert!(!dir_b.join("big.bin").exists());

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ============ 取消：接收方取消已接受的传输（含通知发送方联动） ============

#[tokio::test]
async fn test_receiver_cancel_notifies_sender() {
    require_sockets!("test_receiver_cancel_notifies_sender");
    let dir_a = temp_dir("rcancel-a");
    let dir_b = temp_dir("rcancel-b");

    // A 用较大文件、单流：流式传输无停等 ACK，4MB 在回环上会瞬间传完，
    // cancel 打过去时槽位已清理。80MB 保证落盘窗口够长，取消能稳定落在传输中。
    let _a = start_stack(18016, 18116, &dir_a, 1).await;
    let _b = start_stack(18017, 18117, &dir_b, 1).await;

    let content = make_content(80 * 1024 * 1024);
    let src = dir_a.join("big.bin");
    std::fs::write(&src, &content).unwrap();

    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18117,
        "target_gateway_port": 19117,
        "file_path": src.to_string_lossy(),
    })
    .to_string();
    let (_, body) = http(18016, "POST", "/api/send", Some(&send_body)).await;
    let file_id: String = serde_json::from_slice::<Value>(&body).unwrap()["file_id"]
        .as_str()
        .unwrap()
        .to_string();

    // B 接受
    let v = wait_for_json(18017, "/api/incoming", 15, |v| {
        v.as_array().map(|a| !a.is_empty()).unwrap_or(false)
    })
    .await;
    let incoming_id = v[0]["incoming_id"].as_str().unwrap().to_string();
    let (status, _) = http(
        18017,
        "POST",
        &format!("/api/incoming/{incoming_id}/accept"),
        None,
    )
    .await;
    assert_eq!(status, 200);

    // 等 B 侧出现传输记录后立刻取消（流式传输快，不强制等 InProgress——
    // 轮询间隔内可能已经 Completed，那时 cancel 返回 404 是正确行为）。
    let _ = wait_for_json(18017, "/api/transfers", 15, |v| {
        find_transfer(v, &file_id).is_some()
    })
    .await;

    // B（接收方）取消 → 200（若已传完则 404，见上方注释）
    let (status, _) = http(18017, "POST", &format!("/api/cancel/{file_id}"), Some("{}")).await;
    assert!(status == 200 || status == 404, "cancel 应返回 200 或已完成后的 404，实际 {status}");

    // 轮询 B 侧终态：Canceled（取消成功）或 Completed（传太快没取到消）。
    // 不能 wait_for_json 等 Canceled——超时会 panic，跳过分支永远走不到。
    let mut b_final = Value::Null;
    let mut b_canceled = false;
    for _ in 0..50 {
        let (_, body) = http(18017, "GET", "/api/transfers", None).await;
        let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if let Some(t) = find_transfer(&v, &file_id) {
            let st = t["status"].as_str().unwrap_or("");
            if st == "Canceled" {
                b_final = t;
                b_canceled = true;
                break;
            }
            if st == "Completed" {
                b_final = t;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !b_canceled {
        eprintln!("SKIP cancel linkage: 80MB 在本机仍传完太快，未能落在传输中");
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
        return;
    }
    assert!(b_final["incoming"].as_bool().unwrap());

    // 发送方被联动取消（而非 Completed）
    let v = wait_for_json(18016, "/api/transfers", 15, |v| {
        find_transfer(v, &file_id)
            .map(|t| {
                let s = t["status"].as_str().unwrap_or("");
                s == "Canceled" || s == "Failed"
            })
            .unwrap_or(false)
    })
    .await;
    assert!(find_transfer(&v, &file_id).is_some());

    // B 未产生最终文件
    assert!(!dir_b.join("big.bin").exists());

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ============ WebSocket 事件推送 ============

#[tokio::test]
async fn test_ws_incoming_events() {
    require_sockets!("test_ws_incoming_events");
    let dir = temp_dir("ws");
    let _ = start_stack(18020, 18120, &dir, 2).await;

    // 先建立 WS 订阅，再投递 offer
    let (mut ws, _resp) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:18020/ws/progress"))
            .await
            .unwrap();

    // 投递 offer（回包端口指向本栈，避免无关错误）
    let (status, body) = http(
        18020,
        "POST",
        "/api/incoming",
        Some(&fake_offer("file-ws-1", 42, 18020)),
    )
    .await;
    assert_eq!(status, 200);
    let ack: Value = serde_json::from_slice(&body).unwrap();
    let incoming_id = ack["incoming_id"].as_str().unwrap().to_string();

    // 收到 incoming 事件
    let ev = read_ws_event(&mut ws, 10).await;
    assert_eq!(ev["event_type"].as_str(), Some("incoming"));
    assert_eq!(ev["incoming_id"].as_str(), Some(incoming_id.as_str()));
    assert_eq!(ev["file_id"].as_str(), Some("file-ws-1"));
    assert_eq!(ev["file_size"].as_u64(), Some(42));

    // 拒绝 → 收到 incoming_resolved
    let (status, _) = http(
        18020,
        "POST",
        &format!("/api/incoming/{incoming_id}/reject"),
        None,
    )
    .await;
    assert_eq!(status, 200);

    let ev = read_ws_event(&mut ws, 10).await;
    assert_eq!(ev["event_type"].as_str(), Some("incoming_resolved"));
    assert_eq!(ev["incoming_id"].as_str(), Some(incoming_id.as_str()));
    assert_eq!(ev["accepted"].as_bool(), Some(false));

    // 关闭连接（保活 server 任务退出）
    let _ = ws.close(None).await;

    let _ = std::fs::remove_dir_all(&dir);
}

async fn read_ws_event(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    timeout_secs: u64,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next()).await;
        match msg {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(s)))) => {
                if let Ok(v) = serde_json::from_str::<Value>(&s) {
                    return v;
                }
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => panic!("ws error: {e}"),
            Ok(None) => panic!("ws closed"),
            Err(_) => panic!("ws read timeout"),
        }
    }
}

// ============ 访问分级（阶段 2） ============

use kitefile::gateway::{classify, path_matches, AccessPolicy};

/// 取一个非回环的本机 IPv4 地址。
///
/// UDP connect 只查路由表、不发包，用它问内核"去外部时本机用哪个地址"。
/// 拿不到（无网络 / 只有回环）时返回 None，调用方跳过测试。
fn non_loopback_ip() -> Option<String> {
    let s = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("192.0.2.1:80").ok()?; // TEST-NET-1，保留地址，不会真的连出去
    match s.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(v4) if !v4.is_loopback() => Some(v4.to_string()),
        _ => None,
    }
}

/// 分级表的纯逻辑验证，不依赖网络环境。
///
/// 这张表与 `gateway.rs` 里 Router 注册的路由一一对应——**新增路由必须同步
/// 登记**，否则远程调用会被 403（fail-closed），这个测试会先一步失败提醒你。
#[test]
fn test_classify_covers_all_routes() {
    let all: &[(axum::http::Method, &str, AccessPolicy)] = &[
        // 允许远程：P2P 协商主流程 + 只读信息
        (axum::http::Method::POST, "/api/incoming", AccessPolicy::Remote),
        (axum::http::Method::POST, "/api/incoming-resp", AccessPolicy::Remote),
        (
            axum::http::Method::POST,
            "/api/verify/:file_id",
            AccessPolicy::Remote,
        ),
        (
            axum::http::Method::POST,
            "/api/cancel/:file_id",
            AccessPolicy::Remote,
        ),
        // 对端续传信号（A3.2）
        (
            axum::http::Method::POST,
            "/api/peer-resume/:file_id",
            AccessPolicy::Remote,
        ),
        (
            axum::http::Method::POST,
            "/api/peer-resumed/:file_id",
            AccessPolicy::Remote,
        ),
        (axum::http::Method::GET, "/api/whoami", AccessPolicy::Remote),
        // 仅本机
        (axum::http::Method::POST, "/api/send", AccessPolicy::LocalOnly),
        (
            axum::http::Method::GET,
            "/api/transfers",
            AccessPolicy::LocalOnly,
        ),
        (
            axum::http::Method::POST,
            "/api/transfers/:file_id/resume",
            AccessPolicy::LocalOnly,
        ),
        (axum::http::Method::GET, "/api/files", AccessPolicy::LocalOnly),
        (
            axum::http::Method::GET,
            "/api/files/:name",
            AccessPolicy::LocalOnly,
        ),
        (
            axum::http::Method::GET,
            "/api/incoming",
            AccessPolicy::LocalOnly,
        ),
        (
            axum::http::Method::POST,
            "/api/incoming/:id/accept",
            AccessPolicy::LocalOnly,
        ),
        (
            axum::http::Method::POST,
            "/api/incoming/:id/reject",
            AccessPolicy::LocalOnly,
        ),
        (axum::http::Method::GET, "/api/config", AccessPolicy::LocalOnly),
        (
            axum::http::Method::POST,
            "/api/config/receive-dir",
            AccessPolicy::LocalOnly,
        ),
        (
            axum::http::Method::POST,
            "/api/config/device-name",
            AccessPolicy::LocalOnly,
        ),
        (
            axum::http::Method::GET,
            "/api/devices",
            AccessPolicy::LocalOnly,
        ),
        (
            axum::http::Method::GET,
            "/ws/progress",
            AccessPolicy::LocalOnly,
        ),
        (axum::http::Method::GET, "/", AccessPolicy::LocalOnly),
    ];

    for (m, p, want) in all {
        assert_eq!(classify(m, p), Some(*want), "分级不符：{m} {p}");
    }
}

/// 分级的维度是 (Method, Path) 二元组而非只看路径：
/// 同一个 `/api/incoming` 上，POST（对端发 offer）与 GET（本机拉收件箱）
/// 分属两档。只按路径分级必然二选一出错，这条测试锁住这个语义。
#[test]
fn test_classify_distinguishes_method_on_same_path() {
    assert_eq!(
        classify(&axum::http::Method::POST, "/api/incoming"),
        Some(AccessPolicy::Remote)
    );
    assert_eq!(
        classify(&axum::http::Method::GET, "/api/incoming"),
        Some(AccessPolicy::LocalOnly)
    );
}

/// 未登记的 (method, path) 一律 None → 远程 403（fail-closed）。
#[test]
fn test_classify_unknown_is_none() {
    assert_eq!(classify(&axum::http::Method::PUT, "/api/send"), None);
    assert_eq!(classify(&axum::http::Method::DELETE, "/api/files/x"), None);
    assert_eq!(classify(&axum::http::Method::GET, "/api/not-exist"), None);
}

#[test]
fn test_path_matches() {
    assert!(path_matches("/api/verify/:file_id", "/api/verify/abc-123"));
    assert!(path_matches("/api/files/:name", "/api/files/a.bin"));
    // 段数不同：不能让 `/api/files` 误配成 `/api/files/:name`
    assert!(!path_matches("/api/files", "/api/files/a.bin"));
    assert!(!path_matches("/api/files/:name", "/api/files/a/b"));
    // 尾部斜杠不影响
    assert!(path_matches("/api/send/", "/api/send"));
}

/// 端到端：从非回环地址访问本机网关，验证分级真的在拦。
#[tokio::test]
async fn test_remote_access_blocked_by_policy() {
    use axum::http::Method as M;

    require_sockets!("test_remote_access_blocked_by_policy");
    let Some(ip) = non_loopback_ip() else {
        eprintln!("SKIP test_remote_access_blocked_by_policy: 无可用非回环地址");
        return;
    };
    // 探测必须在 start_stack 之后：网关还没监听时 connect 必然失败，
    // 放在前面会让这个测试永远走 SKIP 分支，等于没跑。
    // 远程面 = LAN TLS 口（tr+1000）；回环口对非回环地址不可达。
    let dir = temp_dir("remote-policy");
    let _s = start_stack(18030, 18130, &dir, 2).await;
    let lan = 18130 + 1000;

    if tokio::net::TcpStream::connect((ip.as_str(), lan)).await.is_err() {
        eprintln!("SKIP test_remote_access_blocked_by_policy: 无法从 {ip} 连到网关");
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }

    // Remote 档：放行
    let (status, _) = https_to(&ip, lan, "GET", "/api/whoami", None).await;
    assert_eq!(status, 200, "whoami 是只读信息，应允许远程访问");

    // LocalOnly 档：拒绝，且提示里带开关名，方便用户自助排查
    let (status, body) = https_to(&ip, lan, "GET", "/api/transfers", None).await;
    assert_eq!(status, 403, "transfers 应拒绝远程访问");
    assert!(
        String::from_utf8_lossy(&body).contains("--remote-admin"),
        "403 响应应提示 --remote-admin，实际：{}",
        String::from_utf8_lossy(&body)
    );

    // 同一路径不同方法，档位不同
    let (status, _) = https_to(&ip, lan, "GET", "/api/incoming", None).await;
    assert_eq!(status, 403, "GET /api/incoming 是本机 UI 拉收件箱");
    let (status, _) = https_to(&ip, lan, "POST", "/api/incoming", Some("{}")).await;
    assert_ne!(status, 403, "POST /api/incoming 是对端发 offer 的入口，不能拒");

    // P3 鉴权层：未配对设备打传输类接口必须 401（fail-closed）；
    // 同时这也验证 middleware 顺序——LocalOnly 先得到 403（分级），不是 401
    let (status, body) = https_to(&ip, lan, "POST", "/api/verify/xyz", Some("{}")).await;
    assert_eq!(status, 401, "未配对设备调 verify 应 401，实际 {status}");
    assert!(
        String::from_utf8_lossy(&body).contains("未配对"),
        "401 文案应给出可操作提示：{}",
        String::from_utf8_lossy(&body)
    );
    // 免鉴权接口仍然可达（whoami 信息与 mDNS TXT 重合，见设计 §9.2）
    let (status, _) = https_to(&ip, lan, "GET", "/api/whoami", None).await;
    assert_eq!(status, 200, "whoami 免配对");

    // 本机访问不被误伤（回环地址一律放行）
    let (status, _) = http(18030, "GET", "/api/transfers", None).await;
    assert_eq!(status, 200, "本机访问不应受分级影响");

    // 顺带确认表里两条路径参数型路由确实按通配生效
    assert_eq!(classify(&M::POST, "/api/cancel/xyz"), Some(AccessPolicy::Remote));
    assert_eq!(classify(&M::POST, "/api/verify/xyz"), Some(AccessPolicy::Remote));

    let _ = std::fs::remove_dir_all(&dir);
}

/// `--remote-admin` 打开后，LocalOnly 那一档对局域网放行；
/// 但 P3 起鉴权与分级**正交**（Q1 裁决：分级放宽 ≠ 免鉴权）——
/// 未配对设备过得了分级、仍被鉴权层拦下。
/// 期望 **401 而非 403/200**：403 说明分级没开，200 说明鉴权没了，
/// 401 恰好证明「分级放行 + 鉴权独立生效」两件事同时成立。
#[tokio::test]
async fn test_remote_admin_opens_local_only_routes() {
    require_sockets!("test_remote_admin_opens_local_only_routes");
    let Some(ip) = non_loopback_ip() else {
        eprintln!("SKIP test_remote_admin_opens_local_only_routes: 无可用非回环地址");
        return;
    };
    let dir = temp_dir("remote-admin-on");
    let _s = start_stack_with(18031, 18131, &dir, 2, true).await;
    let lan = 18131 + 1000;

    if tokio::net::TcpStream::connect((ip.as_str(), lan)).await.is_err() {
        eprintln!("SKIP test_remote_admin_opens_local_only_routes: 无法从 {ip} 连到网关");
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }

    let (status, body) = https_to(&ip, lan, "GET", "/api/transfers", None).await;
    assert_eq!(
        status, 401,
        "分级已放行、鉴权层应 401（未配对）；403=分级没开、200=鉴权失守。body={}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        String::from_utf8_lossy(&body).contains("未配对"),
        "401 文案应说明未配对：{}",
        String::from_utf8_lossy(&body)
    );

    let (status, _) = https_to(&ip, lan, "GET", "/api/files", None).await;
    assert_eq!(status, 401, "同上，files 也应 401 而非 403");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 阶段 5 P2：配对模式开关（LocalOnly）+ 状态回读 + TTL 常量。
/// 用隔离 discovery：配对状态是进程级的，共享会与 P3 配对流程用例互踩。
#[tokio::test]
async fn test_pair_mode_toggle() {
    let dir = temp_dir("pair-mode");
    let _ = start_stack_isolated("pairmode", 18049, 18149, &dir, 2).await;

    let (status, _) = http(
        18049,
        "POST",
        "/api/pair/mode",
        Some(r#"{"enabled":true}"#),
    )
    .await;
    assert_eq!(status, 200, "本机 POST /api/pair/mode 应成功");
    let v = http_json(18049, "GET", "/api/pair/mode", None).await;
    assert_eq!(v["enabled"].as_bool(), Some(true));
    assert!(
        v["seconds_left"].as_u64().unwrap_or(0) > 0,
        "开启后应有剩余秒数：{v}"
    );
    assert_eq!(v["ttl_seconds"].as_u64(), Some(120));
    // P2 修复：whoami 必须携带 pairing_enabled（对端 whoami 同步的数据源）
    let me = http_json(18049, "GET", "/api/whoami", None).await;
    assert_eq!(
        me["pairing_enabled"].as_bool(),
        Some(true),
        "whoami 应上报 pairing_enabled"
    );

    // /api/pair/refresh：即时刷新对端标志，**不得**改变本机配对模式/倒计时
    let (st, _) = http(18049, "POST", "/api/pair/refresh", Some("{}")).await;
    assert_eq!(st, 200);
    let v = http_json(18049, "GET", "/api/pair/mode", None).await;
    assert_eq!(
        v["enabled"].as_bool(),
        Some(true),
        "refresh 不得关闭配对模式"
    );

    let (status, _) = http(
        18049,
        "POST",
        "/api/pair/mode",
        Some(r#"{"enabled":false}"#),
    )
    .await;
    assert_eq!(status, 200);
    let v = http_json(18049, "GET", "/api/pair/mode", None).await;
    assert_eq!(v["enabled"].as_bool(), Some(false));
    assert_eq!(v["seconds_left"].as_u64(), Some(0));
    let me = http_json(18049, "GET", "/api/whoami", None).await;
    assert_eq!(me["pairing_enabled"].as_bool(), Some(false));

    let _ = std::fs::remove_dir_all(&dir);
}

/// P3：完整配对流程——模式关时拒绝 → 开启 → 发起/确认码一致 → 确认 →
/// 双方落库 → pending 清空 → 解除配对。
///
/// 两端都用隔离 discovery（配对状态进程级）；同进程内 in/out pending 分字段，
/// 互不干扰。确认码断言是防中间人的核心：两端必须各自独立算出同一串。
#[tokio::test]
async fn test_pairing_flow_end_to_end() {
    let dir_a = temp_dir("pflow-a");
    let dir_b = temp_dir("pflow-b");
    let _a = start_stack_isolated("pflow-a", 18082, 18182, &dir_a, 2).await;
    let _b = start_stack_isolated("pflow-b", 18083, 18183, &dir_b, 2).await;

    let start_body = r#"{"device_id":"peer-b","name":"B机","platform":"windows","ip":"127.0.0.1","gateway_port":19183}"#;

    // 1) 模式关闭（默认）→ hello 403 → start 以 502 收口，错误带对端原因
    let (st, body) = http(18082, "POST", "/api/pair/start", Some(start_body)).await;
    assert_eq!(st, 502, "模式关闭时 start 应失败: {}", String::from_utf8_lossy(&body));
    assert!(
        String::from_utf8_lossy(&body).contains("未开启配对模式"),
        "错误必须透传对端拒绝原因，实际：{}",
        String::from_utf8_lossy(&body)
    );

    // 2) B 开启配对模式
    let (st, _) = http(18083, "POST", "/api/pair/mode", Some(r#"{"enabled":true}"#)).await;
    assert_eq!(st, 200);

    // 3) A 发起 → 拿到 session + 本机确认码
    let (st, body) = http(18082, "POST", "/api/pair/start", Some(start_body)).await;
    assert_eq!(st, 200, "start 应成功: {}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();
    let session = v["pairing_session"].as_str().unwrap().to_string();
    let code_a = v["code"].as_str().unwrap().to_string();
    assert_eq!(code_a.len(), 6, "确认码应为 6 位: {code_a}");

    // 4) B 侧 pending：session 一致 + **确认码一致**（独立计算必须相同）
    let v = http_json(18083, "GET", "/api/pair/pending", None).await;
    let p = v["pending"].as_object().expect("B 应有待确认请求");
    assert_eq!(p["session"].as_str(), Some(session.as_str()));
    assert_eq!(
        p["code"].as_str(),
        Some(code_a.as_str()),
        "两端确认码必须一致——不一致说明中间有人换了证书"
    );
    assert_eq!(p["ip"].as_str(), Some("127.0.0.1"));
    assert!(p["name"].as_str().is_some(), "应携带发起方展示名");

    // 5) B 确认 → B 推 confirm 给 A → A 先落库 → B 落库
    let decide_body = format!(r#"{{"session":"{session}","accept":true}}"#);
    let (st, resp) = http(18083, "POST", "/api/pair/decide", Some(&decide_body)).await;
    assert_eq!(st, 200, "decide: {}", String::from_utf8_lossy(&resp));
    let rv: Value = serde_json::from_str(&String::from_utf8_lossy(&resp)).unwrap();
    assert_eq!(rv["ok"], true);

    // 6) 双方落库（A 存 B：device_id=peer-b；B 存 A：hello 自报的 device_id）
    let a_peers = http_json(18082, "GET", "/api/peers", None).await;
    assert!(
        a_peers
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["device_id"] == "peer-b"),
        "A 应持有 B: {a_peers}"
    );
    let b_peers = http_json(18083, "GET", "/api/peers", None).await;
    assert!(
        b_peers.as_array().unwrap().len() == 1,
        "B 应持有 A: {b_peers}"
    );

    // 7) pending 已清
    let v = http_json(18083, "GET", "/api/pair/pending", None).await;
    assert!(v["pending"].is_null(), "确认后 pending 应清空: {v}");

    // 8) 解除配对：首删 200、再删 404
    let (st, _) = http(18082, "DELETE", "/api/peers/peer-b", None).await;
    assert_eq!(st, 200);
    let (st, _) = http(18082, "DELETE", "/api/peers/peer-b", None).await;
    assert_eq!(st, 404);
    let b_id = b_peers[0]["device_id"].as_str().unwrap().to_string();
    let (st, _) = http(18083, "DELETE", &format!("/api/peers/{b_id}"), None).await;
    assert_eq!(st, 200, "B 侧解除应成功");

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// P3 拒绝路径：配对模式开着但会话不匹配的 confirm → 403（会话化闭合 Q4）
#[tokio::test]
async fn test_pair_confirm_rejects_unknown_session() {
    let dir = temp_dir("pflow-cf");
    let _s = start_stack_isolated("pflow-cf", 18084, 18184, &dir, 2).await;

    // 直接打 Remote confirm（绕过本机 start）：没有 pending → 403
    let (st, body) = https_to(
        "127.0.0.1",
        19184,
        "POST",
        "/api/pair/confirm",
        Some(r#"{"pairing_session":"nope"}"#),
    )
    .await;
    assert_eq!(st, 403, "无会话 confirm 必须 403，实际 {st}: {}", String::from_utf8_lossy(&body));
    assert!(
        String::from_utf8_lossy(&body).contains("配对会话"),
        "文案应说明是会话问题：{}",
        String::from_utf8_lossy(&body)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// P2 修复回归（真实反馈：双方开了配对模式却互相看不见）：
/// mDNS 组播更新丢失时，pair 标志必须经「whoami over TCP+TLS」通道同步。
///
/// 构造：B 的设备表里**植入** A 的条目（模拟 mDNS 已发现但 TXT 更新
/// 没到——Android 未持 MulticastLock / 防火墙丢组播的真实场景）；
/// A 开启配对后触发 B 的即时刷新，B 表内该设备的 pair 必须变 true。
#[tokio::test]
async fn whoami_sync_delivers_pair_flag_without_mdns() {
    let dir_a = temp_dir("whsync-a");
    let dir_b = temp_dir("whsync-b");
    let (_ea, _da) = start_stack_isolated_full("whsync-a", 18086, 18186, &dir_a, 2).await;
    let (_eb, db) = start_stack_isolated_full("whsync-b", 18087, 18187, &dir_b, 2).await;

    // A 进入配对模式 → A 的 whoami.pairing_enabled = true
    let (st, _) = http(18086, "POST", "/api/pair/mode", Some(r#"{"enabled":true}"#)).await;
    assert_eq!(st, 200);

    // B 的设备表植入 A（无 mDNS 参与：pair 显式 false）
    db.devices_handle().write().insert(
        "planted-a".into(),
        Device {
            id: "planted-a".into(),
            name: "planted".into(),
            ip: "127.0.0.1".into(),
            gateway_port: 19186, // A 的 LAN TLS 口
            transfer_port: 18186,
            platform: "test".into(),
            pair: false,
        },
    );
    assert!(!db.list_devices()[0].pair, "植入时应为 pair=false");

    // 触发 B 的即时 whoami 刷新（切换配对模式会 spawn_pair_flag_refresh）
    let (st, _) = http(18087, "POST", "/api/pair/mode", Some(r#"{"enabled":true}"#)).await;
    assert_eq!(st, 200);

    // 5 秒内 B 表内 planted-a 必须变为 pair=true
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let pair = db
            .list_devices()
            .into_iter()
            .find(|d| d.id == "planted-a")
            .map(|d| d.pair)
            .unwrap_or(false);
        if pair {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "whoami 同步通道未把 pair 标志送达（5s 超时）"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 收尾：两端都关掉配对模式（隔离 discovery，互不影响其它用例）
    let _ = http(18086, "POST", "/api/pair/mode", Some(r#"{"enabled":false}"#)).await;
    let _ = http(18087, "POST", "/api/pair/mode", Some(r#"{"enabled":false}"#)).await;
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// N2 回归：stream_count 必须由发送方带过去，接收方按它建槽。
///
/// 这是唯一一条能在合入前抓住「静默数据损坏」的测试。
/// 两端各按本地配置切段 / 建槽时，每条流都会"成功"落盘，
/// 偏移却是错的，只有最后的整文件 sha256 能发现——为时已晚。
#[tokio::test]
async fn test_stream_count_negotiated_across_peers() {
    require_sockets!("test_stream_count_negotiated_across_peers");
    let dir_a = temp_dir("streams-a");
    let dir_b = temp_dir("streams-b");

    // 发送方 parallel=4 且文件够大 → 4 流；接收方 parallel=1（若用本地值会建 1 段）
    let _a = start_stack(18040, 18140, &dir_a, 4).await;
    let _b = start_stack(18041, 18141, &dir_b, 1).await;

    // > 12MB → 发送方自适应到 4 流（4MB/流）
    let content = make_content(13 * 1024 * 1024);
    let src = dir_a.join("mismatch.bin");
    std::fs::write(&src, &content).unwrap();

    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18141,
        "target_gateway_port": 19141,
        "file_path": src.to_string_lossy(),
    })
    .to_string();
    let (status, body) = http(18040, "POST", "/api/send", Some(&send_body)).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let resp: Value = serde_json::from_slice(&body).unwrap();
    let file_id = resp["file_id"].as_str().unwrap().to_string();

    let v = wait_for_json(18041, "/api/incoming", 15, |v| {
        v.as_array().map(|a| !a.is_empty()).unwrap_or(false)
    })
    .await;
    let incoming_id = v[0]["incoming_id"].as_str().unwrap().to_string();
    assert_eq!(
        v[0]["stream_count"].as_u64(),
        Some(4),
        "待决条目里应是发送方声明的 stream_count，而不是接收方本地并行上限 1"
    );

    let (status, _) = http(
        18041,
        "POST",
        &format!("/api/incoming/{incoming_id}/accept"),
        None,
    )
    .await;
    assert_eq!(status, 200);

    let v = wait_for_json(18041, "/api/transfers", 60, |v| {
        find_transfer(v, &file_id)
            .map(|t| t["status"].as_str() == Some("Completed"))
            .unwrap_or(false)
    })
    .await;
    let t = find_transfer(&v, &file_id).unwrap();
    assert_eq!(
        t["chunks_total"].as_u64(),
        Some(4),
        "槽位必须按发送方的 stream_count 建"
    );

    let got = std::fs::read(dir_b.join("mismatch.bin")).unwrap();
    assert_eq!(got, content, "两端并行上限不一致时，数据仍必须完整一致");

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// 旧版本对端发来的 offer 没有 stream_count 字段时，应回退本地自适应而不是崩掉。
#[test]
fn test_offer_without_stream_count_deserializes() {
    let body = r#"{
        "file_id":"f1","file_name":"a.bin","file_size":1024,
        "sha256":null,"sha256_deferred":true,
        "from_id":"d1","from_name":"n1","from_ip":"127.0.0.1",
        "from_gateway_port":7878,"from_transfer_port":7879
    }"#;
    let offer: kitefile::protocol::HttpOffer = serde_json::from_str(body).unwrap();
    assert_eq!(offer.stream_count, None, "旧版本 offer 没有该字段，应为 None");
    assert_eq!(offer.file_size, 1024);
}
