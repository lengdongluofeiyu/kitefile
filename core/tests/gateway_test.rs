//! Gateway 集成测试：覆盖全部 HTTP / WebSocket 接口
//!
//! 每个测试用独立端口对（gateway/transfer），支持并行运行。
//! 全链路测试：A 发送 → B 确认 → 多流传输 → 校验落盘 → 双端进度。

use ftcore::{DiscoveryService, EngineConfig, HttpGateway, TransferEngine};
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
    let dir = std::env::temp_dir().join(format!("ftcore-gw-test-{}-{}", tag, std::process::id()));
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
    chunk_size: usize,
    parallel: usize,
) -> Arc<TransferEngine> {
    let config = EngineConfig {
        device_name: format!("test-{}", gw_port),
        gateway_port: gw_port,
        transfer_port: tr_port,
        parallel_streams: parallel,
        chunk_size,
        receive_dir: recv_dir.to_path_buf(),
    };
    let discovery = shared_discovery();
    let transfer = Arc::new(TransferEngine::new(
        tr_port,
        parallel,
        chunk_size,
        recv_dir.to_path_buf(),
    ));
    transfer.clone().spawn_receiver().await.unwrap();

    let gateway = HttpGateway::new(discovery, transfer.clone(), Arc::new(config));
    tokio::spawn(async move {
        let _ = gateway.run(gw_port).await;
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
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let body_bytes = body.map(|b| b.as_bytes().to_vec()).unwrap_or_default();
    let req = format!(
        "{} {} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        method, path, port, body_bytes.len()
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
    let _ = start_stack(18001, 18101, &dir, 65536, 2).await;

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
    let _ = start_stack(18002, 18102, &dir, 65536, 2).await;

    // 设备列表：合法 JSON 数组（本机单栈场景可能为空）
    let v = http_json(18002, "GET", "/api/devices", None).await;
    assert!(v.is_array());

    // 根路由
    let (status, body) = http(18002, "GET", "/", None).await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("ftcore gateway"));

    let _ = std::fs::remove_dir_all(&dir);
}

// ============ 文件列表 / 下载 ============

#[tokio::test]
async fn test_files_list_and_download() {
    require_sockets!("test_files_list_and_download");
    let dir = temp_dir("files");
    let _ = start_stack(18003, 18103, &dir, 65536, 2).await;

    let content = b"hello-ftcore-download".to_vec();
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
    let _ = start_stack(18004, 18104, &dir, 65536, 2).await;

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
    let _ = start_stack(18005, 18105, &dir, 65536, 2).await;

    let (status, _) = http(18005, "POST", "/api/cancel/nonexistent-id", Some("{}")).await;
    assert_eq!(status, 404);

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
    let _ = start_stack(18006, 18106, &dir, 65536, 2).await;

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
    let _ = start_stack(18007, 18107, &dir, 65536, 2).await;

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

    // chunk 64KB、2 并行流：150KB 文件 → 3 chunks，覆盖多流 + 末块不满
    let _a = start_stack(18010, 18110, &dir_a, 65536, 2).await;
    let _b = start_stack(18011, 18111, &dir_b, 65536, 2).await;

    let content = make_content(150_000);
    let src = dir_a.join("hello.bin");
    std::fs::write(&src, &content).unwrap();

    // A 发起发送
    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18111,
        "target_gateway_port": 18011,
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
    // sha256 延后补发：发送方后台并行计算，offer 不等它（大文件弹窗即时出现）。
    // 因此 offer 里 sha256 为空、sha256_deferred = true；真正的值在全部 chunk ACK
    // 后由 POST /api/verify/:file_id 补发，接收方校验通过才 finalize。
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
    assert_eq!(t["bytes_transferred"].as_u64(), Some(150_000));
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

    let _a = start_stack(18012, 18112, &dir_a, 65536, 2).await;
    let _b = start_stack(18013, 18113, &dir_b, 65536, 2).await;

    let content = make_content(50_000);
    let src = dir_a.join("reject.bin");
    std::fs::write(&src, &content).unwrap();

    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18113,
        "target_gateway_port": 18013,
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

    let _a = start_stack(18014, 18114, &dir_a, 65536, 2).await;
    let _b = start_stack(18015, 18115, &dir_b, 65536, 2).await;

    let content = make_content(50_000);
    let src = dir_a.join("cancel.bin");
    std::fs::write(&src, &content).unwrap();

    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18115,
        "target_gateway_port": 18015,
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

    // B 未产生文件
    assert!(!dir_b.join("cancel.bin").exists());

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ============ 取消：接收方取消已接受的传输（含通知发送方联动） ============

#[tokio::test]
async fn test_receiver_cancel_notifies_sender() {
    require_sockets!("test_receiver_cancel_notifies_sender");
    let dir_a = temp_dir("rcancel-a");
    let dir_b = temp_dir("rcancel-b");

    // A 用小 chunk、单流 → 传输较慢，保证取消发生在传输中
    let _a = start_stack(18016, 18116, &dir_a, 8192, 1).await;
    let _b = start_stack(18017, 18117, &dir_b, 8192, 1).await;

    let content = make_content(300_000);
    let src = dir_a.join("big.bin");
    std::fs::write(&src, &content).unwrap();

    let send_body = json!({
        "target_ip": "127.0.0.1",
        "target_port": 18117,
        "target_gateway_port": 18017,
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

    // 等 B 侧接收进度出现（接收槽已建立）
    let _ = wait_for_json(18017, "/api/transfers", 15, |v| {
        find_transfer(v, &file_id).is_some()
    })
    .await;

    // B（接收方）取消 → 200
    let (status, _) = http(18017, "POST", &format!("/api/cancel/{file_id}"), Some("{}")).await;
    assert_eq!(status, 200);

    // B 侧视角 Canceled
    let v = wait_for_json(18017, "/api/transfers", 15, |v| {
        find_transfer(v, &file_id)
            .map(|t| t["status"].as_str() == Some("Canceled"))
            .unwrap_or(false)
    })
    .await;
    let t = find_transfer(&v, &file_id).unwrap();
    assert!(t["incoming"].as_bool().unwrap());

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
    let _ = start_stack(18020, 18120, &dir, 65536, 2).await;

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
