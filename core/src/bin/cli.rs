//! CLI 入口：用于测试核心引擎，无需 UI
//!
//! 用法：
//!     kitefile-cli daemon [--remote-admin]   启动守护进程（发现 + 接收 + HTTP 网关）
//!     kitefile-cli list-devices              列出已发现的设备
//!     kitefile-cli send <ip> <path>          向对端发送文件
//!
//! `--remote-admin`：把「仅本机」那一档 HTTP 接口（发文件 / 读接收目录 / 改配置）
//! 也对局域网放开，用于开发期拿一台设备遥控另一台。默认关闭。
//! **开着意味着局域网内任何人都能让本机外传文件并读取接收目录**，
//! 只在可信网络里临时开。

// Windows release：编译成 GUI 子系统，桌面端 Process.start 拉起时不会弹终端。
// Debug 保持 console，方便开发期直接看 stdout。
// 注意：`windows_subsystem` 是 crate 级属性，必须放在文件最顶。
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

use std::sync::Arc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Release Windows 无控制台：tracing 改写 daemon.log，否则磁盘上无线索
    #[cfg(all(windows, not(debug_assertions)))]
    {
        let dir = kitefile::platform::default_receive_dir();
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("daemon.log");
        let filter = EnvFilter::from_default_env()
            .add_directive("kitefile=info".parse().unwrap_or_else(|_| "info".parse().unwrap()));
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(file) => {
                tracing_subscriber::fmt()
                    .with_env_filter(filter)
                    .with_writer(std::sync::Mutex::new(file))
                    .init();
            }
            Err(_) => {
                tracing_subscriber::fmt().with_env_filter(filter).init();
            }
        }
    }
    #[cfg(not(all(windows, not(debug_assertions))))]
    {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env().add_directive("kitefile=info".parse()?),
            )
            .init();
    }

    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("daemon") => {
            let remote_admin = args.iter().skip(2).any(|a| a == "--remote-admin");
            run_daemon(remote_admin).await
        }
        Some("list-devices") => list_devices().await,
        Some("send") => {
            let ip = args.get(2).cloned().ok_or_else(|| anyhow::anyhow!("usage: send <ip> <path>"))?;
            let path = args.get(3).cloned().ok_or_else(|| anyhow::anyhow!("usage: send <ip> <path>"))?;
            send(ip, path).await
        }
        _ => {
            eprintln!("kitefile-cli <daemon [--remote-admin] | list-devices | send <ip> <path>>");
            Ok(())
        }
    }
}

async fn run_daemon(allow_remote_admin: bool) -> anyhow::Result<()> {
    let mut config = kitefile::EngineConfig::default();
    config.allow_remote_admin = allow_remote_admin;

    // 端口退避：默认端口可能落在系统保留区间（Windows 上 Hyper-V / WSL /
    // Docker 会动态保留成片端口），bind 失败报 os error 10013。
    // 必须在构造 discovery / transfer 之前定下来——mDNS 的 TXT 会把实际端口
    // 广播出去，对端靠它建连。
    // 回环口（UI 用）按 127.0.0.1 探测，LAN TLS 口按 0.0.0.0 探测，
    // 与实际 bind 地址一致（P1 端口重划）。
    let gateway_port =
        kitefile::pick_available_port_on("127.0.0.1", kitefile::GATEWAY_PORT_CANDIDATES)
            .unwrap_or_else(|| os_pick_port(config.gateway_port));
    let lan_tls_port = kitefile::pick_available_port(kitefile::LAN_TLS_PORT_CANDIDATES)
        .unwrap_or_else(|| os_pick_port(config.lan_tls_port));
    let transfer_port = kitefile::pick_available_port(kitefile::TRANSFER_PORT_CANDIDATES)
        .unwrap_or_else(|| os_pick_port(config.transfer_port));
    if gateway_port != config.gateway_port
        || lan_tls_port != config.lan_tls_port
        || transfer_port != config.transfer_port
    {
        warn!(
            gateway_port,
            lan_tls_port,
            transfer_port, "default ports unavailable, fell back"
        );
        config.gateway_port = gateway_port;
        config.lan_tls_port = lan_tls_port;
        config.transfer_port = transfer_port;
    }

    // 身份持久化：id/名称复用上次的，重启后 mDNS 注册同一服务实例，
    // 对端设备表按 id 覆盖同一条记录（否则每次重启都被当成“新设备”）
    let identity = kitefile::discovery::load_or_create_identity(
        &config.receive_dir,
        &config.device_name,
    );
    config.device_name = identity.name.clone();

    // mDNS TXT 广播的是 LAN TLS 端口：对端拿它建跨机连接
    let discovery = Arc::new(kitefile::DiscoveryService::new(
        config.device_name.clone(),
        identity.id,
        config.lan_tls_port,
        config.transfer_port,
        Some(kitefile::discovery::identity_marker_path(&config.receive_dir)),
    )?);

    let transfer = Arc::new(kitefile::TransferEngine::new(
        config.transfer_port,
        config.parallel_streams,
        config.receive_dir.clone(),
    ));
    // 信任上下文（P4）：数据面成员校验 + 出站 pin；须在 spawn_receiver 之前
    transfer.set_trust(discovery.devices_handle());

    Arc::clone(&transfer).spawn_receiver().await?;
    Arc::clone(&discovery).spawn_event_loop(tokio::runtime::Handle::current());

    let gateway = kitefile::HttpGateway::new(discovery, transfer, Arc::new(config.clone()));
    gateway.run().await?;
    Ok(())
}

async fn list_devices() -> anyhow::Result<()> {
    let config = kitefile::EngineConfig::default();
    let self_id = "list-only".to_string();
    let discovery = Arc::new(kitefile::DiscoveryService::new(
        config.device_name,
        self_id,
        config.lan_tls_port,
        config.transfer_port,
        None, // 一次性查询实例，无需身份持久化
    )?);
    Arc::clone(&discovery).spawn_event_loop(tokio::runtime::Handle::current());

    // 监听 5 秒
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let devices = discovery.list_devices();
    if devices.is_empty() {
        println!("(no devices discovered)");
    } else {
        for d in devices {
            println!(
                "{}\t{}\t{}:{}\tplatform={}",
                d.id, d.name, d.ip, d.transfer_port, d.platform
            );
        }
    }
    Ok(())
}

/// 一次 TLS HTTP GET，只取响应体。失败一律返回 None（调用方继续试下一个端口）。
async fn http_get_body(
    host: &str,
    port: u16,
    path: &str,
    receive_dir: &std::path::Path,
) -> Option<String> {
    kitefile::httpc::http_get_tls(host, port, path, receive_dir, None)
        .await
        .ok()
}

/// 探测对端真实在用的一对端口。
///
/// 对端可能因为默认端口被系统保留而退避到备选，所以不能假定 7880/7879。
/// 依次试候选 **LAN TLS** 端口（回环口对端不可达），命中后从 whoami 响应里
/// 读出它实际通告的端口。
async fn probe_peer_ports(ip: &str, receive_dir: &std::path::Path) -> anyhow::Result<(u16, u16)> {
    for &p in kitefile::LAN_TLS_PORT_CANDIDATES {
        let Some(body) = http_get_body(ip, p, "/api/whoami", receive_dir).await else {
            continue;
        };
        let Ok(j) = serde_json::from_str::<serde_json::Value>(&body) else {
            continue;
        };
        let gw = j
            .get("gateway_port")
            .and_then(|v| v.as_u64())
            .unwrap_or(p as u64) as u16;
        if let Some(tp) = j.get("transfer_port").and_then(|v| v.as_u64()) {
            return Ok((gw, tp as u16));
        }
    }
    Err(anyhow::anyhow!(
        "peer {ip} 在候选端口 {:?} 上都没有响应，确认对端 daemon 已启动且在同一网段",
        kitefile::LAN_TLS_PORT_CANDIDATES
    ))
}

/// 候选全被占用时的兜底：让 OS 随机分配（bind port 0 必成功）
fn os_pick_port(preferred: u16) -> u16 {
    match std::net::TcpListener::bind(("0.0.0.0", 0)) {
        Ok(l) => l.local_addr().map(|a| a.port()).unwrap_or(preferred),
        Err(_) => preferred,
    }
}

async fn send(ip: String, path: String) -> anyhow::Result<()> {
    let mut config = kitefile::EngineConfig::default();

    // CLI send 需要本机 gateway 可达（接收方回包 /api/incoming-resp 会打过来），
    // 因此拉起完整栈：discovery + receiver + gateway。
    // 若本机已有 daemon 占用默认端口，则退避到备选端口。
    // 退避：CLI 常和常驻 daemon 同时存在，两边都要能起得来
    let gateway_port =
        kitefile::pick_available_port_on("127.0.0.1", kitefile::GATEWAY_PORT_CANDIDATES)
            .unwrap_or_else(|| os_pick_port(config.gateway_port));
    let lan_tls_port = kitefile::pick_available_port(kitefile::LAN_TLS_PORT_CANDIDATES)
        .unwrap_or_else(|| os_pick_port(config.lan_tls_port));
    let transfer_port = kitefile::pick_available_port(kitefile::TRANSFER_PORT_CANDIDATES)
        .unwrap_or_else(|| os_pick_port(config.transfer_port));
    if gateway_port != config.gateway_port
        || lan_tls_port != config.lan_tls_port
        || transfer_port != config.transfer_port
    {
        warn!(
            gateway_port,
            lan_tls_port,
            transfer_port, "default ports unavailable, fell back"
        );
    }
    config.gateway_port = gateway_port;
    config.lan_tls_port = lan_tls_port;
    config.transfer_port = transfer_port;

    let self_id = format!("cli-{}", uuid::Uuid::new_v4().simple());
    let discovery = Arc::new(kitefile::DiscoveryService::new(
        config.device_name.clone(),
        self_id.clone(),
        lan_tls_port,
        transfer_port,
        None, // 一次性 CLI 发送，临时身份即可
    )?);
    let transfer = Arc::new(kitefile::TransferEngine::new(
        transfer_port,
        config.parallel_streams,
        config.receive_dir.clone(),
    ));
    // 信任上下文（P4）：数据面成员校验 + 出站 pin
    transfer.set_trust(discovery.devices_handle());

    transfer.clone().spawn_receiver().await?;
    discovery.clone().spawn_event_loop(tokio::runtime::Handle::current());

    // gateway 后台运行（主要用途：接收 /api/incoming-resp 回包）
    let gateway = kitefile::HttpGateway::new(discovery.clone(), transfer.clone(), Arc::new(config.clone()));
    tokio::spawn(async move {
        if let Err(e) = gateway.run().await {
            tracing::error!(error = %e, "gateway exited");
        }
    });

    // 回包地址必须是真实本机 IP（不能用 127.0.0.1，跨机时对端无法回包）
    let self_ip = discovery.self_ip().unwrap_or_else(|| "127.0.0.1".into());

    // 对端端口**不能写死**：它也可能因为默认端口被系统保留而退避过。
    // 依次试候选 LAN TLS 端口，从 /api/whoami 里读它真实在用的端口。
    let (peer_gateway_port, peer_transfer_port) =
        probe_peer_ports(&ip, &config.receive_dir).await?;
    info!(
        peer = %ip,
        gateway_port = peer_gateway_port,
        transfer_port = peer_transfer_port,
        "peer ports resolved"
    );

    let mut handle = transfer
        .send_file(
            ip,
            peer_transfer_port,
            peer_gateway_port,
            std::path::PathBuf::from(path),
            None,
            self_id,
            config.device_name.clone(),
            self_ip,
            // 对端回包走本机的 LAN TLS 口（回环口对端不可达）
            lan_tls_port,
            // CLI 一次只发一个文件，不涉及批次
            None,
        )
        .await?;

    while let Some(p) = handle.next_progress().await {
        println!(
            "[{:?}] {:>10} / {:<10}  {:.2}%  {:.2} MB/s",
            p.status,
            p.bytes_transferred,
            p.file_size,
            if p.file_size > 0 {
                p.bytes_transferred as f64 / p.file_size as f64 * 100.0
            } else {
                0.0
            },
            p.speed_bps as f64 / 1_048_576.0
        );
    }
    Ok(())
}
