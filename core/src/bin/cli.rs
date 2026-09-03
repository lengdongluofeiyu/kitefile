//! CLI 入口：用于测试核心引擎，无需 UI
//!
//! 用法：
//!     ftcore-cli daemon              启动守护进程（发现 + 接收 + HTTP 网关）
//!     ftcore-cli list-devices        列出已发现的设备
//!     ftcore-cli send <ip> <path>    向对端发送文件

use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("ftcore=info".parse()?))
        .init();

    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("daemon") => run_daemon().await,
        Some("list-devices") => list_devices().await,
        Some("send") => {
            let ip = args.get(2).cloned().ok_or_else(|| anyhow::anyhow!("usage: send <ip> <path>"))?;
            let path = args.get(3).cloned().ok_or_else(|| anyhow::anyhow!("usage: send <ip> <path>"))?;
            send(ip, path).await
        }
        _ => {
            eprintln!("ftcore-cli <daemon | list-devices | send <ip> <path>>");
            Ok(())
        }
    }
}

async fn run_daemon() -> anyhow::Result<()> {
    let mut config = ftcore::EngineConfig::default();

    // 身份持久化：id/名称复用上次的，重启后 mDNS 注册同一服务实例，
    // 对端设备表按 id 覆盖同一条记录（否则每次重启都被当成“新设备”）
    let identity = ftcore::discovery::load_or_create_identity(
        &config.receive_dir,
        &config.device_name,
    );
    config.device_name = identity.name.clone();

    let discovery = Arc::new(ftcore::DiscoveryService::new(
        config.device_name.clone(),
        identity.id,
        config.gateway_port,
        config.transfer_port,
        Some(ftcore::discovery::identity_marker_path(&config.receive_dir)),
    )?);

    let transfer = Arc::new(ftcore::TransferEngine::new(
        config.transfer_port,
        config.parallel_streams,
        config.chunk_size,
        config.receive_dir.clone(),
    ));

    Arc::clone(&transfer).spawn_receiver().await?;
    Arc::clone(&discovery).spawn_event_loop(tokio::runtime::Handle::current());

    let gateway = ftcore::HttpGateway::new(discovery, transfer, Arc::new(config));
    gateway.run(7878).await?;
    Ok(())
}

async fn list_devices() -> anyhow::Result<()> {
    let config = ftcore::EngineConfig::default();
    let self_id = "list-only".to_string();
    let discovery = Arc::new(ftcore::DiscoveryService::new(
        config.device_name,
        self_id,
        config.gateway_port,
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

/// 选一个空闲端口：优先 preferred，被占用时依次尝试备选
fn pick_free_port(preferred: u16, alternatives: &[u16]) -> u16 {
    let mut candidates = vec![preferred];
    candidates.extend_from_slice(alternatives);
    for p in candidates {
        if std::net::TcpListener::bind(("0.0.0.0", p)).is_ok() {
            return p;
        }
    }
    // 全部被占用：让 OS 随机分配（TcpListener::bind port 0 必成功）
    match std::net::TcpListener::bind(("0.0.0.0", 0)) {
        Ok(l) => l.local_addr().map(|a| a.port()).unwrap_or(preferred),
        Err(_) => preferred,
    }
}

async fn send(ip: String, path: String) -> anyhow::Result<()> {
    let mut config = ftcore::EngineConfig::default();

    // CLI send 需要本机 gateway 可达（接收方回包 /api/incoming-resp 会打过来），
    // 因此拉起完整栈：discovery + receiver + gateway。
    // 若本机已有 daemon 占用默认端口，则退避到备选端口。
    let gateway_port = pick_free_port(config.gateway_port, &[17878, 27878]);
    let transfer_port = pick_free_port(config.transfer_port, &[17879, 27879]);
    config.gateway_port = gateway_port;
    config.transfer_port = transfer_port;

    let self_id = format!("cli-{}", uuid::Uuid::new_v4().simple());
    let discovery = Arc::new(ftcore::DiscoveryService::new(
        config.device_name.clone(),
        self_id.clone(),
        gateway_port,
        transfer_port,
        None, // 一次性 CLI 发送，临时身份即可
    )?);
    let transfer = Arc::new(ftcore::TransferEngine::new(
        transfer_port,
        config.parallel_streams,
        config.chunk_size,
        config.receive_dir.clone(),
    ));

    transfer.clone().spawn_receiver().await?;
    discovery.clone().spawn_event_loop(tokio::runtime::Handle::current());

    // gateway 后台运行（主要用途：接收 /api/incoming-resp 回包）
    let gateway = ftcore::HttpGateway::new(discovery.clone(), transfer.clone(), Arc::new(config.clone()));
    tokio::spawn(async move {
        if let Err(e) = gateway.run(gateway_port).await {
            tracing::error!(error = %e, "gateway exited");
        }
    });

    // 回包地址必须是真实本机 IP（不能用 127.0.0.1，跨机时对端无法回包）
    let self_ip = discovery.self_ip().unwrap_or_else(|| "127.0.0.1".into());

    let mut handle = transfer
        .send_file(
            ip,
            // 对端端口（目标机未改配置时为默认值）
            7879,
            7878,
            std::path::PathBuf::from(path),
            None,
            self_id,
            config.device_name.clone(),
            self_ip,
            gateway_port,
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
