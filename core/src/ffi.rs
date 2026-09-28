//! FFI 绑定层（预留 iOS / macOS / Android NDK 集成用）
//!
//! 当 Flutter / 原生端需要直接把 Rust 核心嵌入应用（无需独立守护进程）时，
//! 通过本模块暴露 C ABI 函数，配合 Dart `dart:ffi` 调用。
//!
//! 注意：
//! - 当前桌面端选择"应用 + 守护进程"模式（通过 HTTP 通信），更易调试
//! - 嵌入式集成（iOS / Android 资源约束）使用本 FFI 接口
//! - 函数命名约定：`kitefile_*`，参数全部为 C 兼容类型
//! - 字符串：传入 const char*（UTF-8），返回 const char* 由调用方负责释放
//!
//! 构建为动态库：在 Cargo.toml 中 `crate-type = ["cdylib", "rlib"]`
//! Flutter 端通过 `flutter_rust_bridge` 或手写 `dart:ffi` binding 调用。

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::runtime::Runtime;
use tracing::{error, info, warn};

use crate::{EngineConfig, HttpGateway, TransferEngine};
use crate::discovery::DiscoveryService;

/// FFI 侧日志：写入 `<receive_dir>/daemon.log`（同时同步一份到 stdout）。
///
/// 为什么必须有：Android 上 FFI 此前**零 tracing 订阅器**，所有
/// `warn!/error!`（端口绑定失败、gateway 启动失败……）全部静默——
/// 真实反馈「手机端内嵌守护进程未就绪」只见结果、查无原因。
///
/// - 启动时若旧日志 >1MB 先截断（诊断只需要最近一次启动的现场）
/// - 目录不可写时退化为仅 stdout（**写不进目录本身往往就是故障现场**，
///   此时 receive_dir 的权限问题会由 NodeIdentity 的降级路径兜住并记日志）
/// - 过滤 `kitefile=info`（与 cli 一致；设备上设 RUST_LOG 可覆盖）
fn init_ffi_logging(receive_dir: &std::path::Path) {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let log_path = receive_dir.join("daemon.log");
        let _ = std::fs::create_dir_all(receive_dir);
        if let Ok(md) = std::fs::metadata(&log_path) {
            if md.len() > 1_048_576 {
                let _ = std::fs::write(&log_path, b"");
            }
        }

        struct LogWriter {
            path: std::path::PathBuf,
        }
        impl std::io::Write for LogWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                use std::io::Write as _;
                // stdout：能被 adb logcat 抓到就多一条通道（抓不到不影响文件）
                let _ = std::io::stdout().write_all(buf);
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?;
                f.write_all(buf)?;
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let filter = tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("kitefile=info".parse().expect("static directive"));
        let writer_path = log_path.clone();
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(move || LogWriter {
                path: writer_path.clone(),
            })
            .try_init();
    });
}

/// FFI 上下文句柄（不透明指针）
pub struct FfiContext {
    pub runtime_handle: tokio::runtime::Handle,
    /// 进程内 tokio 运行时：shutdown 时随上下文一起丢弃，
    /// 从而中止 gateway / receiver / discovery 等 spawn 出去的任务。
    pub runtime: tokio::runtime::Runtime,
    pub discovery: Arc<DiscoveryService>,
    pub transfer: Arc<TransferEngine>,
    pub config: Arc<EngineConfig>,
}

// 全局单例（FFI 简化为单例；如需多实例可改为返回句柄）
//
// 用 Mutex 而不是 `static mut`：后者在多线程下读写是数据竞争（Rust 2024 起
// 连"取一个共享引用"都会告警），而守护进程里的 gateway 线程与调用方线程
// 会同时碰这个变量。
//
// **没有 OnceLock 门闩**（A3.7）：init/shutdown 必须对称——shutdown 把槽位置回
// None 后，下一次 init 要能完整重建上下文。OnceLock「只准写一次」会让
// 二次 init 永远拿不到新上下文（daemon 起不来），已删除。
static CONTEXT: Mutex<Option<Arc<FfiContext>>> = Mutex::new(None);

/// 取锁，被毒化时也照常使用里面的数据。
///
/// FFI 边界上**不能 panic**（跨 FFI unwind 是未定义行为）。前一个持锁者
/// panic 会把 Mutex 毒化，默认的 `unwrap()` 会跟着 panic 并穿过 FFI 边界，
/// 所以这里把毒化的锁恢复成可用状态再继续——本模块保护的都是"缓存型"数据，
/// 残缺的旧值也比崩溃好。
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// 把字符串存进静态槽位并返回裸指针。
///
/// 返回的指针只在**下一次调用同一个 FFI 函数之前**有效——槽位会被覆盖，
/// 这是这些接口的既定契约（见各函数的文档注释）。
/// 槽位本身是静态的，所以指针不会悬垂；用 Mutex 是为了让覆盖动作不再有数据竞争。
fn store(ptr_slot: &'static Mutex<Option<CString>>, s: CString) -> *const c_char {
    let mut guard = lock(ptr_slot);
    *guard = Some(s);
    // guard 在这里 drop，但 CString 仍活在同一个静态槽位里，指针保持有效
    guard.as_ref().map_or(std::ptr::null(), |s| s.as_ptr())
}

/// 初始化引擎。device_name / receive_dir 为 null 时使用默认值。
/// （Android 上建议由调用方传入应用专属外部存储目录，如
///   /storage/emulated/0/Android/data/<pkg>/files/kitefile，无需存储权限）
/// 返回 0 表示成功，-1 表示失败。
///
/// **对称性（A3.7）**：已初始化时幂等返回 0；`kitefile_shutdown` 之后
/// 槽位为空，本函数可完整重建上下文（不再有「只准写一次」的全局门闩）。
///
/// # Safety
/// `device_name` / `receive_dir` 必须是合法的 C 字符串（可为 null）
#[no_mangle]
pub unsafe extern "C" fn kitefile_init(
    device_name: *const c_char,
    receive_dir: *const c_char,
) -> i32 {
    // 幂等：已在运行直接成功（重复 init 不重建、不报错）
    if lock(&CONTEXT).is_some() {
        return 0;
    }

    let device_name = if device_name.is_null() {
        None
    } else {
        CStr::from_ptr(device_name).to_str().ok().map(String::from)
    };
    let receive_dir = if receive_dir.is_null() {
        None
    } else {
        CStr::from_ptr(receive_dir).to_str().ok().map(String::from)
    };

    let mut config = EngineConfig::default();
    if let Some(name) = device_name {
        config.device_name = name;
    }
    if let Some(dir) = receive_dir {
        config.receive_dir = std::path::PathBuf::from(dir);
    }

    // 尽早开日志：之后的端口退避、身份、绑定……每一步失败都要留现场
    init_ffi_logging(&config.receive_dir);
    info!(
        device = %config.device_name,
        receive_dir = %config.receive_dir.display(),
        "kitefile_init starting"
    );

    let runtime = match Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            error!("failed to create runtime: {}", e);
            return -1;
        }
    };
    let runtime_handle = runtime.handle().clone();

    // 端口退避。必须在构造 discovery / transfer **之前**定下来：
    // mDNS 的 TXT 会把实际端口广播出去，对端靠它建连，所以这里改端口
    // 对端依然能正确发现；反过来若先按 7878 注册 mDNS、再发现绑不上，
    // 对端就会拿着一个错误的端口去连。
    //
    // 为什么要退避：Windows 上 Hyper-V / WSL / Docker 会动态保留成片 TCP
    // 端口，7878 可能正好落在保留区，bind 失败报 os error 10013，
    // 且保留区间每次开机都可能变 —— 症状是 daemon 间歇性起不来。
    // 回环口（UI 用）按 127.0.0.1 探测，LAN TLS 口按 0.0.0.0 探测
    if let Some(p) =
        crate::pick_available_port_on("127.0.0.1", crate::GATEWAY_PORT_CANDIDATES)
    {
        if p != config.gateway_port {
            warn!(from = config.gateway_port, to = p, "gateway port unavailable, fell back");
            config.gateway_port = p;
        }
    } else {
        warn!("no gateway port available; will try default and likely fail");
    }
    if let Some(p) = crate::pick_available_port(crate::LAN_TLS_PORT_CANDIDATES) {
        if p != config.lan_tls_port {
            warn!(from = config.lan_tls_port, to = p, "lan tls port unavailable, fell back");
            config.lan_tls_port = p;
        }
    } else {
        warn!("no lan tls port available; will try default and likely fail");
    }
    if let Some(p) = crate::pick_available_port(crate::TRANSFER_PORT_CANDIDATES) {
        if p != config.transfer_port {
            warn!(from = config.transfer_port, to = p, "transfer port unavailable, fell back");
            config.transfer_port = p;
        }
    } else {
        warn!("no transfer port available; will try default and likely fail");
    }

    // 身份持久化：id/名称复用上次的，重启后 mDNS 注册同一服务实例，
    // 对端设备表按 id 覆盖同一条记录（否则每次重启都被当成“新设备”）
    let identity = crate::discovery::load_or_create_identity(
        &config.receive_dir,
        &config.device_name,
    );
    config.device_name = identity.name.clone();
    let self_id = identity.id;
    let identity_path =
        Some(crate::discovery::identity_marker_path(&config.receive_dir));

    // mDNS TXT 广播 LAN TLS 端口：对端拿它建跨机连接
    let discovery = match DiscoveryService::new(
        config.device_name.clone(),
        self_id.clone(),
        config.lan_tls_port,
        config.transfer_port,
        identity_path.clone(),
    ) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            // mDNS 不可用（如组播被路由器/系统限制）不阻塞引擎：
            // 降级为离线模式，传输与网关功能照常
            warn!("discovery init failed, fallback to offline mode: {}", e);
            Arc::new(DiscoveryService::new_offline(
                config.device_name.clone(),
                self_id,
                identity_path,
            ))
        }
    };

    let transfer = Arc::new(TransferEngine::new(
        config.transfer_port,
        config.parallel_streams,
        config.receive_dir.clone(),
    ));
    // 信任上下文（P4）：数据面成员校验 + 出站 pin；须在 spawn_receiver 之前
    transfer.set_trust(discovery.devices_handle());

    let ctx = Arc::new(FfiContext {
        runtime_handle: runtime_handle.clone(),
        runtime,
        discovery,
        transfer,
        config: Arc::new(config),
    });

    // spawn 接收端：任务只持有 transfer（不持有 Arc<FfiContext>，
    // 否则 shutdown 时上下文引用成环、runtime 永远放不掉）
    let receiver_transfer = ctx.transfer.clone();
    runtime_handle.spawn(async move {
        if let Err(e) = receiver_transfer.clone().spawn_receiver().await {
            error!("receiver spawn failed: {}", e);
        }
    });

    // spawn 发现事件循环
    let handle = runtime_handle.clone();
    let discovery_clone = ctx.discovery.clone();
    runtime_handle.spawn(async move { discovery_clone.spawn_event_loop(handle) });

    // spawn HTTP 网关：同样只搬走所需 Arc，不搬 ctx
    let gateway_discovery = ctx.discovery.clone();
    let gateway_transfer = ctx.transfer.clone();
    let gateway_config = ctx.config.clone();
    runtime_handle.spawn(async move {
        let gateway = HttpGateway::new(gateway_discovery, gateway_transfer, gateway_config);
        if let Err(e) = gateway.run().await {
            error!("gateway run failed: {}", e);
        }
    });

    *lock(&CONTEXT) = Some(ctx);
    info!("kitefile_init done (0 = ok); 网关/接收/发现任务已 spawn，等待端口就绪");
    0
}

/// 获取本机信息 JSON：{"id","name","platform","gateway_port","transfer_port"}
/// 返回的字符串由 kitefile 负责管理，调用方不应释放；下一次调用后可能失效。
///
/// # Safety
/// 仅在 kitefile_init 成功后调用
#[no_mangle]
pub unsafe extern "C" fn kitefile_whoami_json() -> *const c_char {
    static LAST: Mutex<Option<CString>> = Mutex::new(None);
    // clone 出 Arc 就立刻释放锁，不要持着锁去做 JSON 序列化
    let ctx = match lock(&CONTEXT).clone() {
        Some(c) => c,
        None => return std::ptr::null(),
    };

    let me = serde_json::json!({
        "id": ctx.discovery.self_id(),
        "name": ctx.discovery.self_name(),
        "platform": crate::platform::platform_name(),
        // 本机 UI 走回环明文口；跨机对端走 lan_tls_port（mDNS/whoami 广告的也是它）
        "gateway_port": ctx.config.gateway_port,
        "lan_tls_port": ctx.config.lan_tls_port,
        "transfer_port": ctx.config.transfer_port,
        "pairing_enabled": ctx.discovery.pairing_status().0,
    });
    store(&LAST, CString::new(me.to_string()).unwrap_or_default())
}

/// 获取当前已发现的设备列表 JSON 数组
#[no_mangle]
pub unsafe extern "C" fn kitefile_list_devices_json() -> *const c_char {
    static LAST: Mutex<Option<CString>> = Mutex::new(None);
    let ctx = match lock(&CONTEXT).clone() {
        Some(c) => c,
        None => return std::ptr::null(),
    };
    let devices = ctx.discovery.list_devices();
    store(
        &LAST,
        CString::new(serde_json::to_string(&devices).unwrap_or_default()).unwrap_or_default(),
    )
}

/// 发起发送
/// - target_ip：UTF-8 C 字符串
/// - target_port：目标传输端口，传 0 使用默认
/// - file_path：UTF-8 C 字符串
/// 返回 file_id（UTF-8 C 字符串，调用方不释放），失败返回 null
#[no_mangle]
pub unsafe extern "C" fn kitefile_send_file(
    target_ip: *const c_char,
    target_port: u16,
    file_path: *const c_char,
) -> *const c_char {
    static LAST: Mutex<Option<CString>> = Mutex::new(None);
    if target_ip.is_null() || file_path.is_null() {
        return std::ptr::null();
    }
    let ctx = match lock(&CONTEXT).clone() {
        Some(c) => c,
        None => return std::ptr::null(),
    };
    let ip = match CStr::from_ptr(target_ip).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null(),
    };
    let path = match CStr::from_ptr(file_path).to_str() {
        Ok(s) => std::path::PathBuf::from(s),
        Err(_) => return std::ptr::null(),
    };
    let port = if target_port == 0 {
        ctx.config.transfer_port
    } else {
        target_port
    };

    let transfer = ctx.transfer.clone();
    let config = ctx.config.clone();
    let self_id = ctx.discovery.self_id().to_string();
    let self_name = ctx.discovery.self_name().to_string();
    let self_ip = ctx.discovery.self_ip().unwrap_or_else(|| "127.0.0.1".into());
    // 目标是跨机设备：默认按对端同款 LAN TLS 端口试（与旧版"两端同配置"
    // 的假设一致；端口退避差异由 mDNS TXT / whoami 覆盖的路径处理）
    let target_gateway_port = config.lan_tls_port;

    // FFI 同步返回：spawn 到 runtime 后通过 channel 拿回 file_id，
    // 传输在后台进行，进度通过 gateway 的 /api/transfers 与 /ws/progress 查询。
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    ctx.runtime_handle.spawn(async move {
        match transfer
            .clone()
            .send_file(
                ip,
                port,
                target_gateway_port,
                path,
                None,
                self_id,
                self_name,
                self_ip,
                // 对端回包地址：本机 LAN TLS 口（回环口对端不可达）
                config.lan_tls_port,
                // FFI 层是"一次发一个文件"的入口，批量由 UI 走 HTTP /api/send，
                // 所以这里永远按单文件处理。
                None,
            )
            .await
        {
            Ok(mut handle) => {
                let file_id = handle.file_id.clone();
                let _ = tx.send(file_id);
                // 进度写入 engine 快照并广播（与 HTTP /api/send 一致）
                while let Some(p) = handle.next_progress().await {
                    transfer.publish_progress(p).await;
                }
            }
            Err(_) => {
                let _ = tx.send(String::new());
            }
        }
    });

    let file_id = match rx.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(id) if !id.is_empty() => id,
        _ => return std::ptr::null(),
    };

    store(&LAST, CString::new(file_id).unwrap_or_default())
}

/// 释放引擎资源（应用退出 / 重启 daemon 前调用）。
///
/// 对称性（A3.7）：
/// - 丢弃上下文 → 随之丢弃进程内 tokio 运行时 → gateway / receiver /
///   discovery 任务全部中止，监听端口释放；
/// - 槽位置回 None，**之后可以再次 `kitefile_init`** 完整重建。
#[no_mangle]
pub unsafe extern "C" fn kitefile_shutdown() {
    let ctx = lock(&CONTEXT).take();
    if let Some(ctx) = ctx {
        // 尽量走 shutdown_background：不等待 spawn_blocking（如哈希计算）
        // 收尾，立即释放端口。失败（仍有并发 FFI 调用持有引用）时
        // 随最后一个 Arc 丢弃触发 Runtime::drop，语义一致只是可能多等一会儿。
        match Arc::try_unwrap(ctx) {
            Ok(inner) => inner.runtime.shutdown_background(),
            Err(_shared) => {
                warn!("shutdown raced with in-flight FFI call; runtime will stop when last reference drops");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    /// A3.7：init / shutdown 对称。
    /// - 首次 init 成功；已初始化时重复 init 幂等返回 0；
    /// - shutdown 后可**再次 init**（回归点：旧实现用 OnceLock 门闩，
    ///   shutdown 后二次 init 永远失败，daemon 起不来）。
    #[test]
    fn init_shutdown_init_roundtrip() {
        let dir = std::env::temp_dir().join(format!("kitefile-ffi-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let cdir = CString::new(dir.to_string_lossy().as_bytes()).unwrap();

        unsafe {
            assert_eq!(
                kitefile_init(std::ptr::null(), cdir.as_ptr()),
                0,
                "首次 init 应成功"
            );
            assert_eq!(
                kitefile_init(std::ptr::null(), cdir.as_ptr()),
                0,
                "已初始化时重复 init 应幂等成功"
            );
            kitefile_shutdown();
            assert_eq!(
                kitefile_init(std::ptr::null(), cdir.as_ptr()),
                0,
                "shutdown 后必须能再次 init（对称性）"
            );
            kitefile_shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
