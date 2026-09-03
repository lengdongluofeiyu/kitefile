//! FFI 绑定层（预留 iOS / macOS / Android NDK 集成用）
//!
//! 当 Flutter / 原生端需要直接把 Rust 核心嵌入应用（无需独立守护进程）时，
//! 通过本模块暴露 C ABI 函数，配合 Dart `dart:ffi` 调用。
//!
//! 注意：
//! - 当前桌面端选择"应用 + 守护进程"模式（通过 HTTP 通信），更易调试
//! - 嵌入式集成（iOS / Android 资源约束）使用本 FFI 接口
//! - 函数命名约定：`ftcore_*`，参数全部为 C 兼容类型
//! - 字符串：传入 const char*（UTF-8），返回 const char* 由调用方负责释放
//!
//! 构建为动态库：在 Cargo.toml 中 `crate-type = ["cdylib", "rlib"]`
//! Flutter 端通过 `flutter_rust_bridge` 或手写 `dart:ffi` binding 调用。

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::{error, warn};

use crate::{EngineConfig, HttpGateway, TransferEngine};
use crate::discovery::DiscoveryService;

/// FFI 上下文句柄（不透明指针）
pub struct FfiContext {
    pub runtime_handle: tokio::runtime::Handle,
    pub discovery: Arc<DiscoveryService>,
    pub transfer: Arc<TransferEngine>,
    pub config: Arc<EngineConfig>,
}

// 全局单例（FFI 简化为单例；如需多实例可改为返回句柄）
static mut CONTEXT: Option<Arc<FfiContext>> = None;
static INIT_LOCK: std::sync::OnceLock<()> = std::sync::OnceLock::new();

/// 初始化引擎。device_name / receive_dir 为 null 时使用默认值。
/// （Android 上建议由调用方传入应用专属外部存储目录，如
///   /storage/emulated/0/Android/data/<pkg>/files/ftcore，无需存储权限）
/// 返回 0 表示成功，-1 表示失败。
///
/// # Safety
/// `device_name` / `receive_dir` 必须是合法的 C 字符串（可为 null）
#[no_mangle]
pub unsafe extern "C" fn ftcore_init(
    device_name: *const c_char,
    receive_dir: *const c_char,
) -> i32 {
    INIT_LOCK.get_or_init(|| {
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

        let runtime = match Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                error!("failed to create runtime: {}", e);
                return;
            }
        };
        let runtime_handle = runtime.handle().clone();

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

        let discovery = match DiscoveryService::new(
            config.device_name.clone(),
            self_id.clone(),
            config.gateway_port,
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
            config.chunk_size,
            config.receive_dir.clone(),
        ));

        let ctx = Arc::new(FfiContext {
            runtime_handle: runtime_handle.clone(),
            discovery,
            transfer,
            config: Arc::new(config),
        });

        // spawn 接收端
        let ctx_clone = ctx.clone();
        runtime_handle.spawn(async move {
            if let Err(e) = ctx_clone.transfer.clone().spawn_receiver().await {
                error!("receiver spawn failed: {}", e);
            }
        });

        // spawn 发现事件循环
        let handle = runtime_handle.clone();
        let discovery_clone = ctx.discovery.clone();
        runtime_handle.spawn(async move { discovery_clone.spawn_event_loop(handle) });

        // spawn HTTP 网关
        let ctx_clone = ctx.clone();
        let port = ctx.config.gateway_port;
        runtime_handle.spawn(async move {
            let gateway = HttpGateway::new(
                ctx_clone.discovery.clone(),
                ctx_clone.transfer.clone(),
                ctx_clone.config.clone(),
            );
            if let Err(e) = gateway.run(port).await {
                error!("gateway run failed: {}", e);
            }
        });

        // 保持 runtime 不被销毁
        std::mem::forget(runtime);

        CONTEXT = Some(ctx);
    });

    if unsafe { CONTEXT.is_some() } {
        0
    } else {
        -1
    }
}

/// 获取本机信息 JSON：{"id","name","platform","gateway_port","transfer_port"}
/// 返回的字符串由 ftcore 负责管理，调用方不应释放；下一次调用后可能失效。
///
/// # Safety
/// 仅在 ftcore_init 成功后调用
#[no_mangle]
pub unsafe extern "C" fn ftcore_whoami_json() -> *const c_char {
    static mut LAST: Option<CString> = None;
    let ctx = match CONTEXT.as_ref() {
        Some(c) => c,
        None => return std::ptr::null(),
    };

    let me = serde_json::json!({
        "id": ctx.discovery.self_id(),
        "name": ctx.discovery.self_name(),
        "platform": crate::platform::platform_name(),
        "gateway_port": ctx.config.gateway_port,
        "transfer_port": ctx.config.transfer_port,
    });
    let s = CString::new(me.to_string()).unwrap_or_default();
    LAST = Some(s);
    LAST.as_ref().unwrap().as_ptr()
}

/// 获取当前已发现的设备列表 JSON 数组
#[no_mangle]
pub unsafe extern "C" fn ftcore_list_devices_json() -> *const c_char {
    static mut LAST: Option<CString> = None;
    let ctx = match CONTEXT.as_ref() {
        Some(c) => c,
        None => return std::ptr::null(),
    };
    let devices = ctx.discovery.list_devices();
    let s = CString::new(serde_json::to_string(&devices).unwrap_or_default())
        .unwrap_or_default();
    LAST = Some(s);
    LAST.as_ref().unwrap().as_ptr()
}

/// 发起发送
/// - target_ip：UTF-8 C 字符串
/// - target_port：目标传输端口，传 0 使用默认
/// - file_path：UTF-8 C 字符串
/// 返回 file_id（UTF-8 C 字符串，调用方不释放），失败返回 null
#[no_mangle]
pub unsafe extern "C" fn ftcore_send_file(
    target_ip: *const c_char,
    target_port: u16,
    file_path: *const c_char,
) -> *const c_char {
    static mut LAST: Option<CString> = None;
    if target_ip.is_null() || file_path.is_null() {
        return std::ptr::null();
    }
    let ctx = match CONTEXT.as_ref() {
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
    let target_gateway_port = config.gateway_port;

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
                config.gateway_port,
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

    let s = CString::new(file_id).unwrap_or_default();
    LAST = Some(s);
    LAST.as_ref().unwrap().as_ptr()
}

/// 释放引擎资源（应用退出时调用）
#[no_mangle]
pub unsafe extern "C" fn ftcore_shutdown() {
    CONTEXT = None;
}
