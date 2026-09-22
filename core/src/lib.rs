//! ftcore — 局域网文件传输核心引擎
//!
//! 三个核心子系统：
//! - [`discovery`]：mDNS 设备发现
//! - [`transfer`]：多流 TCP 并行流式传输
//! - [`gateway`]：HTTP/WebSocket 网关，供 Web 前端调用
//!
//! 平台支持矩阵：Windows / Android / iOS / macOS
//! 平台相关代码以 cfg 分隔，新增平台只需补 platform 模块。

pub mod discovery;
pub mod transfer;
pub mod gateway;
pub mod protocol;
pub mod storage;
pub mod slot_meta;
pub mod platform;
pub mod ffi;

pub use discovery::{Device, DeviceId, DiscoveryService};
pub use transfer::{TransferEngine, TransferHandle, TransferProgress, TransferStatus};
pub use gateway::HttpGateway;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("discovery error: {0}")]
    Discovery(String),
    #[error("transfer error: {0}")]
    Transfer(String),
    #[error("gateway error: {0}")]
    Gateway(String),
    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

pub type Result<T> = std::result::Result<T, CoreError>;

/// 核心引擎运行配置
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// 设备显示名
    pub device_name: String,
    /// HTTP 网关监听端口（默认 7878，Web 前端访问）
    pub gateway_port: u16,
    /// TCP 传输端口（默认 7879）
    pub transfer_port: u16,
    /// 并行流数量上限（默认 = CPU 核数，封顶 8）。
    /// 实际流数按文件大小自适应，见 `protocol::compute_stream_count`。
    pub parallel_streams: usize,
    /// 接收目录
    pub receive_dir: std::path::PathBuf,
    /// 是否允许来自其他设备的「管理类」HTTP 调用（默认 false）
    ///
    /// 网关把接口分成两档：
    /// - 仅本机：`/api/send`、`/api/files`、`/api/transfers`、`/api/config*` 等控制本机的操作
    /// - 允许远程：P2P 协商必需的 `/api/incoming`、`/api/incoming-resp`、
    ///   `/api/verify/:id`、`/api/cancel/:id` 以及只读的 `/api/whoami`
    ///
    /// 置为 true 时「仅本机」那一档也对局域网放开，用于移动端把对端 IP 当遥控器
    /// 的开发场景。**默认关闭**：开着意味着局域网内任何人都能让本机外传文件、
    /// 读取接收目录。
    pub allow_remote_admin: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        let parallel_streams = std::thread::available_parallelism()
            .map(|n| n.get().min(8))
            .unwrap_or(4);
        Self {
            device_name: format!("{}-{}", whoami_fallback(), rand_short()),
            gateway_port: 7878,
            transfer_port: 7879,
            parallel_streams,
            receive_dir: crate::platform::default_receive_dir(),
            allow_remote_admin: false,
        }
    }
}

/// HTTP 网关端口候选（首选 + 备选）
///
/// 为什么不能只认一个端口：Windows 上 Hyper-V / WSL / Docker 会**动态保留**
/// 成片 TCP 端口，用
/// `netsh interface ipv4 show excludedportrange protocol=tcp` 能看到。
/// 7878 就可能正好落在某个保留区间里，此时 bind 会失败并报
/// `os error 10013`（WSAEACCES，一种访问权限不允许的套接字操作）。
/// 保留区间每次开机都可能不同，所以症状是**间歇性**的：
/// 有时 daemon 起得来，有时起不来，很容易误判成别的问题。
pub const GATEWAY_PORT_CANDIDATES: &[u16] = &[7878, 17878, 27878];

/// TCP 数据通道端口候选（与上面一一对应）
pub const TRANSFER_PORT_CANDIDATES: &[u16] = &[7879, 17879, 27879];

/// 从候选里挑一个当前能 bind 的端口，全被占用时返回 None。
///
/// 注意这是"先探再绑"，两者之间理论上有竞态；本机启动瞬间窗口极小，
/// 而且真撞上了也只是退化成启动失败并记 error，不会静默出错。
pub fn pick_available_port(candidates: &[u16]) -> Option<u16> {
    for &p in candidates {
        if std::net::TcpListener::bind(("0.0.0.0", p)).is_ok() {
            return Some(p);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_available_port_returns_none_when_all_taken() {
        let blocker = std::net::TcpListener::bind(("0.0.0.0", 0)).unwrap();
        let taken = blocker.local_addr().unwrap().port();
        assert!(
            pick_available_port(&[taken]).is_none(),
            "唯一候选被占用时必须返回 None，调用方才知道要兜底"
        );
    }

    #[test]
    fn pick_available_port_skips_taken_and_picks_next() {
        let taken_sock = std::net::TcpListener::bind(("0.0.0.0", 0)).unwrap();
        let taken = taken_sock.local_addr().unwrap().port();
        let probe = std::net::TcpListener::bind(("0.0.0.0", 0)).unwrap();
        let free = probe.local_addr().unwrap().port();
        drop(probe);

        assert_eq!(
            pick_available_port(&[taken, free]),
            Some(free),
            "应跳过被占用的第一个候选，选中后面的可用端口"
        );
    }

    #[test]
    fn port_candidate_lists_are_same_length() {
        // gateway 与 transfer 的候选一一对应，少了任何一个都会让退避后
        // 两个端口错配（一端通告 7878/17879 这种组合）
        assert_eq!(GATEWAY_PORT_CANDIDATES.len(), TRANSFER_PORT_CANDIDATES.len());
    }
}

fn whoami_fallback() -> String {
    std::env::var("USERNAME")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "device".into())
}

fn rand_short() -> String {
    let id = uuid::Uuid::new_v4();
    id.to_string().split('-').next().unwrap_or("0000").to_string()
}
