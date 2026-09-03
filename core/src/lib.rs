//! ftcore — 局域网文件传输核心引擎
//!
//! 三个核心子系统：
//! - [`discovery`]：mDNS 设备发现
//! - [`transfer`]：多流 TCP 并行传输
//! - [`gateway`]：HTTP/WebSocket 网关，供 Web 前端调用
//!
//! 平台支持矩阵：Windows / Android / iOS / macOS
//! 平台相关代码以 cfg 分隔，新增平台只需补 platform 模块。

pub mod discovery;
pub mod transfer;
pub mod gateway;
pub mod protocol;
pub mod storage;
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
    /// 并行流数量（默认 = CPU 核数，封顶 8）
    pub parallel_streams: usize,
    /// 单个分块大小（默认 16MB）
    pub chunk_size: usize,
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
            chunk_size: 16 * 1024 * 1024,
            receive_dir: crate::platform::default_receive_dir(),
            allow_remote_admin: false,
        }
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
