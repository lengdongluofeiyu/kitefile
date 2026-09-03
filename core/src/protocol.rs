//! 传输协议：握手消息 + 数据帧格式
//!
//! 协议设计：
//! 1. 控制通道（单条 TCP）：发送 offer/ack/resume 等控制消息（JSON 行）
//! 2. 数据通道（N 条 TCP，并行）：传输分块二进制
//!
//! 握手示例：
//! ```json
//! {"version":1,"type":"offer","file_name":"a.mp4","file_size":10737418240,
//!  "file_id":"uuid","chunk_size":16777216,"chunk_count":640,
//!  "sha256":"abc...","resume_token":null}
//! ```

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;
pub const SERVICE_TYPE: &str = "_ftcore._tcp.local.";

/// 控制消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    /// 发送方 → 接收方：发起传输提议
    Offer {
        version: u32,
        file_name: String,
        file_size: u64,
        file_id: String,
        chunk_size: usize,
        chunk_count: u64,
        sha256: String,
        resume_token: Option<String>,
    },
    /// 接收方 → 发送方：接受提议
    Accept {
        file_id: String,
        transfer_port: u16,
        parallel_streams: usize,
    },
    /// 接收方 → 发送方：拒绝
    Reject {
        file_id: String,
        reason: String,
    },
    /// 接收方 → 发送方：单个 chunk 接收确认
    ChunkAck {
        file_id: String,
        chunk_id: u64,
        ok: bool,
    },
    /// 发送方 → 接收方：整文件完成
    Complete {
        file_id: String,
    },
    /// 任意一方：取消传输
    Cancel {
        file_id: String,
        reason: String,
    },
}

impl ControlMessage {
    /// 序列化为一行 JSON（以 \n 结尾），用于控制通道
    pub fn to_line(&self) -> anyhow::Result<String> {
        let mut s = serde_json::to_string(self)?;
        s.push('\n');
        Ok(s)
    }

    pub fn from_line(s: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(s.trim())?)
    }
}

/// 数据帧：每个 TCP 数据流的前导头
/// 每个数据帧前加固定大小的二进制头，后跟 chunk_size 字节数据
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DataFrameHeader {
    /// 文件 ID 的前 8 字节（用于校验路由）
    pub file_id_prefix: u64,
    /// chunk 序号
    pub chunk_id: u64,
    /// 实际数据长度（可能小于 chunk_size，针对最后一块）
    pub data_len: u32,
    /// 预留
    pub _reserved: u32,
}

/// HTTP offer：发送方 daemon 调用接收方 gateway 时 POST 的请求体
///
/// 流程：
/// 1. 发送方 daemon → 接收方 daemon：POST /api/incoming  (body = HttpOffer)
/// 2. 接收方 daemon → UI（WebSocket 推送 HttpIncomingEvent）
/// 3. UI → 接收方 daemon：POST /api/incoming/{id}/accept 或 /reject
/// 4. 接收方 daemon → 发送方 daemon：POST /api/incoming-resp (body = HttpIncomingResponse)
/// 5. 发送方 daemon 收到 Accept 后启动 TCP 多流传输
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpOffer {
    /// 发送方 file_id（用于后续 TCP 数据流匹配）
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
    /// 整文件 sha256（hex；可空）
    pub sha256: Option<String>,
    /// true = sha256 由发送方边传边算，全部 chunk 发完后通过
    /// POST /api/verify/:file_id 补发（大文件不再阻塞 offer，弹窗即时出现）
    #[serde(default)]
    pub sha256_deferred: bool,
    /// 发送方 device id
    pub from_id: String,
    /// 发送方 device 显示名
    pub from_name: String,
    /// 发送方 IP（接收方回包用）
    pub from_ip: String,
    /// 发送方 gateway 端口（接收方回包用）
    pub from_gateway_port: u16,
    /// 发送方 TCP transfer 端口（接收方回 Accept 后用，daemon 已知也行）
    pub from_transfer_port: u16,
}

/// 接收方 daemon 内部为每个 offer 生成的待决定条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingEntry {
    /// 接收方生成的本次 incoming 唯一 ID（用于 UI 决策路由）
    pub incoming_id: String,
    /// 关联发送方 file_id（用于后续 TCP 接收路由）
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub sha256: Option<String>,
    #[serde(default)]
    pub sha256_deferred: bool,
    pub from_id: String,
    pub from_name: String,
    pub from_ip: String,
    pub from_gateway_port: u16,
    pub from_transfer_port: u16,
    /// 创建时间戳（Unix ms）
    pub created_at: u64,
    /// 决策：None=待决定, Some(true)=已接受, Some(false)=已拒绝
    pub decision: Option<bool>,
}

/// WebSocket 推送给 UI 的事件（区分进度 / 传入请求）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum WsEvent {
    /// 传输进度（原 TransferProgress）
    Progress {
        #[serde(flatten)]
        progress: crate::transfer::TransferProgress,
    },
    /// 新传入请求待决定
    Incoming {
        #[serde(flatten)]
        entry: IncomingEntry,
    },
    /// 传入请求已被处理（UI 可关闭对应弹窗）
    IncomingResolved {
        incoming_id: String,
        accepted: bool,
    },
}

/// 接收方 daemon → 发送方 daemon 的 Offer 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpIncomingResponse {
    pub file_id: String,
    pub accepted: bool,
    pub reason: Option<String>,
    /// 接收方 transfer_port（发送方据此发 TCP 数据流；与 mDNS 一致，发送方已知，可省略）
    pub transfer_port: u16,
}

impl DataFrameHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        // 小端序，简单 memcpy
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.file_id_prefix.to_le_bytes());
        buf[8..16].copy_from_slice(&self.chunk_id.to_le_bytes());
        buf[16..20].copy_from_slice(&self.data_len.to_le_bytes());
        buf[20..24].copy_from_slice(&self._reserved.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> anyhow::Result<Self> {
        if buf.len() < Self::SIZE {
            anyhow::bail!("data frame header too short");
        }
        Ok(Self {
            file_id_prefix: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            chunk_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            data_len: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            _reserved: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
        })
    }
}
