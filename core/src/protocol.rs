//! 传输协议：握手消息 + 数据帧格式
//!
//! 协议分两条通道：
//! 1. **控制通道 = HTTP**（复用 7878 网关）。offer / accept / reject / cancel /
//!    verify 都是普通 HTTP 请求，没有自定义的 TCP 控制连接——握手要双向跨机
//!    调用，走 HTTP 能直接复用网关已有的路由与访问分级。
//!    完整时序见 [`HttpOffer`] 的文档注释。
//! 2. **数据通道 = N 条并行 TCP**（默认 7879），只传分块二进制，见 [`DataFrameHeader`]。
//!
//! 握手请求体示例（`POST /api/incoming`，字段以 [`HttpOffer`] 为准）：
//! ```json
//! {"file_id":"uuid","file_name":"a.mp4","file_size":10737418240,
//!  "sha256":null,"sha256_deferred":true,
//!  "from_id":"dev-1","from_name":"laptop","from_ip":"192.168.1.5",
//!  "from_gateway_port":7878,"from_transfer_port":7879}
//! ```
//!
//! 两点容易看错，说明一下：
//! - `sha256` 常为 null 且 `sha256_deferred=true`：整文件哈希由发送方边传边算，
//!   全部 chunk 发完后经 `POST /api/verify/:file_id` 补发，大文件的弹窗不等它。
//! - 请求体里**没有** `chunk_size`：分块大小目前是发送方的本地配置，不随 offer
//!   协商。两端配置不一致会静默产生错位数据，详见方案 N2。

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;
pub const SERVICE_TYPE: &str = "_ftcore._tcp.local.";

/// 控制消息
///
/// 数据通道上目前只跑一种：接收方给发送方的 chunk 确认（[`ControlMessage::ChunkAck`]）。
/// 握手（offer / accept / reject / cancel / verify）全部走 HTTP，见文件头说明。
///
/// 保留 enum 而不是退化成裸 struct，是为了将来加消息类型时不用改函数签名。
/// 这里原先还有 Offer / Accept / Reject / Complete / Cancel 五个变体，
/// 对应"单条 TCP 控制通道"的早期设计，从未被调用过（含 resume_token），已删除。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    /// 接收方 → 发送方：单个 chunk 接收确认
    ChunkAck {
        file_id: String,
        chunk_id: u64,
        ok: bool,
    },
}

impl ControlMessage {
    /// 序列化为一行 JSON（以 \n 结尾），用于数据通道上的 ChunkAck
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
    /// 发送方的分块大小。**接收方建槽必须用这个值，不能用本地配置**——
    /// 两端配置不一致会让 chunk 偏移整体错位，而每个块都会"成功"落盘，
    /// 只有最后的整文件 sha256 能发现，为时已晚（方案 N2）。
    ///
    /// 用 `Option` 而非 `#[serde(default)]`：旧版本 daemon 发来的 offer 没有这个字段，
    /// 若给默认值 0，接收方会拿 0 去算偏移和 chunk_count（除零 / 全错位），
    /// 比"字段缺失"本身危险得多。None 表示对端没说，接收方回退本地配置并告警。
    #[serde(default)]
    pub chunk_size: Option<usize>,
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
    /// 一次多选发送共享的批次 ID。
    ///
    /// 同一批的 offer 带相同值，接收端可以把它们合成一张
    /// "XXX 想发 5 个文件" 的卡片，一次接受或拒绝整批，
    /// 不用逐个点。None = 单文件发送（或旧版本发送端，行为不变）。
    #[serde(default)]
    pub batch_id: Option<String>,
    /// 本文件在批次内的序号（0 起）与批次总数，供 UI 显示 "3/5"。
    /// 单文件发送时为 None。
    #[serde(default)]
    pub batch_index: Option<u32>,
    #[serde(default)]
    pub batch_total: Option<u32>,
    /// 发送方 IP（接收方回包用）
    pub from_ip: String,
    /// 发送方 gateway 端口（接收方回包用）
    pub from_gateway_port: u16,
    /// 发送方 TCP transfer 端口（接收方回 Accept 后用，daemon 已知也行）
    pub from_transfer_port: u16,
}

/// 一次多选发送里，单个文件所属的批次信息。
///
/// 同批的所有 offer 带同一个 `batch_id`，接收端据此把它们合成一张卡片，
/// 让用户一次接受/拒绝整批，而不是逐个点。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendBatchInfo {
    pub batch_id: String,
    /// 本文件在批次内的序号（0 起）
    pub index: u32,
    /// 批次内文件总数
    pub total: u32,
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
    /// 发送方声明的分块大小（透传自 `HttpOffer::chunk_size`）。
    /// 接受时用它建槽，不能用接收方本地配置——理由同 `HttpOffer::chunk_size`。
    #[serde(default)]
    pub chunk_size: Option<usize>,
    pub sha256: Option<String>,
    #[serde(default)]
    pub sha256_deferred: bool,
    pub from_id: String,
    pub from_name: String,
    pub from_ip: String,
    pub from_gateway_port: u16,
    pub from_transfer_port: u16,
    /// 批次 ID，透传自 `HttpOffer::batch_id`。同批的条目可一次决策。
    #[serde(default)]
    pub batch_id: Option<String>,
    /// 本条目在批次内的位置，透传自 `HttpOffer::batch_index` / `batch_total`
    #[serde(default)]
    pub batch_index: Option<u32>,
    #[serde(default)]
    pub batch_total: Option<u32>,
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
