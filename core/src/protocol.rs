//! 传输协议：握手消息 + 数据流格式
//!
//! 协议分两条通道：
//! 1. **控制通道 = HTTP**（复用 7878 网关）。offer / accept / reject / cancel /
//!    verify 都是普通 HTTP 请求，没有自定义的 TCP 控制连接——握手要双向跨机
//!    调用，走 HTTP 能直接复用网关已有的路由与访问分级。
//! 2. **数据通道 = N 条并行 TCP**（默认 7879）。每条流一去不回：
//!    [`StreamHeader`] 一次 + 连续原始字节，**没有分块 ACK、没有停等**。
//!    流内顺序写文件偏移，背压靠 TCP 滑动窗口。流数由文件大小自适应
//!    （见 [`compute_stream_count`]），小文件单流、大文件拉满并行。
//!
//! 为何不做应用层分块 ACK：停等模型下每块都要等「对端落盘」才能发下一块，
//! 节奏卡顿和吞吐上限都出在这里。整文件 sha256（发完经 `/api/verify` 补发）
//! 负责端到端校验；中途中断则整传失败（暂无按段续传）。

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 2;
pub const SERVICE_TYPE: &str = "_ftcore._tcp.local.";

/// 数据流头：每条 TCP 连接一次，后跟 `data_len` 字节原始数据。
///
/// 同一文件的 N 条流各写不同字节区间，区间由 [`stream_layout`] 切分，
/// 两端必须用相同的 `stream_count` 计算（由 offer 携带）。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct StreamHeader {
    /// 文件 ID 的前 8 字节（路由到接收槽）
    pub file_id_prefix: u64,
    /// 流序号（0..stream_count），完成位图按下标记
    pub stream_id: u32,
    pub _reserved: u32,
    /// 本流数据写入文件的起始偏移
    pub start_offset: u64,
    /// 本流字节数（连接上后跟这么多原始数据，读满即本流结束）
    pub data_len: u64,
}

impl StreamHeader {
    pub const SIZE: usize = 32;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.file_id_prefix.to_le_bytes());
        buf[8..12].copy_from_slice(&self.stream_id.to_le_bytes());
        buf[12..16].copy_from_slice(&self._reserved.to_le_bytes());
        buf[16..24].copy_from_slice(&self.start_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.data_len.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> anyhow::Result<Self> {
        if buf.len() < Self::SIZE {
            anyhow::bail!("stream header too short");
        }
        Ok(Self {
            file_id_prefix: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            stream_id: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            _reserved: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            start_offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            data_len: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        })
    }
}

/// 按文件大小自适应并行流数：小文件单流（省握手），大文件拉满 `max_parallel`。
///
/// 阈值 4MB/流：几百 KB 的文件开 8 条 TCP 纯属浪费；几十 MB 以上再铺开。
pub fn compute_stream_count(file_size: u64, max_parallel: usize) -> u32 {
    const MIN_BYTES_PER_STREAM: u64 = 4 * 1024 * 1024;
    let max = max_parallel.max(1) as u64;
    if file_size == 0 {
        return 1;
    }
    let by_size = (file_size + MIN_BYTES_PER_STREAM - 1) / MIN_BYTES_PER_STREAM;
    by_size.clamp(1, max) as u32
}

/// 把 `[0, file_size)` 切成 `stream_count` 段连续字节区间 `[(start, len), …]`。
///
/// 尽量均分，余数摊给前面的流。空文件退化为一段 `(0, 0)`。
/// 两端必须用同一 `stream_count` 调用本函数，否则偏移会错位。
pub fn stream_layout(file_size: u64, stream_count: u32) -> Vec<(u64, u64)> {
    let n = stream_count.max(1) as u64;
    if file_size == 0 {
        return vec![(0, 0)];
    }
    let base = file_size / n;
    let rem = file_size % n;
    let mut out = Vec::with_capacity(n as usize);
    let mut off = 0u64;
    for i in 0..n {
        let len = base + if i < rem { 1 } else { 0 };
        out.push((off, len));
        off += len;
    }
    out
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
    /// 发送方决定的并行流数。**接收方建槽必须用这个值**算分段布局
    /// （[`stream_layout`]），两端不一致会让字节区间错位，而每条流都会
    /// "成功"写入错误偏移，只有整文件 sha256 能发现（方案 N2）。
    ///
    /// `Option` + 无默认 0：旧版本 offer 没有该字段时视为缺失而不是 0 流。
    #[serde(default)]
    pub stream_count: Option<u32>,
    /// 整文件 sha256（hex；可空）
    pub sha256: Option<String>,
    /// true = sha256 由发送方边传边算，全部数据发完后通过
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
    /// 发送方声明的流数（透传自 `HttpOffer::stream_count`）。
    /// 接受时用它建槽——理由同 `HttpOffer::stream_count`。
    #[serde(default)]
    pub stream_count: Option<u32>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_count_adapts_to_file_size() {
        // 几百 KB：单流
        assert_eq!(compute_stream_count(300 * 1024, 8), 1);
        // 2MB：仍单流（未到 4MB/流）
        assert_eq!(compute_stream_count(2 * 1024 * 1024, 8), 1);
        // 8MB：2 流
        assert_eq!(compute_stream_count(8 * 1024 * 1024, 8), 2);
        // 32MB+：拉满
        assert_eq!(compute_stream_count(32 * 1024 * 1024, 8), 8);
        // 封顶 max_parallel
        assert_eq!(compute_stream_count(1024 * 1024 * 1024, 3), 3);
        // 空文件
        assert_eq!(compute_stream_count(0, 8), 1);
    }

    #[test]
    fn layout_covers_file_without_gap_or_overlap() {
        for size in [0u64, 1, 7, 10, 1024, 10_000] {
            for n in [1u32, 2, 3, 8] {
                let layout = stream_layout(size, n);
                // 空文件固定一段 (0,0)，与 n 无关
                let expect_len = if size == 0 { 1 } else { n.max(1) as usize };
                assert_eq!(layout.len(), expect_len, "size={size} n={n}");
                let total: u64 = layout.iter().map(|(_, len)| len).sum();
                assert_eq!(total, size, "size={size} n={n}");
                let mut expect = 0u64;
                for (start, len) in &layout {
                    assert_eq!(*start, expect);
                    expect += len;
                }
            }
        }
    }

    #[test]
    fn stream_header_roundtrip() {
        let h = StreamHeader {
            file_id_prefix: 0x0123_4567_89ab_cdef,
            stream_id: 3,
            _reserved: 0,
            start_offset: 1 << 40,
            data_len: 5 * 1024 * 1024 * 1024,
        };
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), StreamHeader::SIZE);
        let back = StreamHeader::from_bytes(&bytes).unwrap();
        assert_eq!(back.file_id_prefix, h.file_id_prefix);
        assert_eq!(back.stream_id, h.stream_id);
        assert_eq!(back.start_offset, h.start_offset);
        assert_eq!(back.data_len, h.data_len);
    }
}
