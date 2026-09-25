//! 数据面错误分类（修复方案 A3.4）
//!
//! **主原则**：换个连接、原样重写同一段，有没有可能变好？
//! 可能 → 可重试；再试也改变不了结果 → 不可重试。
//!
//! 裁决靠 [`FailureKind`]（由 `io::ErrorKind` / 所处阶段映射而来）
//! 与 [`Phase`]，**禁止**靠错误字符串匹配。
//!
//! | 判定 | 处置 |
//! |------|------|
//! | 可重试 | 段级自动重试（3 次尝试，0.5s/1.5s 退避）→ 耗尽转 `Interrupted` |
//! | 不可重试 | 直接 `Failed`（或用户取消 → `Canceled`） |
//!
//! 灰色地带（接受后立刻断连）按可重试处理；resume 时发现槽位已无
//! （[`FailureKind::ProtocolViolation`] 的 unknown-prefix 分支）→ `Failed`，禁止无限续。

use std::io;

/// 错误所处阶段。同一 `io::ErrorKind` 在不同阶段裁决不同：
/// `TimedOut` 在 Connect 是连接超时、在 Send/Recv 是空闲超时；
/// `NotFound` 在 LocalRead 是源文件丢失，在 Recv 是槽位消失。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// TCP 建连
    Connect,
    /// 发送侧写 socket
    Send,
    /// 接收侧读 socket
    Recv,
    /// 本地源文件读（发送侧）
    LocalRead,
    /// 本地磁盘写（接收侧落盘）
    DiskWrite,
    /// 协议校验（帧头 / 布局 / 槽位）
    Protocol,
    /// sha256 校验
    Verify,
    /// 对端判决（reject 等）
    Peer,
    /// 用户决策等待（60s，非传输故障）
    Decision,
    /// 用户主动取消
    Canceled,
}

/// 错误类别。与修复方案 A3.4 分类表逐行对应，测试锁定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    // ---- 可重试（瞬时网络类） ----
    /// 建连超时（对端不可达 / 防火墙丢包）
    ConnectTimeout,
    /// 连接暂时失败（connection refused 等，daemon 可能正在重启）
    ConnectFailed,
    /// 连接重置（ECONNRESET / ConnectionAborted）
    ConnectionReset,
    /// 管道断开（EPIPE / BrokenPipe）
    BrokenPipe,
    /// 传输中途 EOF / 意外关闭（对端进程死掉）
    UnexpectedEof,
    /// 读/写连续无进展（链路假死）
    IdleTimeout,
    /// 其余网络类读写错误
    NetworkIo,

    // ---- 不可重试 ----
    /// 源文件丢失 / 本地读失败
    SourceError,
    /// 磁盘满
    DiskFull,
    /// 无权限
    PermissionDenied,
    /// 路径非法
    InvalidPath,
    /// 布局错乱 / 帧头非法 / stream_id 越界 / 槽位不存在
    ProtocolViolation,
    /// 对端明确拒绝（写盘失败、协议 reject）
    PeerRejected,
    /// 整文件 sha256 不匹配（不走段续）
    Sha256Mismatch,
    /// 用户决策 60s 超时（非传输故障，不进自动重试）
    DecisionTimeout,
    /// 用户取消
    Canceled,
    /// 其余本地/内部错误
    Internal,
}

/// 结构化传输错误：分类 + 阶段 + 细节。
#[derive(Debug, Clone)]
pub struct TransferFailure {
    pub kind: FailureKind,
    pub phase: Phase,
    pub detail: String,
}

impl TransferFailure {
    pub fn new(kind: FailureKind, phase: Phase, detail: impl Into<String>) -> Self {
        Self {
            kind,
            phase,
            detail: detail.into(),
        }
    }

    /// 可重试判定（A3.4 分类表「可重试」列）。
    pub fn is_retryable(&self) -> bool {
        use FailureKind::*;
        matches!(
            self.kind,
            ConnectTimeout
                | ConnectFailed
                | ConnectionReset
                | BrokenPipe
                | UnexpectedEof
                | IdleTimeout
                | NetworkIo
        )
    }

    /// 用户可读的短原因（UI 副文案用，如「连接超时」）。
    pub fn describe(&self) -> String {
        use FailureKind::*;
        let head = match self.kind {
            ConnectTimeout => "连接超时",
            ConnectFailed => "连接失败",
            ConnectionReset => "连接被重置",
            BrokenPipe => "连接断开",
            UnexpectedEof => "连接意外关闭",
            IdleTimeout => "传输空闲超时",
            NetworkIo => "网络错误",
            SourceError => "源文件读取失败",
            DiskFull => "磁盘空间不足",
            PermissionDenied => "没有文件访问权限",
            InvalidPath => "路径非法",
            ProtocolViolation => "协议错误",
            PeerRejected => "对端拒绝",
            Sha256Mismatch => "校验失败",
            DecisionTimeout => "等待对方确认超时",
            Canceled => "已取消",
            Internal => "内部错误",
        };
        head.to_string()
    }

    pub fn canceled(detail: impl Into<String>) -> Self {
        Self::new(FailureKind::Canceled, Phase::Canceled, detail)
    }

    pub fn protocol(detail: impl Into<String>) -> Self {
        Self::new(FailureKind::ProtocolViolation, Phase::Protocol, detail)
    }
}

impl std::fmt::Display for TransferFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:?})", self.describe(), self.kind)
    }
}

impl std::error::Error for TransferFailure {}

/// Windows / Unix 磁盘满与配额错误码。
fn is_disk_full(e: &io::Error) -> bool {
    match e.raw_os_error() {
        // Windows: ERROR_DISK_FULL(112) ERROR_HANDLE_DISK_FULL(39)
        // Unix: ENOSPC(28) EDQUOT(122)
        Some(112) | Some(39) | Some(28) | Some(122) => true,
        _ => matches!(e.kind(), io::ErrorKind::StorageFull),
    }
}

/// 由 `io::Error` + 阶段映射到结构化分类。
///
/// 这是全仓唯一的 io 错误裁决入口。裁决顺序：
/// 1. 磁盘满（raw os code）优先——任何阶段出现都不可重试；
/// 2. **本地阶段（LocalRead / DiskWrite）整体不可重试**——再试也一样；
/// 3. 网络阶段（Connect / Send / Recv）按 `io::ErrorKind` 细分瞬时类别；
/// 4. 协议/判决阶段兜底为 Internal。
pub fn from_io(e: &io::Error, phase: Phase) -> TransferFailure {
    use io::ErrorKind as K;
    use FailureKind as F;
    use Phase as P;

    if is_disk_full(e) {
        return TransferFailure::new(F::DiskFull, phase, e.to_string());
    }

    let kind = match phase {
        // ---- 本地阶段：不可重试（A3.4「本地条件未坏」子标准不满足） ----
        P::LocalRead => match e.kind() {
            K::PermissionDenied => F::PermissionDenied,
            K::InvalidInput => F::InvalidPath,
            // 源文件丢失 / 本地读失败（含 NotFound 与其他 IO 错）
            _ => F::SourceError,
        },
        P::DiskWrite => match e.kind() {
            K::PermissionDenied => F::PermissionDenied,
            _ => F::Internal,
        },
        // ---- 网络阶段：瞬时网络类可重试 ----
        P::Connect => match e.kind() {
            K::TimedOut => F::ConnectTimeout,
            K::ConnectionRefused | K::HostUnreachable | K::NetworkUnreachable => F::ConnectFailed,
            K::ConnectionReset | K::ConnectionAborted => F::ConnectionReset,
            K::BrokenPipe => F::BrokenPipe,
            _ => F::NetworkIo,
        },
        P::Send | P::Recv => match e.kind() {
            K::TimedOut => F::IdleTimeout,
            K::ConnectionReset | K::ConnectionAborted => F::ConnectionReset,
            K::BrokenPipe => F::BrokenPipe,
            K::UnexpectedEof => F::UnexpectedEof,
            _ => F::NetworkIo,
        },
        // ---- 协议/判决阶段：不走网络重试 ----
        P::Protocol | P::Verify | P::Peer | P::Decision | P::Canceled => F::Internal,
    };
    TransferFailure::new(kind, phase, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    fn io_err(kind: ErrorKind) -> Error {
        Error::new(kind, "x")
    }

    /// A3.4 分类表「可重试」行：逐项锁定。
    #[test]
    fn retryable_table_is_locked() {
        let cases = [
            (FailureKind::ConnectTimeout, Phase::Connect),   // connect 超时
            (FailureKind::ConnectFailed, Phase::Connect),    // 连接暂时失败
            (FailureKind::ConnectionReset, Phase::Send),     // 连接重置
            (FailureKind::BrokenPipe, Phase::Send),          // 管道断开
            (FailureKind::UnexpectedEof, Phase::Recv),       // 传输中途 EOF
            (FailureKind::IdleTimeout, Phase::Recv),         // 空闲超时
            (FailureKind::NetworkIo, Phase::Send),           // 发送侧 TCP 写出失败
        ];
        for (kind, phase) in cases {
            let f = TransferFailure::new(kind, phase, "t");
            assert!(f.is_retryable(), "{kind:?} 应可重试");
        }
    }

    /// A3.4 分类表「不可重试」行：逐项锁定。
    #[test]
    fn fatal_table_is_locked() {
        let cases = [
            (FailureKind::PeerRejected, Phase::Peer),            // 对端明确拒绝
            (FailureKind::DiskFull, Phase::DiskWrite),           // 磁盘满
            (FailureKind::PermissionDenied, Phase::DiskWrite),   // 无权限
            (FailureKind::InvalidPath, Phase::LocalRead),        // 路径非法
            (FailureKind::SourceError, Phase::LocalRead),        // 源文件丢失/本地读失败
            (FailureKind::ProtocolViolation, Phase::Protocol),   // 布局/帧头/越界
            (FailureKind::Sha256Mismatch, Phase::Verify),        // sha256 不匹配
            (FailureKind::DecisionTimeout, Phase::Decision),     // 60s 决策超时
            (FailureKind::Canceled, Phase::Canceled),            // 用户取消
        ];
        for (kind, phase) in cases {
            let f = TransferFailure::new(kind, phase, "t");
            assert!(!f.is_retryable(), "{kind:?} 不应可重试");
        }
    }

    /// 同一 TimedOut：Connect 阶段=连接超时，其余=空闲超时。
    #[test]
    fn timed_out_maps_by_phase() {
        let e = io_err(ErrorKind::TimedOut);
        let f = from_io(&e, Phase::Connect);
        assert_eq!(f.kind, FailureKind::ConnectTimeout);
        assert!(f.is_retryable());

        let f = from_io(&e, Phase::Send);
        assert_eq!(f.kind, FailureKind::IdleTimeout);
        assert!(f.is_retryable());

        let f = from_io(&e, Phase::Recv);
        assert_eq!(f.kind, FailureKind::IdleTimeout);
    }

    /// NotFound：LocalRead=源文件丢失（不可重试）；DiskWrite=内部错误（不可重试）
    #[test]
    fn not_found_maps_by_phase() {
        let e = io_err(ErrorKind::NotFound);
        assert_eq!(from_io(&e, Phase::LocalRead).kind, FailureKind::SourceError);
        assert!(!from_io(&e, Phase::LocalRead).is_retryable());
        // 磁盘写阶段的 NotFound（.part 消失）：内部错误，不可重试
        let f = from_io(&e, Phase::DiskWrite);
        assert_eq!(f.kind, FailureKind::Internal);
        assert!(!f.is_retryable());
    }

    /// 网络类错误在 Recv/Send 阶段可重试；本地读阶段同错误不可重试。
    #[test]
    fn network_vs_local_read_classification() {
        for kind in [
            ErrorKind::ConnectionReset,
            ErrorKind::BrokenPipe,
            ErrorKind::UnexpectedEof,
        ] {
            let e = io_err(kind);
            assert!(from_io(&e, Phase::Recv).is_retryable(), "{kind:?} 网络侧应可重试");
            // 同名 kind 出现在 LocalRead 只可能是本地读语义
            let f = from_io(&e, Phase::LocalRead);
            assert!(!f.is_retryable(), "{kind:?} 本地读侧应不可重试");
        }
    }

    /// Windows/Unix 磁盘满错误码 → DiskFull（不可重试）
    #[test]
    fn disk_full_raw_os_errors_are_fatal() {
        let e = Error::from_raw_os_error(112); // ERROR_DISK_FULL
        let f = from_io(&e, Phase::DiskWrite);
        assert_eq!(f.kind, FailureKind::DiskFull);
        assert!(!f.is_retryable());

        let e = Error::from_raw_os_error(28); // ENOSPC
        assert_eq!(from_io(&e, Phase::DiskWrite).kind, FailureKind::DiskFull);
    }

    /// PermissionDenied 在本地两阶段恒不可重试（网络阶段 socket 不会报它，
    /// 出现也按瞬时网络类处理，不进本断言）。
    #[test]
    fn permission_denied_always_fatal() {
        let e = io_err(ErrorKind::PermissionDenied);
        for phase in [Phase::LocalRead, Phase::DiskWrite] {
            let f = from_io(&e, phase);
            assert_eq!(f.kind, FailureKind::PermissionDenied);
            assert!(!f.is_retryable());
        }
    }

    /// UI 文案：describe 对关键类别给出稳定的中文短语
    #[test]
    fn describe_is_stable_chinese() {
        assert_eq!(
            TransferFailure::new(FailureKind::ConnectTimeout, Phase::Connect, "").describe(),
            "连接超时"
        );
        assert_eq!(
            TransferFailure::new(FailureKind::IdleTimeout, Phase::Recv, "").describe(),
            "传输空闲超时"
        );
        assert_eq!(
            TransferFailure::canceled("").describe(),
            "已取消"
        );
    }
}
