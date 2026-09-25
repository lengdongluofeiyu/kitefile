//! 超时模型：三层语义，默认值**只在本文件定义一次**，各处复用（修复方案 A3.1）
//!
//! | 层 | 含义 | 覆盖 |
//! |----|------|------|
//! | 连接建立 | [`CONNECT_TIMEOUT`] TCP connect 上限 | 数据流连接、HTTP 客户端 |
//! | 空闲超时 | [`IDLE_TIMEOUT`] 读/写连续无进展 | 数据流收发、HTTP 响应读 |
//! | 任务总超时 | [`TASK_TOTAL_TIMEOUT`] 可选 | 单次传输任务 |
//!
//! 超时错误在传输中段按**可重试**处理（见 `fault` 模块 A3.4）；
//! 用户决策 60s 超时（`INCOMING_DECISION_TIMEOUT_SECS`）不是传输故障，不进重试。

use std::future::Future;
use std::io;
use std::time::Duration;

/// 连接建立上限：TCP connect（数据流、HTTP 客户端）。
///
/// 局域网内 10s 连不上基本可判定路径不可达（对端挂了 / 网段不通）。
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// 空闲超时：读/写**连续无进展**的上限。
///
/// 不是「单次 read 系统调用耗时上限」——有字节推进就重置计时。
/// 30s 零进展意味着链路或对端/磁盘已经出问题。
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP 响应读取上限（从发出请求到读完整个响应体）。
///
/// 低于发送方等待决策的 70s 窗口：offer / verify / cancel 都是
/// 「对端收到即处理」的短请求，30s 读不到响应视为链路故障。
pub const HTTP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// 单次传输任务总超时（可选，防止极端慢速占坑）。
///
/// `None` = 不设上限（默认：慢速链路只要还在推进就允许继续）。
pub const TASK_TOTAL_TIMEOUT: Option<Duration> = None;

/// 段失败自动重试：每段共 3 次尝试（1 次 + 自动重试 2 次，A3.3）。
pub const STREAM_RETRY_ATTEMPTS: u32 = 3;

/// 自动重试退避：第 1 次重试等 0.5s，第 2 次等 1.5s。
pub const STREAM_RETRY_BACKOFF: [Duration; 2] = [Duration::from_millis(500), Duration::from_millis(1500)];

/// 施加连接建立超时：超时 → [`io::ErrorKind::TimedOut`]。
pub async fn with_connect_timeout<F, T>(fut: F) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    tokio::time::timeout(CONNECT_TIMEOUT, fut)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))?
}

/// 施加空闲超时：`fut` 在 `limit` 内没有产出即判超时 → [`io::ErrorKind::TimedOut`]。
///
/// 用于「单次读/写调用」级别：调用返回（无论读到多少字节）即视为有进展。
pub async fn with_idle_timeout<F, T>(fut: F, limit: Duration) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    tokio::time::timeout(limit, fut)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "idle timeout"))?
}

/// 施加响应读取超时（见 [`HTTP_RESPONSE_TIMEOUT`]）。
pub async fn with_response_timeout<F, T>(fut: F) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    tokio::time::timeout(HTTP_RESPONSE_TIMEOUT, fut)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "http response timeout"))?
}

/// 可注入的超时组（测试闸门 E：「超时可注入或缩短配置」）。
///
/// 默认值仍只来自本模块的常量；测试可在构造 `TransferEngine` 后、
/// `Arc::new` 之前覆写字段以缩短超时，避免用大文件/真等待拖时间。
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    /// 连接建立上限（数据流 connect）
    pub connect: Duration,
    /// 空闲上限（数据流读/写）
    pub idle: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect: CONNECT_TIMEOUT,
            idle: IDLE_TIMEOUT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 连接超时必须把「永远不完成」的 future 收敛成 TimedOut 错误。
    #[tokio::test(start_paused = true)]
    async fn connect_timeout_converges_pending_future() {
        let res: io::Result<()> = with_connect_timeout(std::future::pending()).await;
        let e = res.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
    }

    /// 空闲超时同理，且时间推进是自动的（start_paused）。
    #[tokio::test(start_paused = true)]
    async fn idle_timeout_converges_pending_future() {
        let res: io::Result<()> =
            with_idle_timeout(std::future::pending(), Duration::from_secs(5)).await;
        let e = res.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
    }

    /// 有进展（future 正常完成）时不误杀。
    #[tokio::test(start_paused = true)]
    async fn idle_timeout_passes_through_completed_future() {
        let res: io::Result<u32> = with_idle_timeout(
            async { Ok::<u32, io::Error>(42) },
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(res.unwrap(), 42);
    }

    /// 响应超时收敛行为一致。
    #[tokio::test(start_paused = true)]
    async fn response_timeout_converges_pending_future() {
        let res: io::Result<()> = with_response_timeout(std::future::pending()).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    /// 退避表与尝试次数的契约：3 次尝试 = 2 次退避（A3.3）。
    #[test]
    fn retry_policy_matches_attempts() {
        assert_eq!(STREAM_RETRY_ATTEMPTS as usize, STREAM_RETRY_BACKOFF.len() + 1);
    }
}
