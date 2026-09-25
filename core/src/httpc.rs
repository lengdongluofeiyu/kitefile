//! 纯 TCP 的 HTTP POST JSON 客户端（全仓**唯一**实现，A3.1）
//!
//! 历史上 `transfer.rs` 与 `gateway.rs` 各有一份拷贝，行为漂移且都没超时：
//! connect 可无限阻塞、响应 `read_to_end` 可无限等待（对端收到请求但
//! 不回包时整个调用方任务挂死）。合并到本模块后统一施加
//! [`timeouts::CONNECT_TIMEOUT`] / [`timeouts::HTTP_RESPONSE_TIMEOUT`]。

use crate::timeouts;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use std::io;
use std::time::Duration;

/// POST JSON，返回响应体字符串。
///
/// 超时语义见 `timeouts` 模块：connect 上限 + 响应读取上限。
pub async fn http_post_json(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
) -> io::Result<String> {
    http_post_json_with(
        host,
        port,
        path,
        body,
        timeouts::CONNECT_TIMEOUT,
        timeouts::HTTP_RESPONSE_TIMEOUT,
    )
    .await
}

/// 可注入超时的实现（测试用；生产代码走 [`http_post_json`]）。
pub async fn http_post_json_with(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
    connect_timeout: Duration,
    response_timeout: Duration,
) -> io::Result<String> {
    let mut stream = tokio::time::timeout(connect_timeout, TcpStream::connect((host, port)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;
    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path, host, port, body.len(), body
    );
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    tokio::time::timeout(response_timeout, stream.read_to_end(&mut response))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "http response timeout"))??;

    let response_str = String::from_utf8_lossy(&response);
    let body_start = response_str
        .find("\r\n\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no header/body sep"))?;
    Ok(response_str[body_start + 4..].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// 对端 accept 后不回包：响应超时必须收敛（而不是 read_to_end 挂死）。
    #[tokio::test]
    async fn response_timeout_fires_when_peer_never_replies() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _sock = listener.accept().await;
            // accept 后一直不写响应，保持连接
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        let start = std::time::Instant::now();
        let res = http_post_json_with(
            "127.0.0.1",
            port,
            "/api/x",
            "{}",
            Duration::from_secs(5),
            Duration::from_millis(200),
        )
        .await;
        let e = res.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "应在 ~200ms 收敛，实际 {:?}",
            start.elapsed()
        );
    }

    /// 正常回包不受超时影响（回归保护：合并实现不能破坏现网行为）。
    #[tokio::test]
    async fn normal_response_passes_through() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
            let _ = sock.write_all(resp).await;
        });
        let body = http_post_json_with(
            "127.0.0.1",
            port,
            "/api/x",
            "{}",
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(body, "ok");
    }
}
