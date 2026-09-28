//! 纯 TCP 的 HTTP POST JSON 客户端（全仓**唯一**实现，A3.1）
//!
//! 历史上 `transfer.rs` 与 `gateway.rs` 各有一份拷贝，行为漂移且都没超时：
//! connect 可无限阻塞、响应 `read_to_end` 可无限等待（对端收到请求但
//! 不回包时整个调用方任务挂死）。合并到本模块后统一施加
//! [`timeouts::CONNECT_TIMEOUT`] / [`timeouts::HTTP_RESPONSE_TIMEOUT`]。
//!
//! 阶段 5 P1 起，**所有跨机（LAN）调用一律走 `*_tls` 变体**：
//! 明文版本只服务回环 / 测试场景。TLS 客户端加载 `receive_dir` 下的
//! 本机身份证书（双向认证的客户端侧），P1 尚不做服务端 pin
//! （peers 表与 pin 校验在 P3 接入，见 `docs/design-pairing-encryption.md`）。

use crate::timeouts;
use std::io;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// POST JSON，返回响应体字符串（明文；仅回环 / 测试）。
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
    let req = build_req(host, port, path, body);
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    read_response(stream, response_timeout).await
}

/// POST JSON over TLS（阶段 5 P1：跨机 HTTP 唯一入口）。
///
/// - `receive_dir`：加载 `identity_cert.pem` / `identity_key.pem`（本机身份）
/// - `pinned_fp`：`Some` 时服务端证书必须匹配（P3 起由调用方传入）；
///   P1 传 `None`——先加密、后认证
pub async fn http_post_json_tls(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
    receive_dir: &Path,
) -> io::Result<String> {
    http_post_json_tls_with(
        host,
        port,
        path,
        body,
        receive_dir,
        None,
        timeouts::CONNECT_TIMEOUT,
        timeouts::HTTP_RESPONSE_TIMEOUT,
    )
    .await
}

/// 可注入 pin 与超时的 TLS 实现（P3 pin 校验与测试用）。
#[allow(clippy::too_many_arguments)]
pub async fn http_post_json_tls_with(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
    receive_dir: &Path,
    pinned_fp: Option<&str>,
    connect_timeout: Duration,
    response_timeout: Duration,
) -> io::Result<String> {
    let (mut stream, _server_fp) =
        tls_connect(host, port, receive_dir, pinned_fp, connect_timeout).await?;
    let req = build_req(host, port, path, body);
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    read_response(stream, response_timeout).await
}

/// GET over TLS，返回响应体字符串（CLI 探测对端 whoami 用）。
pub async fn http_get_tls(
    host: &str,
    port: u16,
    path: &str,
    receive_dir: &Path,
    pinned_fp: Option<&str>,
) -> io::Result<String> {
    let (mut stream, _server_fp) = tls_connect(
        host,
        port,
        receive_dir,
        pinned_fp,
        timeouts::CONNECT_TIMEOUT,
    )
    .await?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    read_response(stream, timeouts::HTTP_RESPONSE_TIMEOUT).await
}

/// 建立 TLS 连接（含握手超时）。返回 (流, 服务端证书指纹)。
///
/// 指纹来自**握手收到的服务端证书**（不是任何响应体声称）——
/// 配对确认码必须基于亲眼所见，否则中间人可以两头喂同一个假码。
async fn tls_connect(
    host: &str,
    port: u16,
    receive_dir: &Path,
    pinned_fp: Option<&str>,
    connect_timeout: Duration,
) -> io::Result<(tokio_rustls::client::TlsStream<TcpStream>, Option<String>)> {
    let identity = crate::tls::NodeIdentity::load_or_create(receive_dir)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("加载 TLS 身份失败: {e}")))?;
    let client_cfg = crate::tls::client_config(&identity, pinned_fp)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("TLS 客户端配置失败: {e}")))?;
    let connector = tokio_rustls::TlsConnector::from(client_cfg);

    let tcp = tokio::time::timeout(connect_timeout, TcpStream::connect((host, port)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;
    let server_name = server_name_for(host)?;
    let stream = tokio::time::timeout(connect_timeout, connector.connect(server_name, tcp))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "tls handshake timeout"))?
        // 握手失败按传输类错误（非 InvalidData——InvalidData 保留给
        // 「对端应用层明确判决」，调用方靠 kind 区分离线 vs 被拒）
        .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, format!("tls: {e}")))?;
    let server_fp = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|c| c.first())
        .map(|c| crate::tls::fingerprint(c.as_ref()));
    Ok((stream, server_fp))
}

/// POST JSON over TLS，同时返回**握手所见**的服务端证书指纹。
/// 配对流程（pair/start → hello）专用。
pub async fn http_post_json_tls_peer_fp(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
    receive_dir: &Path,
) -> io::Result<(String, Option<String>)> {
    let (mut stream, server_fp) =
        tls_connect(host, port, receive_dir, None, timeouts::CONNECT_TIMEOUT).await?;
    let req = build_req(host, port, path, body);
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    let resp = read_response(stream, timeouts::HTTP_RESPONSE_TIMEOUT).await?;
    Ok((resp, server_fp))
}

fn server_name_for(host: &str) -> io::Result<rustls::pki_types::ServerName<'static>> {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(rustls::pki_types::ServerName::IpAddress(ip.into()));
    }
    rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid host {host}")))
}

fn build_req(host: &str, port: u16, path: &str, body: &str) -> String {
    format!(
        "POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path, host, port, body.len(), body
    )
}

/// 读完整响应并拆出 body；非 2xx → `InvalidData`（与 transport 错误可区分）。
async fn read_response<S>(mut stream: S, response_timeout: Duration) -> io::Result<String>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut response = Vec::new();
    tokio::time::timeout(response_timeout, stream.read_to_end(&mut response))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "http response timeout"))??;

    let response_str = String::from_utf8_lossy(&response);
    let body_start = response_str
        .find("\r\n\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no header/body sep"))?;

    // 非 2xx 视为错误（工作流 B）：对端明确拒绝（协议版本不符、404 等）
    // 必须让调用方看见，而不是把错误页当成功响应吞掉。
    // kind 用 InvalidData 以区别 transport 错误（refused/reset/timeout 不产生它）：
    // 调用方靠 kind 区分「对端明确判决」与「链路不通」（如 resume 的 404 vs 离线文案）。
    let status = response_str
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        // 摘一段响应体进错误：调用方（如配对流程）要能看见对端的拒绝原因，
        // 不然只剩 "http status 403" 无法排查。仍保持 InvalidData kind
        //（对端明确判决 vs 链路不通的区分），且字符串仍含状态码。
        let snippet: String = response_str[body_start + 4..].chars().take(200).collect();
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("http status {status}: {snippet}"),
        ));
    }
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

    /// 非 2xx → `InvalidData`（与 transport 错误可区分）。
    /// resume 文案（404=任务已结束 vs 离线）与 offer 重试短路都依赖这个 kind。
    #[tokio::test]
    async fn non_2xx_maps_to_invalid_data() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let resp =
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 3\r\nConnection: close\r\n\r\nnope";
            let _ = sock.write_all(resp).await;
        });
        let err = http_post_json_with(
            "127.0.0.1",
            port,
            "/api/x",
            "{}",
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("404"), "错误应带状态码：{err}");
    }
}
