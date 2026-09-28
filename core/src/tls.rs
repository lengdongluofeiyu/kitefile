//! TLS 身份与配置（阶段 5 · P1）
//!
//! 每台设备一把长期 Ed25519 自签证书，与 `.kitefile-identity` 同目录持久化
//! （`identity_cert.pem` / `identity_key.pem`）。配对（P3）交换的就是这张证书；
//! 之后跨机连接在 TLS 层完成双向认证：
//!
//! - 服务端：请求客户端证书（P1 接受任意，P3 起按 peers 表放行）
//! - 客户端：`client_config(..., Some(fp))` 校验服务端证书指纹 pin
//!
//! 指纹 = 证书 DER 的 SHA-256（对自签单证书场景与 SPKI 哈希等价：
//! pin 的就是握手时收到的那张证书，且证书永不重签）。

use std::path::Path;
use std::sync::{Arc, Once};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error as TlsError, ServerConfig,
    SignatureScheme,
};
use tracing::info;

use crate::{CoreError, Result};

/// 证书 PEM 文件名（daemon 数据目录下，与身份标记同级）
pub const CERT_FILE: &str = "identity_cert.pem";
/// 私钥 PEM 文件名
pub const KEY_FILE: &str = "identity_key.pem";

/// 本机 TLS 身份：Ed25519 自签证书 + 私钥 + 指纹。
///
/// 持久化到标记文件目录，daemon 每次启动复用 —— 指纹不变，
/// 对端 peers 表里的 pin 才能持续有效。
#[derive(Debug)]
pub struct NodeIdentity {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
    fp: String,
}

impl NodeIdentity {
    /// 加载或创建持久化身份。
    ///
    /// - 两个 PEM 文件都在且可解析 → 复用（指纹不变）
    /// - 不存在 → 生成新证书并落盘；写盘失败返回 Err
    ///   （静默退化成内存身份会让下次启动换指纹、对端 pin 全失效）
    ///
    /// CN 优先取同目录 `.kitefile-identity` 里的 device_id（仅调试可读性，
    /// 安全性不依赖 CN——我们按整证书指纹 pin，不校验名字）。
    pub fn load_or_create(base_dir: &Path) -> Result<Arc<Self>> {
        install_crypto_provider();

        let cert_path = base_dir.join(CERT_FILE);
        let key_path = base_dir.join(KEY_FILE);
        if cert_path.exists() && key_path.exists() {
            let cert = CertificateDer::from_pem_file(&cert_path)
                .map_err(|e| CoreError::Gateway(format!("读取 {CERT_FILE} 失败: {e}")))?;
            let key = PrivateKeyDer::from_pem_file(&key_path)
                .map_err(|e| CoreError::Gateway(format!("读取 {KEY_FILE} 失败: {e}")))?;
            let fp = fingerprint(cert.as_ref());
            return Ok(Arc::new(Self { cert, key, fp }));
        }

        let cn = read_marker_device_id(base_dir).unwrap_or_else(|| "kitefile-node".to_string());
        let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519)
            .map_err(|e| CoreError::Gateway(format!("生成 Ed25519 密钥失败: {e}")))?;
        let mut params = rcgen::CertificateParams::new(vec![cn.clone()])
            .map_err(|e| CoreError::Gateway(format!("证书参数失败: {e}")))?;
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn.clone());
        let cert = params
            .self_signed(&key_pair)
            .map_err(|e| CoreError::Gateway(format!("自签证书失败: {e}")))?;

        let cert_pem = cert.pem();
        let key_pem = key_pair.serialize_pem();

        std::fs::create_dir_all(base_dir)?;
        std::fs::write(&cert_path, cert_pem.as_bytes())?;
        // 私钥收紧权限（Windows 上 fs::set_permissions 只认 read-only 位，
        // 完整 0600 语义留给 unix；尽力而为，失败不阻断）
        write_key_file(&key_path, key_pem.as_bytes())?;

        let cert = CertificateDer::from_pem_slice(cert_pem.as_bytes())
            .map_err(|e| CoreError::Gateway(format!("证书 PEM 回读失败: {e}")))?;
        let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
            .map_err(|e| CoreError::Gateway(format!("私钥 PEM 回读失败: {e}")))?;
        let fp = fingerprint(cert.as_ref());
        info!(fp = %fp, cn = %cn, "TLS identity created");
        Ok(Arc::new(Self { cert, key, fp }))
    }

    /// 证书指纹（小写 hex，SHA-256 of DER）
    pub fn fp(&self) -> &str {
        &self.fp
    }

    pub fn cert_der(&self) -> &CertificateDer<'static> {
        &self.cert
    }
}

/// 证书 DER 的 SHA-256（小写 hex）
pub fn fingerprint(cert_der: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(cert_der);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// TLS accept 循环注入到每个 HTTP 请求的对端证书指纹。
///
/// - `Some(fp)`：对端在 TLS 握手里出示了客户端证书（rustls 已验证
///   CertificateVerify，即证明持有私钥）
/// - `None`：对端没出示证书（P1 阶段合法——鉴权尚未启用）
///
/// P1 只注入、不拦截；P3 的 auth_guard 读它查 peers 表。
#[derive(Clone, Debug)]
pub struct PeerFingerprint(pub Option<String>);

/// 安装进程级默认加密后端（ring）。幂等，可重复调用。
pub fn install_crypto_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// 服务端配置：请求客户端证书但**接受任意**（P1 不改鉴权行为）。
/// P3 在其上叠加应用层查表（PeerFingerprint 扩展）。
pub fn server_config(id: &NodeIdentity) -> Result<Arc<ServerConfig>> {
    install_crypto_provider();
    let cfg = ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(AcceptAnyClientCert))
        .with_single_cert(vec![id.cert.clone()], id.key.clone_key())
        .map_err(|e| CoreError::Gateway(format!("TLS 服务端配置失败: {e}")))?;
    Ok(Arc::new(cfg))
}

/// 客户端配置。
///
/// - `pinned_fp = None`：接受任意服务端证书（P1 现状 / 配对期）
/// - `pinned_fp = Some(fp)`：服务端证书必须精确匹配 pin，否则握手失败
///
/// 始终出示本机客户端证书（mTLS 身份）。
pub fn client_config(id: &NodeIdentity, pinned_fp: Option<&str>) -> Result<Arc<ClientConfig>> {
    install_crypto_provider();
    let verifier: Arc<dyn ServerCertVerifier> = match pinned_fp {
        Some(fp) => Arc::new(PinnedServerCert { fp: fp.to_string() }),
        None => Arc::new(AcceptAnyServerCert),
    };
    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![id.cert.clone()], id.key.clone_key())
        .map_err(|e| CoreError::Gateway(format!("TLS 客户端配置失败: {e}")))?;
    Ok(Arc::new(cfg))
}

// ---------------------------------------------------------------------------
// 危险验证器（P1 语义；P3 会把"查表放行"下沉到应用层/服务端配置）
// ---------------------------------------------------------------------------

/// 进程默认 provider（安装后必存在；ring 安装失败且无其他 provider 时 panic
/// ——这是环境级故障，带着错误的 provider 继续跑 TLS 只会更糟）。
fn provider() -> &'static rustls::crypto::CryptoProvider {
    if let Some(p) = rustls::crypto::CryptoProvider::get_default() {
        return &**p;
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    rustls::crypto::CryptoProvider::get_default()
        .map(|p| &**p)
        .expect("rustls crypto provider unavailable")
}

fn verify_sig_12(
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
) -> std::result::Result<HandshakeSignatureValid, TlsError> {
    rustls::crypto::verify_tls12_signature(
        message,
        cert,
        dss,
        &provider().signature_verification_algorithms,
    )
}

fn verify_sig_13(
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
) -> std::result::Result<HandshakeSignatureValid, TlsError> {
    rustls::crypto::verify_tls13_signature(
        message,
        cert,
        dss,
        &provider().signature_verification_algorithms,
    )
}

fn supported_schemes() -> Vec<SignatureScheme> {
    provider()
        .signature_verification_algorithms
        .supported_schemes()
}

/// 服务端用：请求但可选客户端证书，接受任意证书。
///
/// 握手层仍会验证客户端的 CertificateVerify 签名（见 `verify_tls13_signature`），
/// 所以「出示了证书」已经隐含「持有该证书私钥」——应用层 PoP 不再需要。
#[derive(Debug)]
struct AcceptAnyClientCert;

impl ClientCertVerifier for AcceptAnyClientCert {
    fn offer_client_auth(&self) -> bool {
        true
    }
    fn client_auth_mandatory(&self) -> bool {
        // 可选：P1 不改鉴权行为；whoami 等免配对接口也依赖"可以不带证书"。
        // P3 的应用层 auth_guard 按接口决定要不要 401。
        false
    }
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }
    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, TlsError> {
        Ok(ClientCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        verify_sig_12(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        verify_sig_13(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        supported_schemes()
    }
}

/// 客户端用：接受任意服务端证书（配对期 / P1 过渡）。
#[derive(Debug)]
struct AcceptAnyServerCert;

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        verify_sig_12(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        verify_sig_13(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        supported_schemes()
    }
}

/// 客户端用：服务端证书必须精确匹配指纹 pin。
#[derive(Debug)]
struct PinnedServerCert {
    fp: String,
}

impl ServerCertVerifier for PinnedServerCert {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, TlsError> {
        if fingerprint(end_entity.as_ref()) == self.fp {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(TlsError::InvalidCertificate(
                CertificateError::UnknownIssuer,
            ))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        verify_sig_12(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        verify_sig_13(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        supported_schemes()
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// 从 `.kitefile-identity` 读 device_id 当证书 CN（读不到就回退，不报错）
fn read_marker_device_id(base_dir: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(base_dir.join(".kitefile-identity")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let id = v.get("id")?.as_str()?.to_string();
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

fn write_key_file(path: &Path, bytes: &[u8]) -> Result<()> {
    std::fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let base = std::env::var("FTCORE_TEST_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("kitefile-tls-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// SHA-256("abc") 标准向量：指纹算法不能漂移
    #[test]
    fn fingerprint_is_sha256_hex() {
        assert_eq!(
            fingerprint(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// 首次生成 → 落盘 → 二次加载指纹不变（对端 pin 依赖这一点）
    #[test]
    fn load_or_create_persists_and_stable_fp() {
        let dir = temp_dir("persist");
        let a = NodeIdentity::load_or_create(&dir).unwrap();
        assert_eq!(a.fp().len(), 64, "指纹应为 64 位 hex");
        assert!(dir.join(CERT_FILE).exists());
        assert!(dir.join(KEY_FILE).exists());

        let b = NodeIdentity::load_or_create(&dir).unwrap();
        assert_eq!(a.fp(), b.fp(), "二次加载必须复用同一身份");
    }

    /// 两个目录 = 两台设备：指纹必须不同
    #[test]
    fn distinct_dirs_get_distinct_identities() {
        let d1 = temp_dir("id1");
        let d2 = temp_dir("id2");
        let a = NodeIdentity::load_or_create(&d1).unwrap();
        let b = NodeIdentity::load_or_create(&d2).unwrap();
        assert_ne!(a.fp(), b.fp());
    }

    /// 完整握手回环：服务端收客户端证书、客户端 accept-any，数据可往返
    #[tokio::test]
    async fn handshake_accept_any_roundtrip() {
        let srv_dir = temp_dir("hs-srv");
        let cli_dir = temp_dir("hs-cli");
        let srv_id = NodeIdentity::load_or_create(&srv_dir).unwrap();
        let cli_id = NodeIdentity::load_or_create(&cli_dir).unwrap();

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        let server_cfg = server_config(&srv_id).unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(server_cfg);
            let mut tls = acceptor.accept(tcp).await.unwrap();
            // 服务端必须能看到客户端证书（mTLS 身份在服务端可见）
            let peer_certs = tls.get_ref().1.peer_certificates().map(|c| c.len());
            assert_eq!(peer_certs, Some(1), "服务端应收到 1 张客户端证书");
            let mut buf = [0u8; 4];
            tokio::io::AsyncReadExt::read_exact(&mut tls, &mut buf).await.unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut tls, &buf).await.unwrap();
        });

        let client_cfg = client_config(&cli_id, None).unwrap();
        let connector = tokio_rustls::TlsConnector::from(client_cfg);
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let server_name = rustls::pki_types::ServerName::IpAddress(
            std::net::IpAddr::V4(Ipv4Addr::LOCALHOST).into(),
        );
        let mut tls = connector.connect(server_name, tcp).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut tls, b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut tls, &mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    }

    /// pin 校验：指纹对 → 握手成功；指纹错 → 握手失败
    #[tokio::test]
    async fn pinned_verifier_accepts_match_rejects_mismatch() {
        let srv_dir = temp_dir("pin-srv");
        let cli_dir = temp_dir("pin-cli");
        let other_dir = temp_dir("pin-other");
        let srv_id = NodeIdentity::load_or_create(&srv_dir).unwrap();
        let cli_id = NodeIdentity::load_or_create(&cli_dir).unwrap();
        let other_id = NodeIdentity::load_or_create(&other_dir).unwrap();

        // 正确 pin
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_cfg = server_config(&srv_id).unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(server_cfg);
            let _ = acceptor.accept(tcp).await;
        });
        let connector =
            tokio_rustls::TlsConnector::from(client_config(&cli_id, Some(srv_id.fp())).unwrap());
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let name =
            rustls::pki_types::ServerName::IpAddress(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST).into());
        let result = connector.connect(name, tcp).await;
        assert!(result.is_ok(), "正确指纹应握手成功: {:?}", result.err());

        // 错误 pin（pin 成另一台设备的指纹）
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_cfg = server_config(&srv_id).unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(server_cfg);
            let _ = acceptor.accept(tcp).await;
        });
        let connector =
            tokio_rustls::TlsConnector::from(client_config(&cli_id, Some(other_id.fp())).unwrap());
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let name =
            rustls::pki_types::ServerName::IpAddress(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST).into());
        let result = connector.connect(name, tcp).await;
        assert!(result.is_err(), "错误指纹必须握手失败");
    }
}
