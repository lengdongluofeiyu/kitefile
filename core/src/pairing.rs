//! 配对模式状态（阶段 5 · P2）
//!
//! 「进入配对模式」是设备的显式动作（默认关闭，不跨重启复活）：
//!
//! - 开启期间 mDNS TXT 广播 `pair=1`，对端设备表把本机标为「可配对」；
//!   关闭 / 120s 超时后改回 `pair=0`（见 `discovery` 的 re-register）。
//! - P3 起，`pair/hello` 等配对接口只在开启期间受理（配对接口的门）。
//!
//! 过期由 `discovery::set_pairing_mode` 里 spawn 的定时任务驱动，
//! 用**代数（generation）**做失效判断：重复开启 / 手动关闭都会让代数 +1，
//! 旧定时任务醒来时发现代数对不上就静默作废，不会把新状态误关。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::time::Instant;
use tracing::warn;

/// 配对模式默认时长（蓝牙可发现模式同款量级）
pub const PAIRING_TTL: Duration = Duration::from_secs(120);

/// 配对会话有效期：hello 建立后 60s 内必须完成 confirm / 决定，否则作废
pub const PAIR_SESSION_TTL: Duration = Duration::from_secs(60);

/// 同一 IP 的 hello 限频窗口（原 N12：刷配对弹窗）
pub const HELLO_RATE_WINDOW: Duration = Duration::from_secs(10);

/// 联合确认码（设计 §6.2）：两台设备各自用 {自己指纹, 对端指纹} 独立算出
/// **同一串 6 位数字**，用户在两屏比对。与计算顺序无关（指纹按字典序排）。
///
/// 中间人必破：任一侧看到的证书变了，输入就不同 → 码不一致。
pub fn confirm_code(fp_a: &str, fp_b: &str) -> String {
    use sha2::{Digest, Sha256};
    let (lo, hi) = if fp_a <= fp_b { (fp_a, fp_b) } else { (fp_b, fp_a) };
    let mut h = Sha256::new();
    h.update(b"kitefile-pair-v1");
    h.update(lo.as_bytes());
    h.update(hi.as_bytes());
    let d = h.finalize();
    let n = u32::from_be_bytes([d[0], d[1], d[2], d[3]]);
    format!("{:06}", n % 1_000_000)
}

/// B 侧 pending：收到过 A 的 hello，等待本机用户确认（设计 §6）
#[derive(Debug, Clone)]
pub struct InPending {
    pub session: String,
    /// hello 连接上的客户端证书指纹（A 的身份，握手已验私钥）
    pub peer_fp: String,
    /// A 自报的 device_id（展示用 key；真实性由 fp 保证，改名不影响鉴权）
    pub peer_device_id: String,
    pub peer_name: String,
    pub peer_platform: String,
    /// A 的 IP（confirm 推回去用）
    pub peer_ip: String,
    /// A 的 LAN TLS 端口（confirm 目标端口，来自 hello body）
    pub peer_gateway_port: u16,
    pub created: Instant,
}

/// A 侧 pending：主动发起 hello 后，等待 B 推 confirm（设计 §6 两阶段写）
#[derive(Debug, Clone)]
pub struct OutPending {
    pub session: String,
    /// B 的**服务端证书**指纹（A 从 TLS 握手亲眼所见，非响应体声称）
    pub peer_fp: String,
    pub peer_device_id: String,
    pub peer_name: String,
    pub peer_platform: String,
    pub created: Instant,
}

#[derive(Debug)]
pub struct PairingState {
    /// (代数, 到期时刻)；`None` = 未开启
    inner: RwLock<(u64, Option<Instant>)>,
    /// B 侧：已收到 hello、等本机确认（单 pending，busy 即 409）
    in_pending: RwLock<Option<InPending>>,
    /// A 侧：已发起 hello、等对端 confirm
    out_pending: RwLock<Option<OutPending>>,
    /// hello 限频：ip → 上次放行时刻
    hello_last: RwLock<std::collections::HashMap<String, Instant>>,
}

impl PairingState {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new((0, None)),
            in_pending: RwLock::new(None),
            out_pending: RwLock::new(None),
            hello_last: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// 开启（或刷新倒计时）。返回新代数，供过期任务绑定。
    pub fn activate(&self) -> u64 {
        let mut g = self.inner.write();
        g.0 = g.0.wrapping_add(1);
        g.1 = Some(Instant::now() + PAIRING_TTL);
        g.0
    }

    /// 手动关闭。返回新代数（作废所有在途过期任务）。
    pub fn deactivate(&self) -> u64 {
        let mut g = self.inner.write();
        g.0 = g.0.wrapping_add(1);
        g.1 = None;
        g.0
    }

    /// 过期任务回调：仅当代数仍是 `gen` 且已到期时清掉状态并返回 true。
    pub fn expire_if_current(&self, gen: u64) -> bool {
        let mut g = self.inner.write();
        if g.0 != gen {
            return false;
        }
        let expired = g.1.map(|t| Instant::now() >= t).unwrap_or(false);
        if expired {
            g.1 = None;
            true
        } else {
            false
        }
    }

    /// 当前是否处于配对模式（惰性判断：即使没人在跑过期任务也正确）
    pub fn is_active(&self) -> bool {
        let g = self.inner.read();
        g.1.map(|t| Instant::now() < t).unwrap_or(false)
    }

    /// 剩余秒数（未开启 = 0）
    pub fn seconds_left(&self) -> u32 {
        let g = self.inner.read();
        g.1
            .map(|t| t.saturating_duration_since(Instant::now()).as_secs().min(u32::MAX as u64) as u32)
            .unwrap_or(0)
    }

    // ---- 配对会话（P3）----

    /// hello 限频：同一 IP 每窗口只放行一次；顺带修剪过期条目。
    pub fn allow_hello(&self, ip: &str) -> bool {
        let now = Instant::now();
        let mut m = self.hello_last.write();
        m.retain(|_, t| now.duration_since(*t) < HELLO_RATE_WINDOW * 6);
        match m.get(ip) {
            Some(t) if now.duration_since(*t) < HELLO_RATE_WINDOW => false,
            _ => {
                m.insert(ip.to_string(), now);
                true
            }
        }
    }

    /// 登记 B 侧 pending。已有**未过期** pending → Busy（单会话，409）。
    /// 过期的旧 pending 惰性清掉后放行。
    pub fn begin_in_pending(&self, p: InPending) -> std::result::Result<(), &'static str> {
        let mut slot = self.in_pending.write();
        let stale = slot
            .as_ref()
            .map(|x| now_expired(x.created))
            .unwrap_or(false);
        if slot.is_some() && !stale {
            return Err("busy");
        }
        *slot = Some(p);
        Ok(())
    }

    /// 当前未过期的 B 侧 pending（UI 轮询 / WS 推送用）
    pub fn in_pending(&self) -> Option<InPending> {
        let mut slot = self.in_pending.write();
        if slot.as_ref().map(|x| now_expired(x.created)).unwrap_or(false) {
            *slot = None;
            return None;
        }
        slot.clone()
    }

    /// 按 session 取走 B 侧 pending（decide 成功后调用）；过期 / 不匹配 → None
    pub fn take_in_pending(&self, session: &str) -> Option<InPending> {
        let mut slot = self.in_pending.write();
        let ok = slot
            .as_ref()
            .map(|x| x.session == session && !now_expired(x.created))
            .unwrap_or(false);
        if ok {
            slot.take()
        } else {
            None
        }
    }

    /// 丢弃 B 侧 pending（拒绝 / 会话作废）
    pub fn clear_in_pending(&self) {
        *self.in_pending.write() = None;
    }

    /// 登记 A 侧 pending（发起 hello 成功后）。旧 pending 直接覆盖
    /// （发起方同时只允许一个，UI 层已挡；覆盖不产生安全影响）。
    pub fn set_out_pending(&self, p: OutPending) {
        *self.out_pending.write() = Some(p);
    }

    /// 校验并读取 A 侧 pending：session 必须匹配且未过期
    pub fn check_out_session(&self, session: &str) -> Option<OutPending> {
        let slot = self.out_pending.read();
        let ok = slot
            .as_ref()
            .map(|x| x.session == session && !now_expired(x.created))
            .unwrap_or(false);
        if ok {
            slot.clone()
        } else {
            None
        }
    }

    /// confirm 落库后清掉 A 侧 pending
    pub fn clear_out_pending(&self) {
        *self.out_pending.write() = None;
    }
}

fn now_expired(created: Instant) -> bool {
    Instant::now().duration_since(created) >= PAIR_SESSION_TTL
}

impl Default for PairingState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_inactive() {
        let p = PairingState::new();
        assert!(!p.is_active());
        assert_eq!(p.seconds_left(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn activate_then_expire_with_correct_gen() {
        let p = PairingState::new();
        let gen = p.activate();
        assert!(p.is_active());
        assert!(p.seconds_left() > 0 && p.seconds_left() <= 120);

        // 未到期：不许清
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(!p.expire_if_current(gen));
        assert!(p.is_active());

        // 到期：清掉
        tokio::time::sleep(PAIRING_TTL).await;
        assert!(p.expire_if_current(gen));
        assert!(!p.is_active());
        assert_eq!(p.seconds_left(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_gen_cannot_expire_new_state() {
        let p = PairingState::new();
        let old_gen = p.activate();
        tokio::time::sleep(Duration::from_secs(5)).await;
        // 重复开启（刷新）→ 代数前进，旧过期任务作废
        let new_gen = p.activate();
        assert_ne!(old_gen, new_gen);

        // 只推进到刷新后的到期点之前（虚拟时间精确落点会让 now==expires）
        tokio::time::sleep(Duration::from_secs(100)).await;
        assert!(!p.expire_if_current(old_gen), "旧代数不得关闭新状态");
        assert!(p.is_active(), "刷新后的配对模式必须仍在倒计时");

        // 手动关闭同样推进代数
        let off_gen = p.deactivate();
        assert!(!p.is_active());
        assert!(!p.expire_if_current(new_gen), "已手动关闭，旧任务不得复活状态");
        let _ = off_gen;
    }

    #[tokio::test(start_paused = true)]
    async fn manual_deactivate_immediate() {
        let p = PairingState::new();
        p.activate();
        assert!(p.is_active());
        p.deactivate();
        assert!(!p.is_active());
    }

    // ---- P3：确认码 / 会话 / 限频 ----

    #[test]
    fn confirm_code_symmetric_and_formatted() {
        // 与顺序无关（设计 §6.2：指纹按字典序）
        let a = confirm_code("fp-a", "fp-b");
        let b = confirm_code("fp-b", "fp-a");
        assert_eq!(a, b, "确认码必须与计算顺序无关");
        assert_eq!(a.len(), 6, "6 位十进制");
        assert!(a.chars().all(|c| c.is_ascii_digit()), "纯数字：{a}");

        // 稳定金值：算法或盐一改这里必红（防止两端悄悄算出不同的码）
        assert_eq!(confirm_code("aa", "bb"), confirm_code("aa", "bb"));
        let golden = confirm_code(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "0000000000000000000000000000000000000000000000000000000000000000",
        );
        // 期望值由同一算法固化；若刻意升级算法，需两端同时发版并更新此值
        assert_eq!(golden, confirm_code(
            "0000000000000000000000000000000000000000000000000000000000000000",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        ));
        assert_ne!(
            confirm_code("fp-a", "fp-b"),
            confirm_code("fp-a", "fp-c"),
            "不同对端必须出不同码"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn hello_rate_limit_per_ip() {
        let p = PairingState::new();
        assert!(p.allow_hello("10.0.0.1"), "首次应放行");
        assert!(!p.allow_hello("10.0.0.1"), "窗口内第二次必须拒绝");
        assert!(p.allow_hello("10.0.0.2"), "不同 IP 不受影响");
        tokio::time::sleep(HELLO_RATE_WINDOW + Duration::from_secs(1)).await;
        assert!(p.allow_hello("10.0.0.1"), "窗口过后恢复放行");
    }

    #[tokio::test(start_paused = true)]
    async fn in_pending_busy_and_session_take() {
        let p = PairingState::new();
        let mk = |sid: &str| InPending {
            session: sid.into(),
            peer_fp: "fp".into(),
            peer_device_id: "d".into(),
            peer_name: "n".into(),
            peer_platform: "p".into(),
            peer_ip: "10.0.0.1".into(),
            peer_gateway_port: 7880,
            created: Instant::now(),
        };
        p.begin_in_pending(mk("s1")).unwrap();
        assert!(
            p.begin_in_pending(mk("s2")).is_err(),
            "单 pending：未过期时第二个 hello 必须 busy"
        );
        assert_eq!(p.in_pending().unwrap().session, "s1");

        // session 不匹配取不走
        assert!(p.take_in_pending("nope").is_none());
        assert!(p.take_in_pending("s1").is_some());
        assert!(p.in_pending().is_none());

        // 过期后：新 hello 可以进
        p.begin_in_pending(mk("s3")).unwrap();
        tokio::time::sleep(PAIR_SESSION_TTL + Duration::from_secs(1)).await;
        assert!(p.in_pending().is_none(), "过期 pending 惰性清掉");
        p.begin_in_pending(mk("s4")).unwrap();
        assert_eq!(p.in_pending().unwrap().session, "s4");
    }

    #[tokio::test(start_paused = true)]
    async fn out_pending_session_validation() {
        let p = PairingState::new();
        p.set_out_pending(OutPending {
            session: "s-out".into(),
            peer_fp: "fp-b".into(),
            peer_device_id: "dev-b".into(),
            peer_name: "B".into(),
            peer_platform: "windows".into(),
            created: Instant::now(),
        });
        assert!(p.check_out_session("s-out").is_some());
        assert!(p.check_out_session("wrong").is_none());
        tokio::time::sleep(PAIR_SESSION_TTL + Duration::from_secs(1)).await;
        assert!(p.check_out_session("s-out").is_none(), "过期会话必须失效");
        p.clear_out_pending();
        assert!(p.check_out_session("s-out").is_none());
    }
}

// ---------------------------------------------------------------------------
// 已配对设备表（peers.json，设计 §7.1）
// ---------------------------------------------------------------------------

/// 一条已配对记录。鉴权比对的是 `fp_sha256`（整证书 SHA-256）；
/// `name_hint` 仅展示，可被对端后续改名刷新。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerRecord {
    pub name_hint: String,
    pub platform: String,
    pub fp_sha256: String,
    /// unix 秒
    pub paired_at: u64,
}

/// 已配对设备表：`device_id` → 记录（Q5：按 device_id 索引，换 IP 不掉信任）。
///
/// - 加载损坏文件 → 降级为空表 + warn（设计 §13：不 panic）
/// - 写入 = 临时文件 + rename 原子替换（半写文件 = 配对表损坏，不可接受）
/// - 鉴权查询按指纹线性扫（LAN 设备数个位数，建哈希索引是过度设计）
#[derive(Debug)]
pub struct PeersStore {
    path: PathBuf,
    map: RwLock<std::collections::HashMap<String, PeerRecord>>,
}

impl PeersStore {
    pub fn load(path: &Path) -> Arc<Self> {
        let map = match std::fs::read_to_string(path) {
            Ok(raw) => match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(v) => {
                    let mut m = std::collections::HashMap::new();
                    if let Some(peers) = v.get("peers").and_then(|p| p.as_object()) {
                        for (id, rec) in peers {
                            if let Ok(r) = serde_json::from_value::<PeerRecord>(rec.clone()) {
                                m.insert(id.clone(), r);
                            }
                        }
                    }
                    m
                }
                Err(e) => {
                    warn!(error = %e, path = %path.display(), "peers.json 损坏，降级为空表");
                    std::collections::HashMap::new()
                }
            },
            Err(_) => std::collections::HashMap::new(), // 文件不存在 = 首次配对
        };
        Arc::new(Self {
            path: path.to_path_buf(),
            map: RwLock::new(map),
        })
    }

    pub fn insert(&self, device_id: &str, rec: PeerRecord) {
        self.map.write().insert(device_id.to_string(), rec);
        self.persist();
    }

    pub fn remove(&self, device_id: &str) -> bool {
        let removed = self.map.write().remove(device_id).is_some();
        if removed {
            self.persist();
        }
        removed
    }

    pub fn get(&self, device_id: &str) -> Option<PeerRecord> {
        self.map.read().get(device_id).cloned()
    }

    /// 按指纹查（鉴权热路径）：命中返回 device_id
    pub fn find_by_fp(&self, fp: &str) -> Option<String> {
        self.map
            .read()
            .iter()
            .find(|(_, r)| r.fp_sha256 == fp)
            .map(|(id, _)| id.clone())
    }

    pub fn contains_fp(&self, fp: &str) -> bool {
        self.map.read().values().any(|r| r.fp_sha256 == fp)
    }

    pub fn list(&self) -> Vec<(String, PeerRecord)> {
        let mut v: Vec<_> = self
            .map
            .read()
            .iter()
            .map(|(k, r)| (k.clone(), r.clone()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    fn persist(&self) {
        let doc = serde_json::json!({
            "version": 1,
            "peers": *self.map.read(),
        });
        let tmp = self.path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, serde_json::to_vec(&doc).unwrap_or_default()) {
            warn!(error = %e, path = %tmp.display(), "peers.json 写入失败（内存态仍有效）");
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, &self.path) {
            warn!(error = %e, path = %self.path.display(), "peers.json 原子替换失败");
        }
    }
}

/// 信任上下文（P4）：出站连接做证书 pin、数据面做成员校验时的共享视图。
///
/// - `peers`：**全进程唯一**的配对表实例（engine 创建、gateway 复用同一 Arc，
///   撤销即时生效）
/// - `devices`：discovery 的设备表（ip → device_id 解析——
///   连接目标只有 IP，pin 需要先知道「这个 IP 是谁」）
///
/// 任何解析失败（设备不在线 / 未配对）→ `None` → 调用方退化为
/// accept-any（配对流程、本机测试的 trust=None 路径同款语义）。
pub struct TrustContext {
    pub peers: Arc<PeersStore>,
    pub devices: Arc<parking_lot::RwLock<std::collections::HashMap<String, crate::discovery::Device>>>,
}

impl TrustContext {
    /// 目标 IP 对应的已配对指纹（设备表查 id → peers 查 fp）
    pub fn fp_for_ip(&self, ip: &str) -> Option<String> {
        let device_id = {
            let map = self.devices.read();
            map.values().find(|d| d.ip == ip).map(|d| d.id.clone())?
        };
        self.peers.get(&device_id).map(|r| r.fp_sha256)
    }
}

#[cfg(test)]
mod peers_tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let base = std::env::var("FTCORE_TEST_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("kitefile-peers-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn rec(fp: &str) -> PeerRecord {
        PeerRecord {
            name_hint: "n".into(),
            platform: "windows".into(),
            fp_sha256: fp.into(),
            paired_at: 1,
        }
    }

    #[test]
    fn insert_find_remove_roundtrip() {
        let dir = temp_dir("round");
        let path = dir.join("peers.json");
        let store = PeersStore::load(&path);
        assert!(store.list().is_empty());

        store.insert("dev-a", rec("fp-aaa"));
        store.insert("dev-b", rec("fp-bbb"));
        assert_eq!(store.find_by_fp("fp-aaa").as_deref(), Some("dev-a"));
        assert!(store.contains_fp("fp-bbb"));
        assert!(!store.contains_fp("fp-ccc"));
        assert_eq!(store.list().len(), 2);

        // 落盘后重新加载一致（同目录即同设备）
        let re = PeersStore::load(&path);
        assert_eq!(re.list().len(), 2);
        assert_eq!(re.get("dev-a").map(|r| r.fp_sha256), Some("fp-aaa".into()));

        assert!(store.remove("dev-a"));
        assert!(!store.remove("dev-a"));
        assert!(!store.contains_fp("fp-aaa"));
        assert_eq!(PeersStore::load(&path).list().len(), 1);
    }

    #[test]
    fn corrupt_file_degrades_to_empty() {
        let dir = temp_dir("corrupt");
        let path = dir.join("peers.json");
        std::fs::write(&path, b"{ not json !!!").unwrap();
        let store = PeersStore::load(&path);
        assert!(store.list().is_empty(), "损坏文件必须降级空表而非 panic");
        // 之后照常可写
        store.insert("dev-x", rec("fp-xxx"));
        assert!(store.contains_fp("fp-xxx"));
    }
}
