//! mDNS 设备发现
//!
//! 使用 mdns-sd crate 注册本机服务并发现局域网内其他设备。
//! 服务类型：`_ftcore._tcp.local.`

use crate::protocol::SERVICE_TYPE;
use crate::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};

/// 设备唯一标识
pub type DeviceId = String;

/// 身份持久化标记文件名（存放在 daemon 数据目录下，与接收目录标记同目录）
const IDENTITY_MARKER: &str = ".ftcore-identity";

/// 设备身份：id（mDNS 服务实例名，全网唯一）+ 显示名。
///
/// 持久化到标记文件，daemon 每次启动复用 —— 重启后 id 不变，
/// 对端设备表按 id 覆盖同一条记录，不会出现“两个相同 IP 的重复设备”。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeviceIdentity {
    pub id: DeviceId,
    pub name: String,
}

/// 身份标记文件路径（daemon 数据目录下）
pub fn identity_marker_path(base_dir: &Path) -> PathBuf {
    base_dir.join(IDENTITY_MARKER)
}

/// 加载或创建持久化身份。
///
/// - 标记文件存在且合法 → 复用（用户改过的名字、既有设备 id 都保留）
/// - 不存在 / 损坏 → 用 `requested_name` 生成新身份并落盘
///
/// 注意：`requested_name` 带随机后缀时（如默认的 "Xiaomi-a1b2c3"），
/// 只有首次会用到，后续启动一律以文件里的为准。
pub fn load_or_create_identity(base_dir: &Path, requested_name: &str) -> DeviceIdentity {
    let marker = identity_marker_path(base_dir);
    if let Ok(content) = std::fs::read_to_string(&marker) {
        if let Ok(ident) = serde_json::from_str::<DeviceIdentity>(&content) {
            if !ident.id.is_empty() && !ident.name.is_empty() {
                return ident;
            }
        }
    }
    let ident = DeviceIdentity {
        id: uuid::Uuid::new_v4().simple().to_string(),
        name: requested_name.to_string(),
    };
    let _ = std::fs::create_dir_all(base_dir);
    if let Err(e) = std::fs::write(&marker, serde_json::to_string(&ident).unwrap_or_default()) {
        warn!(error = %e, "persist identity failed");
    }
    ident
}

/// 局域网内发现到的设备
#[derive(Debug, Clone, serde::Serialize)]
pub struct Device {
    pub id: DeviceId,
    pub name: String,
    pub ip: String,
    pub gateway_port: u16,
    pub transfer_port: u16,
    pub platform: String,
}

/// 发现服务：注册本机并发现局域网内其他设备
pub struct DiscoveryService {
    /// None = 离线模式（未注册 mDNS，仅提供 self 信息）
    daemon: Option<ServiceDaemon>,
    self_id: DeviceId,
    /// 改名需要运行期更新（读多写少）
    self_name: RwLock<String>,
    devices: Arc<RwLock<HashMap<DeviceId, Device>>>,
    /// 身份标记文件路径（改名时持久化）；None = 临时实例（CLI list 等一次性场景）
    identity_path: Option<PathBuf>,
    gateway_port: u16,
    transfer_port: u16,
}

impl DiscoveryService {
    /// 启动发现服务
    ///
    /// - `self_name`：本机显示名
    /// - `self_id`：设备唯一标识（应来自持久化身份，见 `load_or_create_identity`）
    /// - `identity_path`：身份标记文件路径（支持改名持久化；临时实例传 None）
    pub fn new(
        self_name: String,
        self_id: DeviceId,
        gateway_port: u16,
        transfer_port: u16,
        identity_path: Option<PathBuf>,
    ) -> Result<Self> {
        let daemon = ServiceDaemon::new().map_err(|e| crate::CoreError::Discovery(e.to_string()))?;

        let info = build_service_info(&self_id, &self_name, gateway_port, transfer_port)?;
        daemon
            .register(info)
            .map_err(|e| crate::CoreError::Discovery(e.to_string()))?;

        info!(name = %self_name, id = %self_id, "discovery registered");

        Ok(Self {
            daemon: Some(daemon),
            self_id,
            self_name: RwLock::new(self_name),
            devices: Arc::new(RwLock::new(HashMap::new())),
            identity_path,
            gateway_port,
            transfer_port,
        })
    }

    /// 离线模式：不注册 mDNS（UDP 不可用 / 无网络环境），
    /// 仅提供 self_id / self_name / self_ip，设备列表恒为空。
    /// 其余子系统（传输 / 网关）不受影响。
    pub fn new_offline(
        self_name: String,
        self_id: DeviceId,
        identity_path: Option<PathBuf>,
    ) -> Self {
        warn!("discovery in offline mode (no mDNS)");
        Self {
            daemon: None,
            self_id,
            self_name: RwLock::new(self_name),
            devices: Arc::new(RwLock::new(HashMap::new())),
            identity_path,
            gateway_port: 7878,
            transfer_port: 7879,
        }
    }

    /// 修改设备显示名：更新内存 + 持久化到身份标记 + 重新广播 mDNS。
    ///
    /// mdns-sd 的 register 支持对同一实例重复调用以更新服务信息
    /// （重新通告 TXT 属性）；实例名（id）不变，对端设备表同一条目被覆盖。
    pub fn set_name(&self, new_name: &str) -> Result<()> {
        let new_name = new_name.trim();
        if new_name.is_empty() {
            return Err(crate::CoreError::Discovery(
                "device name cannot be empty".into(),
            ));
        }
        if new_name == *self.self_name.read() {
            return Ok(());
        }

        // 持久化（best-effort：写失败不影响本次生效，只是重启后回旧名）
        if let Some(p) = &self.identity_path {
            let ident = DeviceIdentity {
                id: self.self_id.clone(),
                name: new_name.to_string(),
            };
            if let Err(e) = std::fs::write(p, serde_json::to_string(&ident).unwrap_or_default()) {
                warn!(error = %e, "persist device name failed");
            }
        }
        *self.self_name.write() = new_name.to_string();

        // 重新注册 mDNS（广播新名字）
        if let Some(daemon) = &self.daemon {
            let info = build_service_info(&self.self_id, new_name, self.gateway_port, self.transfer_port)?;
            daemon
                .register(info)
                .map_err(|e| crate::CoreError::Discovery(e.to_string()))?;
            info!(name = %new_name, "device renamed, mDNS re-announced");
        }
        Ok(())
    }

    /// 后台轮询发现事件，更新设备表
    pub fn spawn_event_loop(self: Arc<Self>, rt: tokio::runtime::Handle) {
        let receiver = match self.daemon.as_ref().map(|d| d.browse(SERVICE_TYPE)) {
            Some(Ok(r)) => r,
            _ => return, // 离线模式或 browse 失败：无事件可轮询
        };

        // 设备下线检测：应用层主动探测（mDNS 事件不可用于此目的，见 spawn_probe_loop 注释）
        spawn_probe_loop(self.devices.clone(), rt.clone());

        rt.spawn(async move {
            loop {
                match receiver.recv_async().await {
                    Ok(event) => match event {
                        ServiceEvent::ServiceFound(_ty, fullname) => {
                            info!(%fullname, "discovered device");
                        }
                        ServiceEvent::ServiceResolved(info) => {
                            let id = info.get_fullname().split('.').next().unwrap_or("").to_string();
                            let props = info.get_properties();
                            let name = props
                                .iter()
                                .find(|p| p.key() == "name")
                                .map(|p| p.val_str().to_string())
                                .unwrap_or_else(|| id.clone());
                            let platform = props
                                .iter()
                                .find(|p| p.key() == "platform")
                                .map(|p| p.val_str().to_string())
                                .unwrap_or_default();
                            let gateway_port = props
                                .iter()
                                .find(|p| p.key() == "gateway_port")
                                .and_then(|p| p.val_str().parse().ok())
                                .unwrap_or(7878u16);
                            let transfer_port = props
                                .iter()
                                .find(|p| p.key() == "transfer_port")
                                .and_then(|p| p.val_str().parse().ok())
                                .unwrap_or(7879u16);
                            let addrs: Vec<IpAddr> =
                                info.get_addresses().iter().copied().collect();
                            let ip = pick_peer_address(&addrs);

                            let device = Device {
                                id: id.clone(),
                                name: name.clone(),
                                ip,
                                gateway_port,
                                transfer_port,
                                platform,
                            };
                            info!(?device, "resolved device");
                            self.devices.write().insert(id, device);
                        }
                        ServiceEvent::ServiceRemoved(_ty, fullname) => {
                            let id = fullname.split('.').next().unwrap_or("").to_string();
                            info!(%fullname, "device removed");
                            self.devices.write().remove(&id);
                        }
                        other => {
                            warn!(?other, "mdns event");
                        }
                    },
                    Err(e) => {
                        warn!(error = %e, "mdns recv error");
                        break;
                    }
                }
            }
        });
    }

    /// 当前已发现的设备列表（排除自身）
    pub fn list_devices(&self) -> Vec<Device> {
        self.devices
            .read()
            .values()
            .filter(|d| d.id != self.self_id)
            .cloned()
            .collect()
    }

    pub fn self_id(&self) -> &str {
        &self.self_id
    }

    pub fn self_name(&self) -> String {
        self.self_name.read().clone()
    }

    /// 本机首选 IPv4 地址（用于发送方在 offer 时告诉接收方「我从哪 IP 来」）
    pub fn self_ip(&self) -> Option<String> {
        my_ipv4_addrs().into_iter().next()
    }
}

/// 设备下线检测：应用层 TCP 探测。
///
/// **为什么不能用 mDNS 事件判断下线**（mdns-sd 0.11.5 源码确认）：
/// `ServiceResolved` 只在记录**首次**进入缓存时发一次。对端周期性的刷新
/// 响应走 `add_or_update` 的"已存在"分支（reset_ttl、updated=false），
/// 不触发任何事件。因此"多久没收到 mDNS 事件"区分不了在线/离线——
/// 曾经基于这个错误假设做过 5 分钟过期清理，结果是守护进程跑满 5 分钟后
/// 把**所有**设备（含在线的）从表里删光，而 mdns-sd 缓存里的 PTR 还活着
/// （TTL 75 分钟）不会重新发 ServiceFound，设备从此消失、两端互相看不到。
///
/// **本方案**：每 60 秒对表内每个设备的 gateway 端口做一次 TCP 握手
/// （握手成功即代表对端 daemon 在监听，不发 HTTP 请求），连续 3 次失败
/// 才移除——防 gateway 瞬时忙导致误判。强杀进程发不出 mDNS goodbye 的
/// 僵尸条目在 ~3 分钟内被清掉，在线设备零误伤。
fn spawn_probe_loop(
    devices: Arc<RwLock<HashMap<DeviceId, Device>>>,
    rt: tokio::runtime::Handle,
) {
    rt.spawn(async move {
        let mut fails: HashMap<DeviceId, u32> = HashMap::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let snapshot: Vec<(DeviceId, String, u16)> = {
                let map = devices.read();
                map.iter()
                    .map(|(id, d)| (id.clone(), d.ip.clone(), d.gateway_port))
                    .collect()
            };
            for (id, ip, port) in snapshot {
                // 空 IP（解析失败）的条目也按探测失败处理
                let alive = if ip.is_empty() {
                    false
                } else {
                    tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        tokio::net::TcpStream::connect((ip.as_str(), port)),
                    )
                    .await
                    .map(|r| r.is_ok())
                    .unwrap_or(false)
                };

                if alive {
                    fails.remove(&id);
                } else {
                    let n = fails.entry(id.clone()).or_insert(0);
                    *n += 1;
                    if *n >= 3 {
                        info!(%id, ip, "device probe failed 3 times, removing from device table");
                        devices.write().remove(&id);
                        fails.remove(&id);
                    }
                }
            }
        }
    });
}

/// 构造 mDNS 服务注册信息（id 作实例名与主机名，name 等走 TXT 属性）
fn build_service_info(
    self_id: &str,
    self_name: &str,
    gateway_port: u16,
    transfer_port: u16,
) -> Result<ServiceInfo> {
    // 取本机所有 IPv4 地址，作为可被发现的地址
    let my_ips = my_ipv4_addrs();
    if my_ips.is_empty() {
        return Err(crate::CoreError::Discovery(
            "no available IPv4 address".into(),
        ));
    }

    let mut properties = HashMap::new();
    properties.insert("id".into(), self_id.to_string());
    properties.insert("name".into(), self_name.to_string());
    properties.insert("platform".into(), crate::platform::platform_name().to_string());
    properties.insert("gateway_port".into(), gateway_port.to_string());
    properties.insert("transfer_port".into(), transfer_port.to_string());

    let hostname = format!("{}.local.", self_id);
    let host_ipv4: Vec<&str> = my_ips.iter().map(|s| s.as_str()).collect();
    ServiceInfo::new(
        SERVICE_TYPE,
        self_id,
        &hostname,
        &host_ipv4[..],
        transfer_port,
        properties,
    )
    .map_err(|e| crate::CoreError::Discovery(e.to_string()))
}

/// 从对端通告的多个地址里挑一个真正能连上的。
///
/// 多网卡机器（装了 VMware / Hyper-V / Docker 就会有虚拟网卡）上，mDNS 会把
/// **所有**网卡的地址都放进同一条 A 记录集合里。原来直接取第一个，而集合的
/// 迭代顺序是不确定的——实测在同一台机器上能先后解析出 192.168.17.1、
/// 192.168.73.1、172.23.48.1 三个虚拟网卡地址，真实网卡 10.124.68.246(WLAN)
/// 反而选不中，对端拿到这种地址必然连不上。
///
/// 策略：优先选与本机某个网卡**同网段**的那个。都没有同网段的（比如跨网段
/// 场景）再退回原来的"取第一个"，保证不会比修复前更差。
fn pick_peer_address(candidates: &[IpAddr]) -> String {
    let mut nets = my_ipv4_nets();
    // 把主网卡（走默认路由的那张）所在网段排到最前，优先匹配。
    // 否则"同网段"条件太宽松：自己发现自己时，每个虚拟网卡地址都跟自己
    // 同网段，先撞上哪个是不确定的（实测会选中 VMware 的 192.168.73.1，
    // 而真实出口是 WLAN 的 10.124.68.246）。
    if let Some(primary) = primary_ipv4() {
        if let Some(pos) = nets.iter().position(|(ip, _)| *ip == primary) {
            let entry = nets.remove(pos);
            nets.insert(0, entry);
        }
    }
    pick_peer_address_with_nets(candidates, &nets)
}

/// 本机主用 IPv4：通往默认路由的那个源地址。
///
/// 做法是 UDP connect 到一个**不需要可达**的目标再读 local_addr ——
/// connect 只查路由表、不发任何包，所以既无网络开销也无隐私顾虑。
/// 拿不到（比如没有默认路由）就返回 None，调用方退回普通顺序。
fn primary_ipv4() -> Option<Ipv4Addr> {
    use std::net::UdpSocket;
    let sock = UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect("8.8.8.8:80").ok()?;
    match sock.local_addr().ok()?.ip() {
        IpAddr::V4(v4) => Some(v4),
        _ => None,
    }
}

fn pick_peer_address_with_nets(
    candidates: &[IpAddr],
    nets: &[(Ipv4Addr, Ipv4Addr)],
) -> String {
    // 外层必须是**网段**而不是候选地址。
    // 上一版写反了（外层候选、内层网段），结果主网卡优先级形同虚设：
    // 虚拟网卡地址同样"与本机某网卡同网段"，先撞上哪个全看候选集合的
    // 迭代顺序，于是一会儿选中 10.124.68.246(WLAN)、一会儿 192.168.17.1(VMware)。
    // 外层必须是**网段**而不是候选地址。
    // 上一版写反了（外层候选、内层网段），结果主网卡优先级形同虚设：
    // 虚拟网卡地址同样"与本机某网卡同网段"，先撞上哪个全看候选集合的
    // 迭代顺序，于是一会儿选中 10.124.68.246(WLAN)、一会儿 192.168.17.1(VMware)。
    for (mine, mask) in nets {
        for c in candidates {
            if let IpAddr::V4(v4) = c {
                if in_same_subnet(*v4, *mine, *mask) {
                    return v4.to_string();
                }
            }
        }
    }
    candidates
        .iter()
        .next()
        .map(|a| a.to_string())
        .unwrap_or_default()
}

fn in_same_subnet(a: Ipv4Addr, b: Ipv4Addr, mask: Ipv4Addr) -> bool {
    (u32::from(a) & u32::from(mask)) == (u32::from(b) & u32::from(mask))
}

/// 本机各网卡的 (IPv4 地址, 子网掩码)
pub fn my_ipv4_nets() -> Vec<(Ipv4Addr, Ipv4Addr)> {
    let mut out = Vec::new();
    if let Ok(ifaces) = get_if_addrs::get_if_addrs() {
        for iface in ifaces {
            // if_addrs 0.13 的掩码在 IfAddr::V4 里，Interface 本身没有 netmask()
            if let get_if_addrs::IfAddr::V4(v4) = &iface.addr {
                if v4.ip.is_loopback() || v4.ip.is_unspecified() {
                    continue;
                }
                out.push((v4.ip, v4.netmask));
            }
        }
    }
    out
}

/// 获取本机所有非环回 IPv4 地址
pub fn my_ipv4_addrs() -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(ifaces) = get_if_addrs::get_if_addrs() {
        for iface in ifaces {
            if let IpAddr::V4(v4) = iface.ip() {
                if !v4.is_loopback() && !v4.is_unspecified() {
                    out.push(v4.to_string());
                }
            }
        }
    }
    out
}

// get_if_addrs 是平台支持库；这里直接引入以避免单独 crate 列表
mod get_if_addrs {
    pub use if_addrs::*;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }
    fn v4(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    /// 真实场景：本机有 WLAN(10.124.68.246/16) 和 VMware(192.168.73.1/24)，
    /// 对端把三个网卡地址都通告了过来，必须选中跟我们同网段的那个。
    #[test]
    fn prefers_address_on_my_subnet_over_virtual_adapters() {
        let nets = vec![
            (v4("10.124.68.246"), v4("255.255.0.0")),
            (v4("192.168.73.1"), v4("255.255.255.0")),
        ];
        let candidates = vec![ip("192.168.17.1"), ip("172.23.48.1"), ip("10.124.5.9")];
        assert_eq!(
            pick_peer_address_with_nets(&candidates, &nets),
            "10.124.5.9",
            "不能选中虚拟网卡地址，否则对端连不上"
        );
    }

    /// 主网卡网段虽然在 nets[0]，但候选里**虚拟网卡地址排在前面**——
    /// 必须仍然选中主网卡网段的地址。
    ///
    /// 这条抓的是上一版的实现错误：当时外层遍历候选、内层遍历网段，
    /// 于是 192.168.17.1 会先撞上"与本机某网卡同网段"而胜出，
    /// 主网卡优先级等于没做（表现为选中的 IP 时好时坏）。
    #[test]
    fn primary_subnet_wins_even_when_virtual_addr_comes_first() {
        let nets = vec![
            (v4("10.124.68.246"), v4("255.255.0.0")), // 主网卡，已排到最前
            (v4("192.168.17.1"), v4("255.255.255.0")),
        ];
        let candidates = vec![ip("192.168.17.1"), ip("172.23.48.1"), ip("10.124.5.9")];
        assert_eq!(
            pick_peer_address_with_nets(&candidates, &nets),
            "10.124.5.9",
            "主网卡网段必须优先，不能因为候选顺序而选中虚拟网卡地址"
        );
    }

    #[test]
    fn falls_back_to_first_when_nothing_matches() {
        let nets = vec![(v4("10.124.68.246"), v4("255.255.0.0"))];
        let candidates = vec![ip("192.168.17.1"), ip("172.23.48.1")];
        assert_eq!(
            pick_peer_address_with_nets(&candidates, &nets),
            "192.168.17.1",
            "没匹配到时行为应与修复前一致（取第一个），不能变得更差"
        );
    }

    #[test]
    fn empty_candidates_yield_empty_string() {
        let nets = vec![(v4("10.0.0.1"), v4("255.255.255.0"))];
        assert_eq!(pick_peer_address_with_nets(&[], &nets), "");
    }

    #[test]
    fn subnet_check_respects_netmask() {
        assert!(in_same_subnet(
            v4("10.124.5.9"),
            v4("10.124.68.246"),
            v4("255.255.0.0")
        ));
        assert!(!in_same_subnet(
            v4("10.125.5.9"),
            v4("10.124.68.246"),
            v4("255.255.0.0")
        ));
        assert!(!in_same_subnet(
            v4("10.124.5.9"),
            v4("10.124.68.246"),
            v4("255.255.255.0")
        ));
    }
}
