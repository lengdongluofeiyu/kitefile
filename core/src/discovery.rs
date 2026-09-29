//! mDNS 设备发现
//!
//! 使用 mdns-sd crate 注册本机服务并发现局域网内其他设备。
//! 服务类型：`_kitefile._tcp.local.`

use crate::protocol::SERVICE_TYPE;
use crate::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// 设备唯一标识
pub type DeviceId = String;

/// 身份持久化标记文件名（存放在 daemon 数据目录下，与接收目录标记同目录）
const IDENTITY_MARKER: &str = ".kitefile-identity";

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
    /// 对端是否处于配对模式（TXT `pair=1`）。旧版客户端不发该键 → false。
    /// P2：UI 显示「可配对」；P3：未配对设备只在此为 true 时展示/可发起配对。
    #[serde(default)]
    pub pair: bool,
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
    /// 配对模式状态（P2）：驱动 mDNS `pair` TXT 与 P3 的配对接口门
    pairing: Arc<crate::pairing::PairingState>,
    /// LAN whoami 扫描防重入（配对页 1s ticker + 切换配对模式可能叠跑）
    scanning: Arc<AtomicBool>,
}

/// LAN whoami 扫描并发与单点超时：网段里大量主机会在 connect 段快速失败，
/// 必须短超时 + 并发，否则 120s 配对窗口会被扫描本身吃掉。
const SCAN_CONCURRENCY: usize = 32;
const SCAN_PROBE_TIMEOUT: Duration = Duration::from_millis(450);

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

        let info = build_service_info(&self_id, &self_name, gateway_port, transfer_port, false)?;
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
            pairing: Arc::new(crate::pairing::PairingState::new()),
            scanning: Arc::new(AtomicBool::new(false)),
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
            pairing: Arc::new(crate::pairing::PairingState::new()),
            scanning: Arc::new(AtomicBool::new(false)),
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

        // 重新注册 mDNS（广播新名字；pair 标志按当前状态带上）
        self.reregister_service()?;
        info!(name = %new_name, "device renamed, mDNS re-announced");
        Ok(())
    }

    /// 按当前状态（名字 / 端口 / pair 标志）重新注册 mDNS 服务。
    /// mdns-sd 对同一实例重复 register 即更新 TXT（service_daemon.rs:286）。
    fn reregister_service(&self) -> Result<()> {
        let Some(daemon) = &self.daemon else {
            return Ok(()); // 离线模式无 mDNS 可更
        };
        let name = self.self_name.read().clone();
        let pair = self.pairing.is_active();
        let info = build_service_info(&self.self_id, &name, self.gateway_port, self.transfer_port, pair)?;
        daemon
            .register(info)
            .map_err(|e| crate::CoreError::Discovery(e.to_string()))?;
        Ok(())
    }

    /// identity 标记路径 → receive_dir（TLS 身份与标记同目录）
    fn receive_dir_from(identity_path: Option<&Path>) -> Option<PathBuf> {
        identity_path.and_then(|p| p.parent()).map(|p| p.to_path_buf())
    }

    /// 即时同步已知设备的 pair 标志：主动向表内每个设备拉一次 whoami，
    /// 把对方的 `pairing_enabled` 合并进本机设备表。**不改变**本机配对模式
    /// 状态与倒计时。
    ///
    /// 公开给 `/api/pair/refresh`：添加设备页搜索期间定期触发，兜住
    /// 「对端比本机晚开启配对模式」的同步窗口（切换时只刷当时表内的设备）。
    ///
    /// 为什么不能只靠 mDNS：pair=1 靠 unsolicited 组播公告传播，而
    /// Android 未持 MulticastLock 时系统直接丢组播、防火墙也可能拦
    /// （真实反馈：双方都开了配对模式却互相看不见）。whoami 走
    /// TCP+TLS 产品命脉通道，能传文件这条路就通。同机双 daemon 的
    /// 传播测试（mdns_pair_propagation_test）证明 mDNS 逻辑本身无恙——
    /// 这里兜的是通道层。
    ///
    /// **P2 补强（手机配对页看不到电脑）**：whoami 同步只覆盖**表里已有**
    /// 的设备；mDNS 完全没把对端推进设备表时，刷新再多次也只会对着空表。
    /// 因此这里额外触发 [`Self::discover_via_whoami_scan`]：按本机网段
    /// 主动探 LAN TLS whoami，把「组播丢了但 TCP 还通」的对端找回来。
    pub fn refresh_pair_flags(&self) {
        // ① 已知设备 whoami 同步（不改变本机配对模式）
        if let Some(dir) = Self::receive_dir_from(self.identity_path.as_deref()) {
            let devices = Arc::clone(&self.devices);
            let spawned = tokio::runtime::Handle::try_current().map(|h| {
                h.spawn(async move {
                    let snapshot: Vec<(DeviceId, Device)> = {
                        let map = devices.read();
                        map.iter().map(|(id, d)| (id.clone(), d.clone())).collect()
                    };
                    for (id, mut dev) in snapshot {
                        sync_device_via_whoami(&mut dev, &dir).await;
                        devices.write().insert(id, dev);
                    }
                    info!("pair flag refresh over known devices done");
                })
            });
            if spawned.is_err() {
                warn!("no runtime for pair flag refresh; will catch up via 60s probe");
            }
        }
        // ② 局域网主动扫描：未知设备也拉进表（mDNS 静默时的发现兜底）
        self.discover_via_whoami_scan();
    }

    /// 局域网 whoami 主动扫描：探测本机各网段上 LAN TLS 口的 `/api/whoami`，
    /// 把应答设备 upsert 进表（含 `pair` = 对端 `pairing_enabled`）。
    ///
    /// **为什么必须有这条路**：配对页只展示本机 daemon 设备表；表的来源
    /// 原先只有 mDNS。组播被 Android/路由器/防火墙丢掉时，双方都开了
    /// 配对模式也「互相看不见」。whoami 是免配对、走 TCP+TLS 的产品
    /// 命脉通道——能建连就能发现，不依赖 TTL 续期语义。
    pub fn discover_via_whoami_scan(&self) {
        let ips: Vec<String> = lan_scan_targets(&my_ipv4_nets())
            .into_iter()
            .map(|ip| ip.to_string())
            .collect();
        // 本机实际 LAN TLS 口 + 候选口（对端可能因端口保留退避）
        let mut ports = vec![self.gateway_port];
        ports.extend(crate::LAN_TLS_PORT_CANDIDATES.iter().copied());
        ports.dedup();
        self.discover_via_whoami_scan_with(&ips, &ports);
    }

    /// 可注入探测目标的扫描入口（测试用；生产走 [`Self::discover_via_whoami_scan`]）。
    pub fn discover_via_whoami_scan_with(&self, ips: &[String], ports: &[u16]) {
        let Some(dir) = Self::receive_dir_from(self.identity_path.as_deref()) else {
            return; // 临时实例（CLI list 等）：无身份目录，跳过
        };
        if ips.is_empty() || ports.is_empty() {
            return;
        }
        // 防重入：1s ticker 的 /api/pair/refresh 与切换配对模式可能叠跑
        if self.scanning.swap(true, Ordering::SeqCst) {
            return;
        }
        let devices = Arc::clone(&self.devices);
        let self_id = self.self_id.clone();
        let scanning = Arc::clone(&self.scanning);
        let ips = ips.to_vec();
        let ports = ports.to_vec();
        let spawned = tokio::runtime::Handle::try_current().map(|h| {
            let scanning = Arc::clone(&scanning);
            h.spawn(async move {
                // 复位标志：扫描结束 / 提前 return 都必须放开下一轮
                let _clear = ClearFlag(&scanning);
                let Ok(identity) = crate::tls::NodeIdentity::load_or_create(&dir) else {
                    warn!("lan whoami scan: load identity failed, skip");
                    return;
                };
                let sem = Arc::new(tokio::sync::Semaphore::new(SCAN_CONCURRENCY));
                let mut handles = Vec::with_capacity(ips.len() * ports.len());
                for ip in &ips {
                    for &port in &ports {
                        let ip = ip.clone();
                        let sem = Arc::clone(&sem);
                        let identity = Arc::clone(&identity);
                        handles.push(tokio::spawn(async move {
                            let Ok(_permit) = sem.acquire().await else {
                                return None;
                            };
                            let fut = crate::httpc::http_get_tls_identity(
                                &ip,
                                port,
                                "/api/whoami",
                                &identity,
                                None,
                                SCAN_PROBE_TIMEOUT,
                                SCAN_PROBE_TIMEOUT,
                            );
                            match tokio::time::timeout(SCAN_PROBE_TIMEOUT, fut).await {
                                Ok(Ok(body)) => Some((ip, port, body)),
                                _ => None,
                            }
                        }));
                    }
                }
                let mut hits = 0usize;
                for h in handles {
                    if let Ok(Some((ip, port, body))) = h.await {
                        hits += 1;
                        if let Some(dev) = device_from_whoami(&ip, port, &body, &self_id) {
                            info!(
                                %ip,
                                port,
                                id = %dev.id,
                                pair = dev.pair,
                                "lan whoami scan discovered device"
                            );
                            devices.write().insert(dev.id.clone(), dev);
                        }
                    }
                }
                info!(
                    probes = ips.len() * ports.len(),
                    hits,
                    "lan whoami scan done"
                );
            })
        });
        if spawned.is_err() {
            self.scanning.store(false, Ordering::SeqCst);
            warn!("no runtime for lan whoami scan");
        }
    }

    /// 进入 / 退出配对模式（P2）。返回 (是否开启, 剩余秒数)。
    ///
    /// - 开启：mDNS TXT 置 `pair=1`（对端设备表标「可配对」），并 spawn
    ///   一个 120s 过期任务——到点自动退出并把 TXT 改回 `pair=0`。
    ///   重复开启刷新倒计时（代数前进，旧任务作废）。
    /// - 关闭：立即生效，同样推进代数作废旧任务。
    /// - 重启后默认关闭（状态只在内存，符合设计 §5）。
    ///
    /// 只从 async 上下文调用（gateway handler）；无运行时时不 spawn 过期任务，
    /// 惰性判断（`is_active`）仍会正确按到期时间收敛。
    pub fn set_pairing_mode(self: Arc<Self>, on: bool) -> Result<(bool, u32)> {
        let gen = if on {
            self.pairing.activate()
        } else {
            self.pairing.deactivate()
        };
        // 先落状态再广播：即使 register 失败，is_active 语义也正确（UI 可见）
        if let Err(e) = self.reregister_service() {
            warn!(error = %e, "pairing mode mDNS re-register failed");
        }
        // 即时向已知设备同步 pair 标志（mDNS 公告可能被组播过滤/防火墙丢弃）
        self.refresh_pair_flags();

        if on {
            let this = Arc::clone(&self);
            let spawned = tokio::runtime::Handle::try_current().map(|h| {
                h.spawn(async move {
                    tokio::time::sleep(crate::pairing::PAIRING_TTL).await;
                    if this.pairing.expire_if_current(gen) {
                        let _ = this.reregister_service();
                        info!("pairing mode expired, pair flag cleared");
                    }
                })
            });
            if spawned.is_err() {
                warn!("no runtime for pairing TTL task; lazy expiry only");
            }
        }
        info!(on, "pairing mode set");
        Ok((self.pairing.is_active(), self.pairing.seconds_left()))
    }

    /// 当前配对模式状态 (是否开启, 剩余秒数)
    pub fn pairing_status(&self) -> (bool, u32) {
        (self.pairing.is_active(), self.pairing.seconds_left())
    }

    /// 配对协议状态（P3：pending 会话 / 限频 / 确认码所需）
    pub fn pairing(&self) -> Arc<crate::pairing::PairingState> {
        Arc::clone(&self.pairing)
    }

    /// 后台轮询发现事件，更新设备表
    pub fn spawn_event_loop(self: Arc<Self>, rt: tokio::runtime::Handle) {
        let receiver = match self.daemon.as_ref().map(|d| d.browse(SERVICE_TYPE)) {
            Some(Ok(r)) => r,
            _ => return, // 离线模式或 browse 失败：无事件可轮询
        };

        // 设备下线检测：应用层主动探测（mDNS 事件不可用于此目的，见 spawn_probe_loop 注释）
        spawn_probe_loop(
            self.devices.clone(),
            Self::receive_dir_from(self.identity_path.as_deref()),
            rt.clone(),
        );

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
                            let pair = props
                                .iter()
                                .find(|p| p.key() == "pair")
                                .map(|p| p.val_str() == "1")
                                .unwrap_or(false);
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
                                pair,
                            };
                            info!(?device, "resolved device");
                            // 发现即同步：mDNS 带 pair=1 则直接入库；为 false 时
                            // 主动拉一次 whoami——对端可能刚开启配对模式，其
                            // unsolicited 组播公告可能被过滤/防火墙丢弃（真实反馈：
                            // 手机配对页看不到电脑）。不等 60s 探活周期。
                            let need_sync = !device.pair;
                            self.devices.write().insert(id.clone(), device);
                            if need_sync {
                                let devices = Arc::clone(&self.devices);
                                let dir = Self::receive_dir_from(self.identity_path.as_deref());
                                if let Some(dir) = dir {
                                    if let Ok(h) = tokio::runtime::Handle::try_current() {
                                        h.spawn(async move {
                                            let Some(mut d) =
                                                devices.read().get(&id).cloned()
                                            else {
                                                return; // 已被移除
                                            };
                                            sync_device_via_whoami(&mut d, &dir).await;
                                            devices.write().insert(id, d);
                                        });
                                    }
                                }
                            }
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

    /// 设备表句柄（ip → device 解析供信任层查指纹用；阶段 5 P4）
    pub fn devices_handle(
        &self,
    ) -> Arc<RwLock<HashMap<DeviceId, Device>>> {
        Arc::clone(&self.devices)
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
/// （握手成功即代表对端 daemon 在监听），连续 3 次失败才移除——防
/// gateway 瞬时忙导致误判。强杀进程发不出 mDNS goodbye 的僵尸条目在
/// ~3 分钟内被清掉，在线设备零误伤。
///
/// **P2 增强**：TCP 存活时顺带 TLS GET `/api/whoami`，把 name/platform/
/// 端口/**pairing_enabled** 合并进设备表（见 [`sync_device_via_whoami`]）。
/// 这条路是产品命脉通道（能传文件就说明可达），专门兜住 mDNS 组播
/// 单点依赖——真实反馈：双方都开了配对模式却互相看不见，根因之一是
/// mDNS 公告被组播过滤（Android 未持 MulticastLock）/防火墙丢弃。
fn spawn_probe_loop(
    devices: Arc<RwLock<HashMap<DeviceId, Device>>>,
    receive_dir: Option<PathBuf>,
    rt: tokio::runtime::Handle,
) {
    rt.spawn(async move {
        let mut fails: HashMap<DeviceId, u32> = HashMap::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let snapshot: Vec<(DeviceId, Device)> = {
                let map = devices.read();
                map.iter().map(|(id, d)| (id.clone(), d.clone())).collect()
            };
            for (id, dev) in snapshot {
                let ip = dev.ip.clone();
                let port = dev.gateway_port;
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
                    // 存活 → 拉 whoami 同步字段（失败保持原值，不降级）
                    if let Some(dir) = receive_dir.as_ref() {
                        let mut dev = dev;
                        sync_device_via_whoami(&mut dev, dir).await;
                        devices.write().insert(id, dev);
                    }
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

/// TLS GET 对端 `/api/whoami` 并把字段合并进 `device`（原地修改）。
///
/// - 同步：`name` / `platform` / `gateway_port` / `transfer_port` /
///   **`pairing_enabled`（→ device.pair）**
/// - 任何失败（超时 / TLS / 解析）**保持原值**——探活的存活判定与字段
///   同步解耦，whoami 挂了只是字段不刷新，不导致设备被误移除
/// - whoami 是免配对接口（设计 §9.2），未配对/配对模式外都可拉取；
///   TLS 客户端 accept-any（无 pin 依赖）
async fn sync_device_via_whoami(device: &mut Device, receive_dir: &Path) {
    if device.ip.is_empty() {
        return;
    }
    let ip = device.ip.clone();
    let lan_port = device.gateway_port;
    let fut = crate::httpc::http_get_tls(&ip, lan_port, "/api/whoami", receive_dir, None);
    let body = match tokio::time::timeout(std::time::Duration::from_secs(3), fut).await {
        Ok(Ok(b)) => b,
        _ => return,
    };
    merge_whoami_into_device(device, &body);
}

/// 把 whoami JSON 合并进已有 Device（探活同步与扫描共用）。
fn merge_whoami_into_device(device: &mut Device, body: &str) {
    let Ok(j) = serde_json::from_str::<serde_json::Value>(body) else {
        return;
    };
    if let Some(v) = j.get("name").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            device.name = v.to_string();
        }
    }
    if let Some(v) = j.get("platform").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            device.platform = v.to_string();
        }
    }
    if let Some(v) = j.get("gateway_port").and_then(|v| v.as_u64()) {
        if v > 0 && v <= u16::MAX as u64 {
            device.gateway_port = v as u16;
        }
    }
    if let Some(v) = j.get("transfer_port").and_then(|v| v.as_u64()) {
        if v > 0 && v <= u16::MAX as u64 {
            device.transfer_port = v as u16;
        }
    }
    if let Some(v) = j.get("pairing_enabled").and_then(|v| v.as_bool()) {
        device.pair = v;
    }
}

/// 从 whoami 应答构造设备条目（LAN 扫描 upsert 用）。
/// `probed_port` 是探测用的端口，响应里缺 `gateway_port` 时兜底。
fn device_from_whoami(ip: &str, probed_port: u16, body: &str, self_id: &str) -> Option<Device> {
    let j: serde_json::Value = serde_json::from_str(body).ok()?;
    let id = j.get("id").and_then(|v| v.as_str())?.to_string();
    if id.is_empty() || id == self_id {
        return None;
    }
    let name = j
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(&id)
        .to_string();
    let platform = j
        .get("platform")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let gateway_port = j
        .get("gateway_port")
        .and_then(|v| v.as_u64())
        .filter(|&v| v > 0 && v <= u16::MAX as u64)
        .map(|v| v as u16)
        .unwrap_or(probed_port);
    let transfer_port = j
        .get("transfer_port")
        .and_then(|v| v.as_u64())
        .filter(|&v| v > 0 && v <= u16::MAX as u64)
        .map(|v| v as u16)
        .unwrap_or(7879);
    let pair = j
        .get("pairing_enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Some(Device {
        id,
        name,
        ip: ip.to_string(),
        gateway_port,
        transfer_port,
        platform,
        pair,
    })
}

/// 扫描用目标 IP：各网段主机地址（去重、不含本机）。
pub fn lan_scan_targets(nets: &[(Ipv4Addr, Ipv4Addr)]) -> Vec<Ipv4Addr> {
    let mut out: Vec<Ipv4Addr> = Vec::new();
    let mut seen = HashSet::new();
    for (ip, mask) in nets {
        for host in subnet_scan_hosts(*ip, *mask) {
            if seen.insert(host) {
                out.push(host);
            }
        }
    }
    out
}

/// 单网段扫描主机列表。
///
/// `/24` 及更细的掩码扫整段（去掉网络/广播/本机）；掩码更粗
/// （如 `/16`）时只扫本机所在的 `/24`——企业网整段扫描会把配对窗口耗尽。
fn subnet_scan_hosts(ip: Ipv4Addr, mask: Ipv4Addr) -> Vec<Ipv4Addr> {
    let mask_bits = u32::from(mask).count_ones();
    if mask_bits >= 24 {
        let ip_u = u32::from(ip);
        let network = ip_u & u32::from(mask);
        let count = 1u32 << (32 - mask_bits);
        let broadcast = network + count - 1;
        (network + 1..broadcast)
            .map(Ipv4Addr::from)
            .filter(|h| *h != ip)
            .collect()
    } else {
        let base = u32::from(ip) & 0xFFFF_FF00;
        (1..255u32)
            .map(|i| Ipv4Addr::from(base + i))
            .filter(|h| *h != ip)
            .collect()
    }
}

/// mDNS 广告地址：优先主网卡 + 同网段地址，避免多网卡机器
/// （VMware / Hyper-V）把虚拟地址一并广播，对端 pick_peer_address
/// 选错网卡后连不上。
fn lan_advertise_addrs() -> Vec<String> {
    let nets = my_ipv4_nets();
    let Some(primary) = primary_ipv4() else {
        return my_ipv4_addrs();
    };
    let primary_mask = nets
        .iter()
        .find(|(ip, _)| *ip == primary)
        .map(|(_, m)| *m)
        .unwrap_or_else(|| Ipv4Addr::new(255, 255, 255, 0));
    let mut out = vec![primary.to_string()];
    for (ip, _) in &nets {
        if *ip == primary {
            continue;
        }
        if in_same_subnet(*ip, primary, primary_mask) {
            out.push(ip.to_string());
        }
    }
    if out.is_empty() {
        out.push(primary.to_string());
    }
    out
}

/// 扫描任务结束时复位防重入标志（panic 也复位）。
struct ClearFlag<'a>(&'a AtomicBool);
impl Drop for ClearFlag<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// 构造 mDNS 服务注册信息（id 作实例名与主机名，name 等走 TXT 属性）
fn build_service_info(
    self_id: &str,
    self_name: &str,
    gateway_port: u16,
    transfer_port: u16,
    pair_active: bool,
) -> Result<ServiceInfo> {
    // 只广告可达 LAN 地址（主网卡 + 同网段），避免虚拟网卡把对端引偏
    let host_ipv4 = lan_advertise_addrs();
    if host_ipv4.is_empty() {
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
    // 配对模式标志（P2）：旧版客户端忽略未知键，向后兼容
    properties.insert(
        "pair".into(),
        if pair_active { "1" } else { "0" }.to_string(),
    );

    let hostname = format!("{}.local.", self_id);
    let host_refs: Vec<&str> = host_ipv4.iter().map(|s| s.as_str()).collect();
    ServiceInfo::new(
        SERVICE_TYPE,
        self_id,
        &hostname,
        &host_refs[..],
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

    // ---- 阶段 5 P2：配对模式 ----

    /// TXT 必须携带 `pair` 键：对端据此标「可配对」；旧版客户端忽略未知键。
    #[test]
    fn service_info_carries_pair_flag() {
        if my_ipv4_addrs().is_empty() {
            eprintln!("SKIP service_info_carries_pair_flag: 无可用 IPv4");
            return;
        }
        let on = build_service_info("id-a", "name-a", 7880, 7879, true).unwrap();
        let pair = on
            .get_properties()
            .iter()
            .find(|p| p.key() == "pair")
            .map(|p| p.val_str().to_string());
        assert_eq!(pair.as_deref(), Some("1"), "开启配对模式应广播 pair=1");

        let off = build_service_info("id-a", "name-a", 7880, 7879, false).unwrap();
        let pair = off
            .get_properties()
            .iter()
            .find(|p| p.key() == "pair")
            .map(|p| p.val_str().to_string());
        assert_eq!(pair.as_deref(), Some("0"), "默认应广播 pair=0");
    }

    /// set_pairing_mode：开启 → 状态可见；手动关立即生效；
    /// 再开启 → 120s 虚拟时间后过期任务自动关闭。
    #[tokio::test(start_paused = true)]
    async fn pairing_mode_toggles_and_auto_expires() {
        let svc = Arc::new(DiscoveryService::new_offline(
            "t".into(),
            "t-id".into(),
            None,
        ));

        let (on, left) = svc.clone().set_pairing_mode(true).unwrap();
        assert!(on, "开启后状态应为 active");
        assert!(left > 0 && left <= 120, "剩余秒数应在 (0,120]：{left}");
        assert!(svc.pairing_status().0);

        let (on2, _) = svc.clone().set_pairing_mode(false).unwrap();
        assert!(!on2, "手动关闭必须立即生效");
        assert!(!svc.pairing_status().0);

        // 再开启 → TTL 过期任务接管
        svc.clone().set_pairing_mode(true).unwrap();
        tokio::time::sleep(crate::pairing::PAIRING_TTL + std::time::Duration::from_secs(1))
            .await;
        assert!(
            !svc.pairing_status().0,
            "120s 后必须自动退出配对模式（重启不复活的前提是内存态）"
        );
    }

    /// /24 网段：扫除网络/广播/本机外的全部主机。
    #[test]
    fn subnet_scan_hosts_covers_slash24_without_self() {
        let hosts = subnet_scan_hosts(v4("192.168.31.230"), v4("255.255.255.0"));
        assert_eq!(hosts.len(), 253, "254 - 本机 = 253");
        assert!(!hosts.contains(&v4("192.168.31.230")), "不能扫自己");
        assert!(!hosts.contains(&v4("192.168.31.0")), "不能扫网络地址");
        assert!(!hosts.contains(&v4("192.168.31.255")), "不能扫广播地址");
        assert!(hosts.contains(&v4("192.168.31.1")));
    }

    /// 大网段（/16）只扫本机所在 /24，避免配对窗口被扫描耗尽。
    #[test]
    fn subnet_scan_hosts_limits_large_masks_to_slash24() {
        let hosts = subnet_scan_hosts(v4("10.124.68.246"), v4("255.255.0.0"));
        assert_eq!(hosts.len(), 253);
        assert!(hosts.iter().all(|h| u32::from(*h) & 0xFFFF_FF00 == 0x0A7C4400));
        assert!(!hosts.contains(&v4("10.124.0.1")), "不能扫整个 /16");
    }

    #[test]
    fn lan_scan_targets_dedupes_across_nets() {
        let nets = vec![
            (v4("192.168.31.230"), v4("255.255.255.0")),
            (v4("192.168.31.230"), v4("255.255.255.0")),
        ];
        let targets = lan_scan_targets(&nets);
        let unique: HashSet<_> = targets.iter().copied().collect();
        assert_eq!(targets.len(), unique.len(), "重复网段必须去重");
        assert_eq!(targets.len(), 253);
    }

    #[test]
    fn device_from_whoami_parses_pair_and_ports() {
        let body = r#"{
            "id":"abc","name":"PC","platform":"windows",
            "gateway_port":7880,"transfer_port":7879,
            "pairing_enabled":true
        }"#;
        let d = device_from_whoami("192.168.31.10", 7880, body, "self-id").unwrap();
        assert_eq!(d.id, "abc");
        assert_eq!(d.gateway_port, 7880);
        assert!(d.pair, "whoami.pairing_enabled 必须落到 device.pair");

        // 响应缺 gateway_port 时用探测端口兜底
        let body2 = r#"{"id":"x","name":"x","platform":"android","pairing_enabled":false}"#;
        let d2 = device_from_whoami("10.0.0.2", 17880, body2, "self-id").unwrap();
        assert_eq!(d2.gateway_port, 17880);
        assert!(!d2.pair);

        // 自己 / 空 id 不能入库
        assert!(device_from_whoami("10.0.0.2", 7880, body, "abc").is_none());
        assert!(device_from_whoami("10.0.0.2", 7880, r#"{"id":""}"#, "self").is_none());
    }
}
