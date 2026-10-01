# LocalSend 对标分析（按 KiteFile 新架构重新校准）

> 状态：**分析留档，2026-09-30**。本文取代此前口头对比结论——那个版本的对比
> 基于「阶段 4 + 停等模型 + 明文传输」的旧 KiteFile，两大短板（明文、停等吞吐）
> 已在阶段 5 与工作流 A 中修复，结论需要校准。
>
> 对标对象：LocalSend `main @ c5bbe363`（2026-09-29，app 1.18.2+64），
> 浅克隆于 `E:\zheten2.0\tmp\localsend\`（未跟踪目录，可随时删除）。
> KiteFile 基线：`main @ f7776fe`（2026-09-30，阶段 5 完成 + 设备管理）。
> 所有结论均锚到两边代码文件，实现 agent 可直接核对。

---

## 0. 一页纸结论

| 维度 | 结论 |
|---|---|
| 架构路线 | **双向验证**：LocalSend 正把网络层从 Dart 迁往 Rust core（与我们同向）；差异在壳核连接方式 |
| 安全模型 | **KiteFile 反超**：mTLS 配对 + 确认码 + 全链路 TLS，强于 LocalSend 的「指纹展示 + 可选 PIN」；证书算法也领先（Ed25519 vs RSA 2048） |
| 传输吞吐 | **打平**：LocalSend 用 HTTP 流式，我们用多流分段（N 条 TCP 流各写一段）；都无应用层块级 ACK，吞吐都不受 RTT 摆布 |
| 可靠性/续传 | **KiteFile 领先**：段级自动重试 + 可续中断状态机 + slot_meta 预留；LocalSend 断了整文件重传，无任何续传 |
| 设备发现 | **LocalSend 领先**：UDP 组播（53317）+ 子网扫描兜底，比 mDNS 简单可控（我们踩过虚拟网卡 IP 坑） |
| 交付生态 | **LocalSend 大幅领先**：CLI / Docker 无头 / 60+ 语言 / CI 成熟 / 协议文档独立成仓 |

一句话：**旧对比里「明文 + 停等」两个致命项已清零，安全性反超；剩下真正值得抄的只有发现机制和生态工程。**

---

## 1. 双方架构速览

### LocalSend（monorepo，六个成员）

```text
app(Flutter 全平台+Web, Dart≈12万行)   cli(Rust)   server(Rust 无头/Docker)
        │ flutter_rust_bridge                │              │
        ▼                                    ▼              ▼
localsend_isolates(FRB 桥)  ──►  packages/core(Rust 协议核心, feature-gate)
typed_isolates(Dart isolate 调度)        crypto/discovery/multicast/http/webrtc
```

- 协议 v2（REST over HTTPS）：`register → prepare-upload → upload → cancel`，
  外加 `prepare-download`（拉取模式）与 web send（浏览器直发）。
- 发现：**UDP 组播 53317**（`core/src/multicast/mod.rs:39`），自广播 JSON +
  `scan_subnet` 兜底；无 mDNS。
- 迁移中：发送路径已进 Rust（`api/http.rs` 的 register/prepare_upload/upload），
  **接收端 server 仍是 Dart shelf**（`app/lib/provider/network/server/`）——双轨期。

### KiteFile（一个 Rust 引擎 + 双端 Flutter 壳）

```text
desktop(Flutter)  mobile(Flutter, FFI 进程内)
        │  127.0.0.1:7878 HTTP/WS（本机契约）
        ▼
core(kitefile crate) ── 对外面全 TLS ──► 对端 KiteFile
   discovery(mDNS _kitefile._tcp) / gateway / transfer / pairing / tls
```

- 端口（阶段 5 重划，`lib.rs:106-112`）：7878 只绑回环（UI/WS）；
  7880 LAN TLS 网关（`0.0.0.0`，候选 [7880,17880,27880]）；7879 数据通道
  TLS 包装 + 出站 pin（候选 [7879,17879,27879]）。
- 传输（`341f63c` 停等废弃）：文件切 N 段，N 条 TCP 流各写一段，流内顺序写
  无分块 ACK；`StreamHeader`（file_id 前缀/stream_id/start_offset/data_len）
  + 原始字节；整文件 sha256 仍唯一校验（/api/verify）。

---

## 2. 逐项对比（实现级）

### 2.1 安全模型 —— KiteFile 反超，且不是小胜

| | LocalSend | KiteFile（阶段 5） |
|---|---|---|
| 身份 | 每设备密钥对，指纹=证书哈希 | `.kitefile-identity` + Ed25519 自签证书（`tls.rs`） |
| 证书算法 | **RSA 2048**（`crypto/cert.rs:36`，测试用；生成路径同为 rsa） | **Ed25519**（rustls 0.23 + rcgen 0.13） |
| 跨机认证 | TLS 全程加密；**身份靠指纹人工比对（可选）** | **mTLS 客户端证书**：服务端按 peers 表放行，客户端 pin 服务端指纹 |
| 信任授予 | 「接受传输」即信任 + 可选 PIN 弹窗 | **先配对后信任**：确认码两阶段 + 双屏比对；未配对 fail-closed 403 |
| 撤销 | 无明确设备管理概念（收藏设备≠信任） | 删除设备 = 删 peers 记录 = 撤销 pin |
| 刷弹窗攻击 | 未配对设备可直接 offer（靠 PIN 拦） | 未配对根本到不了 offer；pair/hello 同 IP 限频 |

要点：LocalSend 的安全边界是「加密总能通，身份靠人眼」；我们是「身份先行，
加密兜底」。它防被动嗅探靠 TLS，防冒充只靠用户肯不肯比对指纹；我们把冒充
挡在配对关口。**已配对设备被攻陷的边界两边相同**（本设计文档 §2 已写明不防）。

可核对：`core/src/tls.rs`（NodeIdentity/pin）、`core/src/pairing.rs`、
`docs/design-pairing-encryption.md` §2 vs `localsend/packages/core/src/crypto/`。

### 2.2 传输与可靠性 —— 吞吐打平，可靠性我们领先

| | LocalSend | KiteFile |
|---|---|---|
| 传输通道 | HTTPS POST（HTTP body 流式） | 裸 TCP 7879 + TLS 包装 |
| 流控 | TCP 窗口（无应用层 ACK） | 同：多流并行、流内顺序写、无分块 ACK |
| 断了怎么办 | session 作废，**整文件重传** | **可续中断状态机 + 段级自动重试**（A3.2/A3.3） |
| 完整性 | 传输中无校验；文件级 sha256 靠上层 | 整文件 sha256（/api/verify 补发）+ 原子 rename |
| 跨重启续传 | 无 | slot_meta 结构保留、**未接线**（明确的下一个大件） |

历史教训留档：旧停等模型（逐块 ACK）曾在 RTT>30ms 时吞吐骤降，这正是当初
对照 LocalSend「HTTP 流式不受 RTT 摆布」发现的差距；`341f63c` 改多流分段后
两边模型等价（都是「让 TCP 自己撑窗口」）。**不要再翻案回停等。**

### 2.3 设备发现 —— LocalSend 的设计更省心

| | LocalSend | KiteFile |
|---|---|---|
| 机制 | UDP 组播 53317，自广播 JSON | mDNS（mdns-sd 0.11.5） |
| 多网卡 | 自主控制广播地址，天然可控 | 踩过 Hyper-V 虚拟网卡选错 IP（dcf57bf 修） |
| 兜底 | `scan_subnet` 全子网扫描 | LAN whoami 主动扫描（b84b92c，性质类似但更重） |

mDNS 给我们换来的是「标准协议」的名分和 zeroconf 生态兼容，但代价是行为
黑盒（要读 mdns-sd 源码才能确认）、TXT 记录体积受限。LocalSend 30 行组播
代码做到了同样的事。**建议**：不急于推翻 mDNS，但若真机回归再出发现类
怪病，UDP 组播是备选方案（工作量小，协议字段可平移）。

### 2.4 壳核连接 —— 两条路线，各自代价

| | LocalSend（FRB 进程内嵌） | KiteFile（外置 daemon + HTTP 契约） |
|---|---|---|
| 进程模型 | Rust 编进 app，无独立进程 | 桌面子进程 daemon；移动端 FFI 进程内 |
| 痛点 | 编译链复杂（cargokit）、调试难、FRB 代码生成侵入 | exe 查找顺序、替换 exe 需重启、**移动端 tracing 日志全丢（B1 未做）** |
| 契约可测性 | 好但耦合紧，Dart/Rust 类型生成同步 | **HTTP 契约 curl 即测**，UI 换壳零成本 |
| 无头场景 | 需另起 server crate（它确实另起了） | CLI 与 daemon 天然同一套 |

结论：**不必推翻**。但「移动端无日志」这条，LocalSend 的 FRB 流式回传
（`StreamSink` 把 Rust 日志推回 Dart）值得抄思路——B1 做日志落文件时，
让 ffi 侧日志同时进 Flutter 展示层，成本不高。

### 2.5 交付生态 —— 差距最大，但多数不在当前优先级

| LocalSend 有 | 我们 | 值不值得抄 |
|---|---|---|
| CLI（终端收发/配对/脚本化） | 已有 kitefile-cli（daemon + send） | ✅ 已平齐 |
| Docker 无头 server | 无 | 需要部署 NAS 时再说 |
| 协议文档独立成仓（localsend/protocol） | 协议散在 docs/ 设计文档里 | ✅ **建议抄**：抽出 `docs/protocol.md` 单文件，跨端对账省很多口舌 |
| 60+ 语言（Weblate） | 纯中文硬编码 | 当前自用，不做 |
| CI 矩阵（6 平台产物） | GitHub Actions（rust+android） | 已有雏形，够用 |

---

## 3. 校准后的行动清单（按优先级）

1. **（P1，已有共识）跨重启续传接线** —— 对 LocalSend 的净领先项，slot_meta
   字段已留好；它自己完全没有此能力。
2. **（P2）抽协议单文档** —— `docs/protocol.md`：控制面路由表 + StreamHeader +
   offer/verify 字段 + TLS/pin 语义，对标 localsend/protocol 的做法。
3. **（P2，B1 顺带）移动端日志**：FFI 侧 tracing 事件桥接回 Flutter（参考
   LocalSend `api/logging.rs` + `StreamSink` 模式），落文件 + 开发者页查看。
4. **（P3，备选）发现机制**：真机回归若再现发现怪病，评估 UDP 组播替代 mDNS
   （参照 `multicast/mod.rs`，约 200 行）。
5. **（不做）**：PIN/指纹人工比对（mTLS 已覆盖其威胁）、Web send、i18n、Docker。

## 4. 事实出处索引

| 事实 | 位置 |
|---|---|
| LocalSend RSA 2048 证书 | `localsend/packages/core/src/crypto/cert.rs:33-36` |
| LocalSend 组播端口 | `localsend/packages/core/src/multicast/mod.rs:39` |
| LocalSend v2 DTO（fingerprint/PIN 字段） | `localsend/packages/core/src/http/dto_v2.rs` |
| LocalSend 发送路径 Rust 化 | `localsend/packages/localsend_isolates/rust/src/api/http.rs` |
| LocalSend 接收端仍是 Dart | `localsend/app/lib/provider/network/server/controller/` |
| KiteFile TLS 身份/Ed25519/pin | `core/src/tls.rs` 头注释与 `NodeIdentity` |
| KiteFile 端口候选 | `core/src/lib.rs:106-112` |
| KiteFile 多流分段（停等废弃） | `341f63c`、`docs/repair-plan-reliability.md` §0 |
| KiteFile 段级重试/可续中断 | 09-26 提交（A3.2/A3.3） |
| KiteFile mTLS 配对 | `core/src/pairing.rs`、`docs/design-pairing-encryption.md` |
| KiteFile 发现修复 | `dcf57bf`（mDNS IP 优先级）、`b84b92c`（LAN whoami 扫描） |
