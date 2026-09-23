# KiteFile 修复方案（第三轮定稿）

日期：2026-09-03
本轮工作：核对代码实际状态 → 复核前两轮裁决 → 补充新发现 → 标注存疑项
审查方式：逐条对源码取证，不采信方案自述结论。所有行号为本次实测。

---

## 零、当前进度核对

仓库根 `filetransfer/`，仅一次提交 `73a84fa`，工作区 `M scripts/build-all.ps1`。

| 阶段 | 方案要求 | 代码实际状态 | 证据 | 结论 |
|---|---|---|---|---|
| 1 | `?fileName` 改保守写法 | ✅ 已改 | `mobile/lib/main.dart:456` `if (fileName != null) body['file_name'] = fileName;` | 完成 |
| 1 | git init + 首次提交 | ✅ 已提交 | `73a84fa`，`git status` 仅 1 个 M | 完成 |
| 1 | 根 `.gitignore` | ✅ 41 行，`target/` `dist/` `*.part` `!Cargo.lock` 齐全 | `.gitignore:2,8,12,40` | 完成 |
| 1 | `.gitattributes` | ✅ 44 行，`*.ps1 text eol=crlf` | `.gitattributes:6` | 完成 |
| 1 | 构建闸门 | ✅ 已提交 | `build-all.ps1:109-131`，位于 `Stop-FtcoreProcesses`(135) 之前 | 完成 |
| 1 | 闸门必须绿 | ✅ 全绿 | `cargo test` 25 passed / 0 failed；两端 `flutter analyze` 均无问题 | 完成（N15 已解除） |
| 2 | 删 CORS | ✅ 已删 | `gateway.rs` 不再出现 `CorsLayer`；`tower-http` 依赖一并移除 | 完成 |
| 2 | 接口分级 | ✅ 已加 | `gateway.rs::classify` 按 (Method, Path) 分档 + `access_guard` middleware，用 `route_layer` 挂载 | 完成 |
| 2 | ConnectInfo 注入 | ✅ 已改 | `run()` 改用 `into_make_service_with_connect_info::<SocketAddr>()` | 完成（N1 解除） |
| 2 | `--remote-admin` | ✅ 已有 | `EngineConfig::allow_remote_admin`（默认 false）+ `cli.rs daemon --remote-admin` | 完成 |
| 3a | `unbounded` → `channel(256)` | ❌ 未动 | `transfer.rs:683` `mpsc::unbounded_channel()` | 未开始 |
| 3b | 假注释改正 | ❌ 未动 | `transfer.rs:6,7` / `storage.rs:7` 原文照旧 | 未开始 |
| 3c | cache 淘汰 | ❌ 未动 | `transfer.rs:215` 无上限无顺序队列 | 未开始 |
| 3d | 死代码清理 | 🟡 部分 | `tower-http` 已在阶段 2 移除；`protocol.rs` 五个变体、另两个依赖未动 | 部分完成 |
| 3e | `static mut` → `OnceLock` | ❌ 未动 | `ffi.rs:33,169,190,213` 四处 | 未开始 |
| 4 | 连接复用 | ❌ 未动 | `transfer.rs:993` connect 在 while 内 | 未开始 |
| 5a–5e | — | ❌ 全部未动 | 无 token / 无加密 / 无共享包 / 无手动 IP / 无粒度细化 | 未开始 |

**一句话：阶段 1、2 已落地，闸门全绿（31 项测试 + 两端分析）；阶段 3 起为零，其中 3d 已部分完成。**

---

## 一、前两轮已达成的裁决（不再重开）

| 编号 | 结论 | 落地阶段 |
|---|---|---|
| P0-1 | 分级表 key 改为 `(Method, Path)` 二元组，路径参数做前缀切分 | 2 |
| P0-2 | token 必须双向分发；**响应体不得携带凭据**；`/api/pair` 改为纯推端点 | 5a |
| P0-3 | 96-bit 全随机 nonce 随密文发送；断言做硬失败而非注释 | 5b |
| P1-1 | 定死**停等**模型（与现状一致），文档写明 RTT>30ms 吞吐下降 | 4 |
| P1-2 | 续传**复用上次 file_id**，使 HKDF salt 稳定 | 4 |
| P1-3 | 改 **per-peer token**，撤销=删该条记录 | 5a |
| P2-1 | 总表补 `5a ← 4`、`5b ← 4` | 总表 |
| P2-2 | 终态超时分支记 `warn!`，不静默 | 3a |
| P2-3 | VecDeque 惰性消费，淘汰用 `continue`；全进行中时退化为淘汰最早 | 3c |
| P2-4 | 删 enum 变体同步改 `test_control_message_roundtrip` | 3d |

**P1-1 补充实测**：`send_chunk`（`transfer.rs:1057`）写完 header+data 后**已在等 ChunkAck**，即现状就是停等。改连接复用后仍停等，无需改 ACK 语义，P1-1 的裁决与现状完全吻合。

---

## 二、本轮新发现

> 编号 N1–N16。N1–N3 为阻塞级，N4–N8 为重要级。

### N1【阻塞·阶段 2】`axum::serve` 未启用 ConnectInfo → 本机判定根本拿不到 peer IP

**证据** `gateway.rs:147`

```rust
axum::serve(listener, app)   // ← 没有 into_make_service_with_connect_info
```

axum 中 peer 地址靠 `ConnectInfo<SocketAddr>` 传递，**必须由 serve 时注入才会进 extensions**。当前写法下，middleware 里 `req.extensions().get::<ConnectInfo<SocketAddr>>()` 恒为 `None`。

**后果**：分级中间件必然二选一 —— "取不到就放行"→ 分级形同虚设；"取不到就拒绝"→ 全部 403，工具彻底不可用。**这是阶段 2 一动手就会撞上的第一堵墙**，方案原文与 P0-1 都未涉及。

**修法**

```rust
axum::serve(
    listener,
    app.into_make_service_with_connect_info::<SocketAddr>(),
)
```

配套三条：

1. middleware 取 `req.extensions().get::<ConnectInfo<SocketAddr>>()`；
2. **取不到一律拒绝（fail-closed）**，并记 `error!`。绝不可"取不到就放行"——那等于给绕过分级留后门；
3. `.with_state()` 之后 Router 变 `Router<()>`，仍可调用该方法，类型上无阻碍（axum 0.7）。

**验收**：单元测试断言"未注入 ConnectInfo 时请求被拒"，防将来有人改回 `axum::serve(listener, app)` 而静默失效。

---

### N2【阻塞·阶段 4】`chunk_size` 不跨端协商 → 既有数据损坏隐患 + 续传阻塞

**证据**

- `chunk_size` 是**本地配置项**：`lib.rs:53` `Config.chunk_size`，默认 `16 * 1024 * 1024`（`lib.rs:68`）
- `HttpOffer`（`protocol.rs:99-119`）**没有 chunk_size 字段**
- 接收方建槽用的是**自己的**配置：`storage.rs:123/127` `create_slot(..., chunk_size)` → `chunk_count = ceil(file_size / chunk_size)`
- 发送方切片用的是**自己的**配置：`transfer.rs:678-679`

**既有 bug（当前就存在，只是没被触发）**：若 A 配 16MB、B 配 8MB，A 发 chunk 0（offset 0, len 16MB），B 的槽 `chunk_size=8MB`，`write_chunk` 按 `min(file_size-offset, slot.chunk_size)`（`storage.rs:205`）只写 8MB，且 chunk_id 语义错位 → **静默数据损坏 + sha256 不匹配**。两端都用默认值才侥幸没暴露。

**对阶段 4 的阻塞性**：续传的 bitmap 按 chunk_id 索引，而 **chunk_id 的含义完全依赖 chunk_size**。B 若中途改过配置，或旧 `.part` 是用不同 chunk_size 写的，bitmap 与新切块错位 → 续传产出**静默损坏的文件**。这比"续传失败"严重得多。

**修法**

1. `HttpOffer` 增加 `chunk_size: u64`；接收方 accept 建槽时**以 offer 携带的值为准**，不使用本地配置；
2. 本地配置的语义改为"**作为发送方时的切片大小**"，不再影响接收；
3. `.part` 元数据（`file_id.meta` 或槽信息）记录建槽时的 `chunk_size`；续传时校验一致，不一致则**丢弃旧 `.part` 从头传**（不要尝试换算，换算逻辑是 bug 温床）；
4. 加集成测试：两端 chunk_size 配置不同，断言传输后 sha256 一致。

---

### N3【阻塞·阶段 4】P1-2 只解决了"接收方如何识别"，没解决"发送方如何得知旧 file_id"

**问题**：P1-2 裁决"续传复用上次 file_id"，代价描述为"接收方在 incoming-resp 里回传 `last_file_id`"。但发送方 A **每次传输都新生成 file_id**，它根本不知道要去匹配谁 —— 若没有来源，续传永远触发不了。

**修法（B 侧匹配 → A 侧切换）**

```
A 发 offer：{file_id: NEW, file_name, file_size, source_mtime, chunk_size}
  → B 按 (file_name, file_size) 扫描 receive_dir 找 .part
  → 命中且 source_mtime 与 .part 元数据一致
  → B 在 incoming-resp 回：{resume_file_id: OLD, received_chunks: [已完成 chunk_id...]}
  → A 丢弃 NEW，改用 OLD 作为本次传输的 file_id 发剩余块
  → B 复用旧槽位（.part 已存在）
```

需要改的结构体：

| 结构体 | 新增字段 | 证据 |
|---|---|---|
| `HttpOffer` | `chunk_size: u64`、`source_mtime: Option<u64>` | `protocol.rs:99` |
| `HttpIncomingResponse` | `resume_file_id: Option<String>`、`received_chunks: Option<Vec<u64>>` | `protocol.rs:168`，现仅 4 字段 |

**顺带确认**：`source_mtime` 不能省。否则"同名同大小但内容已改"会被误判为可续传 → 损坏文件。`file_id.meta` 方案因此仍要保留（存 mtime + chunk_size + 旧 file_id），P1-2 说的"可省掉 file_id.meta"**不成立**，撤回该结论。

**并发边缘**：同一对端同时发起两个相同文件的传输 → `resume_file_id` 相同 → 槽位冲突。建议 B 侧对"已被活跃传输占用的 file_id"直接拒绝续传（回 `resume_file_id: None`，走全新传输，落到 `name (1).ext`）。

---

### N4【重要·阶段 2】`/api/devices` 与 `/ws/progress` 无远程调用方，却归到"允许远程"

**证据**

- `/ws/progress`：桌面端 `kDaemonWs = 'ws://127.0.0.1:7878/ws/progress'`（`desktop/lib/main.dart:22`）；移动端 `_wsBase` 由 `_daemonHost` 拼出，默认 `127.0.0.1`（`mobile/lib/main.dart:223-227`）
- `/api/devices`：两端 UI 均只连本机 daemon

**后果**：分级表把二者归入"允许远程"，是**无收益地扩大攻击面** —— WS 流包含文件名、进度、`file_path`；`/api/devices` 泄漏本机发现的全部设备（IP、名称、平台）。

**修法**：二者归**仅本机**；`/ws/progress` 在 `allow_remote_admin=true` 时放行（遥控模式需要）。

**分级原则（建议写进代码注释）**：归"允许远程"的唯一理由是**存在远程调用方**，不是"敏感度低"。凡举不出调用方的接口，一律仅本机。

---

### N5【重要·阶段 5a】纯推模型解决了"响应体带凭据"，但留下"推送合法性"缺口

用户本轮的反驳成立：`/api/pair` 若在响应体回 token，就是未认证响应携带凭据，且该端点在配对表建立前必须对未知来源开放 → 沦为 token 分发机。**纯推方向正确，采纳。**

但纯推模型引入新的待定问题：**接收推送的一方，凭什么接受这条推送？**

- B 推给 A：A 侧若无任何校验就落库，则**任何局域网设备可自注册**并拿到访问权 → token 体系退化成"谁都能领票"，比不做鉴权只是多了个仪式。
- 用户提的"A 侧'用户选中该设备发起传输'即为意图表达"在**A 主动发起**的场景成立，但攻击者冷启动向 A 推送时，A 没有这层意图上下文。

**修法：用 pending offer 表做意图凭证（不牺牲 UX）**

| 方向 | 触发时机 | 接收方校验 | 是否弹窗 |
|---|---|---|---|
| B → A 推 B_token | B 用户点「信任」后 | A 检查"我确实向该 (ip, device_id) 发过 offer 且未过期" | 否（A 已主动发起） |
| A → B 推 A_token | A 收到 B 的推送后 | B 检查"我刚对该 (ip, device_id) 点过信任" | 否（B 已确认） |
| 冷启动推送 | 无上下文 | 无 pending 记录 → **转人工确认弹窗** | 是 |

**残留竞态（存疑，见 Q3）**：攻击者可在 B 推给 A 之后、A 推给 B 之前抢先以 A_id 向 B 推一个假 token。后果是 B 用错 token 访问 A 被拒 —— **可用性受损，非机密性受损**（攻击者拿不到 A 的数据，A 的入站校验用的是 A 自己颁出的 token）。缓解：已存在 peer 记录时**拒绝覆盖**，需用户显式"重新配对"。

---

### N6【重要·阶段 5a】数据通道 AUTH 的 token 方向未写明，极易实现反

**证据**：数据连接是**发送方 A 主动 connect 接收方 B 的 7879**（`transfer.rs:993` `TcpStream::connect((target_ip, target_port))`）。

所以 A 应出示的是 **B 颁给 A 的 token**（`token_peer_gave_me`），**不是 A 自己的 token**。方案原文只说"AUTH \<token\>"，未定方向。方向搞反的表现是永远认证失败，且错误信息会指向错误的地方，排查成本高。

**写法**：数据通道握手第一行 `AUTH <token_peer_gave_me>\n`；与 HTTP 侧的 `Authorization: Bearer` 用同一个 token。

---

### N7【重要·阶段 5b】方案的字节数描述有误，且随机 nonce 改变了帧长

方案原文："加密后长度 +12（tag）+16（可选）"。

- GCM **标准 tag 是 16 字节**，12 字节是 **nonce**。原文把两者写反了。
- 采纳 P0-3（全随机 nonce）后，每 chunk 实际增加 **12 (nonce) + 16 (tag) = 28 字节**，不是 +12。

**实现要点**

- nonce 与 tag 都放进 **data 区**（nonce 前置、tag 后置），**帧头 24 字节不动**，`data_len` = 12 + 密文长 + 16；
- `data_len` 是 `u32`（`protocol.rs:85`），16MB + 28 无溢出风险 ✓；
- 接收端现在是 `vec![0u8; header.data_len]`（`transfer.rs:396`）按 `data_len` 分配，**不能**再按 `chunk_size` 硬编码，否则解密后长度对不上。

---

### N8【重要·阶段 5b】"别多分配一份 16MB"需要预留 capacity，否则要么多分配要么 panic

方案只说"原地替换或复用同一 Vec"，但没说怎么落。当前读文件是 `let mut buf = vec![0u8; read_len]`（`transfer.rs:975`），长度恰好等于明文长，**没有 tag/nonce 的余量**。

**写法**

```rust
let mut buf = Vec::with_capacity(read_len + 28);   // nonce + tag 余量
buf.resize(read_len, 0);
f.read_exact(&mut buf)?;
// 加密：encrypt_in_place 会 push 16 字节 tag，需要 Vec 有余量（已有 capacity）
// nonce 单独 12 字节，随 header 之后发送
```

用 `aes-gcm` 的 `encrypt_in_place`（避免二次分配）。8 流并行下，多分配一份就是 +128MB 峰值 —— 这正是方案想避免的，但按现写法直接 `encrypt` 会撞上。

---

### N9【次要·阶段 4】`send_chunk` 等 ACK 的 60 秒窗口内无取消检查

**证据**：cancel 检查只在 worker 的 while 循环开头（`transfer.rs:961`）；`send_chunk` 内部（`transfer.rs:1057-1100`）只 `timeout(60s, read_until)`，不感知取消。

**后果**：用户点取消后，最多要等 **60 秒**才生效（8 个 worker 各自卡在等 ACK）。

**修法**

```rust
tokio::select! {
    r = tokio::time::timeout(Duration::from_secs(20), reader.read_until(b'\n', &mut ack)) => r,
    _ = cancel_rx.changed() => return Err(/* canceled */),
}
```

顺带：60 秒 ACK 超时在"复用连接 + 重试"模型下过长（重试本就是为了快速换连接），建议**缩到 20 秒**。

---

### N10【次要·阶段 2】未匹配路径 / 未列方法的默认策略未定义

分级表是白名单，但"表外请求怎么办"方案没写。

**建议**

- 远程请求 → 表外默认 **403**（fail-closed）；
- 本机请求 → 表外默认放行；
- 加测试：**枚举 Router 上所有 route，断言每条都在分级表中**（防新增接口漏配而静默裸奔）。

**顺带**：删掉 `CorsLayer` 后，OPTIONS 预检会走 404。已确认 Dart 原生 `HttpClient` 不发 OPTIONS ✓，无影响；但分级表要显式处理 OPTIONS（否则落入"表外 403"，行为正确但日志会噪音，可显式 204）。

---

### N11【次要·阶段 3a】"10 处 send 调用点"实际是 6 处，且接收端不走这条 channel

**证据**

- `progress_tx.send` 实际 6 处：`transfer.rs:711, 747, 774, 804, 919, 1010`
- **接收端进度走 `publish_progress()`**（`transfer.rs:266`、`428`），不经 `progress_tx`

**含义**：阶段 3a 的 channel 契约**只约束发送端**。接收端进度不受 `try_send` 丢弃影响 ✓，但也不受新契约保护。设计文档要写清这是**两条独立路径**，否则后来者会误以为改一处就全覆盖。

---

### N12【次要·阶段 5a】未配对设备可无限发 offer → 弹窗骚扰 / 社工

5a 落地前，`POST /api/incoming` 对全网开放且无频率限制。攻击者可循环发 offer 刷弹窗，或伪造设备名诱导用户点「信任」。

**缓解**（5a 内顺手做）：同一未配对 IP 的 offer 限频（如 10 秒 1 次，超限静默丢弃并记 `warn!`）；弹窗显示**来源 IP + 平台**（方案已提）。

---

### N13【次要·阶段 5b】`key_check` 的 nonce 处理

握手期用 key 加密固定明文做校验。若采用全随机 nonce，需把 nonce 一并发送；建议直接用**全 0 nonce**，并在注释写明"该 nonce 在本次连接生命周期内仅出现这一次，之后所有 chunk 均用随机 nonce"。

---

### N14【次要·阶段 5e】节流的量级核算

"每传输每 100ms 一条"× 8 流 = 80 条/秒，进 `broadcast(256)` → 缓冲 3.2 秒，WS 消费者不卡 3 秒就不会 Lagged ✓ 可接受。但**多传输并发时线性增长**，建议再加一层**全局节流**（所有传输合计每 50ms 一条）。

---

### N15【环境阻塞·已解决】C 盘写满导致 `%TEMP%` 不可用，症状伪装成 rustc ICE

**结论：已解除，闸门全绿。根因不是代码问题，也不需要删任何文件。**

#### 症状（具有迷惑性，记录以防复发）

```
error: the compiler unexpectedly panicked. This is a bug     ← 看起来像 rustc 的 bug
  17: core::option::expect_failed
  18: rustc_metadata::rmeta::encoder::encode_metadata
error: linking with `link.exe` failed: exit code: 1201
  LINK : fatal error LNK1201: 写入程序数据库"...pdb"时出错
```

第一次遇到时很容易往"增量缓存损坏"或"工具链 bug"上查，实际都不是。

#### 真实根因

`df -h` 显示 **C 盘 200G 用满，仅剩 300MB**（Git Bash 的 `/` 就挂在这里）。
`rustc` 与 MSVC `link.exe` 默认把临时文件和 `.pdb` 写进 `%TEMP%`（即
`C:\Users\<user>\AppData\Local\Temp`），盘满 → 写入失败 → rustc 在
`encode_metadata` 里 `expect` 失败而 ICE，link.exe 报 LNK1201。

验证方式：向临时目录 `dd` 200MB，只写入 146MB 就 ENOSPC，确认盘满。

#### 解法（无需清理、无需 `cargo clean`）

`build-all.ps1` 原本已把 `CARGO_HOME` / `RUSTUP_HOME` / `PUB_CACHE` /
`GRADLE_USER_HOME` / `ANDROID_HOME` 全部指向 E 盘，**唯独漏了 `TEMP` / `TMP`**。
补上后（现 `build-all.ps1:45-47`）立即编译通过：

```powershell
$env:TEMP = 'E:\zheten2.0\.deps\tmp'
$env:TMP  = $env:TEMP
```

手动在 bash 里跑命令时同样需要先导出这两个变量，否则会重现 ICE。

**不要**为此 `cargo clean`：3.4GB 依赖重编耗时远超收益，且 target 本来就在 E 盘。
删 `target/debug/incremental` 也不必要——它同样在 E 盘，不是瓶颈。

#### 顺带发现：C 盘还留着一个歷史 Pub Cache

`C:\Users\lenovo\AppData\Local\Pub\Cache` 是本项目配置 `PUB_CACHE` 之前的遗留物。
当前构建已完全走 `E:\zheten2.0\.deps\pub-cache`，不会再往 C 盘写。
该目录是纯缓存、可重建，**是否删除由你决定**，我不主动动它。

---

## 三、完整修复方案（定稿）

### 阶段 1 · 工程基建

**状态**：✅ 已完成并提交（commit `阶段 1`）。

| 项 | 内容 | 结果 |
|---|---|---|
| 1.1 | 提交 `build-all.ps1` 的闸门改动 | ✅ |
| 1.2 | 跑 `cargo test` + 两端 `flutter analyze` | ✅ 全绿，见下 |
| 1.3 | 单独一个 commit | ✅ |

**闸门实测结果**（此前从未验证过）：

| 检查项 | 结果 |
|---|---|
| `cargo test`（core） | 25 passed / 0 failed（gateway 12 + protocol 6 + storage 7） |
| `flutter analyze`（desktop） | No issues found! |
| `flutter analyze`（mobile） | No issues found! |

**本阶段实际改动的四件事**（比原计划多三项，都是验证时暴露出来的）：

1. **闸门本体**：`build-all.ps1:109-131`，在 `Stop-FtcoreProcesses`(135) 之前 ✓ 正确 ——
   测试不绑定端口，不需要杀进程，杀了反而影响用户正在用的 daemon。
2. **`TEMP`/`TMP` 补重定向**（`build-all.ps1:45-47`）：唯一遗漏的环境变量，
   不补则闸门根本跑不起来。详见 N15。
3. **修过时断言**（`gateway_test.rs:439`）：原断言 `v[0]["sha256"].is_some()`，
   但 `transfer.rs:717` 的 `sha256_deferred` 改动后 offer 不再带 sha256，
   该断言必然红。已改为断言 `sha256_deferred == true` 且 offer 的 `sha256` 为 null，
   并在注释里说明"末尾的文件内容一致断言即证明 verify 补发链路工作"。
   **这是测试过时，不是代码 bug**——deferred 是有意设计（大文件不阻塞弹窗）。
4. **去掉 `build-all.ps1` 的双 BOM**：开头字节曾是 `efbbbf efbbbf`（两个 BOM），
   已字节级修正为单个 BOM。这是当初写 `.gitattributes` 时记录的那个隐患的复发。

**验收**：`git log` 两条提交；闸门失败时 `exit 1` 且不产 `dist/`。

---

### 阶段 2 · Gateway 止血

**状态**：✅ 已完成并提交。31 项测试全绿，两端 `flutter analyze` 均无问题。

**已落地的四项**

| 项 | 落地位置 |
|---|---|
| 2.1 ConnectInfo 注入（N1） | `run()` 改用 `into_make_service_with_connect_info::<SocketAddr>()` |
| 2.2 删 CORS | `CorsLayer` 及其 import 全删；`tower-http` 依赖零使用后一并移除 |
| 2.3 `allow_remote_admin` | `EngineConfig::allow_remote_admin`（默认 false）+ `cli.rs daemon --remote-admin` |
| 2.4 分级 middleware | `classify()` + `access_guard()`，经 `route_layer` 挂载 |

#### 三个实现上的判断（与原文案略有出入，说明一下）

**1. 用 `route_layer` 而不是 `.layer()`。**
`route_layer` 只对**已注册的路由**生效，404 / 405 的请求根本不进 middleware。
换成 `.layer()` 会对所有请求生效（含 404），反而多一个面。
这顺带把 N10「表外请求默认策略」收敛掉了：未注册方法（如 `PUT /api/send`）
在路由层就被 405 挡下，到不了 middleware。

**2. 分级表保持闭合。**
原文案的分级表没列 `GET /`。已补上并标 LocalOnly——当前它只返回一行提示文本，
但显式登记能让"表里缺一条"和"新增路由忘了登记"区分开：
前者是设计选择，后者是真遗漏，日志里的措辞不该把两者混为一谈。
将来若在这里挂 Web 前端静态资源，需要改判为 Remote。

**3. 403 响应体带上了开关名。**
被拒时返回 `"该接口仅限本机访问。若需从其他设备控制本机，请用 --remote-admin 启动守护进程。"`。
局域网里排查问题的人不一定能看到服务端的 `warn!` 日志，提示得跟着响应走。

**修订后的分级表**（含 N4 修正）

| 策略 | 接口 | 依据 |
|---|---|---|
| 仅本机 | `POST /api/send`、`GET /api/transfers`、`GET /api/files`、`GET /api/files/:name`、`GET /api/incoming`、`POST /api/incoming/:id/accept`、`POST /api/incoming/:id/reject`、`GET /api/config`、`POST /api/config/*` | 仅本机 UI 调用（已核实：`desktop/main.dart:558,573`；`mobile/main.dart:439`） |
| 仅本机（原为远程，**N4 修正**） | `GET /api/devices`、`GET /ws/progress` | 无远程调用方；`/ws/progress` 在 remote-admin 模式下放行 |
| 允许远程 | `POST /api/incoming`、`POST /api/incoming-resp`、`POST /api/verify/:file_id`、`POST /api/cancel/:file_id`、`GET /api/whoami` | P2P 主流程 + 5d 手动添加需要 whoami |

**本机判定**：`addr.ip().is_loopback()`（覆盖 `127.0.0.1` / `::1` / `::ffff:127.0.0.1`）。**不要**把本机 LAN IP 算作本机 —— 遥控模式下 UI 连的是对端，判成"本机"就直接绕过分级。

**测试（四条 + 二条新增）**

1. 远程 `POST /api/incoming` → 200
2. 远程 `GET /api/incoming` → 403
3. 本机 `GET /api/incoming` → 200
4. 远程 `GET /api/files` → 403
5. **新增**：未注入 ConnectInfo 时请求被拒（防 N1 回归）
6. **新增**：枚举所有 route，断言均已分类（防漏配裸奔）

**状态**：✅ 已完成并提交。测试 31 项全绿（新增 6 项）。

**落地记录（与定稿的三处偏差，都是动手时发现的）**

1. **用 `route_layer` 而非 `layer`，顺带把 N10 闭合了。**
   `route_layer` 只对「已注册的路由」生效，404 / 405 的请求根本不进 middleware，
   所以压根不存在"未匹配路径该怎么判"这个问题——N10 不必再定默认策略。
   （换成 `.layer()` 反而会把 404 也卷进来，平白多一个面。）

2. **分级表补上 `GET /`，让表保持闭合。**
   原先表里没有首页路由，远程访问 `/` 会落进"未列入"分支，日志报
   "该接口未列入访问分级表"，措辞误导排查。现在 18 条已注册路由全部登记在册，
   `test_classify_covers_all_routes` 逐一断言，**新增路由忘了登记会先在这里红**。
   首页当前判 LocalOnly，并注明"将来挂静态资源需改判 Remote"。

3. **ConnectInfo 取不到时按「非本机」处理**（fail-closed）。
   若哪天忘了注入，症状是本机 UI 全部 403——动静很大，藏不住，不会静默放过。

4. **403 响应体里带 `--remote-admin` 提示**，用户被拦时能自助排查，不必翻日志。

5. **移除了 `tower-http` 依赖**：它当初只为 CorsLayer 而引，删掉 CORS 后零使用。

**顺带修掉的两个既存问题（都不是阶段 2 的改动引起的）**

- `test_receiver_cancel_notifies_sender` 是个**时序 race**：
  原参数 300KB / 8192B 只有 37 个块，回环上几毫秒就传完，cancel 打过去时槽位
  已清理 → 404。C 盘写满那阵子磁盘 IO 慢、传输耗时够长才"看着是好的"，
  IO 一恢复就暴露。现改为 4MB / 1KB = 4096 块，叠加停等（每块等一次 ChunkAck）
  把传输窗口拉到数百毫秒；等待条件也从"记录出现"收紧为"status == InProgress"。
  取证方式：临时打印 B 侧 transfers，看到 `"status":"Completed"`、`chunks_done:37/37` 坐实。
- 同用例末尾断言的文件名写成了 `cancel.bin`，而源文件叫 `big.bin`，
  断言恒真、等于没在检查"取消后不应落盘"。已修正。

---

### 阶段 3 · 契约对齐 + 死代码清理

| 项 | 内容 | 易错点 |
|---|---|---|
| 3a | `unbounded_channel()` → `mpsc::channel(256)`；`InProgress` 用 `try_send`（满则丢）；终态用 `send().await` + 5s 超时 + `warn!` | 波及 6 处 send（`transfer.rs:711,747,774,804,919,1010`）与 `TransferHandle` 字段、`do_send` 签名。**终态绝不能一刀切 try_send**，否则 UI 卡 99%。接收端走 `publish_progress`，不受影响（N11） |
| 3b | 改正假注释：`transfer.rs:6`（bounded）、`transfer.rs:7`（xxhash3）、`storage.rs:7`（断点续传） | 规则固化：**注释只写已存在的行为，意图写 TODO** |
| 3c | `progress_cache` 上限 200；另维护 `VecDeque<String>` 记插入顺序，仅 file_id 首次出现时 push | 惰性消费（`continue` 不是 `break`）；全进行中时退化为淘汰最早条目，防空转 |
| 3d | 删 `ControlMessage::{Offer, Accept, Reject, Complete, Cancel}`（`protocol.rs:22+`）；删 `handle_incoming_offer`（`transfer.rs:575`，已核实 `src/` 与两端 Dart 均零调用）；删 `Cargo.toml:36,48,50` 的 `xxhash-rust` / `bytes` / `dashmap` | 同步改 `tests/protocol_test.rs` 的 `test_control_message_roundtrip`（`34,49,59,65,66` 行）；删依赖后**必须**跑一次 `cargo build --release --target aarch64-linux-android` 确认无 feature 隐式依赖 |
| 3e | `ffi.rs:33` `CONTEXT` → `OnceLock<Arc<FfiContext>>`；`169,190,213` 三处 `static mut LAST` 同样处理 | `std::mem::forget(runtime)`（`:150`）**是有意为之**，保留并加注释。验收：`cargo build` 后 8 个 `static_mut_refs` 警告全部消失 |

---

### 阶段 4 · 传输可靠性（**动手前先出错误恢复模型设计**）

**先定死的三件事**

1. **停等**（与现状一致，`send_chunk` 本就在等 ACK）。文档写明"RTT > 30ms 吞吐下降"。
2. **chunk_size 由发送方决定并随 offer 传递**（N2，先修，否则续传必然损坏数据）。
3. **续传身份识别**：`file_name + file_size + source_mtime` 匹配 → B 回 `resume_file_id` + `received_chunks` → A 切换 file_id → 复用旧槽位（N3）。

**改动清单**

| 项 | 内容 |
|---|---|
| 4.0 | `HttpOffer` 加 `chunk_size` + `source_mtime`；`HttpIncomingResponse` 加 `resume_file_id` + `received_chunks`；接收方建槽用 offer 的 chunk_size（N2/N3） |
| 4.1 | `TcpStream::connect` 移出 while（`transfer.rs:993`），每 worker 一条长连接 |
| 4.2 | 接收端 `serve_data_stream`（`transfer.rs:385`）加 while 循环，读到 EOF 才返回 |
| 4.3 | 有限重试：chunk 失败 → 重开连接从未确认块继续 → 耗尽才整体 Failed |
| 4.4 | `send_chunk` 内加取消检查 + ACK 超时 60s → 20s（N9） |
| 4.5 | `finalize` 的 rename 冲突自动改为 `name (1).ext` |
| 4.6 | 加 offer 限频（N12，可挪到 5a） |

**测试**

- 多块小文件（chunk_size 调小，覆盖连接复用的多块路径）
- 传输中途 drop 连接 → 重试成功
- **两端 chunk_size 配置不同** → sha256 仍一致（N2 回归）
- 传 1GB 文件 → 50% 强制中断 → 重发 → 最终 sha256 与源文件一致（N3 回归）
- 传一半点取消 → 两侧均立即停止（N9 回归）

---

### 阶段 5a · 配对 token

**模型**：TOFU + **纯推交换**（本轮修正）

```
首次（未配对）
  A POST /api/incoming offer（无 token；N12 限频）
  → B 弹窗「新设备 X 请求连接」（显示来源 IP + 平台）
  → 用户点「信任」
  → B  POST /api/pair → A   body: {device_id: B_id, token: token_B_for_A}    【纯推，不索回】
  → A 校验 pending offer（我确实给这个 ip+device_id 发过 offer）
  → A  POST /api/pair → B   body: {device_id: A_id, token: token_A_for_B}    【纯推，不索回】
  → B 校验（我刚对它点过信任）
  → 双方持久化 (peer_id → {token_i_gave, token_they_gave_me}, created_at)
  → B 才回 incoming-resp
```

**硬性约束**

- `/api/pair` **响应体永不携带凭据**，只回 200 空体（本轮用户修正，采纳）
- **per-peer token**（P1-3）：每个 peer 单独 32 字节随机；撤销 = 删该条
- 常数时间比较（`subtle` 或手写），防时序侧信道
- 四个跨机接口全部带 `Authorization: Bearer <token_peer_gave_me>`
- 数据通道首行 `AUTH <token_peer_gave_me>`（**N6 方向**）
- WS 认证：Dart `WebSocket.connect(url, headers:)` 传 header

**安全边界（写进文档，诚实声明）**：token 是明文头，被动嗅探可重放 → **防顺手攻击者，不防抓包盯着你的攻击者**。彻底解决靠 5b。

**验收**：A 传给 B 后**立刻** B 传给 A；B 传输中点取消，A 侧进度条立即停止。

---

### 阶段 5b · 数据通道加密

**范围**：只加密 7879 数据通道；HTTP 控制面靠 token，不加密。理由：自签证书在 LAN 场景成本远超收益；控制面泄漏的是元数据，数据面才是文件本体。**边界写进文档。**

| 项 | 内容 |
|---|---|
| 密钥 | `HKDF-SHA256(pairing_token, salt = file_id, info = "kitefile-data")`；file_id 续传复用 → salt 稳定（P1-2） |
| nonce | **96-bit 全随机，随密文发送**（P0-3）；断言做硬失败 |
| 帧布局 | `[24B header][12B nonce][ciphertext][16B tag]`，`data_len = 12 + ct + 16`（N7） |
| 内存 | `Vec::with_capacity(read_len + 28)` + `encrypt_in_place`，不多分配 16MB（N8） |
| 握手 | `AUTH <token>` 之后加 `ENC aes-256-gcm <key_check>`；key_check 用全 0 nonce，仅此一次（N13） |
| crate | `aes-gcm`（RustCrypto），不自己拼 |

**测试**：同一 chunk_id 在两条不同连接上发送 → nonce 不同；同一 chunk_id 在同一连接发两次 → nonce 不同（P0-3 验收）。

---

### 阶段 5c · Dart 共享 package

结构 `filetransfer/packages/kitefile_client/`（`src/api.dart` / `models.dart` / `format.dart` / `transfers.dart`）。

- **baseUrl 注入**，不写常量（桌面端固定 127.0.0.1:7878，移动端 `_daemonHost` 可变）
- **平台差异留在各自 app**：桌面「打开所在文件夹」、移动端 FileProvider / SAF；共享包只定义 `abstract class FileOpener`
- 路径依赖 `path: ../packages/kitefile_client`，不发包
- **迁移顺序**：models + format + http（纯函数、零风险）→ WS 状态管理（有状态、要回归）→ 弹窗级复用（收益递减，可不做）

---

### 阶段 5d · mDNS 兜底（手动添加 IP）

`POST/GET /api/devices/manual`、`DELETE /api/devices/manual/:id`，持久化；添加时 daemon 主动 `GET http://ip:7878/api/whoami` 验证可达（这也是 `/api/whoami` 必须允许远程的唯一理由）。

- **去重**：按 whoami 拿到的 `device_id` 去重，mDNS 条目优先，手动条目自动隐藏
- 错误提示可操作（"请确认对方已启动且防火墙放行 7878"），不只甩 timeout
- 端口默认隐藏在「高级」折叠里
- 手动设备同样要过 5a 配对

---

### 阶段 5e · 进度粒度 + 瞬时速率

- 发送方在写循环里按 1~4MB 更新 `bytes_done`（`AtomicU64` 已在），ACK 语义不变
- **注释必须压低承诺**："UI 估计值，真实性以 chunk ACK 为准"（反着画饼）
- 双层节流：每传输 100ms 一条 + **全局 50ms 一条**（N14）
- 平均速率 → **最近 3 秒滑动窗口**瞬时速率

---

## 四、存疑清单（需拍板）

> 以下条目我给了倾向性建议，但不构成结论，需要决策。

| # | 阶段 | 存疑点 | 我的倾向 | 备注 |
|---|---|---|---|---|
| Q1 | 2 | `allow_remote_admin=true` 时是否仍需 token 鉴权？ | **仍需**。分级放宽 ≠ 免鉴权，两者正交 | 需在文档中写明，否则实现者会二选一 |
| Q2 | 4 | 续传复用 file_id 会覆盖 `progress_cache`（`transfer.rs:215`，key 即 file_id）里的旧历史条目 | 接受覆盖（续传本就是同一文件的延续） | 若要求保留历史，key 需改 `(file_id, session_seq)`，改动面更大 |
| Q3 | 5a | N5 的竞态：攻击者抢先以 A_id 向 B 推假 token | 已有 peer 记录时**拒绝覆盖**，需用户显式重配对 | 后果是可用性受损非机密性受损，可接受，但要有明确行为定义 |
| Q4 | 5a | 冷启动 pair 推送（无 pending offer 上下文）是否弹窗？ | **弹窗**，与 offer 弹窗复用交互 | 若不弹则必须拒绝，不能静默接受 |
| Q5 | 5a | peer 记录的 key 用 `device_id` 还是 `(device_id, ip)`？ | **`device_id`**（对端换 IP 后仍信任） | 攻击者占用到旧 IP 也过不了 token 校验 |
| Q6 | 4 | `source_mtime` 的精度与跨平台一致性（FAT32 2 秒精度、NTFS/ext4 差异） | 比对时允许 ±2 秒容差；或改用 `(size, mtime)` 双条件 | 无条件信任 mtime 可能误续改过的文件 |
| Q7 | 4 | 并发：同一对端同时发两个相同文件的传输，抢同一 `resume_file_id` | B 侧对活跃槽位**拒绝续传**，走全新传输落到 `name (1).ext` | 简单可靠，不引入锁 |
| Q8 | 5b | tag 是否截断到 8/12 字节省带宽？ | **不截断，用标准 16 字节** | 28 字节/16MB 的开销可忽略，截断降低安全裕度 |
| Q9 | 3d | `ControlMessage` 删到只剩 `ChunkAck` 后，是否保留 enum 形式？ | **保留 enum + 注释**（便于扩展） | 与 P2-4 一致 |
| Q10 | 环境 | ~~磁盘满导致无法验证闸门~~ | ✅ **已解决**：真因是 `%TEMP%` 在写满的 C 盘，重定向到 E 盘即可，**无需清理任何文件** | 详见 N15 |

---

## 五、执行顺序与依赖

```
阶段 1（工程基建）  ← 差"提交 + 验证"，磁盘清理后跑通闸门
  ├── 阶段 2（Gateway 止血）  ← N1 先做；分级表按 (Method, Path)，含 N4 修正
  ├── 阶段 3（契约对齐）      ← N11（6 处非 10 处）、P2-2/3/4
  └── 阶段 4（传输可靠性）    ← 先出错误恢复模型设计
        │                      阻塞项：N2（chunk_size 协商）、N3（旧 file_id 来源）
        ├── 阶段 5a（配对 token）  ← N5（意图凭证）、N6（token 方向）、N12（限频）
        │     └── 阶段 5b（加密）  ← N7（+28 字节）、N8（capacity）、N13（key_check nonce）
        ├── 阶段 5c（Dart 共享包）  ← 放 4 定稿后，避免抽了又返工
        ├── 阶段 5d（手动添加 IP）  ← 依赖 5a 配对
        └── 阶段 5e（进度粒度）     ← 依赖 3，最独立
```

**关键路径**：1 → 2 → 4（设计对齐）→ 5a → 5b。

**顺序理由**：5a/5b 是安全线优先；5c 放 4 之后是因为共享包会动两端 UI，协议层没定稿前抽 UI 会白抽；5d 依赖配对；5e 最独立。

---

## 六、本轮新发现汇总

| 编号 | 严重度 | 阶段 | 一句话 |
|---|---|---|---|
| N1 | 阻塞 | 2 | `axum::serve` 未注入 ConnectInfo，本机判定拿不到 peer IP |
| N2 | 阻塞 | 4 | chunk_size 不跨端协商 → 既有数据损坏隐患 + 续传必然损坏 |
| N3 | 阻塞 | 4 | P1-2 未解决"发送方如何得知旧 file_id" |
| N4 | 重要 | 2 | `/api/devices`、`/ws/progress` 无远程调用方却归远程 |
| N5 | 重要 | 5a | 纯推模型缺"推送合法性校验"→ 自注册退化 |
| N6 | 重要 | 5a | 数据通道 AUTH 的 token 方向未定，易实现反 |
| N7 | 重要 | 5b | tag/nonce 字节数写反，随机 nonce 后是 +28 |
| N8 | 重要 | 5b | 原地加密需预留 capacity，否则多分配或 panic |
| N9 | 次要 | 4 | `send_chunk` 等 ACK 的 60s 内无取消检查 |
| N10 | 次要 | 2 | 表外请求默认策略未定义（建议远程 403 / 本机放行） |
| N11 | 次要 | 3a | send 调用点是 6 处非 10 处；接收端走另一条路径 |
| N12 | 次要 | 5a | 未配对设备可无限发 offer 刷弹窗 |
| N13 | 次要 | 5b | key_check 的 nonce 处理未定义 |
| N14 | 次要 | 5e | 节流需双层（每传输 + 全局） |
| N15 | 环境 | 1 | 磁盘满 + 沙箱拦截，闸门未验证 |
