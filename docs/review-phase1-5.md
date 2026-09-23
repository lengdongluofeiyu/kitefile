# KiteFile 修复方案（阶段 1-5）审查意见

审查日期：2026-09-03
审查对象：构建者出具的《KiteFile 修复方案（定稿）》阶段 1-5
审查方式：逐条核实源码证据，不采信方案中的自述结论

---

## 一、问题总表

| 编号 | 严重度 | 阶段 | 一句话 | 不改的后果 |
|---|---|---|---|---|
| P0-1 | 阻塞 | 2 | 中间件按路径前缀匹配会破坏 `/api/incoming` | 传输完全不可用，或分级形同虚设 |
| P0-2 | 阻塞 | 5a | token 单向分发，B 拿不到 A 的 token | B 无法取消、无法反向传文件 |
| P0-3 | 阻塞 | 5b | nonce 在重试场景重复 | AES-GCM 密钥泄露，比不加密更糟 |
| P1-1 | 重要 | 4 | 停等与流水线描述矛盾 | 高延迟网络跑不满带宽，或设计返工 |
| P1-2 | 重要 | 4 ↔ 5b | 续传 file_id 与加密 salt 耦合 | 续传后已加密的旧块无法解密 |
| P1-3 | 重要 | 5a | 单一 token 与"取消信任"互斥 | 撤销功能形同虚设 |
| P2-1 | 次要 | 总表 | 5a/5b 未标注依赖阶段 4 | 三处改动撞同一函数，返工 |
| P2-2 | 次要 | 3a | 终态超时分支不应静默 | 将来排查"UI 卡 99%"无迹可寻 |
| P2-3 | 次要 | 3c | VecDeque 淘汰会产生孤儿 id | 淘汰失效，cache 继续增长 |
| P2-4 | 次要 | 3d | 删 enum 变体后测试要同步改 | 编译不过 |

---

## 二、阻塞级问题（不改则方案失败）

### P0-1 · 阶段 2 · 中间件"按路径前缀匹配"会破坏 `/api/incoming`

**方案原文**

> 实现方式：axum middleware，按路径前缀匹配分级表，非 loopback 访问本机接口返回 403

**证据**

`core/src/gateway.rs:130`

```rust
.route("/api/incoming", post(incoming_offer).get(list_incoming))
```

同一路径挂了两个方法，且**分属两档**：

| 方法 | handler | 调用方 | 分级 |
|---|---|---|---|
| POST | `incoming_offer` | 对端 daemon 发 offer | 允许远程 |
| GET | `list_incoming` | 本机 UI 拉收件箱 | 仅本机 |

**根因**

路径前缀是一维的，而分级表实际上是二维的（方法 × 路径）。`/api/incoming` 这一个路径上，两个方法需要落到不同档位——只按路径匹配必然二选一。

**后果**

- 若归到"仅本机"：远程 offer 被 403 → **P2P 传输主流程直接断掉**，整个工具不可用
- 若归到"允许远程"：局域网任何人可 `GET /api/incoming` 查看你的收件箱 → 阶段 2 的分级白做

**修法**

分级表的 key 从 `path` 改为 `(Method, Path)` 二元组，中间件里同时取 `req.method()` 与 `req.uri().path()`：

```rust
fn classify(method: &Method, path: &str) -> Policy {
    match (method, path) {
        (&Method::POST, "/api/incoming") => Policy::Remote,
        (&Method::GET,  "/api/incoming") => Policy::LocalOnly,
        // ... 其余按 (method, path) 逐条列
    }
}
```

注意 axum 的路径参数（`/api/files/:name`、`/api/cancel/:file_id`）无法直接字符串全等匹配，需要在 middleware 里对路径做前缀切分：`/api/files/*` 归 GET 档。

**验证**

集成测试覆盖四条：
1. 远程 `POST /api/incoming` → 200
2. 远程 `GET /api/incoming` → 403
3. 本机 `GET /api/incoming` → 200
4. 远程 `GET /api/files` → 403

---

### P0-2 · 阶段 5a · token 单向分发，B 永远拿不到 A 的 token

**方案原文**

> 首次（未配对）：发送方 POST offer（无 token）→ 接收方弹窗 → 用户点「信任」→ 接收方在 HttpIncomingResponse 里附带自己的 token → 发送方按 (device_id → token) 持久化

**证据：四个跨机接口的实际方向**

| 接口 | 方向 | 需要谁的 token |
|---|---|---|
| `POST /api/incoming`（offer） | A → B | B 的 |
| `POST /api/incoming-resp` | **B → A** | **A 的** |
| `POST /api/verify/:file_id` | A → B | B 的 |
| `POST /api/cancel/:file_id` | **B → A** | **A 的** |

`core/src/transfer.rs:317`（取消联动，已核实）：

```rust
let path = format!("/api/cancel/{}", file_id_owned);
if let Err(e) = http_post_json(&from_ip, from_gateway_port, &path, "{}").await {
    warn!(error = %e, "notify sender cancel failed");
}
```

`from_ip` / `from_gateway_port` 是**发送方**的地址——确认取消是接收方跨机回调发送方。

**根因**

首次配对流程只完成了 A→B 这一个方向的授权：B 把自己的 token 给了 A，但没有任何环节把 A 的 token 给 B。

**后果**

1. **B 点"取消"失效**：B 调 A 的 `/api/cancel` 需要 A_token，B 没有 → 403 → `warn!` 一行日志 → **用户看到进度条还在跑，取消像没生效一样**
2. **B 无法主动传文件给 A**：B 发 offer 需要 A_token → 发不出去 → 只能等 A 先发起一次

值得注意：阶段 2 特意把 `/api/cancel/:file_id` 从"仅本机"归位到"允许远程"，正是为了保住这个取消联动功能。**如果 5a 只做单向分发，这个功能会在阶段 5a 被再次打断。**

**修法（推荐）**

加一步独立的配对握手，在 offer 之前完成双向交换：

```
B 收到未配对设备的 offer
  → 用户点「信任」
  → B  POST /api/pair  →  A    body: {"device_id": B_id, "token": B_token}
  → A  200             →  B    body: {"device_id": A_id, "token": A_token}
  → 双方各自持久化 (peer_id → {token_i_gave, token_they_gave_me})
  → B 才回 incoming-resp
```

配对表建议结构（注意是**两个** token）：

```
(peer_id, peer_name, token_i_gave_to_peer, token_peer_gave_me, created_at)
```

- `token_i_gave_to_peer`：我颁给它的，它用来访问我
- `token_peer_gave_me`：它颁给我的，我用来访问它

**备选方案**（不推荐）：`incoming-resp` 里双方互带 token，A 在后续 `verify` 请求里补带 A_token。缺点是依赖"传输一定走到 verify 那一步"——若传输在 verify 前失败或取消，B 仍拿不到 A_token，同一漏洞复发。

**验证**

1. A 传给 B，传完后**立刻** B 传给 A（验证反向可用）
2. B 在传输中点取消，确认 A 侧进度条立即停止（验证 cancel 联动）

---

### P0-3 · 阶段 5b · nonce 在重试场景重复

**方案原文**

> nonce = 96bit，由 (chunk_id 高位 || 递增计数) 构造
> 易错点——nonce 绝不能重：同 key 下 nonce 重复 = AES-GCM 完蛋。用 chunk_id || connection 内序号 构造，块号在传输内唯一、连接序号在连接内唯一，组合必唯一。写注释 + 断言

**根因**

"组合必唯一"这个推论在**阶段 4 的有限重试**下不成立。阶段 4 设计了"chunk 失败 → 有限重试（重开连接、从未确认的块继续）"，于是：

```
chunk_id = 5，连接① 发送   → 序号 0 → nonce = (5, 0)
连接中断
重开连接②，重发 chunk 5    → 序号归零 → nonce = (5, 0)   ← 重复
```

连接序号在**连接内**唯一，但重试会跨连接；chunk_id 在传输内唯一，却会重复出现。两者组合**不是**全局唯一。

**后果**

AES-GCM 在同一密钥下重用 nonce，会泄露认证子密钥 H，攻击者可据此伪造任意密文（Joux 的攻击）。这不是"加密强度下降"，是**认证机制完全失效**——比不加密更糟，因为它提供了虚假的安全感。

**修法（三选一，推荐第一个）**

1. **96-bit 全随机 nonce**，随密文一起发送（12 字节前缀）。GCM 标准做法，2^32 次加密内碰撞概率可接受，且完全不依赖任何状态。最省心。
2. **连接级随机盐**：握手时交换 4 字节随机 salt，每次建连重新生成；nonce = `salt(4B) || counter(8B)`。只要 salt 不重复，nonce 就不重复。
3. **全局单调计数器**：持久化到传输状态，**不随连接重置**。可行但引入额外状态管理。

另外：方案说"写注释 + 断言"，建议把断言做成**硬失败**（检测到重复直接终止连接并报错），而不是留一句注释——这类错误不会在测试里稳定复现，只有线上偶发。

**验证**

单元测试：构造"同一 chunk_id 在两条不同连接上发送"，断言两次产生的 nonce 不同。再构造"同一 chunk_id 在同一连接上发送两次"，断言同样不同。

---

## 三、重要问题（会导致返工或功能缺陷）

### P1-1 · 阶段 4 · 停等与流水线，二者只能选一个

**方案原文的两处描述互相矛盾**

- 连接模型：「循环 header+data→**等ACK**→下一块」→ 这是**停等**（stop-and-wait）
- 易错细节：「改成连发后，**ACK 乱序返回时**要按 chunk_id 匹配，不能按到达顺序」→ 这描述的是**流水线**（pipelining）

停等模式下，一个时刻只有一块在飞，ACK 不可能乱序。这两段描述的是两种不同的设计。

**影响：吞吐上界**

停等的带宽上界 ≈ `chunk_size / RTT`：

| 场景 | RTT | 单流吞吐上界 | 千兆网够不够 |
|---|---|---|---|
| LAN 有线 | ~1 ms | 16 GB/s | 绰绰有余 |
| Wi-Fi 良好 | ~10 ms | 1.6 GB/s | 够 |
| 跨网段 / VPN | ~30 ms | 533 MB/s | 够 |
| 跨城 / Wi-Fi 抖动 | ~100 ms | 160 MB/s | **不够** |

注意这是**单流**上界；8 条流并行理论上乘以 8，但共享同一条物理链路时会互相争抢，实际增益有限。

**建议**

在那页错误恢复模型设计文档里先定死：

1. 目标 RTT 范围是多少？（LAN 工具通常 < 10ms）
2. 选停等还是流水线？

若目标场景是局域网，**停等完全合理**——简单、正确性容易保证、ACK 与块天然一一对应。只是要在文档里写明"本设计在 RTT > 30ms 的场景吞吐会下降"，别让将来的人以为是 bug。

若要求支持高延迟链路，则需要：流水线 + 滑动窗口 + 维护 in-flight 集合 + 按 chunk_id 匹配 ACK（不能按到达顺序）。复杂度显著上升。

**顺带一个复用点**：若选流水线，必须维护"哪些块已发出但未确认"的集合——这个集合恰好就是断点续传需要的 bitmap，两者可以共用一份数据结构。

---

### P1-2 · 跨阶段 4 ↔ 5b · 续传 file_id 与加密 salt 耦合

**方案原文**

- 5b：`key = HKDF(pairing_token, salt = transfer 的 file_id, info = "kitefile-data")`
- 4：续传身份识别用 `(file_name, file_size, chunk_size)`，接收方检查已存在的 .part

**根因**

第 4 阶段的续传识别方案隐含"file_id 每次传输新生成"（否则直接用 file_id 匹配就行，不必绕道文件名+大小）。但 5b 用 file_id 作 HKDF salt——**file_id 一变，密钥就变**。

于是：传输中断 → 续传时新生成 file_id → 新密钥 → `.part` 里已加密的旧块**全部解不开**。

**修法（推荐）**

**续传复用上次的 file_id**，不新生成：

- 好处一：salt 一致 → 密钥一致 → 旧块可解
- 好处二：file_id 本身成为续传标识符，`file_id.meta` 方案可以省掉
- 代价：接收方需要在 `incoming-resp` 里把 `.part` 关联的 `last_file_id` 回传给发送方

**备选**：若坚持每次新生成 file_id，则 HKDF salt 不能再用 file_id，改用一个由 `(file_name + file_size + chunk_size)` 派生的稳定值。这样密钥与 file_id 解耦，续传不受影响。

**验证**

传 1GB 文件，到 50% 强制中断，重新发起传输，校验最终文件的 sha256 与源文件一致。

---

### P1-3 · 阶段 5a · 单一 token 与"取消信任"互斥

**方案原文**

> 每设备一个持久 token（32 字节随机，存 .kitefile-identity，和 device id/name 同文件）
> 「取消信任」按钮：清掉一条记录，下次对方来连重新走配对弹窗

**根因**

如果校验逻辑是"请求携带的 token == **我自己的** token"，那么清掉某条 peer 记录，并不影响对方手里那张票——对方仍然持有你的 token，校验照样通过。

**真要撤销，只能轮换本机 token**，代价是所有已配对设备全部失效、需要重新配对。这与"清掉一条记录"的设计意图相悖。

**修法**

改成 **per-peer token**：

- B 配对时为 A **单独生成**一个 32 字节随机 token，记录在 `(A_id → token_for_A)`
- 校验：请求携带的 token == 配对表中**该 peer 的** token
- 撤销 = 删掉这一条 → 该 peer 立即失效，其他 peer 不受影响
- 额外收益：单个 token 泄漏不波及其他设备

这与 P0-2 的双向需求正好吻合——配对表存两个 token（我给它的 / 它给我的），既是双向授权，也是可单独撤销的。

---

## 四、次要问题

### P2-1 · 总表 · 5a/5b 应标注依赖阶段 4

总表里 5a 依赖"2 的分级结构"、5b 依赖"5a 的 token"，但两者都要改 `serve_data_stream`：

| 改动 | 阶段 | 位置 |
|---|---|---|
| 加 while 循环（连接复用） | 4 | `serve_data_stream` 主体 |
| 读 AUTH 行 | 5a | `serve_data_stream` 开头 |
| 解密 chunk | 5b | `serve_data_stream` 读取处 |

三处落在同一个函数。若跳过 4 直接做 5a/5b，阶段 4 又要重改一遍。建议总表补上 `5a ← 4`、`5b ← 4`。

### P2-2 · 阶段 3a · 终态超时分支不应静默

终态用 `send().await` + 5 秒超时是安全的：若 `progress_rx` 已被 drop（消费者任务结束），`send()` 会**立即**返回 Err，不会真等满 5 秒。

但超时/失败分支不要 `let _ =` 吞掉，应记 `warn!`。否则将来出现"UI 卡在 99%"时无从排查。

### P2-3 · 阶段 3c · VecDeque 淘汰会产生孤儿 id

若淘汰时只从 HashMap 删除、不同步清理 VecDeque，deque 里会残留已删除的 file_id。下次淘汰 pop 出来的若是孤儿，`remove` 找不到对应项，等于这次淘汰没生效——cache 继续增长，问题没修好。

建议惰性消费：

```rust
while cache.len() > MAX {
    match order.pop_front() {
        Some(id) if cache.contains_key(&id) => { cache.remove(&id); break; }
        Some(_)  => continue,   // 孤儿，继续 pop
        None     => break,
    }
}
```

另外"优先淘汰已终态条目"要注意：若所有条目都是进行中，必须允许淘汰进行中的最早条目，否则 `while` 循环找不到可淘汰项而空转。

### P2-4 · 阶段 3d · 删 enum 变体后测试要同步改

已核实：`ControlMessage::{Offer, Accept, Reject, Complete, Cancel}` 在 `src/` 下**零引用**。

- `Offer` 的 2 处引用：`tests/protocol_test.rs:34`、`59`
- `Cancel` 的 3 处引用：`tests/protocol_test.rs:49`、`65`、`66`

全部集中在 `test_control_message_roundtrip` 这一个测试里。删 enum 变体后必须同步改这个测试，否则编译不过。**不要为了保住测试而留下死代码。**

删除后只剩 `ChunkAck` 一个变体。建议保留 enum 形式（便于将来扩展），但加注释说明"当前仅 ChunkAck 在用"。

---

## 五、核实后确认无误的条目

以下条目我在审查时准备质疑，核实后确认**方案正确**，无需修改：

| 条目 | 核实结论 |
|---|---|
| `Offer`/`Cancel` 可安全删除 | `src/` 零引用，仅 `protocol_test.rs` 出现，同步改测试即可 |
| 闸门放在 `Stop-FtcoreProcesses` 之前 | `gateway_test.rs` 无 `axum::serve` / `TcpListener::bind`；7878/7879 只是传给 `DiscoveryService::new` 的元数据，不绑定端口，不会与运行中的 daemon 冲突 |
| `/api/cancel/:file_id` 归到"允许远程" | `transfer.rs:317` 确认接收方跨机 POST 发送方，归"仅本机"会让取消联动静默失效 |
| 直接删掉整个 `CorsLayer` | Dart 原生 `HttpClient` 不受 CORS 约束，Web 走 Vite 代理同源，当前零受益方 |
| `?fileName` 改保守写法 | `if (fileName != null) body['file_name'] = fileName;` 兼容所有 Dart 版本且不触发 lint |
| 首次提交不带 `dist/` | 二进制进历史后无法彻底清除 |

---

## 六、修正后的阶段依赖建议

```
阶段 1（工程基建）
  ├── 阶段 2（Gateway 止血）        ← 注意 P0-1：按 (method, path) 分级
  ├── 阶段 3（契约对齐）            ← 注意 P2-2、P2-3、P2-4
  └── 阶段 4（传输可靠性）          ← 先出错误恢复模型设计，定死 P1-1
        └── 阶段 5a（配对 token）   ← 依赖 4；注意 P0-2、P1-3
              └── 阶段 5b（加密）   ← 依赖 4、5a；注意 P0-3、P1-2
  ├── 阶段 5c（Dart 共享包）        ← 建议放在 4 定稿后
  ├── 阶段 5d（手动添加 IP）        ← 依赖 5a 配对
  └── 阶段 5e（进度粒度）           ← 依赖 3
```

**关键路径**：阶段 1 → 4（设计对齐）→ 5a → 5b。三处阻塞问题中，P0-1 在阶段 2 独立可修；P0-2 与 P0-3 都在加密这条线上，需要先定死阶段 4 的错误恢复模型。
