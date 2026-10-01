# KiteFile

> 轻如风筝的局域网文件传输。Windows / Android，免数据线、免中转服务器。

KiteFile 在同一局域网内通过 **mDNS 发现 + 多路 TCP 直传** 互传文件，数据不出网关。
对外接口全链路 **TLS**，跨机访问遵循「**先配对、后信任**」。

## 特性

- **先配对后信任**：设备配对采用确认码两阶段确认，跨机认证走 mTLS 客户端证书
  （Ed25519 自签证书，指纹 pin 进 peers 表）；未配对设备 fail-closed，一律 403
- **设备管理**：本机重命名 / 删除设备（即撤销信任）；设备列表显示在线/离线
- **局域网直传**：设备配对后点对点传输，吞吐吃满内网带宽
- **多流自适应**：按文件大小自动决定并行流数（小文件单流，大文件拉满）
- **流式传输**：无分块停等 ACK，进度按字节平滑更新
- **中断可续**：可续中断状态机 + 段级自动重试，传输异常不再整段作废
- **批量发送**：多文件一次确认；接收请求 60 秒未接受自动超时
- **端到端校验**：整文件 SHA-256（发送方补发，接收方 finalize 比对）
- **传输记录持久化**：历史记录落盘，重启后仍可查看；支持打开文件/所在文件夹、
  删除记录（接收记录可勾选同时删除本地文件）
- **桌面托盘常驻**：关窗后仍在后台接收，来文件弹系统通知，点通知回到主界面
- **发送/接收取消**：双向 `/api/cancel` 联动，不会卡在「进行中」

## 目录结构

```
core/       Rust 核心（mDNS 发现、配对/mTLS、TLS 网关、流式传输、FFI）
desktop/    Flutter 桌面端（Windows / macOS 骨架）
mobile/     Flutter 移动端（Android 可用，iOS 预留）
web/        Web 前端（实验）
docs/       协议与阶段设计文档（历史设计，以代码为准；
            对标分析见 docs/localsend-comparison.md）
scripts/    构建脚本
```

## 快速开始

### 依赖

- Rust（stable）
- Flutter 3.4x
- Android：NDK r28+（交叉编译 `aarch64-linux-android`）
- Windows 桌面：Visual Studio C++ 工具链

### 一键构建（Windows）

```powershell
# 全量：Rust 测试闸门 + 桌面端 + Android APK → dist/
.\scripts\build-all.ps1

# 只编 Windows 桌面
.\scripts\build-all.ps1 -SkipAndroid
```

产物：

- `dist/windows/kitefile_desktop.exe`（旁路 `kitefile-cli.exe` / `kitefile.dll`）
- `dist/android/app-release.apk`

### 手动构建

```bash
# Rust 核心
cd core
cargo test
cargo build --release

# Android arm64（先配好 ANDROID_NDK_HOME 或 core/.cargo/config.toml）
cargo build --release --target aarch64-linux-android
cp target/aarch64-linux-android/release/libkitefile.so \
  ../mobile/android/app/src/main/jniLibs/arm64-v8a/

# 桌面端
cd ../desktop && flutter pub get && flutter build windows --release

# 手机端
cd ../mobile && flutter pub get && flutter build apk --release
```

### 开发时环境变量（可选）

构建脚本会把缓存指到脚本里的 `DEPS_ROOT` / `CARGO_HOME` 等路径。
换机器请改 `scripts/rust-env.sh`、`scripts/flutter-env.sh`、`scripts/build-all.ps1`，
或参考下面的环境变量名自行覆盖：

| 变量 | 用途 |
|------|------|
| `ANDROID_NDK_HOME` / `ANDROID_HOME` | Android 交叉编译与 Gradle |
| `JAVA_HOME` | Gradle 需要 JDK 17–21 |
| `CARGO_HOME` / `RUSTUP_HOME` | Rust 工具链位置 |
| `PUB_CACHE` | Flutter/Dart 包缓存 |

## 使用

1. 两台设备装好客户端，连同一 Wi-Fi / 有线局域网
2. **先配对**：在一端进入「添加设备」发起配对，对端展示确认码，
   发起方输入该码并经对端「确认配对」后，两台设备建立信任
3. 设备列表自动出现已配对的对端（在线才可发送），点发送 → 选文件 → 对端确认（默认 60 秒内）
4. 桌面端点 **×** 可「最小化到托盘」，后台仍可接收；系统通知可点回主界面

## 架构一览

```text
┌─────────────┐  mDNS 发现  ┌─────────────┐
│   设备 A    │◄───────────►│   设备 B    │
│ Flutter UI  │             │ Flutter UI  │
│      ▼      │             │      ▲      │
│ kitefile-cli│  7880 TLS   │ kitefile-cli│
│  (daemon)   │◄───────────►│  (daemon)   │
│      ▼      │  控制面+mTLS│      ▲      │
│  N 路 TCP   │────────────►│  流式落盘   │
└─────────────┘ 7879 TLS直传 └─────────────┘
```

- 端口：7878 HTTP 网关只绑 127.0.0.1（本机 UI/WS 专用，不对外）；
  7880 LAN TLS 网关（对外控制面）；7879 TLS 数据通道（占用时按候选列表退避）
- 控制面：HTTP over TLS（offer / accept / cancel / verify），mTLS 客户端证书鉴权
- 数据面：按文件大小切 N 段连续字节，每段一条 TLS 流，TCP 窗口背压
- 校验：整文件 SHA-256，`/api/verify` 延后补发

## 安全模型

- 每台设备持有长期 **Ed25519 自签证书**（与身份文件同目录持久化，指纹不变）
- 配对 = 双屏比对确认码 + 交换证书；此后所有跨机连接在 **TLS 握手层**完成双向认证，
  应用层无凭据下发（无「索取—下发」面）
- 未配对访问一律 403；配对协商接口按 IP 限频
- 撤销信任 = 删除设备（删一条 peers 记录）
- 已知边界：已配对设备本身被攻陷不在防御范围（等同本人操作）；
  局域网内的设备存在性（mDNS 广播）不隐藏

## 平台

| 平台 | 状态 |
|------|------|
| Windows 桌面 | ✅ 可用（托盘 + 系统通知） |
| Android | ✅ 可用 |
| macOS | 代码在，待真机验收 |
| iOS | 接口预留 |
| Web | 实验 |

## License

[MIT](./LICENSE)

## 致谢

- [window_manager](https://pub.dev/packages/window_manager) / [tray_manager](https://pub.dev/packages/tray_manager) / [local_notifier](https://pub.dev/packages/local_notifier)
- [mdns-sd](https://crates.io/crates/mdns-sd)、[rustls](https://crates.io/crates/rustls)、[rcgen](https://crates.io/crates/rcgen)、tokio、axum 等 Rust 生态
