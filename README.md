# Sparrow

> 麻雀传信 —— 局域网秒传文件。Windows / Android，免数据线、免中转服务器。

Sparrow 在同一局域网内通过 **mDNS 发现 + 多路 TCP 直传** 互传文件，数据不出网关。

## 特性

- **局域网直传**：设备发现后点对点传输，吞吐吃满内网带宽
- **多流自适应**：按文件大小自动决定并行流数（小文件单流，大文件拉满）
- **流式传输**：无分块停等 ACK，进度按字节平滑更新
- **批量发送**：多文件一次确认；接收请求 60 秒未接受自动超时
- **端到端校验**：整文件 SHA-256（发送方补发，接收方 finalize 比对）
- **桌面托盘常驻**：关窗后仍在后台接收，来文件弹系统通知，点通知回到主界面
- **发送/接收取消**：双向 `/api/cancel` 联动，不会卡在「进行中」

## 目录结构

```
core/       Rust 核心（mDNS 发现、流式传输、HTTP 网关、FFI）
desktop/    Flutter 桌面端（Windows / macOS 骨架）
mobile/     Flutter 移动端（Android 可用，iOS 预留）
web/        Web 前端（实验）
docs/       协议与阶段设计文档（历史设计，以代码为准）
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

- `dist/windows/ftcore_desktop.exe`（旁路 `ftcore-cli.exe` / `ftcore.dll`）
- `dist/android/app-release.apk`

### 手动构建

```bash
# Rust 核心
cd core
cargo test
cargo build --release

# Android arm64（先配好 ANDROID_NDK_HOME 或 core/.cargo/config.toml）
cargo build --release --target aarch64-linux-android
cp target/aarch64-linux-android/release/libftcore.so \
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
2. 打开 Sparrow，设备列表会自动出现对端
3. 点发送 → 选文件 → 对端确认（默认 60 秒内）
4. 桌面端点 **×** 可「最小化到托盘」，后台仍可接收；系统通知可点回主界面

## 架构一览

```text
┌────────────┐   mDNS    ┌────────────┐
│  设备 A    │◄─────────►│  设备 B    │
│ Flutter UI │           │ Flutter UI │
│     ▼      │           │     ▲      │
│ ftcore-cli │  HTTP 握手 │ ftcore-cli │
│  (daemon)  │◄─────────►│  (daemon)  │
│     ▼      │           │     ▲      │
│  N 路 TCP  │──────────►│  流式落盘  │
└────────────┘  无停等ACK └────────────┘
```

- 控制面：HTTP（offer / accept / cancel / verify）
- 数据面：按文件大小切 N 段连续字节，每段一条 TCP 流，TCP 窗口背压
- 校验：整文件 SHA-256，`/api/verify` 延后补发

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
- [mdns-sd](https://crates.io/crates/mdns-sd)、tokio、axum 等 Rust 生态
