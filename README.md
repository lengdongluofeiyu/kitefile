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

> ⚠️ 先确保已配置环境变量（见下方「开发时环境变量」章节）。
> 若 `scripts/local-env.ps1` 已存在，`build-all.ps1` 会自动加载它。

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

> 手动敲命令时**必须先加载环境**（`build-all.ps1` 的自动加载不适用于手动构建）：
> ```powershell
> . .\scripts\local-env.ps1
> ```

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

### 开发时环境变量（**必需**，非可选）

本项目把 Rust / Gradle / Dart / Android SDK 的缓存全部集中到 `E:\zheten2.0\.deps`，
避免往 C 盘写。**这些变量不设置，构建会失败或把几 GB 缓存写回 C 盘。**

#### 方式一：用 `local-env.ps1`（推荐，Windows）

```powershell
cd E:\zheten2.0\filetransfer
. .\scripts\local-env.ps1     # 注意开头的点号，表示在当前会话生效
.\scripts\build-all.ps1
```

`local-env.ps1` 是**本机配置文件，已被 `.gitignore` 排除，不在仓库里**。
新机器克隆后请先复制模板：

```powershell
copy .\scripts\local-env.ps1.example .\scripts\local-env.ps1
# 然后按实际路径修改
```

`build-all.ps1` 在检测到 `scripts/local-env.ps1` 时会**自动加载**它，
但**手动构建**（直接敲 `cargo` / `flutter` / `gradle`）必须先手动 `.` 加载。

#### 方式二：设为系统环境变量（一次配好，长期有效）

如果不想每次都加载，可把下表变量设到用户级（重启终端后生效）：

```powershell
[Environment]::SetEnvironmentVariable('CARGO_HOME',  'E:\zheten2.0\.deps\cargo',       'User')
[Environment]::SetEnvironmentVariable('RUSTUP_HOME', 'E:\zheten2.0\.deps\rustup',      'User')
[Environment]::SetEnvironmentVariable('PUB_CACHE',   'E:\zheten2.0\.deps\pub-cache',   'User')
[Environment]::SetEnvironmentVariable('ANDROID_HOME',  'E:\zheten2.0\.deps\android-sdk', 'User')
[Environment]::SetEnvironmentVariable('ANDROID_SDK_ROOT','E:\zheten2.0\.deps\android-sdk','User')
[Environment]::SetEnvironmentVariable('GRADLE_USER_HOME','E:\zheten2.0\.deps\gradle',   'User')
[Environment]::SetEnvironmentVariable('FTCORE_DEPS_ROOT','E:\zheten2.0\.deps',          'User')
[Environment]::SetEnvironmentVariable('FTCORE_TEST_TMP','E:\zheten2.0\.deps\tmp',       'User')
[Environment]::SetEnvironmentVariable('JAVA_HOME',   'D:\jdk21\jdk-21.0.12.1+1',       'User')
```

> `JAVA_HOME` 是**全局变量**，改动会影响其他 Java 项目。若本机有其他项目依赖别的 JDK，
> 请改用方式一（`local-env.ps1` 只对当前会话生效，不影响全局）。

#### 变量清单

| 变量 | 本项目取值 | 用途 / 不设的后果 |
|------|-----------|------------------|
| `FTCORE_DEPS_ROOT` | `E:\zheten2.0\.deps` | 所有工具链缓存的根目录 |
| `CARGO_HOME` | `…\.deps\cargo` | Rust 工具链与 crate 缓存；不设则用 `~\.cargo`（C 盘） |
| `RUSTUP_HOME` | `…\.deps\rustup` | Rust 工具链本体；不设则用 `~\.rustup` |
| `PUB_CACHE` | `…\.deps\pub-cache` | Flutter/Dart 包缓存；不设则用 `~\.pub-cache` |
| `ANDROID_HOME` | `…\.deps\android-sdk` | Android SDK 位置；不设则 Gradle 找不到 SDK，**构建直接失败** |
| `ANDROID_SDK_ROOT` | 同上 | 旧版变量名，Gradle/Flutter 兼容用 |
| `GRADLE_USER_HOME` | `…\.deps\gradle` | Gradle 缓存与 wrapper 发行包；不设则依赖重新下载到 C 盘 |
| `FTCORE_TEST_TMP` | `…\.deps\tmp` | Rust 测试临时目录；不设则用系统 TEMP |
| `JAVA_HOME` | `D:\jdk21\jdk-21.0.12.1+1` | Gradle 需要 **JDK 17–21**（项目编译目标为 Java 17，Gradle 9.1） |
| `ANDROID_NDK_HOME` | 可留空 | 交叉编译用；`build-all.ps1` 会从 `ANDROID_HOME` 自动探测（当前 NDK `28.2.13676358`） |

#### 两个常见坑

1. **PowerShell 的变量不会传给子进程的「祖父」**。
   在 PowerShell 里用 `cmd /c xxx` 或 `Start-Process` 启动的程序，
   拿到的环境变量取决于启动时的快照，改完变量要**重开终端**才稳妥。
2. **`JAVA_HOME` 指向不存在的路径时，报错很隐蔽**。
   Gradle 只会说「找不到 Java 运行时」或直接卡住。配完后务必验证：
   ```powershell
   & "$env:JAVA_HOME\bin\java.exe" -version    # 应打印 21.0.x
   ```

#### 验证环境是否就绪

```powershell
cd E:\zheten2.0\filetransfer
. .\scripts\local-env.ps1
& "$env:JAVA_HOME\bin\java.exe" -version      # JDK 21
& "$env:CARGO_HOME\bin\rustup.exe" toolchain list   # stable-x86_64-pc-windows-msvc
flutter --version                              # Flutter 3.44.x
Test-Path "$env:ANDROID_HOME\ndk"              # True
```

#### 首次构建较慢（正常现象）

若清理过构建缓存，首次重建需要：Rust 全量编译（10–30 分钟）、
Gradle 依赖重新下载（需联网）。之后增量构建会快很多。

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
