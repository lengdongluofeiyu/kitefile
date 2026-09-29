# KiteFile 发布说明（工作流 D · 发布卫生）

> 适用范围：Android APK 与 Windows 桌面端的正式发布。
> 测试闸门（每次发布前必须全绿）：`cargo test`（core）+ `flutter analyze` +
> `flutter test`（desktop / mobile / packages/kitefile_shared）。
> CI（`.github/workflows/ci.yml`）在同一闸门之外还会构建 Windows 产物与
> Android APK，并对 daemon 做「whoami 冒烟」。

## 版本兼容性（阶段 5 · 设备配对与加密，破坏性变更）

**新旧版本不能互传**（设计 `docs/design-pairing-encryption.md` §8.4 定案：
不做明文降级，避免协议降级攻击面）。升级后请两端同时更新：

- 跨机控制面从明文 HTTP 换为 **HTTPS（7880）+ 客户端证书**；
  本机 UI 仍走回环明文 7878（CI whoami 冒烟不受影响）。
- 数据通道（7879）全量 TLS；未配对设备在握手层即被拒绝。
- **首次使用需配对**：两台设备在首页设备列表点「**添加设备**」（打开即
  自动进入配对模式，关闭自动退出），页内出现对方后点「配对」，两屏核对
  6 位确认码 → 对方确认即完成。未配对设备无法互发文件——这是预期行为，
  不是故障。
- **配对发现**：除 mDNS 外，开启配对模式时 daemon 会按本局域网段主动
  探测 LAN TLS `whoami`（组播被 Android/路由器丢掉时也能发现对端）。
  仍需：两台设备在同一 Wi-Fi；电脑端也打开「添加设备」页；Windows
  防火墙放行 LAN TLS 端口（7880 及退避候选；非管理员启动时自动加规则
  会失败，需手动放行）。
- 已配对设备上线自动可见、可直接传输；撤销在「设置 → 配对」列表。
- 移动端「遥控模式」已**移除**：前端只连接本机守护进程；跨机控制是
  daemon↔daemon 的 mTLS 通道，Dart HTTP 栈不支持客户端证书，留着 UI 只会
  必然失败。daemon 侧 `--remote-admin` 开关保留（默认关闭、无 UI 入口）。

## Android

### 包名（正式）

| 项 | 值 |
|----|----|
| applicationId / namespace | `org.kitefile.mobile` |
| 旧占位包名 | `com.example.kitefile_mobile`（模板遗留，已废弃） |

**迁移注意**：applicationId 变更后系统视为全新应用——旧 `com.example` 包
不会自动升级覆盖，需先卸载旧包（或在商店按新包名重新上架）。身份持久化
文件（device id）存放在应用专属目录，换包名后会生成新身份，属预期行为。

### 签名策略

- **正式发布**：在 `mobile/android/key.properties` 指定 keystore（**不得提交**，
  `android/.gitignore` 已排除 `key.properties` / `*.jks` / `*.keystore`）：

  ```properties
  storeFile=/abs/path/upload-keystore.jks
  storePassword=***
  keyAlias=upload
  keyPassword=***
  ```

  Gradle 检测到该文件即用 release 签名（`app/build.gradle.kts`）。
- **开发构建**：没有 `key.properties` 时回退 debug 签名，`flutter run`/本地
  `flutter build apk` 照常可用，但**不可上架**。
- keystore 丢失 = 应用无法升级（Android 签名不可更换），务必备份。

### ABI（arm64-only）

- 发布包**只包含 `arm64-v8a`**（`ndk.abiFilters` 已锁定）。
- 原因：内置的 Rust 引擎 `libkitefile.so` 只交叉编译了 arm64；不锁 ABI 的话
  x86_64 / armeabi-v7a 设备能安装但运行期缺 so，启动即崩。
- 影响面：覆盖 2017 年后的绝大多数在用 Android 设备；x86 模拟器需用
  arm64 镜像（Android Studio 虚拟机默认已是 arm64 或可选择）。

### 权限（最小化）

| 权限 | 为什么保留 |
|------|-----------|
| `INTERNET` / `ACCESS_NETWORK_STATE` / `ACCESS_WIFI_STATE` | 局域网传输与发现必需 |
| `CHANGE_WIFI_MULTICAST_STATE` | mDNS 设备发现必需 |
| `FOREGROUND_SERVICE` + `FOREGROUND_SERVICE_DATA_SYNC` | 传输中前台保活（A3.7） |
| `MANAGE_EXTERNAL_STORAGE` | **仅**「设置里把接收目录切到公共目录」需要；默认接收目录是应用专属存储，零存储权限。首次使用该功能时引导用户到系统授权页 |
| `WRITE_EXTERNAL_STORAGE`（maxSdk 29） | 旧设备（Android 10 及以下）自定义目录的兼容项 |

已移除：`READ_EXTERNAL_STORAGE`（33 之前版本也非必需：文件选择走 SAF、
接收目录为应用专属或已授权 MANAGE）。

系统通知（Android 13+ 的 `POST_NOTIFICATIONS`）未声明：前台服务通知由
系统任务管理器展示，不依赖通知权限；若后续要加应用内推送再按需申请。

### 发布构建

```powershell
# 全量（Windows 桌面 + Android APK + 清单）
scripts\build-all.ps1
# Windows 环境缺 %PROGRAMFILES(X86)% 时
scripts\build-all-fix.ps1
```

APK 产物：`mobile\build\app\outputs\flutter-apk\app-release.apk`
（同步拷贝至 `dist\android\`）。CI 侧见 `.github/workflows/ci.yml` 的
`build-android` job（含 Rust arm64 交叉编译与 jniLibs 同步）。

## Windows 桌面

- 产物：`dist\windows\`（`kitefile_desktop.exe` + `kitefile-cli.exe` 守护进程）。
- 冒烟：CI 启动 `kitefile-cli daemon` 后轮询 `GET /api/whoami`，200 即通过。
- 本地构建注意：Flutter Windows 目标需要 `%PROGRAMFILES(X86)%` 存在
  （精简环境缺失时用 `scripts\build-all-fix.ps1`）。

## SDK 版本钉死

| 项 | 值 | 定义处 |
|----|----|--------|
| Flutter | 3.44.2 | `ci.yml` `FLUTTER_VERSION`；本地以安装版本为准 |
| Dart SDK | `^3.12.2` | 各 `pubspec.yaml` |
| Android compileSdk / targetSdk | 36 | `mobile/android/app/build.gradle.kts` |
| Android minSdk | 24 | 同上 |
| Java | 17（编译目标）/ 21（CI 与本地构建 JDK） | gradle.kts / ci.yml |
