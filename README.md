# LAN File Transfer

局域网文件传输工具：跨 Windows / Android / iOS / macOS 平台，含 Web 版。

## 架构

```
filetransfer/
├── core/          Rust 核心引擎（传输 + 发现 + HTTP 网关）
├── web/           React 前端（PWA，纯浏览器 / 配合本机守护进程双模式）
├── desktop/       Flutter 桌面应用（Windows / macOS）
├── mobile/        Flutter 移动端（Android / iOS）
└── docs/          协议规范与设计文档
```

## 平台支持

| 平台 | 状态 | 实现方式 |
|---|---|---|
| Windows 桌面 | 进行中 | Flutter Windows + Rust FFI |
| Android | 计划 | Flutter Android + Rust FFI |
| iOS | 接口预留 | Flutter iOS + Rust FFI |
| macOS | 接口预留 | Flutter macOS + Rust FFI |
| Web | 进行中 | React PWA（两种模式）|

## Web 双模式

1. **本机守护进程模式**：本机已安装桌面端，浏览器通过 `http://localhost:7878` 调用本机 Rust 守护进程，性能等同原生。
2. **纯浏览器模式**：未安装客户端时，浏览器直接通过 WebRTC / HTTP 与对端通信（性能受限）。

## 构建

```bash
# Rust 核心
cd core && cargo build --release

# Web
cd web && npm install && npm run dev

# 桌面端
cd desktop && flutter pub get && flutter run -d windows
```

## 依赖缓存位置（已重定向至 E 盘）

- `CARGO_HOME=E:\zheten2.0\.deps\cargo`
- `RUSTUP_HOME=E:\zheten2.0\.deps\rustup`
- `PUB_CACHE=E:\zheten2.0\.deps\pub-cache`
- npm cache: `E:\zheten2.0\.deps\npm-cache`
