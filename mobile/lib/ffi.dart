/// KiteFile Rust 引擎 FFI 绑定（Android 嵌入模式）
///
/// 打包在 APK 中的 libkitefile.so（jniLibs/arm64-v8a/），
/// 启动时调用 kitefile_init 在 App 进程内拉起完整 daemon：
/// - mDNS 设备发现（失败自动降级离线模式）
/// - TCP 传输引擎（监听 7879）
/// - HTTP/WebSocket 网关（监听 127.0.0.1:7878）
///
/// 之后 Dart 侧统一走 HTTP/WS 调用 127.0.0.1:7878（与桌面端同构）。
///
/// 字符串分配直接绑 libc malloc/free（不依赖 package:ffi）。

library;

import 'dart:convert' show utf8;
import 'dart:ffi';
import 'dart:io' show Platform;

import 'package:flutter/services.dart';

// Rust: i32 kitefile_init(*const c_char, *const c_char)
typedef _FtcoreInitNative = Int32 Function(
  Pointer<Uint8> deviceName,
  Pointer<Uint8> receiveDir,
);
typedef _FtcoreInitDart = int Function(
  Pointer<Uint8> deviceName,
  Pointer<Uint8> receiveDir,
);

// Rust: void kitefile_shutdown()
typedef _FtcoreShutdownNative = Void Function();
typedef _FtcoreShutdownDart = void Function();

// libc malloc / free
typedef _MallocNative = Pointer<Void> Function(IntPtr size);
typedef _MallocDart = Pointer<Void> Function(int size);
typedef _FreeNative = Void Function(Pointer<Void> ptr);
typedef _FreeDart = void Function(Pointer<Void> ptr);

final _MallocDart _malloc = DynamicLibrary.process()
    .lookupFunction<_MallocNative, _MallocDart>('malloc');
final _FreeDart _free = DynamicLibrary.process()
    .lookupFunction<_FreeNative, _FreeDart>('free');

const MethodChannel _nativeChannel = MethodChannel('kitefile/native');

/// Android：应用专属外部存储目录（无需存储权限）
Future<String?> _externalFilesDir() async {
  try {
    return await _nativeChannel.invokeMethod<String>('getExternalFilesDir');
  } on PlatformException {
    return null;
  } on MissingPluginException {
    return null;
  }
}

/// Android：设备型号（如 "Xiaomi 13"），作 daemon 默认设备名。
/// 不传时 Rust 侧回退 USERNAME 环境变量——Android 上不存在，
/// 默认名会变成 "device-xxxx" 这种无信息量的名字。
Future<String?> _deviceModel() async {
  try {
    return await _nativeChannel.invokeMethod<String>('getDeviceModel');
  } on PlatformException {
    return null;
  } on MissingPluginException {
    return null;
  }
}

/// String → NUL 结尾 UTF-8 C 字符串（调用方负责 _free）
Pointer<Uint8> _toNativeUtf8(String s) {
  final units = utf8.encode(s);
  final ptr = _malloc(units.length + 1).cast<Uint8>();
  final view = ptr.asTypedList(units.length + 1);
  view.setAll(0, units);
  view[units.length] = 0;
  return ptr;
}

/// 初始化 Rust daemon（App 进程内）。
///
/// [deviceName]：设备显示名（null 用默认）。
/// 返回 true 表示 daemon 已在本进程内运行。
Future<bool> initFtcoreDaemon({String? deviceName}) async {
  if (!Platform.isAndroid) return false; // 仅 Android 打包了 libkitefile.so

  final lib = DynamicLibrary.open('libkitefile.so');
  final init = lib
      .lookupFunction<_FtcoreInitNative, _FtcoreInitDart>('kitefile_init');

  // 接收目录：应用专属外部存储（无权限问题，文件管理器可见）
  String? receiveDir;
  final dir = await _externalFilesDir();
  if (dir != null) receiveDir = '$dir/kitefile';

  // 默认设备名用机型（调用方未显式指定时）
  deviceName ??= await _deviceModel();
  final namePtr = deviceName == null ? nullptr : _toNativeUtf8(deviceName);
  final dirPtr = receiveDir == null ? nullptr : _toNativeUtf8(receiveDir);
  try {
    return init(namePtr, dirPtr) == 0;
  } finally {
    if (namePtr != nullptr) _free(namePtr.cast());
    if (dirPtr != nullptr) _free(dirPtr.cast());
  }
}

/// 关闭进程内 Rust daemon（A3.7 对称生命周期）。
///
/// - 中止 gateway / 接收 / mDNS 任务并释放端口；
/// - 之后可以再次 [`initFtcoreDaemon`] 完整重建（旧实现 shutdown 后无法二 init）。
/// App 退出（detached）时调用；幂等，未初始化时是 no-op。
Future<void> shutdownFtcoreDaemon() async {
  if (!Platform.isAndroid) return;
  try {
    final lib = DynamicLibrary.open('libkitefile.so');
    final shutdown = lib.lookupFunction<_FtcoreShutdownNative, _FtcoreShutdownDart>(
      'kitefile_shutdown',
    );
    shutdown();
  } catch (e) {
    // ignore: avoid_print
    print('[kitefile] embedded daemon shutdown failed: $e');
  }
}
