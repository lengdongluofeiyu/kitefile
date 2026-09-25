import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart' show rootBundle;
import 'package:kitefile_shared/kitefile_shared.dart';
import 'package:local_notifier/local_notifier.dart';
import 'package:tray_manager/tray_manager.dart';
import 'package:window_manager/window_manager.dart';

// 双端共享业务层（工作流 C）：模型 / §3.5 文案 / 攒批决策只写一份。
// 再导出一次，让同包测试与代码 `import 'main.dart'` 也能拿到这些类型。
export 'package:kitefile_shared/kitefile_shared.dart';

/// KiteFile 桌面端
///
/// 架构：Flutter UI（dart）→ HTTP 调用本机 Rust 守护进程（127.0.0.1:7878）
/// Rust 守护进程在桌面端启动时由 main() 自动拉起（生产环境）；
/// 用户也可手动在 `core/` 目录运行 `cargo run --bin kitefile-cli daemon`。
///
/// 跨平台策略：
/// - Windows / macOS：window_manager 控制窗口
/// - iOS（预留）：使用同 UI 但隐藏窗口控制
/// - Android（移动端走 mobile 项目）：纯 UI

/// daemon 实际监听的端口，由 `DaemonManager` 探测后写入。
/// 未探测到时先用首选，保证 UI 不会因端口未定而崩。
int daemonPort = 7878;

String get kDaemonHttp => 'http://127.0.0.1:$daemonPort';
String get kDaemonWs => 'ws://127.0.0.1:$daemonPort/ws/progress';

/// 全局守护进程管理器（单例）
final DaemonManager daemonManager = DaemonManager();

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  if (Platform.isWindows || Platform.isMacOS || Platform.isLinux) {
    await windowManager.ensureInitialized();
    // 系统通知（Windows toast）：窗口隐藏时来文件提醒
    try {
      await localNotifier.setup(
        appName: 'KiteFile',
        shortcutPolicy: ShortcutPolicy.requireCreate,
      );
    } catch (e) {
      debugPrint('[LocalNotifier] setup failed: $e');
    }
    WindowOptions windowOptions = const WindowOptions(
      size: Size(900, 700),
      minimumSize: Size(500, 400),
      title: 'KiteFile',
    );
    windowManager.waitUntilReadyToShow(windowOptions, () async {
      // 不拦截的话点 × 窗口直接没了，onWindowClose 里的「进托盘/完全退出」弹窗根本弹不出来
      await windowManager.setPreventClose(true);
      await windowManager.show();
      await windowManager.focus();
    });
  }

  // 启动时自动拉起 Rust 守护进程（非阻塞：UI 立即显示，daemon 在后台 ready）
  daemonManager.ensureRunning();
  // 托盘图标常驻（隐藏图标区），窗口关掉后仍可唤起 / 完全退出
  trayService.init();

  runApp(const KiteFileApp());
}

/// 系统托盘：窗口隐藏后仍驻留，可唤起主界面或完全退出（含守护进程）
class TrayService with TrayListener {
  bool _inited = false;

  Future<void> init() async {
    if (!Platform.isWindows && !Platform.isMacOS && !Platform.isLinux) return;
    if (_inited) return;
    _inited = true;
    try {
      final iconPath = await _extractIcon();
      await trayManager.setIcon(iconPath);
      await trayManager.setToolTip('KiteFile - 局域网文件传输');
      await trayManager.setContextMenu(Menu(items: [
        MenuItem(key: 'show_window', label: '显示主界面'),
        MenuItem.separator(),
        MenuItem(key: 'exit_app', label: '完全退出'),
      ]));
      trayManager.addListener(this);
    } catch (e) {
      debugPrint('[TrayService] init failed: $e');
    }
  }

  /// tray_manager 要磁盘上的图标文件路径；从 assets 解一份到临时目录
  Future<String> _extractIcon() async {
    final data = await rootBundle.load('assets/tray_icon.ico');
    final f = File(
        '${Directory.systemTemp.path}${Platform.pathSeparator}kitefile_tray_icon.ico');
    await f.writeAsBytes(data.buffer.asUint8List(), flush: true);
    return f.path;
  }

  @override
  void onTrayIconMouseDown() {
    _showWindow();
  }

  @override
  void onTrayIconRightMouseDown() {
    trayManager.popUpContextMenu();
  }

  @override
  void onTrayMenuItemClick(MenuItem menuItem) {
    switch (menuItem.key) {
      case 'show_window':
        _showWindow();
        break;
      case 'exit_app':
        _exitCompletely();
        break;
    }
  }

  Future<void> _showWindow() async {
    await bringAppToForeground();
  }

  Future<void> _exitCompletely() async {
    await daemonManager.stop();
    await windowManager.destroy();
    exit(0);
  }

  Future<void> destroy() async {
    trayManager.removeListener(this);
  }
}

final TrayService trayService = TrayService();

/// 把主窗口拉到前台（点通知 / 点托盘时用）。
///
/// Windows 有前台锁：单独 `show()+focus()` 经常只是「显示了但没置顶」。
/// 先短暂 `alwaysOnTop` 骗过前台限制，再取消置顶。
Future<void> bringAppToForeground() async {
  try {
    if (await windowManager.isMinimized()) {
      await windowManager.restore();
    }
    await windowManager.setSkipTaskbar(false);
    await windowManager.show();
    await windowManager.setAlwaysOnTop(true);
    await windowManager.focus();
    await Future.delayed(const Duration(milliseconds: 180));
    await windowManager.setAlwaysOnTop(false);
    await windowManager.focus();
  } catch (e) {
    debugPrint('[Window] bring to foreground failed: $e');
  }
}

/// 守护进程管理器
///
/// 职责：
/// - 启动时逐个探测 `kGatewayPortCandidates` 上 /api/whoami 是否响应
///   - 已响应：说明已有 daemon（用户手动启过 / 上次未退出），直接复用
///   - 未响应：spawn 一个 kitefile-cli.exe daemon 子进程
/// - 子进程用 detached 模式：UI 崩溃不会拖死 daemon，正在传的文件不会断
/// - 「完全退出」时才 kill 子进程；「最小化到托盘」只藏窗口，daemon 继续跑
class DaemonManager {
  Process? _process;
  bool _spawned = false;
  bool _isReady = false;

  /// 是否由本进程启动了 daemon（用于判断关闭时是否需要 kill）
  bool get spawnedByUs => _spawned;

  /// daemon 是否已就绪
  bool get isReady => _isReady;

  /// 确保 daemon 在运行。返回 true 表示已就绪（可能本次启动需要等待几秒）。
  Future<void> ensureRunning() async {
    // 1. 先检查是否已经有 daemon 在跑
    if (await _isAlive()) {
      _isReady = true;
      return;
    }

    // 2. 没在跑 → 找 kitefile-cli.exe 并 spawn
    final exePath = await _findDaemonExe();
    if (exePath == null) {
      debugPrint('[DaemonManager] kitefile-cli.exe 未找到，请将 dist/windows/ 一起分发');
      return;
    }

    try {
      _process = await Process.start(
        exePath,
        const ['daemon'],
        mode: ProcessStartMode.detached,
        // 守护进程有自己的日志输出，UI 端不接管 stdout/stderr
        runInShell: false,
      );
      _spawned = true;
      _process!.stdout.listen((_) {}); // 消费 stdout 防止管道阻塞
      _process!.stderr.listen((_) {});
      debugPrint('[DaemonManager] spawned daemon PID=${_process!.pid} from $exePath');
    } catch (e) {
      debugPrint('[DaemonManager] spawn failed: $e');
      return;
    }

    // 3. 轮询等待 daemon 的 HTTP 接口就绪（最多 8 秒）
    for (var i = 0; i < 80; i++) {
      await Future.delayed(const Duration(milliseconds: 100));
      if (await _isAlive()) {
        _isReady = true;
        debugPrint('[DaemonManager] daemon ready after ${(i + 1) * 100}ms');
        return;
      }
    }
    debugPrint('[DaemonManager] daemon failed to become ready within 8s');
  }

  /// 关闭 daemon（应用退出时调用）
  Future<void> stop() async {
    if (_process != null && _spawned) {
      try {
        _process!.kill(ProcessSignal.sigterm);
        debugPrint('[DaemonManager] killed daemon PID=${_process!.pid}');
      } catch (e) {
        debugPrint('[DaemonManager] kill failed: $e');
      }
      _process = null;
      _spawned = false;
      _isReady = false;
    }
  }

  /// 检查 daemon 是否响应。
  ///
  /// 逐个试候选端口：daemon 可能因为默认端口被系统占用而退避到备选，
  /// 所以不能只试 7878。命中后把实际端口写进全局 `daemonPort`，
  /// 后续所有请求（含 WebSocket）都跟着走这个端口。
  Future<bool> _isAlive() async {
    for (final p in kGatewayPortCandidates) {
      try {
        final r = await httpGet('http://127.0.0.1:$p/api/whoami')
            .timeout(const Duration(milliseconds: 500));
        if (r.isNotEmpty) {
          daemonPort = p;
          return true;
        }
      } catch (_) {
        // 该端口没响应，试下一个
      }
    }
    return false;
  }

  /// 查找 kitefile-cli.exe 路径
  /// 1. 与本程序同目录（dist/windows/ 部署模式）
  /// 2. 开发模式：相对路径到 core/target/release/
  /// 3. 开发模式：core/target/debug/
  Future<String?> _findDaemonExe() async {
    final exeDir = File(Platform.resolvedExecutable).parent;
    final candidates = <String>[
      // 1. 与 desktop exe 同目录（生产部署：dist/windows/）
      '${exeDir.path}${Platform.pathSeparator}kitefile-cli.exe',
      // 2. 开发模式：desktop/build/... 上溯 5 级到 filetransfer/core/target/release/
      //    （不依赖绝对路径，仓库放任意盘符都能找到）
      for (var d = exeDir; d.path.length > 3; d = d.parent)
        '${d.path}${Platform.pathSeparator}core${Platform.pathSeparator}target${Platform.pathSeparator}release${Platform.pathSeparator}kitefile-cli.exe',
      for (var d = exeDir; d.path.length > 3; d = d.parent)
        '${d.path}${Platform.pathSeparator}core${Platform.pathSeparator}target${Platform.pathSeparator}debug${Platform.pathSeparator}kitefile-cli.exe',
      // 3. 同目录上一级（备选）
      '${exeDir.parent.path}${Platform.pathSeparator}kitefile-cli.exe',
    ];
    for (final p in candidates) {
      final f = File(p);
      if (await f.exists()) {
        return p;
      }
    }
    return null;
  }
}

class KiteFileApp extends StatelessWidget {
  const KiteFileApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'KiteFile',
      theme: ThemeData(
        useMaterial3: true,
        brightness: Brightness.dark,
        colorSchemeSeed: const Color(0xFF3B82F6),
      ),
      home: const HomePage(),
    );
  }
}

// ============ 数据模型 ============
// 模型与 JSON 安全解析已移至共享包 kitefile_shared（工作流 C，本文件顶部 export）。

// ============ 主页 ============

class HomePage extends StatefulWidget {
  const HomePage({super.key});

  @override
  State<HomePage> createState() => _HomePageState();
}

enum _CloseAction { tray, fullExit }

class _HomePageState extends State<HomePage> with WindowListener {
  WhoAmI? _me;
  List<Device> _devices = [];
  final Map<String, TransferProgress> _progress = {};
  final Map<String, IncomingEntry> _pendingIncoming = {};
  /// 攒批—决策—迟到沿用状态机（工作流 C：共享实现，双端仅此一份）
  late final BatchDecider _batchDecider;
  /// 已经为该 fileId 弹过完成提示，避免重复弹窗
  final Set<String> _notifiedComplete = {};
  /// 已通知过「传输中断」的 file_id（一条传输只弹一次）
  final Set<String> _notifiedInterrupted = {};
  WebSocket? _ws;
  Timer? _refreshTimer;
  bool _daemonOnline = false;
  Timer? _startupPollTimer;

  @override
  void initState() {
    super.initState();
    windowManager.addListener(this);
    _batchDecider = BatchDecider(
      onShowSingle: (entry) {
        if (mounted) _showIncomingDialog(entry);
      },
      onShowBatch: (batchId, entries) {
        if (mounted) _showBatchDialog(batchId, entries);
      },
      onDecideSingle: (incomingId, accept, quiet) {
        if (accept) {
          _acceptIncoming(incomingId, quiet: quiet);
        } else {
          _rejectIncoming(incomingId);
        }
      },
    );
    _initDaemon();
  }

  @override
  void dispose() {
    _batchDecider.dispose();
    _ws?.close();
    _refreshTimer?.cancel();
    _startupPollTimer?.cancel();
    windowManager.removeListener(this);
    super.dispose();
  }

  /// 窗口关闭：默认最小化到托盘（守护进程后台接收）；也可完全退出。
  ///
  /// 必须配合 `setPreventClose(true)`：否则系统直接销毁窗口，
  /// 这里的弹窗/隐藏逻辑没有执行机会。
  @override
  void onWindowClose() async {
    final action = await _showExitDialog();
    switch (action) {
      case _CloseAction.tray:
        await windowManager.hide();
        _notifyTrayResident();
        break;
      case _CloseAction.fullExit:
        await daemonManager.stop();
        await trayService.destroy();
        // destroy() 会绕过 preventClose，真正关掉窗口
        await windowManager.destroy();
        exit(0);
    }
  }

  Future<_CloseAction> _showExitDialog() async {
    if (!mounted) return _CloseAction.fullExit;
    final result = await showDialog<_CloseAction>(
      context: context,
      barrierDismissible: false,
      builder: (_) => AlertDialog(
        title: const Text('关闭 KiteFile'),
        content: const Text(
          '· 选「最小化到托盘」：窗口隐藏、守护进程后台接收；\n'
          '  其他设备仍能发现本机并传文件，来文件会弹系统通知。\n\n'
          '· 选「完全退出」：守护进程一并退出，本机不再被其他设备发现。',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, _CloseAction.fullExit),
            child: const Text('完全退出'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(context, _CloseAction.tray),
            child: const Text('最小化到托盘'),
          ),
        ],
      ),
    );
    return result ?? _CloseAction.tray;
  }

  /// 首次驻留托盘时用系统通知提示，避免用户以为进程没了
  void _notifyTrayResident() {
    try {
      final n = LocalNotification(
        title: 'KiteFile 仍在后台运行',
        body: '已最小化到系统托盘（隐藏图标区）。点击通知或托盘图标可打开主界面。',
      );
      n.onClick = bringAppToForeground;
      n.show();
    } catch (e) {
      debugPrint('[Notify] tray resident failed: $e');
    }
  }

  /// 窗口隐藏/最小化时，来传输请求 → 系统通知
  Future<void> _notifyIncomingIfNeeded(IncomingEntry entry) async {
    try {
      final visible = await windowManager.isVisible();
      final minimized = await windowManager.isMinimized();
      if (visible && !minimized) return;
      final n = LocalNotification(
        title: '${entry.fromName} 想发送文件',
        body: '${entry.fileName}（${formatBytes(entry.fileSize)}）\n点击打开 KiteFile 接收',
      );
      n.onClick = bringAppToForeground;
      n.show();
    } catch (e) {
      debugPrint('[Notify] incoming failed: $e');
    }
  }

  /// 接收完成且窗口不可见时也通知一下
  Future<void> _notifyReceivedIfNeeded(TransferProgress p) async {
    try {
      final visible = await windowManager.isVisible();
      final minimized = await windowManager.isMinimized();
      if (visible && !minimized) return;
      final n = LocalNotification(
        title: '已接收 ${p.fileName}',
        body: '点击打开 KiteFile 查看',
      );
      n.onClick = bringAppToForeground;
      n.show();
    } catch (e) {
      debugPrint('[Notify] received failed: $e');
    }
  }

  /// 传输中断且窗口不可见时通知（§3.5）：点击拉起主窗口去「继续传输」
  Future<void> _notifyInterruptedIfNeeded(TransferProgress p) async {
    try {
      final visible = await windowManager.isVisible();
      final minimized = await windowManager.isMinimized();
      if (visible && !minimized) return;
      final pct = p.fileSize > 0 ? (p.bytesTransferred / p.fileSize * 100).round() : 0;
      final n = LocalNotification(
        title: '传输中断',
        body: '「${p.fileName}」可继续，已完成 $pct%\n点击 KiteFile 继续传输',
      );
      n.onClick = bringAppToForeground;
      n.show();
    } catch (e) {
      debugPrint('[Notify] interrupted failed: $e');
    }
  }

  Future<void> _initDaemon() async {
    // 等待 daemonManager 拉起的 daemon 就绪（最多再轮询 10s）
    _startupPollTimer = Timer.periodic(const Duration(seconds: 1), (t) async {
      if (t.tick > 10) {
        t.cancel();
        return;
      }
      await _fetchWhoAmI();
      if (_daemonOnline) {
        t.cancel();
        _refreshDevices();
        // 周期刷新同时复核 whoami：daemon 被杀后 3s 内徽标掉线（A3.6）
        _refreshTimer = Timer.periodic(const Duration(seconds: 3), (_) {
          _refreshDevices();
          _fetchWhoAmI();
        });
        _connectWs();
      }
    });
    // 同时立即试一次（万一 daemon 已经 ready）
    await _fetchWhoAmI();
    if (_daemonOnline) {
      _startupPollTimer?.cancel();
      _refreshDevices();
      _refreshTimer = Timer.periodic(const Duration(seconds: 3), (_) {
        _refreshDevices();
        _fetchWhoAmI();
      });
      _connectWs();
    }
  }

  Future<void> _fetchWhoAmI() async {
    try {
      final r = await httpGet('$kDaemonHttp/api/whoami');
      final me = WhoAmI.fromJson(jsonDecode(r) as Map<String, dynamic>);
      setState(() {
        _me = me;
        _daemonOnline = true;
      });
    } catch (e) {
      // 在线徽标唯一真值（A3.6）：轮询失败必须掉线，禁止吞掉后仍显示已连接
      debugPrint('[kitefile] whoami poll failed: $e');
      setState(() => _daemonOnline = false);
    }
  }

  Future<void> _refreshDevices() async {
    try {
      final r = await httpGet('$kDaemonHttp/api/devices');
      final list = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
      setState(() {
        _devices = list.map(Device.fromJson).toList();
      });
    } catch (e) {
      // 设备列表是本机 daemon 接口：失败留日志（徽标由 whoami 轮询定真值）
      debugPrint('[kitefile] devices refresh failed: $e');
    }
  }

  Future<void> _connectWs() async {
    try {
      final ws = await WebSocket.connect(kDaemonWs);
      _ws = ws;
      ws.listen(
        (data) {
          if (data is String) {
            try {
              _handleWsMessage(jsonDecode(data) as Map<String, dynamic>);
            } catch (e) {
              debugPrint('[kitefile] ws message parse failed: $e');
            }
          }
        },
        onDone: () {
          // daemon 退出 / WS 断开：立刻掉线真值 + 复核 whoami + 重连
          debugPrint('[kitefile] ws closed');
          if (mounted) setState(() => _daemonOnline = false);
          _ws = null;
          _fetchWhoAmI();
          Future.delayed(const Duration(seconds: 5), () {
            if (mounted) _connectWs();
          });
        },
        onError: (Object e) {
          debugPrint('[kitefile] ws error: $e');
          if (mounted) setState(() => _daemonOnline = false);
        },
      );
    } catch (e) {
      debugPrint('[kitefile] ws connect failed: $e');
      if (mounted) setState(() => _daemonOnline = false);
      // mounted 保护：页面已销毁就不再重连（否则定时器泄漏/幽灵连接）
      Future.delayed(const Duration(seconds: 5), () {
        if (mounted) _connectWs();
      });
    }
  }

  /// WebSocket 消息分发：progress / incoming / incoming_resolved
  void _handleWsMessage(Map<String, dynamic> j) {
    final type = j['event_type'] as String?;
    switch (type) {
      case 'progress':
        final p = TransferProgress.fromJson(j);
        setState(() => _progress[p.fileId] = p);
        // 接收方收到完成事件 → 弹窗提示
        if (p.incoming &&
            p.status == TransferStatus.completed &&
            !_notifiedComplete.contains(p.fileId)) {
          _notifiedComplete.add(p.fileId);
          _showReceivedDialog(p);
          _notifyReceivedIfNeeded(p);
        }
        // 传输中断（§3.5 系统通知）：窗口隐藏时提示可继续
        if (p.status == TransferStatus.interrupted &&
            !_notifiedInterrupted.contains(p.fileId)) {
          _notifiedInterrupted.add(p.fileId);
          _notifyInterruptedIfNeeded(p);
        }
        break;
      case 'incoming':
        final entry = IncomingEntry.fromJson(j);
        _onIncoming(entry);
        _notifyIncomingIfNeeded(entry);
        break;
      case 'incoming_resolved':
        final id = j['incoming_id'] as String;
        setState(() => _pendingIncoming.remove(id));
        break;
      default:
        break;
    }
  }

  /// 收到一个 incoming 请求：去重后交给共享攒批状态机（工作流 C）。
  void _onIncoming(IncomingEntry entry) {
    final id = entry.incomingId;
    if (_pendingIncoming.containsKey(id)) return; // 同一条只处理一次
    _pendingIncoming[id] = entry;
    _batchDecider.handle(entry);
  }

  /// 批量接收弹窗：一次确认整批，不用每个文件点一遍。
  /// entries 已由 BatchDecider 按 batchIndex 排序。
  void _showBatchDialog(String batchId, List<IncomingEntry> entries) {
    if (entries.isEmpty || !mounted) return;
    final totalSize = entries.fold<int>(0, (s, e) => s + e.fileSize);
    var remaining = kDecisionTimeoutSecs;
    Timer? ticker;

    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (_) => AlertDialog(
        title: Row(
          children: [
            const Icon(Icons.download_rounded, size: 24),
            const SizedBox(width: 8),
            Text('收到 ${entries.length} 个文件'),
          ],
        ),
        content: StatefulBuilder(
          builder: (ctx, setDialogState) {
            ticker ??= Timer.periodic(const Duration(seconds: 1), (t) {
              remaining -= 1;
              if (remaining <= 0) {
                t.cancel();
                if (ctx.mounted) Navigator.pop(ctx);
                _onIncomingTimeout(batchId);
                return;
              }
              if (ctx.mounted) setDialogState(() {});
            });
            return SizedBox(
              width: 420,
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  _kv('来自', entries.first.fromName),
                  _kv('合计', formatBytes(totalSize)),
                  const SizedBox(height: 8),
                  ...entries.map(
                    (e) => Padding(
                      padding: const EdgeInsets.symmetric(vertical: 2),
                      child: Row(
                        children: [
                          const Icon(Icons.insert_drive_file,
                              size: 16, color: Colors.grey),
                          const SizedBox(width: 6),
                          Expanded(
                            child: Text('${e.fileName}（${formatBytes(e.fileSize)}）',
                                overflow: TextOverflow.ellipsis,
                                style: const TextStyle(fontSize: 13)),
                          ),
                        ],
                      ),
                    ),
                  ),
                  const SizedBox(height: 10),
                  Text(
                    '$remaining 秒内未接受将自动拒绝',
                    style: TextStyle(
                      fontSize: 12,
                      color: remaining <= 10 ? Colors.orange : Colors.grey,
                    ),
                  ),
                ],
              ),
            );
          },
        ),
        actions: [
          TextButton(
            onPressed: () {
              ticker?.cancel();
              Navigator.pop(context);
              _rejectBatch(batchId);
            },
            child: const Text('全部拒绝'),
          ),
          FilledButton.icon(
            icon: const Icon(Icons.download),
            label: Text('全部接受 (${entries.length})'),
            onPressed: () {
              ticker?.cancel();
              Navigator.pop(context);
              _acceptBatch(batchId);
            },
          ),
        ],
      ),
    ).whenComplete(() => ticker?.cancel());
  }

  Future<void> _acceptBatch(String batchId) async {
    // 先记下决定：迟到到达的同批条目会据此自动接受，不再弹窗（共享状态机）
    _batchDecider.recordDecision(batchId, true);
    try {
      await httpPost('$kDaemonHttp/api/incoming/batch-decide',
          body: jsonEncode({'batch_id': batchId, 'accept': true}));
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('已接受整批，等待对方开始传输…')),
      );
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('接受失败: $e')),
      );
    }
  }

  Future<void> _rejectBatch(String batchId) async {
    _batchDecider.recordDecision(batchId, false);
    try {
      await httpPost('$kDaemonHttp/api/incoming/batch-decide',
          body: jsonEncode({'batch_id': batchId, 'accept': false}));
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('拒绝失败: $e')),
      );
    }
  }

  /// 接收文件弹窗：显示来源、文件名、大小，让用户选择接受/拒绝
  /// 60 秒内未接受自动关闭并提示超时（与 daemon 侧自动拒绝对齐）
  void _showIncomingDialog(IncomingEntry entry) {
    var remaining = kDecisionTimeoutSecs;
    Timer? ticker;
    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (_) => AlertDialog(
        title: const Row(
          children: [
            Icon(Icons.download_rounded, size: 24),
            SizedBox(width: 8),
            Text('收到文件传输请求'),
          ],
        ),
        content: StatefulBuilder(
          builder: (ctx, setDialogState) {
            ticker ??= Timer.periodic(const Duration(seconds: 1), (t) {
              remaining -= 1;
              if (remaining <= 0) {
                t.cancel();
                if (ctx.mounted) Navigator.pop(ctx);
                _onIncomingTimeout(entry.incomingId);
                return;
              }
              if (ctx.mounted) setDialogState(() {});
            });
            return Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                _kv('来自', entry.fromName),
                _kv('地址', '${entry.fromIp}:${entry.fromTransferPort}'),
                const SizedBox(height: 8),
                _kv('文件名', entry.fileName),
                _kv('大小', formatBytes(entry.fileSize)),
                if (entry.sha256 != null && entry.sha256!.isNotEmpty)
                  _kv('SHA256', '${entry.sha256!.substring(0, 12)}…'),
                const SizedBox(height: 10),
                Text(
                  '$remaining 秒内未接受将自动拒绝',
                  style: TextStyle(
                    fontSize: 12,
                    color: remaining <= 10 ? Colors.orange : Colors.grey,
                  ),
                ),
              ],
            );
          },
        ),
        actions: [
          TextButton(
            onPressed: () {
              ticker?.cancel();
              Navigator.pop(context);
              _rejectIncoming(entry.incomingId);
            },
            child: const Text('拒绝'),
          ),
          FilledButton.icon(
            icon: const Icon(Icons.download),
            label: const Text('接受'),
            onPressed: () {
              ticker?.cancel();
              Navigator.pop(context);
              _acceptIncoming(entry.incomingId);
            },
          ),
        ],
      ),
    ).whenComplete(() => ticker?.cancel());
  }

  void _onIncomingTimeout(String incomingId) {
    setState(() => _pendingIncoming.remove(incomingId));
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text('传输请求已超时（60 秒未接受）')),
    );
  }

  Future<void> _acceptIncoming(String id, {bool quiet = false}) async {
    try {
      await httpPost('$kDaemonHttp/api/incoming/$id/accept', body: '');
      if (quiet || !mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('已接受，等待对方开始传输…')),
      );
    } catch (e) {
      if (quiet || !mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('接受失败: $e')),
      );
    }
  }

  Future<void> _rejectIncoming(String id) async {
    try {
      await httpPost('$kDaemonHttp/api/incoming/$id/reject', body: '');
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('拒绝失败: $e')),
      );
    }
  }

  /// 接收完成弹窗
  void _showReceivedDialog(TransferProgress p) {
    showDialog<void>(
      context: context,
      builder: (_) => AlertDialog(
        title: const Row(
          children: [
            Icon(Icons.check_circle, color: Colors.green, size: 24),
            SizedBox(width: 8),
            Text('文件接收完成'),
          ],
        ),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            _kv('文件', p.fileName),
            _kv('大小', formatBytes(p.fileSize)),
            if (p.filePath != null) ...[
              const SizedBox(height: 8),
              const Text('保存位置: ',
                  style: TextStyle(color: Colors.grey, fontSize: 13)),
              SelectableText(p.filePath!,
                  style: const TextStyle(fontSize: 13)),
            ],
          ],
        ),
        actions: [
          if (p.filePath != null)
            TextButton.icon(
              icon: const Icon(Icons.folder_open, size: 16),
              label: const Text('所在文件夹'),
              onPressed: () => revealInFileManager(p.filePath!),
            ),
          if (p.filePath != null)
            FilledButton.icon(
              icon: const Icon(Icons.open_in_new, size: 16),
              label: const Text('打开文件'),
              onPressed: () => openFile(p.filePath!),
            ),
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('关闭'),
          ),
        ],
      ),
    );
  }

  /// 设置弹窗：查看/修改设备名 + 接收目录
  Future<void> _showSettingsDialog() async {
    String? currentDir;
    String? currentName;
    try {
      final r = await httpGet('$kDaemonHttp/api/config');
      final j = jsonDecode(r) as Map<String, dynamic>;
      currentDir = j['receive_dir'] as String?;
      currentName = j['device_name'] as String?;
    } catch (_) {}
    if (!mounted) return;

    final nameController = TextEditingController(text: currentName ?? '');

    await showDialog<void>(
      context: context,
      builder: (dialogCtx) => StatefulBuilder(
        builder: (ctx, setDialogState) => AlertDialog(
          title: const Text('设置'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text('设备名称', style: TextStyle(fontWeight: FontWeight.bold)),
                const SizedBox(height: 6),
                Row(
                  children: [
                    Expanded(
                      child: TextField(
                        controller: nameController,
                        decoration: const InputDecoration(
                          border: OutlineInputBorder(),
                          isDense: true,
                          hintText: '本机在设备列表中显示的名字',
                        ),
                      ),
                    ),
                    const SizedBox(width: 8),
                    FilledButton(
                      onPressed: () async {
                        final name = nameController.text.trim();
                        if (name.isEmpty) return;
                        try {
                          await httpPost(
                            '$kDaemonHttp/api/config/device-name',
                            body: jsonEncode({'device_name': name}),
                          );
                          setDialogState(() => currentName = name);
                          // 重新拉 whoami：_me 只在启动时取过一次，
                          // 不刷新的话「本机信息」会一直显示旧名字。
                          await _fetchWhoAmI();
                          if (ctx.mounted) {
                            ScaffoldMessenger.of(ctx).showSnackBar(
                              SnackBar(content: Text('设备名称已更新：$name')),
                            );
                          }
                        } catch (e) {
                          if (ctx.mounted) {
                            ScaffoldMessenger.of(ctx).showSnackBar(
                              SnackBar(content: Text('设置失败: $e')),
                            );
                          }
                        }
                      },
                      child: const Text('保存'),
                    ),
                  ],
                ),
                const SizedBox(height: 6),
                Text(
                  '重启后名称保留；对端设备列表会立即显示新名字。',
                  style: const TextStyle(fontSize: 12, color: Colors.grey),
                ),
                const Divider(height: 24),
                const Text('接收文件保存位置',
                    style: TextStyle(fontWeight: FontWeight.bold)),
                const SizedBox(height: 6),
                SelectableText(
                  currentDir ?? '（守护进程未运行，无法读取当前设置）',
                  style: TextStyle(
                    fontSize: 13,
                    color: currentDir == null ? Colors.orange : Colors.grey,
                  ),
                ),
                const SizedBox(height: 12),
                FilledButton.icon(
                  icon: const Icon(Icons.folder_open),
                  label: const Text('更改保存位置'),
                  onPressed: () async {
                    // Windows 原生目录选择对话框
                    final dir = await FilePicker.platform.getDirectoryPath(
                      dialogTitle: '选择接收文件的保存目录',
                      lockParentWindow: true,
                    );
                    if (dir == null) return;
                    try {
                      final r = await httpPost(
                        '$kDaemonHttp/api/config/receive-dir',
                        body: jsonEncode({'receive_dir': dir}),
                      );
                      final nd =
                          (jsonDecode(r) as Map<String, dynamic>)['receive_dir'] as String?;
                      setDialogState(() => currentDir = nd ?? dir);
                      if (ctx.mounted) {
                        ScaffoldMessenger.of(ctx).showSnackBar(
                          SnackBar(content: Text('保存位置已更新：$dir')),
                        );
                      }
                    } catch (e) {
                      if (ctx.mounted) {
                        ScaffoldMessenger.of(ctx).showSnackBar(
                          SnackBar(content: Text('设置失败: $e')),
                        );
                      }
                    }
                  },
                ),
                const SizedBox(height: 8),
                Text(
                  '默认：系统 Downloads/kitefile/。更改后，之后接收的文件将保存到新位置。',
                  style: const TextStyle(fontSize: 12, color: Colors.grey),
                ),
              ],
            ),
          ),
          actions: [
            FilledButton(
              onPressed: () => Navigator.pop(ctx),
              child: const Text('关闭'),
            ),
          ],
        ),
      ),
    );
    nameController.dispose();
  }

  Future<void> _sendFile(Device target, String path,
      {String? fileName, SendBatch? batch}) async {
    try {
      final body = <String, dynamic>{
        'target_ip': target.ip,
        'target_port': target.transferPort,
        'target_gateway_port': target.gatewayPort,
        'file_path': path,
        // null-aware element：fileName 为 null 时整条 entry 被跳过，不写 null 进去
        'file_name': ?fileName,
        if (batch != null) ...{
          'batch_id': batch.batchId,
          'batch_index': batch.index,
          'batch_total': batch.total,
        },
      };
      final r = await httpPost('$kDaemonHttp/api/send', body: jsonEncode(body));
      final fileId = (jsonDecode(r) as Map<String, dynamic>)['file_id'] as String;
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('已发起传输: $fileId')),
      );
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('发起失败: $e')),
      );
    }
  }

  Future<void> _cancel(String fileId) async {
    // 404 = 传输已结束/不存在，静默忽略
    try {
      await httpPost('$kDaemonHttp/api/cancel/$fileId', body: '');
    } catch (e) {
      debugPrint('[kitefile] cancel failed: $e');
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('KiteFile'),
        actions: [
          IconButton(
            icon: const Icon(Icons.settings_outlined),
            tooltip: '设置',
            onPressed: _showSettingsDialog,
          ),
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 16),
            child: Center(
              child: Container(
                padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                decoration: BoxDecoration(
                  color: _daemonOnline
                      ? Colors.green.withValues(alpha: 0.2)
                      : Colors.orange.withValues(alpha: 0.2),
                  borderRadius: BorderRadius.circular(12),
                ),
                child: Text(
                  _daemonOnline ? '守护进程已连接' : '守护进程未运行',
                  style: TextStyle(
                    fontSize: 12,
                    color: _daemonOnline ? Colors.green : Colors.orange,
                  ),
                ),
              ),
            ),
          ),
        ],
      ),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          _meSection(),
          const SizedBox(height: 16),
          _devicesSection(),
          const SizedBox(height: 16),
          _transfersSection(),
        ],
      ),
    );
  }

  Widget _meSection() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('本机', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 8),
            if (_me == null)
              const Text('正在拉起守护进程… 若持续未就绪，请检查 kitefile-cli.exe 是否在程序目录。')
            else
              Wrap(
                spacing: 16,
                runSpacing: 8,
                children: [
                  _kv('名称', _me!.name),
                  _kv('平台', _me!.platform),
                  _kv('HTTP', ':${_me!.gatewayPort}'),
                  _kv('传输', ':${_me!.transferPort}'),
                ],
              ),
          ],
        ),
      ),
    );
  }

  Widget _devicesSection() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('设备列表 (${_devices.length})', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 8),
            if (_devices.isEmpty)
              const Padding(
                padding: EdgeInsets.symmetric(vertical: 8),
                child: Text('未发现设备，请确认对端已启动并处于同一局域网。'),
              )
            else
              Column(
                children: _devices.map((d) {
                  return ListTile(
                    title: Text(d.name),
                    subtitle: Text('${d.platform} · ${d.ip}:${d.transferPort}'),
                    trailing: const Icon(Icons.send),
                    onTap: () => _showSendDialog(d),
                  );
                }).toList(),
              ),
          ],
        ),
      ),
    );
  }

  Widget _transfersSection() {
    final items = _progress.values.toList()
      ..sort((a, b) => a.status == TransferStatus.inProgress ? -1 : 1);
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('传输进度 (${items.length})', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 8),
            if (items.isEmpty)
              const Padding(
                padding: EdgeInsets.symmetric(vertical: 8),
                child: Text('暂无传输任务'),
              )
            else
              Column(
                children: items.map(_transferTile).toList(),
              ),
          ],
        ),
      ),
    );
  }

  Widget _transferTile(TransferProgress p) {
    final pct = p.fileSize > 0 ? p.bytesTransferred / p.fileSize : 0.0;
    // 角标与副文案来自共享层（§3.5 唯一源，工作流 C）
    final statusText = statusBadgeLabel(p);
    final statusColor = statusBadgeColor(p);
    final subtitle = transferSubtitle(p);

    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(child: Text(p.fileName, overflow: TextOverflow.ellipsis)),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
                decoration: BoxDecoration(
                  color: statusColor.withValues(alpha: 0.2),
                  borderRadius: BorderRadius.circular(4),
                ),
                child: Text(statusText,
                    style: TextStyle(color: statusColor, fontSize: 12)),
              ),
              if (p.status == TransferStatus.inProgress)
                IconButton(
                  icon: const Icon(Icons.close, size: 16),
                  onPressed: () => _cancel(p.fileId),
                ),
            ],
          ),
          const SizedBox(height: 4),
          LinearProgressIndicator(value: pct),
          const SizedBox(height: 4),
          Text(
            subtitle,
            style: const TextStyle(fontSize: 12, color: Colors.grey),
          ),
          // 已中断（§3.5）：继续传输 / 取消
          if (p.status == TransferStatus.interrupted)
            Row(
              children: [
                TextButton.icon(
                  style: TextButton.styleFrom(
                    visualDensity: VisualDensity.compact,
                    padding: const EdgeInsets.symmetric(horizontal: 8),
                  ),
                  icon: const Icon(Icons.play_arrow, size: 14),
                  label: const Text('继续传输'),
                  onPressed: () => _resume(p.fileId),
                ),
                TextButton(
                  style: TextButton.styleFrom(
                    visualDensity: VisualDensity.compact,
                    padding: const EdgeInsets.symmetric(horizontal: 8),
                  ),
                  child: const Text('取消'),
                  onPressed: () => _cancel(p.fileId),
                ),
              ],
            ),
          // 接收完成的条目：显示保存路径 + 打开文件 / 所在文件夹
          if (p.status == TransferStatus.completed &&
              p.incoming &&
              p.filePath != null) ...[
            const SizedBox(height: 4),
            Text('已保存: ${p.filePath}',
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
                style: const TextStyle(fontSize: 11, color: Colors.grey)),
            Row(
              children: [
                TextButton.icon(
                  style: TextButton.styleFrom(
                    visualDensity: VisualDensity.compact,
                    padding: const EdgeInsets.symmetric(horizontal: 8),
                  ),
                  icon: const Icon(Icons.open_in_new, size: 14),
                  label: const Text('打开文件'),
                  onPressed: () => openFile(p.filePath!),
                ),
                TextButton.icon(
                  style: TextButton.styleFrom(
                    visualDensity: VisualDensity.compact,
                    padding: const EdgeInsets.symmetric(horizontal: 8),
                  ),
                  icon: const Icon(Icons.folder_open, size: 14),
                  label: const Text('所在文件夹'),
                  onPressed: () => revealInFileManager(p.filePath!),
                ),
              ],
            ),
          ],
        ],
      ),
    );
  }

  /// 「继续传输」：只重发未完成段（A3.2）。本机是发送方或接收方均由 daemon 路由。
  Future<void> _resume(String fileId) async {
    try {
      await httpPost('$kDaemonHttp/api/transfers/$fileId/resume', body: '{}');
    } catch (e) {
      // 不静默吞：至少留日志（A3.6 精神）
      debugPrint('[kitefile] resume failed: $e');
    }
  }

  Widget _kv(String k, String v) {
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        Text('$k: ', style: const TextStyle(color: Colors.grey, fontSize: 13)),
        Text(v, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 13)),
      ],
    );
  }

  /// 系统文件选择器选文件（多选）→ 确认列表 → 逐个发送
  void _showSendDialog(Device d) {
    _pickAndSend(d);
  }

  Future<void> _pickAndSend(Device d) async {
    // 1. Windows 原生文件对话框（支持多选）
    final result = await FilePicker.platform.pickFiles(
      type: FileType.any,
      allowMultiple: true,
      dialogTitle: '选择要发送的文件',
      lockParentWindow: true, // 对话框模态于主窗口
    );
    if (!mounted) return;

    // 用户取消
    if (result == null || result.files.isEmpty) return;

    // 2. 过滤出有真实路径的文件
    final picks = result.files.where((f) => f.path != null).toList();
    if (picks.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('无法获取所选文件的路径')),
      );
      return;
    }

    // 3. 确认列表（多选时展示全部，确认后逐个发送）
    showDialog<void>(
      context: context,
      builder: (_) => AlertDialog(
        title: Text('发送到 ${d.name}'),
        content: SizedBox(
          width: 420,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text('目标: ${d.ip}:${d.transferPort}',
                  style: const TextStyle(fontSize: 13, color: Colors.grey)),
              const SizedBox(height: 8),
              Text('已选 ${picks.length} 个文件：',
                  style: const TextStyle(fontWeight: FontWeight.bold)),
              const SizedBox(height: 4),
              ...picks.map(
                (f) => Padding(
                  padding: const EdgeInsets.symmetric(vertical: 2),
                  child: Row(
                    children: [
                      const Icon(Icons.insert_drive_file,
                          size: 16, color: Colors.grey),
                      const SizedBox(width: 6),
                      Expanded(
                        child: Text(
                          '${f.name}（${formatBytes(f.size)}）',
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(fontSize: 13),
                        ),
                      ),
                    ],
                  ),
                ),
              ),
            ],
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () {
              Navigator.pop(context);
              // 多选时整批共享一个 batch_id：接收端据此合成一张卡片，
              // 一次确认即可，不用每个文件点一遍。单文件不带批次，行为不变。
              if (picks.length > 1) {
                final batchId =
                    'b-${DateTime.now().microsecondsSinceEpoch}';
                for (var i = 0; i < picks.length; i++) {
                  _sendFile(d, picks[i].path!,
                      batch: SendBatch(
                          batchId: batchId, index: i, total: picks.length));
                }
              } else {
                _sendFile(d, picks.first.path!);
              }
            },
            child: Text('发送 (${picks.length})'),
          ),
        ],
      ),
    );
  }
}

// ============ 工具函数 ============

/// 用系统默认程序打开文件
Future<void> openFile(String path) async {
  try {
    if (Platform.isWindows) {
      await Process.run('explorer.exe', [path]);
    } else if (Platform.isMacOS) {
      await Process.run('open', [path]);
    } else {
      await Process.run('xdg-open', [path]);
    }
  } catch (_) {}
}

/// 在文件管理器中定位并选中文件
Future<void> revealInFileManager(String path) async {
  try {
    if (Platform.isWindows) {
      // explorer /select,<path>：打开所在目录并选中该文件
      await Process.run('explorer.exe', ['/select,$path']);
    } else if (Platform.isMacOS) {
      await Process.run('open', ['-R', path]);
    } else {
      await Process.run('xdg-open', [File(path).parent.path]);
    }
  } catch (_) {}
}

/// formatBytes / formatSpeed 由共享包 kitefile_shared 提供（工作流 C）。

// ============ 简易 HTTP 客户端 ============
// 不依赖 dio/http，避免额外依赖；如需更复杂功能再引入。

Future<String> httpGet(String url) async {
  final uri = Uri.parse(url);
  final client = HttpClient();
  try {
    final r = await client.getUrl(uri);
    final resp = await r.close();
    final body = await resp.transform(utf8.decoder).join();
    if (resp.statusCode >= 400) {
      throw Exception('HTTP ${resp.statusCode}: $body');
    }
    return body;
  } finally {
    client.close(force: true);
  }
}

Future<String> httpPost(String url, {required String body}) async {
  final uri = Uri.parse(url);
  final client = HttpClient();
  try {
    final r = await client.postUrl(uri);
    r.headers.contentType = ContentType.json;
    r.add(Uint8List.fromList(utf8.encode(body)));
    final resp = await r.close();
    final respBody = await resp.transform(utf8.decoder).join();
    if (resp.statusCode >= 400) {
      throw Exception('HTTP ${resp.statusCode}: $respBody');
    }
    return respBody;
  } finally {
    client.close(force: true);
  }
}
