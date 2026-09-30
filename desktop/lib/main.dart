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

/// daemon 实际监听的端口，由 `DaemonManager` / whoami 探测后写入。
/// 未探测到时先用首选，保证 UI 不会因端口未定而崩。
int daemonPort = 7878;

/// whoami 扫描端口列表（默认 = 共享候选；测试可注入以隔离本机真实 daemon）。
List<int> daemonScanPorts = kGatewayPortCandidates;

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
/// - Windows 下不弹终端：release 版 kitefile-cli 编译为 GUI 子系统
///   （cli.rs `windows_subsystem = "windows"`），Process.start 不会闪控制台；
///   daemon 日志写入 `<receive_dir>/daemon.log`
/// - 「完全退出」时才 kill 子进程；「最小化到托盘」只藏窗口，daemon 继续跑
class DaemonManager {
  Process? _process;
  bool _spawned = false;
  bool _isReady = false;
  /// 单飞：并发调用 ensureRunning 只拉起一次，避免双 daemon 抢端口
  Future<void>? _ensureFuture;

  /// 是否由本进程启动了 daemon（用于判断关闭时是否需要 kill）
  bool get spawnedByUs => _spawned;

  /// daemon 是否已就绪
  bool get isReady => _isReady;

  /// 确保 daemon 在运行。并发调用共享同一 Future，绝不重复 spawn。
  Future<void> ensureRunning() {
    return _ensureFuture ??= _ensureRunningImpl();
  }

  Future<void> _ensureRunningImpl() async {
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
        // 守护进程有自己的日志输出（release 写 daemon.log），UI 不接管 stdout
        runInShell: false,
      );
      _spawned = true;
      // detached 模式下 stdio 仍可访问（空流）；listen 防止句柄堆积
      _process!.stdout.listen((_) {});
      _process!.stderr.listen((_) {});
      debugPrint(
          '[DaemonManager] spawned daemon PID=${_process!.pid} from $exePath');
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
    // 允许 stop 之后再次 ensureRunning（单飞 Future 作废）
    _ensureFuture = null;
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
  /// 已配对设备（GET /api/peers；设备列表展示规则与发送入口依赖它）
  List<Map<String, dynamic>> _peers = [];
  final Map<String, TransferProgress> _progress = {};
  final Map<String, IncomingEntry> _pendingIncoming = {};
  /// 首页传输记录折叠态：接收/发送列表默认收起，完成的任务不占进度区
  bool _recvOpen = false;
  bool _sendOpen = false;
  /// 配对请求弹窗是否打开（WS 之外轮询 pending 时避免重复弹窗）
  bool _pairDialogOpen = false;
  Timer? _pairPendingPoll;
  /// session → 打开中的配对弹窗刷新回调（WS 推送 code_verified 时立即解锁）
  final Map<String, VoidCallback> _pairDialogUpdaters = {};
  /// 攒批—决策—迟到沿用状态机（工作流 C：共享实现，双端仅此一份）
  late final BatchDecider _batchDecider;
  /// 已经为该 fileId 弹过完成提示，避免重复弹窗
  final Set<String> _notifiedComplete = {};
  /// 已通知过「传输中断」的 file_id（一条传输只弹一次）
  final Set<String> _notifiedInterrupted = {};
  WebSocket? _ws;
  /// WS 连接进行中标志：防止 whoami 成功触发与失败重试链并发叠加
  bool _wsConnecting = false;
  Timer? _refreshTimer;
  bool _daemonOnline = false;
  Timer? _startupPollTimer;

  @override
  void initState() {
    super.initState();
    windowManager.addListener(this);
    // WS 之外兜底：每 2s 轮询配对 pending，防 WS 丢 pair_request 事件
    _pairPendingPoll = Timer.periodic(const Duration(seconds: 2), (_) async {
      if (!mounted || _pairDialogOpen || !_daemonOnline) return;
      try {
        final r = await httpGet('$kDaemonHttp/api/pair/pending');
        final j = jsonDecode(r) as Map<String, dynamic>;
        final pend = j['pending'] as Map<String, dynamic>?;
        if (pend != null && pend['session'] is String) {
          _pairDialogOpen = true;
          try {
            await _showPairRequestDialog(pend);
          } finally {
            _pairDialogOpen = false;
          }
        }
      } catch (_) {}
    });
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
    _pairPendingPoll?.cancel();
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
    // 周期轮询**无条件**创建：daemon 可能晚于 UI 就绪（杀软扫描 release exe
    // 首跑、慢盘），也可能因端口退避到备选——启动窗口错过绝不能永久离线
    //（真机踩过：守护进程起来了、徽标永远停在未连接）。
    _refreshTimer = Timer.periodic(const Duration(seconds: 3), (_) {
      _refreshDevices();
      _fetchWhoAmI();
      // 周期对齐：daemon 重启/历史落盘后 UI 也要能刷出来
      _refreshTransfers();
    });
    // 启动期 1s 快速探测只为尽早 ready；超时后由上面的周期轮询继续兜底。
    _startupPollTimer = Timer.periodic(const Duration(seconds: 1), (t) async {
      if (t.tick > 15) {
        t.cancel();
        return;
      }
      await _fetchWhoAmI();
      if (_daemonOnline) {
        t.cancel();
      }
    });
    // 立即试一次（万一 daemon 已经 ready）
    await _fetchWhoAmI();
    if (_daemonOnline) {
      _startupPollTimer?.cancel();
      // 重启后必须拉一次全量：历史在 daemon 的 transfer_history.json，
      // 只靠 WS 进度帧看不到已落盘的终态记录
      await _refreshTransfers();
    }
    _refreshDevices();
  }

  /// 传输全量快照：daemon 缓存/历史是权威状态（重启后 UI 靠它恢复记录）
  Future<void> _refreshTransfers() async {
    try {
      final r = await httpGet('$kDaemonHttp/api/transfers');
      final list = (jsonDecode(r) as Map<String, dynamic>)['transfers'] as List?;
      if (list == null || !mounted) return;
      setState(() {
        for (final t in list) {
          final p = TransferProgress.fromJson(t as Map<String, dynamic>);
          if (p.fileId.isNotEmpty) _progress[p.fileId] = p;
        }
      });
    } catch (e) {
      debugPrint('[kitefile] transfers snapshot failed: $e');
    }
  }

  /// whoami 探测代数：后发起的探测覆盖先发起的，防止慢失败扫描
  /// 把已成功的「在线」状态覆盖成离线（徽标黄一下再变绿的根因）。
  int _whoamiEpoch = 0;

  Future<void> _fetchWhoAmI() async {
    final epoch = ++_whoamiEpoch;
    // 扫描序列：当前已知端口优先，随后全部候选（whoamiScanPorts）。
    // daemon 若因端口保留退避到 17878 等备选，只认默认端口会永远发现不了。
    for (final p in whoamiScanPorts(daemonPort, daemonScanPorts)) {
      try {
        final r = await httpGet('http://127.0.0.1:$p/api/whoami')
            .timeout(const Duration(milliseconds: 1500));
        final me = WhoAmI.fromJson(jsonDecode(r) as Map<String, dynamic>);
        if (!mounted || epoch != _whoamiEpoch) return; // 已被更新的探测取代
        setState(() {
          daemonPort = p;
          _me = me;
          _daemonOnline = true;
        });
        // 首次连上（或 WS 掉线后尚未重连）→ 触发 WS；防抖避免重试链叠加
        if (_ws == null && !_wsConnecting) {
          _connectWs();
        }
        // 连上后对齐一次历史/进度快照
        unawaited(_refreshTransfers());
        return;
      } catch (e) {
        // 该端口没响应/非 daemon 服务，试下一个（不静默：debug 可见）
        debugPrint('[kitefile] whoami @$p failed: $e');
      }
    }
    if (!mounted || epoch != _whoamiEpoch) return;

    // 全部候选失败。若徽标仍是在线，先立刻复测已知端口一次：
    // 单次瞬时超时/并发抖动不应把徽标打成黄灯。
    if (_daemonOnline && daemonScanPorts.isNotEmpty) {
      try {
        final r = await httpGet('http://127.0.0.1:$daemonPort/api/whoami')
            .timeout(const Duration(milliseconds: 800));
        if (r.isNotEmpty && mounted && epoch == _whoamiEpoch) {
          setState(() => _daemonOnline = true);
          return;
        }
      } catch (_) {/* 复测仍失败 → 继续掉线 */}
    }
    if (!mounted || epoch != _whoamiEpoch) return;
    // 在线徽标唯一真值（A3.6）：确认不可达后才掉线
    setState(() => _daemonOnline = false);
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
    // 配对表单独拉：失败不拖垮设备列表
    try {
      final r = await httpGet('$kDaemonHttp/api/peers');
      final list = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
      if (mounted) setState(() => _peers = list);
    } catch (e) {
      debugPrint('[kitefile] peers refresh failed: $e');
    }
  }

  /// 已配对设备优先显示本机改过的 `name_hint`；未改名则回落 mDNS 名。
  /// 重命名仅本机可见（不通知对端）。
  String _peerDisplayName(Device d) {
    for (final p in _peers) {
      if (p['device_id'] == d.id) {
        final hint = (p['name_hint'] as String?)?.trim();
        if (hint != null && hint.isNotEmpty) return hint;
      }
    }
    return d.name;
  }

  /// 顶部置顶提示（弹窗遮挡时也可见）；error=true 用红底。
  void _showTopToast(String message, {bool error = false}) {
    if (!mounted) return;
    final overlay = Overlay.maybeOf(context, rootOverlay: true);
    if (overlay == null) return;
    final entry = OverlayEntry(
      builder: (ctx) {
        final top = MediaQuery.of(ctx).padding.top + 8;
        return Positioned(
          top: top,
          left: 16,
          right: 16,
          child: Material(
            elevation: 12,
            borderRadius: BorderRadius.circular(10),
            color: error ? const Color(0xFFB71C1C) : const Color(0xFF1B5E20),
            child: Padding(
              padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 12),
              child: Row(
                children: [
                  Icon(
                    error ? Icons.error_outline : Icons.check_circle_outline,
                    color: Colors.white,
                    size: 20,
                  ),
                  const SizedBox(width: 8),
                  Expanded(
                    child: Text(
                      message,
                      style: const TextStyle(color: Colors.white, fontSize: 13),
                    ),
                  ),
                ],
              ),
            ),
          ),
        );
      },
    );
    overlay.insert(entry);
    Future.delayed(const Duration(seconds: 4), () {
      try {
        entry.remove();
      } catch (_) {}
    });
  }

  /// 仅改本机显示名（POST /api/peers/:id/rename）
  Future<void> _renamePeer(
      String deviceId, String currentName, {VoidCallback? onDone}) async {
    final ctrl = TextEditingController(text: currentName);
    final ok = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('重命名设备'),
        content: TextField(
          controller: ctrl,
          autofocus: true,
          maxLength: 32,
          decoration: const InputDecoration(
            border: OutlineInputBorder(),
            isDense: true,
            hintText: '仅在本机显示，对方不受影响',
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx, false),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(ctx, true),
            child: const Text('保存'),
          ),
        ],
      ),
    );
    final name = ctrl.text.trim();
    ctrl.dispose();
    if (ok != true || name.isEmpty) return;
    try {
      await httpPost(
        '$kDaemonHttp/api/peers/$deviceId/rename',
        body: jsonEncode({'name': name}),
      );
      await _refreshDevices();
      onDone?.call();
      if (mounted) {
        _showTopToast('已重命名为「$name」（仅本机可见）');
      }
    } catch (e) {
      if (mounted) {
        _showTopToast('重命名失败: $e', error: true);
      }
    }
  }

  /// 删除已配对设备（双向信任撤销）
  Future<void> _deletePeer(String deviceId, String name,
      {VoidCallback? onDone}) async {
    final ok = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('删除设备'),
        content: Text(
          '将解除与「$name」的配对，双向传输信任立即失效。\n'
          '若以后要重新配对，两台设备都需再次打开「添加设备」。',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx, false),
            child: const Text('取消'),
          ),
          FilledButton(
            style: FilledButton.styleFrom(
              backgroundColor: Theme.of(ctx).colorScheme.error,
            ),
            onPressed: () => Navigator.pop(ctx, true),
            child: const Text('删除设备'),
          ),
        ],
      ),
    );
    if (ok != true) return;
    try {
      await httpDelete('$kDaemonHttp/api/peers/$deviceId');
      await _refreshDevices();
      onDone?.call();
      if (mounted) {
        _showTopToast('已删除「$name」');
      }
    } catch (e) {
      if (mounted) {
        _showTopToast('删除失败: $e', error: true);
      }
    }
  }

  Future<void> _connectWs() async {
    // 防抖：whoami 成功触发与失败重试链不得并发叠加（同一时刻至多一条链）
    if (_ws != null || _wsConnecting) return;
    _wsConnecting = true;
    try {
      final ws = await WebSocket.connect(kDaemonWs);
      _ws = ws;
      _wsConnecting = false;
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
          // WS 断开 ≠ daemon 死了。徽标真值只由 whoami 轮询定；
          // 这里只清 WS 句柄并复核 + 重连。若在此直接置离线，会出现
          // 「已连接 → 黄灯 → 又变已连接」抖动（whoami 仍通时被 WS 误伤）。
          debugPrint('[kitefile] ws closed');
          _ws = null;
          _fetchWhoAmI();
          Future.delayed(const Duration(seconds: 5), () {
            if (mounted) _connectWs();
          });
        },
        onError: (Object e) {
          // 同上：不改徽标，只复核 whoami 真值
          debugPrint('[kitefile] ws error: $e');
          _fetchWhoAmI();
        },
      );
    } catch (e) {
      // 连接失败 ≠ 已建立后断开：daemon 存活由 whoami 轮询定真值，
      // 这里只留日志并重连，避免与 whoami 打架导致徽标抖动。
      debugPrint('[kitefile] ws connect failed: $e');
      _wsConnecting = false;
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
      case 'pair_request':
        // 对端发来配对请求（P3 WS 事件）→ 确认码弹窗（防叠开）
        if (!_pairDialogOpen && mounted) {
          _pairDialogOpen = true;
          unawaited(_showPairRequestDialog(j).whenComplete(() {
            _pairDialogOpen = false;
            if (mounted) setState(() {});
          }));
        }
        break;
      case 'pair_code_verified':
        // 发起方已提交确认码 → 打开中的弹窗立即解锁「确认配对」
        final sid = j['session'] as String?;
        if (sid != null) _pairDialogUpdaters[sid]?.call();
        break;
      default:
        break;
    }
  }

  /// 对端配对请求弹窗：展示确认码；发起方输入码并通过后才能点「确认配对」。
  /// 主路径：WS pair_code_verified 即时解锁；轮询 pending 作兜底。
  Future<void> _showPairRequestDialog(Map<String, dynamic> j) async {
    final session = j['session'] as String? ?? '';
    final name = j['name'] as String? ?? '未知设备';
    final ip = j['ip'] as String? ?? '';
    final platform = j['platform'] as String? ?? '';
    var code = j['code'] as String? ?? '';
    var codeVerified = j['code_verified'] == true;
    if (session.isEmpty || !mounted) return;

    Future<void> decide(bool accept) async {
      try {
        await httpPost(
          '$kDaemonHttp/api/pair/decide',
          body: jsonEncode({'session': session, 'accept': accept}),
        );
        await _refreshDevices();
        if (mounted) {
          _showTopToast(
              accept ? '已与 $name 配对' : '已拒绝 $name 的配对请求',
              error: !accept);
        }
      } catch (e) {
        if (mounted) {
          _showTopToast('配对失败: $e', error: true);
        }
      }
    }

    var closed = false;
    Timer? pendingPoll;
    StateSetter? dialogSetState;
    void unlockFromWs() {
      if (closed) return;
      final fn = dialogSetState;
      if (fn == null) return;
      fn(() {
        codeVerified = true;
      });
    }

    _pairDialogUpdaters[session] = unlockFromWs;
    await showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) => StatefulBuilder(builder: (ctx, setDialogState) {
        dialogSetState = setDialogState;
        // 周期轮询兜底；主路径是 WS pair_code_verified 即时解锁
        pendingPoll ??= Timer.periodic(const Duration(seconds: 1), (t) async {
          if (closed || !ctx.mounted) {
            t.cancel();
            return;
          }
          try {
            final r = await httpGet('$kDaemonHttp/api/pair/pending');
            final j2 = jsonDecode(r) as Map<String, dynamic>;
            final pend = j2['pending'] as Map<String, dynamic>?;
            if (pend == null || pend['session'] != session) {
              closed = true;
              t.cancel();
              if (ctx.mounted) Navigator.pop(ctx);
              if (mounted) {
                _showTopToast('配对会话已结束或对方已取消', error: true);
              }
              return;
            }
            final verified = pend['code_verified'] == true;
            final newCode = pend['code'] as String? ?? code;
            if (verified != codeVerified || newCode != code) {
              setDialogState(() {
                codeVerified = verified;
                code = newCode;
              });
            }
          } catch (_) {/* 下一轮再试 */}
        });
        return AlertDialog(
          title: const Row(
            children: [
              Icon(Icons.link_rounded, size: 24),
              SizedBox(width: 8),
              Text('配对请求'),
            ],
          ),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text('$name 请求与本机配对'),
              const SizedBox(height: 4),
              Text('$platform · $ip',
                  style: const TextStyle(fontSize: 12, color: Colors.grey)),
              const SizedBox(height: 16),
              Text(
                codeVerified ? '确认码（对方已输入）' : '确认码（请让对方看清并输入）',
                style: const TextStyle(fontSize: 12, color: Colors.grey),
              ),
              const SizedBox(height: 8),
              Center(
                child: Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 20, vertical: 10),
                  decoration: BoxDecoration(
                    color: Theme.of(ctx).colorScheme.primaryContainer,
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Text(
                    code,
                    style: const TextStyle(
                        fontSize: 36,
                        fontWeight: FontWeight.bold,
                        letterSpacing: 6),
                  ),
                ),
              ),
              const SizedBox(height: 8),
              Text(
                codeVerified
                    ? '对方已输入正确确认码。请确认设备无误后点「确认配对」。'
                    : '请把此确认码念给对方，或让对方看清；由对方在发起配对的设备上输入。'
                        '对方输入正确后，本页「确认配对」才会解锁。',
                style: TextStyle(
                  fontSize: 12,
                  color: codeVerified ? Colors.green[700] : Colors.orange,
                ),
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () {
                closed = true;
                Navigator.pop(ctx);
                decide(false);
              },
              child: const Text('拒绝'),
            ),
            FilledButton(
              onPressed: codeVerified
                  ? () {
                      closed = true;
                      Navigator.pop(ctx);
                      decide(true);
                    }
                  : null,
              child: Text(codeVerified ? '确认配对' : '等待对方输入确认码…'),
            ),
          ],
        );
      }),
    );
    pendingPoll?.cancel();
    closed = true;
    _pairDialogUpdaters.remove(session);
  }

  /// 添加设备（P2 入口改版）：打开即开启配对模式，页内完成
  /// 「发现 → 发起 → 等确认」，关闭页面自动退出配对模式。
  /// 两台设备都打开此页即可互相发现；**120 秒到点即关、不自动续期**
  /// （配对窗口必须是有意义的限时），结束态可点「重试」重新开始一轮。
  Future<void> _openAddDevice() async {
    int ttl;
    try {
      final r = await httpPost('$kDaemonHttp/api/pair/mode',
          body: jsonEncode({'enabled': true}));
      ttl = (jsonDecode(r) as Map<String, dynamic>)['seconds_left'] as int? ?? 120;
    } catch (e) {
      if (mounted) {
        _showTopToast('开启配对模式失败: $e', error: true);
      }
      return;
    }
    if (!mounted) return;

    var devices = <Device>[];
    var peers = <Map<String, dynamic>>[];
    Future<void> pull() async {
      try {
        final r = await httpGet('$kDaemonHttp/api/devices');
        devices = (jsonDecode(r) as List)
            .cast<Map<String, dynamic>>()
            .map(Device.fromJson)
            .toList();
      } catch (_) {/* 设备表拉取失败：沿用上一帧 */}
      try {
        final r = await httpGet('$kDaemonHttp/api/peers');
        peers = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
      } catch (_) {/* 配对表失败：沿用上一帧 */}
    }
    await pull();
    if (!mounted) return;

    String? waitId;
    String? waitName;
    String? waitCode; // pair/start 返回的本机侧确认码（与对方屏上应一致）
    String? waitSession; // pairing_session：提交码/取消必须用它，不能用 device_id
    var codeOk = false; // 发起方已输入正确确认码并已转发给对方
    var codeSubmitting = false;
    final codeCtrl = TextEditingController();
    var expired = false; // 倒计时到点后的结束态（等「重试」重新开启）
    var refreshTick = 0; // 搜索期每 15s 触发一次 pair 标志同步（/api/pair/refresh）
    Timer? ticker;
    var stopped = false;
    StateSetter? waitSetState;
    Future<void> cancelSession() async {
      final sid = waitSession;
      waitId = null;
      waitSession = null;
      codeOk = false;
      codeSubmitting = false;
      codeCtrl.clear();
      waitSetState?.call(() {});
      if (sid != null) {
        try {
          await httpPost('$kDaemonHttp/api/pair/cancel',
              body: jsonEncode({'session': sid}));
        } catch (_) {/* TTL 兜底 */}
      }
    }

    Future<void> shutdown() async {
      stopped = true;
      ticker?.cancel();
      codeCtrl.dispose();
      try {
        await httpPost('$kDaemonHttp/api/pair/mode',
            body: jsonEncode({'enabled': false}));
      } catch (_) {/* 关闭失败靠 TTL 兜底 */}
      await _refreshDevices();
    }

    await showDialog<void>(
      context: context,
      barrierDismissible: true,
      builder: (ctx) => StatefulBuilder(builder: (ctx, setDialogState) {
        waitSetState = setDialogState;
        ticker ??= Timer.periodic(const Duration(seconds: 1), (t) async {
          if (stopped) {
            t.cancel();
            return;
          }
          if (waitId != null) {
            // 等确认中：轮询配对表，对方确认即完成
            try {
              final r = await httpGet('$kDaemonHttp/api/peers');
              final list = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
              if (list.any((p) => p['device_id'] == waitId)) {
                stopped = true;
                t.cancel();
                final name = waitName ?? '对方设备';
                await _refreshDevices();
                if (ctx.mounted) Navigator.pop(ctx);
                if (mounted) {
                  _showTopToast('已与 $name 配对');
                }
                return;
              }
            } catch (_) {/* 轮询失败下一轮再试 */}
            if (codeOk) {
              try {
                final r = await httpGet('$kDaemonHttp/api/pair/out-pending');
                final j = jsonDecode(r) as Map<String, dynamic>;
                if (j['active'] != true && waitId != null) {
                  waitId = null;
                  waitSession = null;
                  codeOk = false;
                  if (ctx.mounted) {
                    setDialogState(() {});
                    _showTopToast('配对会话已过期，请重新发起配对', error: true);
                  }
                }
              } catch (_) {}
            }
            // 倒计时照走：到点即关（confirm 不依赖配对模式，等待不受影响）
            if (!expired) {
              ttl -= 1;
              if (ttl <= 0) {
                ttl = 0;
                expired = true;
                try {
                  await httpPost('$kDaemonHttp/api/pair/mode',
                      body: jsonEncode({'enabled': false}));
                } catch (_) {/* daemon TTL 同时也会关 */}
              }
            }
            if (ctx.mounted) setDialogState(() {});
            return;
          }
          if (expired) return; // 结束态：画面静止，等用户点「重试」
          ttl -= 1;
          if (ttl <= 0) {
            // 到点即关——**不自动续期**，配对窗口的 120s 必须有意义
            ttl = 0;
            expired = true;
            try {
              await httpPost('$kDaemonHttp/api/pair/mode',
                  body: jsonEncode({'enabled': false}));
            } catch (_) {/* daemon TTL 同时也会关 */}
          } else {
            await pull();
            // 每 15s 主动同步一次对端 pair 标志：对端可能比本机晚开启
            // 配对模式，其组播公告可能丢（防火墙/组播过滤）——whoami
            // 是产品命脉通道，不依赖 TTL 续期语义
            refreshTick += 1;
            if (refreshTick >= 15) {
              refreshTick = 0;
              try {
                await httpPost('$kDaemonHttp/api/pair/refresh', body: '{}');
              } catch (_) {/* 下一 tick 再试 */}
              await pull();
            }
          }
          if (ctx.mounted) setDialogState(() {});
        });

        final pairable = devices
            .where((d) =>
                d.pair && !peers.any((p) => p['device_id'] == d.id))
            .toList();
        // 已发现但对方未开启配对模式：列出来置灰——让用户能区分
        //「没发现」和「发现了但对方没开」，而不是对着空白猜
        final notReady = devices
            .where((d) =>
                !d.pair && !peers.any((p) => p['device_id'] == d.id))
            .toList();
        // 配对页也展示**已配对**设备：对方开启配对模式时可「重新配对」
        //（应对单方删除设备后的信任重建）。离线的已配对设备仅展示状态。
        final pairedOnMap = devices
            .where((d) => peers.any((p) => p['device_id'] == d.id))
            .toList();
        final offlinePaired = peers
            .where((p) => !devices.any((d) => d.id == p['device_id']))
            .toList();
        final pairedCount = pairedOnMap.length + offlinePaired.length;
        final anyVisible = pairable.isNotEmpty ||
            notReady.isNotEmpty ||
            pairedOnMap.isNotEmpty ||
            offlinePaired.isNotEmpty;

        return AlertDialog(
          title: const Row(
            children: [
              Icon(Icons.add_circle_outline, size: 24),
              SizedBox(width: 8),
              Text('添加设备'),
            ],
          ),
          content: SizedBox(
            width: 440,
            child: waitId != null
                ? Column(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      Text(codeOk
                          ? '等待 ${waitName ?? '对方'} 确认…'
                          : '正在与 ${waitName ?? '对方'} 配对'),
                      const SizedBox(height: 12),
                      if (!codeOk) ...[
                        const Text(
                          '请看清对方屏幕上的 6 位确认码，输入到本机：',
                          textAlign: TextAlign.center,
                          style: TextStyle(fontSize: 12, color: Colors.grey),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: codeCtrl,
                          autofocus: true,
                          textAlign: TextAlign.center,
                          keyboardType: TextInputType.number,
                          maxLength: 6,
                          style: const TextStyle(
                            fontSize: 28,
                            fontWeight: FontWeight.bold,
                            letterSpacing: 8,
                          ),
                          decoration: const InputDecoration(
                            counterText: '',
                            border: OutlineInputBorder(),
                            isDense: true,
                            hintText: '••••••',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextButton.icon(
                          icon: const Icon(Icons.check, size: 18),
                          label:
                              Text(codeSubmitting ? '提交中…' : '输入完成 · 校验'),
                          onPressed: codeSubmitting
                              ? null
                              : () async {
                                  final input = codeCtrl.text
                                      .replaceAll(RegExp(r'\s'), '');
                                  if (input.length != 6) {
                                    if (ctx.mounted) {
                                      _showTopToast(
                                          '请输入对方屏幕上的 6 位数字',
                                          error: true);
                                    }
                                    return;
                                  }
                                  if (input != waitCode) {
                                    if (ctx.mounted) {
                                      _showTopToast(
                                          '确认码不一致：请核对对方屏幕上的数字，可能有人在中间拦截',
                                          error: true);
                                    }
                                    return;
                                  }
                                  setDialogState(() => codeSubmitting = true);
                                  try {
                                    final sid = waitSession;
                                    if (sid == null || sid.isEmpty) {
                                      setDialogState(
                                          () => codeSubmitting = false);
                                      _showTopToast('配对会话已丢失，请重新发起配对',
                                          error: true);
                                      return;
                                    }
                                    final r = await httpPost(
                                      '$kDaemonHttp/api/pair/verify-forward',
                                      body: jsonEncode(
                                          {'session': sid, 'code': input}),
                                    );
                                    final j =
                                        jsonDecode(r) as Map<String, dynamic>;
                                    if (j['ok'] == true) {
                                      setDialogState(() {
                                        codeOk = true;
                                        codeSubmitting = false;
                                      });
                                      _showTopToast('确认码已提交，等待对方确认');
                                    } else {
                                      setDialogState(
                                          () => codeSubmitting = false);
                                    }
                                  } catch (e) {
                                    setDialogState(
                                        () => codeSubmitting = false);
                                    if (ctx.mounted) {
                                      _showTopToast('提交确认码失败: $e', error: true);
                                    }
                                  }
                                },
                        ),
                        const Text(
                          '码必须与对方屏幕一致；不一致请取消重来。'
                          '提交成功后，对方屏幕上「确认配对」才会解锁。',
                          textAlign: TextAlign.center,
                          style: TextStyle(fontSize: 11, color: Colors.orange),
                        ),
                      ] else ...[
                        Container(
                          padding: const EdgeInsets.symmetric(
                              horizontal: 20, vertical: 10),
                          decoration: BoxDecoration(
                            color: Theme.of(ctx).colorScheme.primaryContainer,
                            borderRadius: BorderRadius.circular(8),
                          ),
                          child: const Icon(Icons.hourglass_top, size: 36),
                        ),
                        const SizedBox(height: 8),
                        const Text(
                          '确认码已提交。请对方在其屏幕上点「确认配对」；'
                          '60 秒未确认将过期。',
                          textAlign: TextAlign.center,
                          style: TextStyle(fontSize: 12, color: Colors.grey),
                        ),
                      ],
                      Row(
                        mainAxisAlignment: MainAxisAlignment.center,
                        children: [
                          TextButton(
                            onPressed: () async {
                              await cancelSession();
                            },
                            child: const Text('取消配对'),
                          ),
                          TextButton(
                            onPressed: () async {
                              await cancelSession();
                            },
                            child: const Text('返回设备列表'),
                          ),
                        ],
                      ),
                    ],
                  )
                : Column(
                    mainAxisSize: MainAxisSize.min,
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Row(
                        children: [
                          const Text('配对模式',
                              style: TextStyle(fontWeight: FontWeight.bold)),
                          const Spacer(),
                          expired
                              ? const Text('已结束',
                                  style: TextStyle(
                                      fontSize: 13, color: Colors.orange))
                              : Text(
                                  '剩余 ${ttl}s',
                                  style: TextStyle(
                                    fontSize: 13,
                                    color: Theme.of(ctx).colorScheme.primary,
                                  ),
                                ),
                        ],
                      ),
                      const SizedBox(height: 4),
                      if (expired) ...[
                        const Text(
                          '配对窗口已关闭：120 秒到点自动退出（停留在本页也不会续期）。需要继续时请重试。',
                          style: TextStyle(fontSize: 12, color: Colors.grey),
                        ),
                        const SizedBox(height: 16),
                        Center(
                          child: FilledButton.icon(
                            icon: const Icon(Icons.refresh, size: 18),
                            label: const Text('重试 · 重新开启 120 秒'),
                            onPressed: () async {
                              try {
                                final r = await httpPost(
                                    '$kDaemonHttp/api/pair/mode',
                                    body: jsonEncode({'enabled': true}));
                                final j =
                                    jsonDecode(r) as Map<String, dynamic>;
                                setDialogState(() {
                                  expired = false;
                                  ttl = (j['seconds_left'] as num?)?.toInt() ??
                                      120;
                                });
                              } catch (e) {
                                if (ctx.mounted) {
                                  _showTopToast('重试失败: $e', error: true);
                                }
                              }
                            },
                          ),
                        ),
                      ] else ...[
                        const Text(
                          '两台设备都打开此页面即可互相发现；出现设备后点「配对」，'
                          '在对方屏幕看清确认码后输入到本机，再由对方点「确认配对」。',
                          style: TextStyle(fontSize: 12, color: Colors.grey),
                        ),
                        const SizedBox(height: 12),
                        if (!anyVisible)
                          const Padding(
                            padding: EdgeInsets.symmetric(vertical: 16),
                            child: Center(
                              child: Text(
                                '正在搜索附近可配对的设备…\n'
                                '（要求对方也停在本页面）\n\n'
                                '若长时间无结果：请确认两台设备在同一 Wi-Fi，'
                                '且对方也打开了「添加设备」页。\n'
                                'Windows 电脑需放行防火墙 7880 端口。',
                                textAlign: TextAlign.center,
                                style: TextStyle(color: Colors.grey),
                              ),
                            ),
                          )
                        else ...[
                          if (pairedOnMap.isNotEmpty) ...[
                            Text('已配对设备',
                                style: TextStyle(
                                    fontSize: 12,
                                    fontWeight: FontWeight.w600,
                                    color: Colors.grey[800])),
                            for (final d in pairedOnMap)
                              ListTile(
                                dense: true,
                                contentPadding: EdgeInsets.zero,
                                title: Text(_peerDisplayName(d)),
                                subtitle: Text(
                                  '${d.platform} · ${d.ip} · 已配对',
                                  style: const TextStyle(fontSize: 12),
                                ),
                                trailing: d.pair
                                    ? FilledButton.tonal(
                                        onPressed: () async {
                                          try {
                                            final r = await httpPost(
                                              '$kDaemonHttp/api/pair/start',
                                              body: jsonEncode({
                                                'device_id': d.id,
                                                'name': d.name,
                                                'platform': d.platform,
                                                'ip': d.ip,
                                                'gateway_port': d.gatewayPort,
                                              }),
                                            );
                                            final j = jsonDecode(r)
                                                as Map<String, dynamic>;
                                            setDialogState(() {
                                              waitId = d.id;
                                              waitSession = j['pairing_session']
                                                  as String?;
                                              waitName = _peerDisplayName(d);
                                              waitCode =
                                                  j['code'] as String? ?? '';
                                              codeOk = false;
                                              codeCtrl.clear();
                                            });
                                          } catch (e) {
                                            if (ctx.mounted) {
                                              _showTopToast(
                                                  '重新配对失败: $e ($kDaemonHttp)',
                                                  error: true);
                                            }
                                          }
                                        },
                                        child: const Text('重新配对'),
                                      )
                                    : const Icon(Icons.lock_outline,
                                        size: 18, color: Colors.grey),
                              ),
                            if (pairedOnMap.any((d) => !d.pair))
                              Padding(
                                padding: const EdgeInsets.only(
                                    left: 12, bottom: 4),
                                child: Text(
                                  '对方未开启配对模式时无法重新配对；请对方也打开本页面。',
                                  style: TextStyle(
                                      fontSize: 11, color: Colors.grey[600]),
                                ),
                              ),
                            const Divider(height: 12),
                          ],
                          if (offlinePaired.isNotEmpty) ...[
                            Text('离线已配对',
                                style: TextStyle(
                                    fontSize: 12,
                                    fontWeight: FontWeight.w600,
                                    color: Colors.grey[800])),
                            for (final p in offlinePaired)
                              ListTile(
                                dense: true,
                                contentPadding: EdgeInsets.zero,
                                leading: const Icon(Icons.cloud_off,
                                    size: 18, color: Colors.grey),
                                title: Text(
                                    (p['name_hint'] as String?) ?? '未知设备'),
                                subtitle: Text(
                                  '${p['platform'] ?? ''} · 离线 · 已配对',
                                  style: const TextStyle(fontSize: 12),
                                ),
                              ),
                            const Divider(height: 12),
                          ],
                          for (final d in notReady)
                            ListTile(
                              dense: true,
                              enabled: false,
                              contentPadding: EdgeInsets.zero,
                              title: Text(d.name,
                                  style: const TextStyle(color: Colors.grey)),
                              subtitle: Text(
                                  '${d.platform} · ${d.ip} · 对方未开启配对模式',
                                  style: const TextStyle(
                                      fontSize: 12, color: Colors.grey)),
                              trailing: const Icon(Icons.lock_outline,
                                  size: 18, color: Colors.grey),
                            ),
                          for (final d in pairable)
                            ListTile(
                              dense: true,
                              contentPadding: EdgeInsets.zero,
                              title: Text(d.name),
                              subtitle: Text('${d.platform} · ${d.ip}',
                                  style: const TextStyle(fontSize: 12)),
                              trailing: FilledButton.tonal(
                                onPressed: () async {
                                  try {
                                    final r = await httpPost(
                                      '$kDaemonHttp/api/pair/start',
                                      body: jsonEncode({
                                        'device_id': d.id,
                                        'name': d.name,
                                        'platform': d.platform,
                                        'ip': d.ip,
                                        'gateway_port': d.gatewayPort,
                                      }),
                                    );
                                    final j = jsonDecode(r)
                                        as Map<String, dynamic>;
                                    setDialogState(() {
                                      waitId = d.id;
                                      waitSession =
                                          j['pairing_session'] as String?;
                                      waitName = _peerDisplayName(d);
                                      waitCode = j['code'] as String? ?? '';
                                      codeOk = false;
                                      codeCtrl.clear();
                                    });
                                  } catch (e) {
                                    if (ctx.mounted) {
                                      _showTopToast('发起配对失败: $e ($kDaemonHttp)', error: true);
                                    }
                                  }
                                },
                                child: const Text('配对'),
                              ),
                            ),
                        ],
                        if (pairedCount > 0) ...[
                          const SizedBox(height: 8),
                          Text('本机已配对 $pairedCount 台设备',
                              style: const TextStyle(
                                  fontSize: 12, color: Colors.grey)),
                        ],
                      ],
                    ],
                  ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.pop(ctx),
              child: const Text('关闭'),
            ),
          ],
        );
      }),
    );
    await shutdown();
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
      _showTopToast('已接受整批，等待对方开始传输…');
    } catch (e) {
      if (!mounted) return;
      _showTopToast('接受失败: $e', error: true);
    }
  }

  Future<void> _rejectBatch(String batchId) async {
    _batchDecider.recordDecision(batchId, false);
    try {
      await httpPost('$kDaemonHttp/api/incoming/batch-decide',
          body: jsonEncode({'batch_id': batchId, 'accept': false}));
    } catch (e) {
      if (!mounted) return;
      _showTopToast('拒绝失败: $e', error: true);
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
    _showTopToast('传输请求已超时（60 秒未接受）', error: true);
  }

  Future<void> _acceptIncoming(String id, {bool quiet = false}) async {
    try {
      await httpPost('$kDaemonHttp/api/incoming/$id/accept', body: '');
      if (quiet || !mounted) return;
      _showTopToast('已接受，等待对方开始传输…');
    } catch (e) {
      if (quiet || !mounted) return;
      _showTopToast('接受失败: $e', error: true);
    }
  }

  Future<void> _rejectIncoming(String id) async {
    try {
      await httpPost('$kDaemonHttp/api/incoming/$id/reject', body: '');
    } catch (e) {
      if (!mounted) return;
      _showTopToast('拒绝失败: $e', error: true);
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

  /// 设置弹窗：配对 + 设备名 + 接收目录
  Future<void> _showSettingsDialog() async {
    String? currentDir;
    String? currentName;
    try {
      final r = await httpGet('$kDaemonHttp/api/config');
      final j = jsonDecode(r) as Map<String, dynamic>;
      currentDir = j['receive_dir'] as String?;
      currentName = j['device_name'] as String?;
    } catch (_) {}
    // 配对状态（开关倒计时 + 已配对列表）
    var pairOn = false;
    var pairTtl = 0;
    var peers = <Map<String, dynamic>>[];
    try {
      final r = await httpGet('$kDaemonHttp/api/pair/mode');
      final j = jsonDecode(r) as Map<String, dynamic>;
      pairOn = j['enabled'] == true;
      pairTtl = (j['seconds_left'] as num?)?.toInt() ?? 0;
    } catch (_) {}
    try {
      final r = await httpGet('$kDaemonHttp/api/peers');
      peers = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
    } catch (_) {}
    if (!mounted) return;

    final nameController = TextEditingController(text: currentName ?? '');
    Timer? pairTicker;

    await showDialog<void>(
      context: context,
      builder: (dialogCtx) => StatefulBuilder(
        builder: (ctx, setDialogState) {
          // 配对模式本地倒计时（服务端 TTL 为准，这里只做展示节拍）
          pairTicker ??= Timer.periodic(const Duration(seconds: 1), (t) {
            if (!pairOn) {
              t.cancel();
              return;
            }
            pairTtl -= 1;
            if (pairTtl <= 0) {
              pairOn = false;
              pairTtl = 0;
              t.cancel();
            }
            if (ctx.mounted) setDialogState(() {});
          });
          return AlertDialog(
          title: const Text('设置'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text('配对', style: TextStyle(fontWeight: FontWeight.bold)),
                const SizedBox(height: 6),
                Row(
                  children: [
                    Switch(
                      value: pairOn,
                      onChanged: (v) async {
                        try {
                          await httpPost(
                            '$kDaemonHttp/api/pair/mode',
                            body: jsonEncode({'enabled': v}),
                          );
                          setDialogState(() {
                            pairOn = v;
                            if (v) pairTtl = 120;
                          });
                        } catch (e) {
                          if (ctx.mounted) {
                            _showTopToast('切换配对模式失败: $e', error: true);
                          }
                        }
                      },
                    ),
                    const SizedBox(width: 8),
                    Text(
                      pairOn ? '配对模式开启（剩余 ${pairTtl}s）' : '配对模式关闭',
                      style: TextStyle(
                        fontSize: 13,
                        color: pairOn
                            ? Theme.of(ctx).colorScheme.primary
                            : Colors.grey,
                      ),
                    ),
                  ],
                ),
                const Text(
                  '开启后 120 秒内，双方设备才能互相发现并配对；配对过的设备不受影响，始终可见。',
                  style: TextStyle(fontSize: 12, color: Colors.grey),
                ),
                if (peers.isNotEmpty) ...[
                  const SizedBox(height: 8),
                  for (final p in peers)
                    ListTile(
                      dense: true,
                      contentPadding: EdgeInsets.zero,
                      leading: Icon(
                        p['online'] == true
                            ? Icons.circle
                            : Icons.circle_outlined,
                        size: 14,
                        color: p['online'] == true ? Colors.green : Colors.grey,
                      ),
                      title: Text(p['name_hint'] as String? ?? '未知设备'),
                      subtitle: Text(
                        '${p['platform'] ?? ''} · 已配对 · 重命名仅本机可见',
                        style: const TextStyle(fontSize: 12),
                      ),
                      trailing: Row(
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          IconButton(
                            icon: const Icon(Icons.edit_outlined, size: 20),
                            tooltip: '重命名（仅本机）',
                            onPressed: () async {
                              final id = p['device_id'] as String? ?? '';
                              final cur = p['name_hint'] as String? ?? '';
                              if (id.isEmpty) return;
                              final ctrl = TextEditingController(text: cur);
                              final ok = await showDialog<bool>(
                                context: ctx,
                                builder: (c) => AlertDialog(
                                  title: const Text('重命名设备'),
                                  content: TextField(
                                    controller: ctrl,
                                    autofocus: true,
                                    maxLength: 32,
                                    decoration: const InputDecoration(
                                      border: OutlineInputBorder(),
                                      isDense: true,
                                      hintText: '仅在本机显示，对方不受影响',
                                    ),
                                  ),
                                  actions: [
                                    TextButton(
                                      onPressed: () =>
                                          Navigator.pop(c, false),
                                      child: const Text('取消'),
                                    ),
                                    FilledButton(
                                      onPressed: () => Navigator.pop(c, true),
                                      child: const Text('保存'),
                                    ),
                                  ],
                                ),
                              );
                              final name = ctrl.text.trim();
                              ctrl.dispose();
                              if (ok != true || name.isEmpty) return;
                              try {
                                await httpPost(
                                  '$kDaemonHttp/api/peers/$id/rename',
                                  body: jsonEncode({'name': name}),
                                );
                                final r =
                                    await httpGet('$kDaemonHttp/api/peers');
                                setDialogState(() {
                                  peers = (jsonDecode(r) as List)
                                      .cast<Map<String, dynamic>>();
                                });
                                await _refreshDevices();
                              } catch (e) {
                                if (ctx.mounted) {
                                  _showTopToast('重命名失败: $e', error: true);
                                }
                              }
                            },
                          ),
                          IconButton(
                            icon: const Icon(Icons.delete_outline, size: 20),
                            tooltip: '删除设备',
                            onPressed: () async {
                              final id = p['device_id'] as String? ?? '';
                              final name =
                                  p['name_hint'] as String? ?? '未知设备';
                              if (id.isEmpty) return;
                              final ok = await showDialog<bool>(
                                context: ctx,
                                builder: (c) => AlertDialog(
                                  title: const Text('删除设备'),
                                  content: Text(
                                    '将解除与「$name」的配对，双向传输信任立即失效。',
                                  ),
                                  actions: [
                                    TextButton(
                                      onPressed: () =>
                                          Navigator.pop(c, false),
                                      child: const Text('取消'),
                                    ),
                                    FilledButton(
                                      style: FilledButton.styleFrom(
                                        backgroundColor: Theme.of(c)
                                            .colorScheme
                                            .error,
                                      ),
                                      onPressed: () => Navigator.pop(c, true),
                                      child: const Text('删除设备'),
                                    ),
                                  ],
                                ),
                              );
                              if (ok != true) return;
                              try {
                                await httpDelete(
                                    '$kDaemonHttp/api/peers/$id');
                                final r =
                                    await httpGet('$kDaemonHttp/api/peers');
                                setDialogState(() {
                                  peers = (jsonDecode(r) as List)
                                      .cast<Map<String, dynamic>>();
                                });
                                await _refreshDevices();
                              } catch (e) {
                                if (ctx.mounted) {
                                  _showTopToast('删除失败: $e', error: true);
                                }
                              }
                            },
                          ),
                        ],
                      ),
                    ),
                ],
                const Divider(height: 24),
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
                            _showTopToast('设备名称已更新：$name');
                          }
                        } catch (e) {
                          if (ctx.mounted) {
                            _showTopToast('设置失败: $e', error: true);
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
                        _showTopToast('保存位置已更新：$dir');
                      }
                    } catch (e) {
                      if (ctx.mounted) {
                        _showTopToast('设置失败: $e', error: true);
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
        );
        },
      ),
    );
    nameController.dispose();
  }

  Future<void> _sendFile(Device target, String path,
      {String? fileName, SendBatch? batch}) async {
    if (!target.online && !_isDeviceOnline(target.id)) {
      if (mounted) {
        _showTopToast('对方设备离线，无法发送文件', error: true);
      }
      return;
    }
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
      _showTopToast('已发起传输: $fileId');
    } catch (e) {
      if (!mounted) return;
      _showTopToast('发起失败: $e', error: true);
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

  /// 主列表：已配对设备全量（含离线）；在线优先排序。
  /// 离线项保留可见并标「离线」，禁止发送。
  List<_PairedDevice> _pairedDeviceRows() {
    final byId = <String, Device>{for (final d in _devices) d.id: d};
    final rows = <_PairedDevice>[];
    for (final p in _peers) {
      final id = p['device_id'] as String? ?? '';
      if (id.isEmpty) continue;
      final dev = byId[id];
      final hint = (p['name_hint'] as String?)?.trim();
      final name = (hint != null && hint.isNotEmpty)
          ? hint
          : (dev?.name.isNotEmpty == true ? dev!.name : '未知设备');
      rows.add(_PairedDevice(
        id: id,
        name: name,
        platform: (p['platform'] as String?) ?? dev?.platform ?? '',
        ip: dev?.ip ?? '',
        transferPort: dev?.transferPort ?? 0,
        gatewayPort: dev?.gatewayPort ?? 0,
        // peers.online = 发现表存活；dev != null 亦视为在线
        online: p['online'] == true || dev != null,
      ));
    }
    rows.sort((a, b) {
      if (a.online != b.online) return a.online ? -1 : 1;
      return a.name.toLowerCase().compareTo(b.name.toLowerCase());
    });
    return rows;
  }

  bool _isDeviceOnline(String id) {
    if (_devices.any((d) => d.id == id)) return true;
    for (final p in _peers) {
      if (p['device_id'] == id && p['online'] == true) return true;
    }
    return false;
  }

  Widget _devicesSection() {
    // 主列表只显示**已配对**设备；新设备的发现与配对统一走「添加设备」
    //（P2 入口改版：配对模式的开关/倒计时/确认码都在添加设备页内）
    final paired = _pairedDeviceRows();
    final onlineCount = paired.where((r) => r.online).length;
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Expanded(
                  child: Text(
                      '设备列表 (${paired.length} · 在线 $onlineCount)',
                      style: Theme.of(context).textTheme.titleMedium),
                ),
                FilledButton.tonalIcon(
                  icon: const Icon(Icons.add),
                  label: const Text('添加设备'),
                  onPressed: _openAddDevice,
                ),
              ],
            ),
            const SizedBox(height: 8),
            if (paired.isEmpty)
              const Padding(
                padding: EdgeInsets.symmetric(vertical: 8),
                child: Text(
                    '暂无已配对设备。点「添加设备」，在两台设备上都打开该页面即可互相发现并配对；配对过的设备从此始终可见。'),
              )
            else
              Column(
                children: paired.map((r) {
                  final on = r.online;
                  return ListTile(
                    enabled: on,
                    leading: Icon(
                      on ? Icons.circle : Icons.circle_outlined,
                      size: 14,
                      color: on ? Colors.green : Colors.grey,
                    ),
                    title: Text(
                      r.name,
                      style: TextStyle(
                        color: on ? null : Colors.grey[700],
                      ),
                    ),
                    subtitle: Text(
                      on
                          ? '${r.platform} · ${r.ip}:${r.transferPort} · 在线'
                          : '${r.platform} · 离线 · 无法发送',
                      style: TextStyle(
                        fontSize: 12,
                        color: on ? null : Colors.grey,
                      ),
                    ),
                    trailing: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        IconButton(
                          icon: Icon(
                            Icons.send,
                            size: 20,
                            color: on ? null : Colors.grey[400],
                          ),
                          tooltip: on ? '发送文件' : '对方离线，无法发送',
                          onPressed: on ? () => _showSendDialog(r.toDevice()) : null,
                        ),
                        PopupMenuButton<String>(
                          tooltip: '设备操作',
                          icon: const Icon(Icons.more_vert, size: 20),
                          onSelected: (v) async {
                            if (v == 'rename') {
                              await _renamePeer(r.id, r.name);
                            } else if (v == 'delete') {
                              await _deletePeer(r.id, r.name);
                            }
                          },
                          itemBuilder: (_) => const [
                            PopupMenuItem(
                              value: 'rename',
                              child: ListTile(
                                leading: Icon(Icons.edit_outlined),
                                title: Text('重命名'),
                                subtitle: Text('仅本机可见'),
                                contentPadding: EdgeInsets.zero,
                              ),
                            ),
                            PopupMenuItem(
                              value: 'delete',
                              child: ListTile(
                                leading: Icon(Icons.delete_outline),
                                title: Text('删除设备'),
                                subtitle: Text('解除配对与信任'),
                                contentPadding: EdgeInsets.zero,
                              ),
                            ),
                          ],
                        ),
                      ],
                    ),
                    onTap: on ? () => _showSendDialog(r.toDevice()) : null,
                  );
                }).toList(),
              ),
          ],
        ),
      ),
    );
  }

  /// 传输进行中（占用进度区）：pending / inProgress / interrupted（可续传）
  static bool _isTransferActive(TransferProgress p) =>
      p.status == TransferStatus.pending ||
      p.status == TransferStatus.inProgress ||
      p.status == TransferStatus.interrupted;

  /// 传输终态（进入接收/发送记录，不占进度区）
  static bool _isTransferTerminal(TransferProgress p) => !_isTransferActive(p);

  Widget _transfersSection() {
    final all = _progress.values.toList()
      ..sort((a, b) {
        if (a.status == TransferStatus.inProgress &&
            b.status != TransferStatus.inProgress) {
          return -1;
        }
        if (b.status == TransferStatus.inProgress &&
            a.status != TransferStatus.inProgress) {
          return 1;
        }
        return 0;
      });
    final active = all.where(_isTransferActive).toList();
    final recvHist =
        all.where((p) => _isTransferTerminal(p) && p.incoming).toList();
    final sendHist =
        all.where((p) => _isTransferTerminal(p) && !p.incoming).toList();

    return Column(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: [
        Card(
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('传输中 (${active.length})',
                    style: Theme.of(context).textTheme.titleMedium),
                const SizedBox(height: 8),
                if (active.isEmpty)
                  const Padding(
                    padding: EdgeInsets.symmetric(vertical: 8),
                    child: Text('暂无进行中的传输任务'),
                  )
                else
                  Column(
                    children: active.map((p) => _transferTile(p)).toList(),
                  ),
              ],
            ),
          ),
        ),
        const SizedBox(height: 16),
        Card(
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: _collapsibleTransferList(
              title: '接收记录',
              count: recvHist.length,
              open: _recvOpen,
              onToggle: () => setState(() => _recvOpen = !_recvOpen),
              items: recvHist,
              emptyText: '暂无接收记录',
              history: true,
            ),
          ),
        ),
        const SizedBox(height: 16),
        Card(
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: _collapsibleTransferList(
              title: '发送记录',
              count: sendHist.length,
              open: _sendOpen,
              onToggle: () => setState(() => _sendOpen = !_sendOpen),
              items: sendHist,
              emptyText: '暂无发送记录',
              history: true,
            ),
          ),
        ),
      ],
    );
  }

  Widget _collapsibleTransferList({
    required String title,
    required int count,
    required bool open,
    required VoidCallback onToggle,
    required List<TransferProgress> items,
    required String emptyText,
    bool history = false,
  }) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        InkWell(
          onTap: onToggle,
          borderRadius: BorderRadius.circular(4),
          child: Padding(
            padding: const EdgeInsets.symmetric(vertical: 4),
            child: Row(
              children: [
                Icon(
                  open ? Icons.expand_less : Icons.expand_more,
                  size: 20,
                  color: Colors.grey[700],
                ),
                const SizedBox(width: 4),
                Text('$title ($count)',
                    style: Theme.of(context)
                        .textTheme
                        .titleSmall
                        ?.copyWith(fontWeight: FontWeight.w600)),
                const Spacer(),
                if (count > 0 && !open)
                  Text('点击查看',
                      style: TextStyle(fontSize: 11, color: Colors.grey[600])),
              ],
            ),
          ),
        ),
        if (open) ...[
          if (items.isEmpty)
            Padding(
              padding: const EdgeInsets.only(left: 24, bottom: 4),
              child: Text(emptyText,
                  style: const TextStyle(color: Colors.grey, fontSize: 12)),
            )
          else
            Column(
              children:
                  items.map((p) => _transferTile(p, history: history)).toList(),
            ),
        ],
      ],
    );
  }

  /// 传输条目；`history=true` 时终态记录带 打开文件/文件夹/删除。
  Widget _transferTile(TransferProgress p, {bool history = false}) {
    final pct = p.fileSize > 0 ? p.bytesTransferred / p.fileSize : 0.0;
    final statusText = statusBadgeLabel(p);
    final statusColor = statusBadgeColor(p);
    final subtitle = transferSubtitle(p);
    final isTerminal = _isTransferTerminal(p);
    final path = p.filePath;

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
          // 历史记录操作：同一行图标按钮（打开文件 / 文件夹 / 删除）
          if (history && isTerminal) ...[
            if (path != null && path.isNotEmpty) ...[
              const SizedBox(height: 4),
              Text('路径: $path',
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(fontSize: 11, color: Colors.grey)),
            ],
            const SizedBox(height: 2),
            Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                if (path != null && path.isNotEmpty) ...[
                  IconButton(
                    tooltip: '打开文件',
                    visualDensity: VisualDensity.compact,
                    iconSize: 18,
                    icon: const Icon(Icons.open_in_new),
                    onPressed: () => openFile(path),
                  ),
                  IconButton(
                    tooltip: '所在文件夹',
                    visualDensity: VisualDensity.compact,
                    iconSize: 18,
                    icon: const Icon(Icons.folder_open),
                    onPressed: () => revealInFileManager(path),
                  ),
                ],
                IconButton(
                  tooltip: '删除记录',
                  visualDensity: VisualDensity.compact,
                  iconSize: 18,
                  color: Colors.red[700],
                  icon: const Icon(Icons.delete_outline),
                  onPressed: () => _confirmDeleteTransferRecord(p),
                ),
              ],
            ),
          ],
        ],
      ),
    );
  }

  /// 删除传输记录；接收记录可选同时删除本地文件。
  Future<void> _confirmDeleteTransferRecord(TransferProgress p) async {
    var deleteLocal = false;
    final canDeleteLocal = p.incoming && p.filePath != null && p.filePath!.isNotEmpty;
    final ok = await showDialog<bool>(
      context: context,
      builder: (ctx) => StatefulBuilder(
        builder: (ctx, setDialogState) => AlertDialog(
          title: const Text('删除记录'),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text('将从列表移除「${p.fileName}」。'),
              if (canDeleteLocal) ...[
                const SizedBox(height: 8),
                CheckboxListTile(
                  value: deleteLocal,
                  onChanged: (v) => setDialogState(() => deleteLocal = v ?? false),
                  title: const Text('同时删除本地文件'),
                  subtitle: Text(p.filePath ?? '', maxLines: 2),
                  contentPadding: EdgeInsets.zero,
                  controlAffinity: ListTileControlAffinity.leading,
                ),
              ],
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.pop(ctx, false),
              child: const Text('取消'),
            ),
            FilledButton(
              style: FilledButton.styleFrom(backgroundColor: Colors.red[700]),
              onPressed: () => Navigator.pop(ctx, true),
              child: const Text('删除'),
            ),
          ],
        ),
      ),
    );
    if (ok != true || !mounted) return;
    // 先删 daemon 持久化历史，再从 UI 移除
    try {
      await httpDelete('$kDaemonHttp/api/transfers/${p.fileId}/history');
    } catch (_) {/* 本地内存仍移除 */}
    setState(() => _progress.remove(p.fileId));
    if (deleteLocal && p.filePath != null && p.filePath!.isNotEmpty) {
      try {
        final f = File(p.filePath!);
        if (await f.exists()) {
          await f.delete();
        }
        // 同步 daemon 接收目录（若文件名可识别）
        final name = f.uri.pathSegments.isNotEmpty ? f.uri.pathSegments.last : '';
        if (name.isNotEmpty) {
          try {
            await httpDelete(
                '$kDaemonHttp/api/files/${Uri.encodeComponent(name)}');
          } catch (_) {/* 本地已删则忽略 daemon 侧失败 */}
        }
        _showTopToast('已删除本地文件');
      } catch (e) {
        _showTopToast('删除本地文件失败: $e', error: true);
      }
    } else {
      _showTopToast('已删除记录');
    }
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
    if (!d.online && !_isDeviceOnline(d.id)) {
      _showTopToast('对方设备离线，无法发送文件', error: true);
      return;
    }
    _pickAndSend(d);
  }

  Future<void> _pickAndSend(Device d) async {
    if (!d.online && !_isDeviceOnline(d.id)) {
      _showTopToast('对方设备离线，无法发送文件', error: true);
      return;
    }
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
      _showTopToast('无法获取所选文件的路径', error: true);
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

/// 已配对设备展示行：peers（全量配对）+ discovery（在线时有 IP/端口）
class _PairedDevice {
  final String id;
  final String name;
  final String platform;
  final String ip;
  final int transferPort;
  final int gatewayPort;
  final bool online;
  const _PairedDevice({
    required this.id,
    required this.name,
    required this.platform,
    required this.ip,
    required this.transferPort,
    required this.gatewayPort,
    required this.online,
  });

  Device toDevice() => Device(
        id: id,
        name: name,
        ip: ip,
        transferPort: transferPort,
        gatewayPort: gatewayPort,
        platform: platform,
        pair: true,
        online: online,
      );
}

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

Future<String> httpDelete(String url) async {
  final uri = Uri.parse(url);
  final client = HttpClient();
  try {
    final r = await client.deleteUrl(uri);
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
