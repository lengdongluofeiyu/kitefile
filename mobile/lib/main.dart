import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:kitefile_shared/kitefile_shared.dart';

// 双端共享业务层（工作流 C）：模型 / §3.5 文案 / 攒批决策只写一份。
// 再导出一次，让同包测试与代码 `import 'main.dart'` 也能拿到这些类型。
export 'package:kitefile_shared/kitefile_shared.dart';

import 'ffi.dart';

/// Android 原生方法通道（打开文件 / 存储权限）
const MethodChannel _nativeChannel = MethodChannel('kitefile/native');

/// 用系统默认应用打开文件（ACTION_VIEW + FileProvider）
///
/// 成功返回 null，失败返回错误信息（供调用方 SnackBar 提示）。
Future<String?> openFileNative(String path) async {
  try {
    await _nativeChannel.invokeMethod('openFile', path);
    return null;
  } on PlatformException catch (e) {
    debugPrint('[openFile] failed: ${e.code} ${e.message}');
    return e.message ?? e.code;
  } on MissingPluginException {
    debugPrint('[openFile] native method not available');
    return '当前平台不支持打开文件';
  }
}

/// KiteFile 移动端（Android / iOS）
///
/// 架构：
/// - Android：App 启动时通过 FFI（libkitefile.so）在本进程内拉起 Rust daemon，
///   Dart UI 统一走 HTTP/WS 调用 127.0.0.1:7878 —— 手机是平等的传输节点。
/// - 前端**只连接本机守护进程**（阶段 5 移除遥控模式：跨机控制需要 mTLS
///   客户端证书，Dart HTTP 栈不支持；daemon 侧 `--remote-admin` 仍保留）。
/// - iOS：daemon 嵌入预留（接口一致）。

/// whoami 扫描端口列表（默认 = 共享候选；测试可注入以隔离本机真实 daemon）。
List<int> daemonScanPorts = kGatewayPortCandidates;

void main() {
  runApp(const KiteFileApp());
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

class _HomePageState extends State<HomePage> with WidgetsBindingObserver {
  // 固定指向本机（阶段 5 取消遥控模式：前端只连本机守护进程，
  // 跨机通信一律由 daemon↔daemon 走 mTLS，不经 Dart UI）
  final String _daemonHost = '127.0.0.1';
  /// daemon 实际监听的网关端口。默认端口可能被系统保留（Android 上少见，
  /// 但与桌面端共用同一套退避逻辑），探测到实际端口后更新。
  int _daemonPort = kGatewayPortCandidates.first;

  String get _httpBase => 'http://$_daemonHost:$_daemonPort';
  String get _wsBase => 'ws://$_daemonHost:$_daemonPort/ws/progress';

  WhoAmI? _me;
  List<Device> _devices = [];
  /// 已配对设备（GET /api/peers；设备列表展示规则与发送入口依赖它）
  List<Map<String, dynamic>> _peers = [];
  List<String> _receivedFiles = [];
  /// 当前接收目录（设置页展示；用于“已接收文件”打开按钮）
  String? _receiveDir;
  final Map<String, TransferProgress> _progress = {};
  /// 待决定的传入请求（incoming_id → entry）
  final Map<String, IncomingEntry> _incoming = {};
  /// 攒批—决策—迟到沿用状态机（工作流 C：共享实现，双端仅此一份）
  late final BatchDecider _batchDecider;
  WebSocket? _ws;
  /// WS 连接进行中标志：防止 whoami 成功触发与失败重试链并发叠加
  bool _wsConnecting = false;
  Timer? _refreshTimer;
  bool _daemonOnline = false;
  /// 前台保活服务当前是否已启动（A3.7；由进行中的传输驱动）
  bool _keepAliveOn = false;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _batchDecider = BatchDecider(
      onShowSingle: (entry) {
        if (mounted) _showIncomingDialog(entry);
      },
      onShowBatch: (batchId, entries) {
        if (mounted) _showBatchDialog(batchId, entries);
      },
      onDecideSingle: (incomingId, accept, quiet) {
        // 手机端单条决策无 quiet 分支（HTTP 失败同样只打日志）
        _decideIncoming(incomingId, accept);
      },
    );
    _bootstrap();
  }

  /// 应用生命周期（A3.7 手机存活）：
  /// - resumed：回前台立即复核在线真值与实时通道（后台期间定时器/WS 回调
  ///   可能被系统挂起，不能让徽标停在旧真值）；
  /// - detached：对称关闭进程内 daemon（释放端口与任务，之后可再次 init）；
  /// - paused/inactive：**不做假动作**——进程内 Rust 引擎照常传输，
  ///   进度是真实状态；进程若被系统杀死，进度随内存清零，重启不会谎报仍在传。
  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    super.didChangeAppLifecycleState(state);
    switch (state) {
      case AppLifecycleState.resumed:
        _fetchWhoAmI();
        if (_ws == null) _connectWs();
        _refreshDevices();
        _refreshReceivedFiles();
        _refreshTransfers();
        break;
      case AppLifecycleState.detached:
        shutdownFtcoreDaemon();
        break;
      default:
        break;
    }
  }

  /// 启动序列：
  /// 1. Android：FFI 拉起本进程内 Rust daemon（监听 127.0.0.1:7878）
  /// 2. 连接 daemon（本机或远程）→ 订阅设备 / 进度 / 事件
  Future<void> _bootstrap() async {
    // Android 上拉起内嵌 daemon（幂等；已初始化则直接返回成功）
    try {
      final ok = await initFtcoreDaemon();
      // 无条件打日志（release 也要能从 logcat 看到 init 结果——
      // 真机反馈「守护进程未就绪」时这是第一手线索）
      debugPrint('[kitefile] embedded daemon init: $ok');
    } catch (e) {
      debugPrint('[kitefile] embedded daemon failed: $e');
    }
    await _initDaemon();
  }

  @override
  void dispose() {
    _batchDecider.dispose();
    WidgetsBinding.instance.removeObserver(this);
    _ws?.close();
    _refreshTimer?.cancel();
    super.dispose();
  }

  Future<void> _initDaemon() async {
    // 内嵌 daemon 的 gateway 端口绑定是异步的：FFI init 返回 ≠ 已可连接。
    // 轮询等待就绪（与桌面端行为一致）。
    await _waitDaemonReady(timeout: const Duration(seconds: 10));
    // 等待期间页面可能已销毁：此时再创建周期轮询会没人回收（pending Timer）
    if (!mounted) return;
    // 周期轮询**无条件**创建：启动窗口错过（慢盘/首跑）绝不能永久离线，
    // 之后的周期 whoami 会继续发现晚启动的 daemon（与桌面端一致）。
    _refreshTimer = Timer.periodic(const Duration(seconds: 3), (_) {
      _refreshDevices();
      _refreshReceivedFiles();
      _fetchWhoAmI();
    });
    if (_daemonOnline) {
      _refreshReceiveDir();
      _refreshDevices();
      _refreshReceivedFiles();
      _refreshTransfers();
      // WS 首连由 _fetchWhoAmI 成功分支触发（带防抖），这里无需重复调用
    }
  }

  /// 轮询 daemon 直到 /api/whoami 可达或超时。
  ///
  /// 每轮都把候选端口试一遍：daemon 可能因为默认端口被占用而退避，
  /// 命中后 `_fetchWhoAmI` 会把实际端口写进 `_daemonPort`。
  Future<void> _waitDaemonReady({required Duration timeout}) async {
    final deadline = DateTime.now().add(timeout);
    while (mounted && DateTime.now().isBefore(deadline)) {
      await _fetchWhoAmI();
      if (_daemonOnline) return;
      await Future.delayed(const Duration(milliseconds: 500));
    }
    // 最后一次机会
    if (mounted && !_daemonOnline) await _fetchWhoAmI();
  }

  Future<void> _refreshReceiveDir() async {
    try {
      final r = await httpGet('$_httpBase/api/config');
      final dir = (jsonDecode(r) as Map<String, dynamic>)['receive_dir'] as String?;
      if (mounted) setState(() => _receiveDir = dir);
    } catch (_) {}
  }

  Future<void> _refreshReceivedFiles() async {
    try {
      final r = await httpGet('$_httpBase/api/files');
      final list = (jsonDecode(r) as List).cast<String>();
      if (mounted) setState(() => _receivedFiles = list);
    } catch (_) {}
  }

  /// 传输全量快照：daemon 缓存是权威状态（WS 断连期间的终态帧会丢，
  /// 回前台时用它对齐，避免本地卡在「进行中」谎报）。
  Future<void> _refreshTransfers() async {
    try {
      final r = await httpGet('$_httpBase/api/transfers');
      final list = (jsonDecode(r) as Map<String, dynamic>)['transfers'] as List?;
      if (list == null || !mounted) return;
      setState(() {
        for (final t in list) {
          final p = TransferProgress.fromJson(t as Map<String, dynamic>);
          if (p.fileId.isNotEmpty) _progress[p.fileId] = p;
        }
      });
      _syncKeepAlive();
    } catch (e) {
      debugPrint('[kitefile] transfers snapshot failed: $e');
    }
  }

  /// 前台保活联动（A3.7）：存在进行中的传输 → 启动原生前台服务；
  /// 全部到终态（或只剩中断/取消/失败）→ 停止。
  /// 失败仅打日志——保活是尽力而为，不阻断传输本身。
  Future<void> _syncKeepAlive() async {
    final active = _progress.values.any((p) =>
        p.status == TransferStatus.inProgress ||
        p.status == TransferStatus.pending);
    if (active == _keepAliveOn) return;
    _keepAliveOn = active;
    try {
      await _nativeChannel.invokeMethod(
          active ? 'startTransferKeepAlive' : 'stopTransferKeepAlive');
    } catch (e) {
      debugPrint('[kitefile] keep-alive toggle($active) failed: $e');
    }
  }

  Future<void> _fetchWhoAmI() async {
    // 扫描序列：当前已知端口优先，随后全部候选（whoamiScanPorts）。
    // daemon 可能因端口保留退避到备选，只认默认端口会「看起来没启动」。
    for (final p in whoamiScanPorts(_daemonPort, daemonScanPorts)) {
      try {
        final r = await httpGet('http://$_daemonHost:$p/api/whoami')
            .timeout(const Duration(milliseconds: 800));
        final me = WhoAmI.fromJson(jsonDecode(r) as Map<String, dynamic>);
        if (!mounted) return;
        setState(() {
          _daemonPort = p;
          _me = me;
          _daemonOnline = true;
        });
        // 首次连上（或 WS 掉线后尚未重连）→ 触发 WS；防抖避免重试链叠加
        if (_ws == null && !_wsConnecting) {
          _connectWs();
        }
        return;
      } catch (_) {
        // 该端口没响应，试下一个
      }
    }
    // 在线徽标唯一真值（A3.6）：所有候选都失败必须掉线，且不静默吞
    debugPrint('[kitefile] whoami unreachable on all candidates (host=$_daemonHost)');
    if (mounted) setState(() => _daemonOnline = false);
  }

  Future<void> _refreshDevices() async {
    try {
      final r = await httpGet('$_httpBase/api/devices');
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
      final r = await httpGet('$_httpBase/api/peers');
      final list = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
      if (mounted) setState(() => _peers = list);
    } catch (e) {
      debugPrint('[kitefile] peers refresh failed: $e');
    }
  }

  /// 对端配对请求弹窗：两屏比对确认码，确认 → POST /api/pair/decide
  Future<void> _showPairRequestDialog(Map<String, dynamic> j) async {
    final session = j['session'] as String? ?? '';
    final name = j['name'] as String? ?? '未知设备';
    final ip = j['ip'] as String? ?? '';
    final platform = j['platform'] as String? ?? '';
    final code = j['code'] as String? ?? '';
    if (session.isEmpty || !mounted) return;

    Future<void> decide(bool accept) async {
      try {
        await httpPost(
          '$_httpBase/api/pair/decide',
          body: jsonEncode({'session': session, 'accept': accept}),
        );
        await _refreshDevices();
        if (mounted) {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Text(accept ? '已与 $name 配对' : '已拒绝 $name 的配对请求'),
            ),
          );
        }
      } catch (e) {
        if (mounted) {
          ScaffoldMessenger.of(context)
              .showSnackBar(SnackBar(content: Text('配对失败: $e')));
        }
      }
    }

    await showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) => AlertDialog(
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
            const Text('确认码（请与对方屏幕核对）',
                style: TextStyle(fontSize: 12, color: Colors.grey)),
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
            const Text(
              '两台设备显示的数字必须一致；不一致请取消，可能有人在中间拦截。',
              style: TextStyle(fontSize: 12, color: Colors.orange),
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () {
              Navigator.pop(ctx);
              decide(false);
            },
            child: const Text('拒绝'),
          ),
          FilledButton(
            onPressed: () {
              Navigator.pop(ctx);
              decide(true);
            },
            child: const Text('确认配对'),
          ),
        ],
      ),
    );
  }

  /// 添加设备（P2 入口改版）：打开即开启配对模式，页内完成
  /// 「发现 → 发起 → 等确认」，关闭页面自动退出配对模式。
  /// 两台设备都打开此页即可互相发现；**120 秒到点即关、不自动续期**
  /// （配对窗口必须是有意义的限时），结束态可点「重试」重新开始一轮。
  Future<void> _openAddDevice() async {
    int ttl;
    try {
      final r = await httpPost('$_httpBase/api/pair/mode',
          body: jsonEncode({'enabled': true}));
      ttl = (jsonDecode(r) as Map<String, dynamic>)['seconds_left'] as int? ?? 120;
    } catch (e) {
      if (mounted) {
        debugPrint('[kitefile] 开启配对模式失败: $e ($_httpBase/api/pair/mode)');
        ScaffoldMessenger.of(context)
            .showSnackBar(SnackBar(content: Text('开启配对模式失败: $e')));
      }
      return;
    }
    if (!mounted) return;

    var devices = <Device>[];
    var peers = <Map<String, dynamic>>[];
    Future<void> pull() async {
      try {
        final r = await httpGet('$_httpBase/api/devices');
        devices = (jsonDecode(r) as List)
            .cast<Map<String, dynamic>>()
            .map(Device.fromJson)
            .toList();
      } catch (_) {/* 沿用上一帧 */}
      try {
        final r = await httpGet('$_httpBase/api/peers');
        peers = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
      } catch (_) {/* 沿用上一帧 */}
    }
    await pull();
    if (!mounted) return;

    String? waitId;
    String? waitName;
    String? waitCode;
    var expired = false; // 倒计时到点后的结束态（等「重试」重新开启）
    var refreshTick = 0; // 搜索期每 15s 触发一次 pair 标志同步（/api/pair/refresh）
    Timer? ticker;
    var stopped = false;
    Future<void> shutdown() async {
      stopped = true;
      ticker?.cancel();
      try {
        await httpPost('$_httpBase/api/pair/mode',
            body: jsonEncode({'enabled': false}));
      } catch (_) {/* 关闭失败靠 TTL 兜底 */}
      await _refreshDevices();
    }

    await showDialog<void>(
      context: context,
      barrierDismissible: true,
      builder: (ctx) => StatefulBuilder(builder: (ctx, setDialogState) {
        ticker ??= Timer.periodic(const Duration(seconds: 1), (t) async {
          if (stopped) {
            t.cancel();
            return;
          }
          if (waitId != null) {
            try {
              final r = await httpGet('$_httpBase/api/peers');
              final list = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
              if (list.any((p) => p['device_id'] == waitId)) {
                stopped = true;
                t.cancel();
                final name = waitName ?? '对方设备';
                await _refreshDevices();
                if (ctx.mounted) Navigator.pop(ctx);
                if (mounted) {
                  ScaffoldMessenger.of(context)
                      .showSnackBar(SnackBar(content: Text('已与 $name 配对')));
                }
                return;
              }
            } catch (_) {/* 轮询失败下一轮再试 */}
            // 倒计时照走：到点即关（confirm 不依赖配对模式，等待不受影响）
            if (!expired) {
              ttl -= 1;
              if (ttl <= 0) {
                ttl = 0;
                expired = true;
                try {
                  await httpPost('$_httpBase/api/pair/mode',
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
              await httpPost('$_httpBase/api/pair/mode',
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
                await httpPost('$_httpBase/api/pair/refresh', body: '{}');
              } catch (_) {/* 下一 tick 再试 */}
              await pull();
            }
          }
          if (ctx.mounted) setDialogState(() {});
        });

        final pairable = devices
            .where((d) => d.pair && !peers.any((p) => p['device_id'] == d.id))
            .toList();
        // 已发现但对方未开启配对模式：列出来置灰——让用户能区分
        //「没发现」和「发现了但对方没开」，而不是对着空白猜
        final notReady = devices
            .where((d) => !d.pair && !peers.any((p) => p['device_id'] == d.id))
            .toList();
        final pairedCount =
            devices.where((d) => peers.any((p) => p['device_id'] == d.id)).length;

        return AlertDialog(
          title: const Row(
            children: [
              Icon(Icons.add_circle_outline, size: 24),
              SizedBox(width: 8),
              Text('添加设备'),
            ],
          ),
          content: SizedBox(
            width: 420,
            child: waitId != null
                ? Column(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      Text('等待 ${waitName ?? '对方'} 确认…'),
                      const SizedBox(height: 12),
                      Container(
                        padding: const EdgeInsets.symmetric(
                            horizontal: 20, vertical: 10),
                        decoration: BoxDecoration(
                          color: Theme.of(ctx).colorScheme.primaryContainer,
                          borderRadius: BorderRadius.circular(8),
                        ),
                        child: Text(
                          waitCode ?? '',
                          style: const TextStyle(
                              fontSize: 36,
                              fontWeight: FontWeight.bold,
                              letterSpacing: 6),
                        ),
                      ),
                      const SizedBox(height: 8),
                      const Text(
                        '请与对方屏幕核对确认码，一致后由对方点「确认配对」；60 秒未确认将过期。',
                        textAlign: TextAlign.center,
                        style: TextStyle(fontSize: 12, color: Colors.grey),
                      ),
                      TextButton(
                        onPressed: () => setDialogState(() => waitId = null),
                        child: const Text('返回设备列表'),
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
                                    '$_httpBase/api/pair/mode',
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
                                  ScaffoldMessenger.of(ctx).showSnackBar(
                                    SnackBar(content: Text('重试失败: $e')),
                                  );
                                }
                              }
                            },
                          ),
                        ),
                      ] else ...[
                        const Text(
                          '两台设备都打开此页面即可互相发现；出现设备后点「配对」，两屏核对同一串确认码。',
                          style: TextStyle(fontSize: 12, color: Colors.grey),
                        ),
                        const SizedBox(height: 8),
                        if (pairable.isEmpty && notReady.isEmpty)
                          const Padding(
                            padding: EdgeInsets.symmetric(vertical: 16),
                            child: Center(
                              child: Text(
                                '正在搜索附近可配对的设备…\n'
                                '（要求对方也停在本页面）\n\n'
                                '若长时间无结果：请确认两台设备在同一 Wi-Fi，'
                                '且电脑端也打开了「添加设备」页。\n'
                                'Windows 电脑需放行防火墙 7880 端口。',
                                textAlign: TextAlign.center,
                                style: TextStyle(color: Colors.grey),
                              ),
                            ),
                          )
                        else ...[
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
                                      '$_httpBase/api/pair/start',
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
                                      waitName = d.name;
                                      waitCode = j['code'] as String? ?? '';
                                    });
                                  } catch (e) {
                                    if (ctx.mounted) {
                                      ScaffoldMessenger.of(ctx).showSnackBar(
                                        SnackBar(
                                            content: Text('发起配对失败: $e ($_httpBase)')),
                                      );
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

  Future<void> _connectWs() async {
    // 防抖：whoami 成功触发与失败重试链不得并发叠加（同一时刻至多一条链）
    if (_ws != null || _wsConnecting) return;
    _wsConnecting = true;
    try {
      final ws = await WebSocket.connect(_wsBase);
      _ws = ws;
      _wsConnecting = false;
      ws.listen(
        (data) {
          if (data is! String) return;
          try {
            final j = jsonDecode(data) as Map<String, dynamic>;
            // 按 event_type 分发：progress / incoming / incoming_resolved
            switch (j['event_type'] as String? ?? 'progress') {
              case 'incoming':
                final entry = IncomingEntry.fromJson(j);
                _onIncoming(entry);
                break;
              case 'incoming_resolved':
                final id = j['incoming_id'] as String?;
                if (id != null && mounted) {
                  setState(() => _incoming.remove(id));
                }
                break;
              case 'pair_request':
                // 对端发来配对请求（P3）→ 确认码弹窗
                _showPairRequestDialog(j);
                break;
              default:
                final p = TransferProgress.fromJson(j);
                if (mounted) {
                  setState(() => _progress[p.fileId] = p);
                  _syncKeepAlive();
                }
            }
          } catch (e) {
            debugPrint('[kitefile] ws message parse failed: $e');
          }
        },
        onDone: () {
          // WS 断开（进程退出/网络断）：立刻掉线真值 + 复核 whoami + 重连
          debugPrint('[kitefile] ws closed');
          if (mounted) setState(() => _daemonOnline = false);
          _ws = null;
          _fetchWhoAmI();
          if (mounted) {
            Future.delayed(const Duration(seconds: 5), () {
              if (mounted) _connectWs();
            });
          }
        },
        onError: (Object e) {
          debugPrint('[kitefile] ws error: $e');
          if (mounted) setState(() => _daemonOnline = false);
        },
      );
    } catch (e) {
      // 连接失败 ≠ 已建立后断开：daemon 存活由 whoami 轮询定真值，
      // 这里只留日志并重连，避免与 whoami 打架导致徽标抖动。
      debugPrint('[kitefile] ws connect failed: $e');
      _wsConnecting = false;
      // mounted 保护：页面已销毁就不再重连（否则定时器泄漏/幽灵连接）
      Future.delayed(const Duration(seconds: 5), () {
        if (mounted) _connectWs();
      });
    }
  }

  /// 收到一个 incoming 请求：登记后交给共享攒批状态机（工作流 C）。
  void _onIncoming(IncomingEntry entry) {
    if (!mounted) return;
    setState(() => _incoming[entry.incomingId] = entry);
    _batchDecider.handle(entry);
  }

  /// 批量接收弹窗：一次确认整批（60 秒未接受自动超时）。
  /// entries 已由 BatchDecider 按 batchIndex 排序。
  void _showBatchDialog(String batchId, List<IncomingEntry> entries) {
    if (entries.isEmpty || !mounted) return;
    final totalSize = entries.fold<int>(0, (s, e) => s + e.fileSize);
    var remaining = kDecisionTimeoutSecs;
    Timer? ticker;

    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (dialogCtx) => AlertDialog(
        title: Text('收到 ${entries.length} 个文件'),
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
            return Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('来自: ${entries.first.fromName} (${entries.first.fromIp})'),
                Text('合计: ${formatBytes(totalSize)}'),
                const SizedBox(height: 8),
                ...entries.map(
                  (e) => Padding(
                    padding: const EdgeInsets.symmetric(vertical: 2),
                    child: Text('${e.fileName}（${formatBytes(e.fileSize)}）',
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(fontSize: 13)),
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
            );
          },
        ),
        actions: [
          TextButton(
            onPressed: () {
              ticker?.cancel();
              Navigator.pop(dialogCtx);
              _decideBatch(batchId, false);
            },
            child: const Text('全部拒绝'),
          ),
          FilledButton(
            onPressed: () {
              ticker?.cancel();
              Navigator.pop(dialogCtx);
              _decideBatch(batchId, true);
            },
            child: Text('全部接受 (${entries.length})'),
          ),
        ],
      ),
    ).whenComplete(() => ticker?.cancel());
  }

  Future<void> _decideBatch(String batchId, bool accept) async {
    // 先记下决定：迟到到达的同批条目会据此自动沿用，不再弹窗（共享状态机）
    _batchDecider.recordDecision(batchId, accept);
    try {
      await httpPost('$_httpBase/api/incoming/batch-decide',
          body: jsonEncode({'batch_id': batchId, 'accept': accept}));
    } catch (_) {}
  }

  /// 接收方确认弹窗：接受 / 拒绝传入请求（60 秒未接受自动超时）
  void _showIncomingDialog(IncomingEntry e) {
    var remaining = kDecisionTimeoutSecs;
    Timer? ticker;
    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (dialogCtx) => AlertDialog(
        title: const Text('收到文件'),
        content: StatefulBuilder(
          builder: (ctx, setDialogState) {
            ticker ??= Timer.periodic(const Duration(seconds: 1), (t) {
              remaining -= 1;
              if (remaining <= 0) {
                t.cancel();
                if (ctx.mounted) Navigator.pop(ctx);
                _onIncomingTimeout(e.incomingId);
                return;
              }
              if (ctx.mounted) setDialogState(() {});
            });
            return Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('来自: ${e.fromName} (${e.fromIp})'),
                const SizedBox(height: 8),
                Text('文件: ${e.fileName}'),
                Text('大小: ${formatBytes(e.fileSize)}'),
                if (e.sha256 != null)
                  Text(
                    'SHA256: ${e.sha256!.substring(0, e.sha256!.length.clamp(0, 16))}…',
                    style: const TextStyle(fontSize: 11, color: Colors.grey),
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
            );
          },
        ),
        actions: [
          TextButton(
            onPressed: () {
              ticker?.cancel();
              Navigator.pop(dialogCtx);
              _decideIncoming(e.incomingId, false);
            },
            child: const Text('拒绝'),
          ),
          FilledButton(
            onPressed: () {
              ticker?.cancel();
              Navigator.pop(dialogCtx);
              _decideIncoming(e.incomingId, true);
            },
            child: const Text('接受'),
          ),
        ],
      ),
    ).whenComplete(() => ticker?.cancel());
  }

  void _onIncomingTimeout(String incomingId) {
    if (mounted) {
      setState(() => _incoming.remove(incomingId));
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('传输请求已超时（60 秒未接受）')),
      );
    }
  }

  Future<void> _decideIncoming(String incomingId, bool accept) async {
    final action = accept ? 'accept' : 'reject';
    try {
      await httpPost('$_httpBase/api/incoming/$incomingId/$action', body: '');
    } catch (_) {}
    if (mounted) {
      setState(() => _incoming.remove(incomingId));
    }
  }

  Future<void> _sendFile(Device target, String path,
      {String? fileName, SendBatch? batch}) async {
    try {
      // 显式判空而非用 Dart 3.8 的 null-aware element（key: ?value）：
      // 后者需要较新的 Flutter/Dart，旧版工具链会编译失败。这里保持最大兼容性。
      final body = <String, dynamic>{
        'target_ip': target.ip,
        'target_port': target.transferPort,
        'target_gateway_port': target.gatewayPort,
        'file_path': path,
      };
      if (fileName != null) body['file_name'] = fileName;
      if (batch != null) {
        body['batch_id'] = batch.batchId;
        body['batch_index'] = batch.index;
        body['batch_total'] = batch.total;
      }
      final r = await httpPost('$_httpBase/api/send', body: jsonEncode(body));
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
    try {
      await httpPost('$_httpBase/api/cancel/$fileId', body: '');
    } catch (e) {
      debugPrint('[kitefile] cancel failed: $e');
    }
  }

  /// 「继续传输」：只重发未完成段（A3.2）。本机是发送方或接收方均由 daemon 路由。
  Future<void> _resume(String fileId) async {
    try {
      await httpPost('$_httpBase/api/transfers/$fileId/resume', body: '{}');
    } catch (e) {
      debugPrint('[kitefile] resume failed: $e');
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('KiteFile'),
        actions: [
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 12),
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
                  _daemonOnline ? '已连接' : '未连接',
                  style: TextStyle(
                    fontSize: 12,
                    color: _daemonOnline ? Colors.green : Colors.orange,
                  ),
                ),
              ),
            ),
          ),
          IconButton(
            icon: const Icon(Icons.settings),
            onPressed: () => _showSettingsDialog(),
          ),
        ],
      ),
      body: ListView(
        padding: const EdgeInsets.all(12),
        children: [
          _meSection(),
          const SizedBox(height: 12),
          _devicesSection(),
          const SizedBox(height: 12),
          _transfersSection(),
          const SizedBox(height: 12),
          _receivedSection(),
        ],
      ),
    );
  }

  Widget _meSection() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Text('本机 / 守护进程', style: Theme.of(context).textTheme.titleMedium),
              ],
            ),
            const SizedBox(height: 6),
            if (_me == null)
              Text(
                _daemonOnline
                    ? '加载中...'
                    : '内嵌守护进程未就绪。请重开应用；若持续失败请反馈日志。',
                style: const TextStyle(color: Colors.grey, fontSize: 13),
              )
            else
              Wrap(
                spacing: 12,
                runSpacing: 4,
                children: [
                  _kv('名称', _me!.name),
                  _kv('平台', _me!.platform),
                  _kv('传输', ':${_me!.transferPort}'),
                ],
              ),
          ],
        ),
      ),
    );
  }

  Widget _devicesSection() {
    // 主列表只显示**已配对**设备；新设备的发现与配对统一走「添加设备」
    //（P2 入口改版：配对模式的开关/倒计时/确认码都在添加设备页内）
    final paired = _devices
        .where((d) => _peers.any((p) => p['device_id'] == d.id))
        .toList();
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Expanded(
                  child: Text('设备列表 (${paired.length})',
                      style: Theme.of(context).textTheme.titleMedium),
                ),
                FilledButton.tonalIcon(
                  icon: const Icon(Icons.add),
                  label: const Text('添加设备'),
                  onPressed: _openAddDevice,
                ),
              ],
            ),
            const SizedBox(height: 6),
            if (paired.isEmpty)
              const Padding(
                padding: EdgeInsets.symmetric(vertical: 6),
                child: Text(
                    '暂无已配对设备。点「添加设备」，在两台设备上都打开该页面即可互相发现并配对；配对过的设备从此始终可见。',
                    style: TextStyle(color: Colors.grey, fontSize: 13)),
              )
            else
              Column(
                children: paired.map((d) {
                  return ListTile(
                    contentPadding: EdgeInsets.zero,
                    title: Text(d.name),
                    subtitle: Text(
                        '${d.platform} · ${d.ip}:${d.transferPort} · 已配对',
                        style: const TextStyle(fontSize: 12)),
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
      ..sort((a, b) {
        if (a.status == TransferStatus.inProgress) return -1;
        if (b.status == TransferStatus.inProgress) return 1;
        return 0;
      });
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('传输进度 (${items.length})', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 6),
            if (items.isEmpty)
              const Padding(
                padding: EdgeInsets.symmetric(vertical: 6),
                child: Text('暂无传输任务', style: TextStyle(color: Colors.grey, fontSize: 13)),
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
    // 角标与副文案来自共享层（§3.5 唯一源，工作流 C）；手机端带方向前缀
    final statusText = statusBadgeLabel(p);
    final statusColor = statusBadgeColor(p);
    final subtitle = transferSubtitle(p, showDirection: true);

    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(child: Text(p.fileName, overflow: TextOverflow.ellipsis, maxLines: 1)),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
                decoration: BoxDecoration(
                  color: statusColor.withValues(alpha: 0.2),
                  borderRadius: BorderRadius.circular(4),
                ),
                child: Text(statusText,
                    style: TextStyle(color: statusColor, fontSize: 11)),
              ),
              if (p.status == TransferStatus.inProgress)
                IconButton(
                  icon: const Icon(Icons.close, size: 16),
                  padding: EdgeInsets.zero,
                  constraints: const BoxConstraints(),
                  onPressed: () => _cancel(p.fileId),
                ),
            ],
          ),
          const SizedBox(height: 4),
          LinearProgressIndicator(value: pct),
          const SizedBox(height: 2),
          Text(
            subtitle,
            style: const TextStyle(fontSize: 11, color: Colors.grey),
          ),
          // 已中断（§3.5）：继续传输 / 取消
          if (p.status == TransferStatus.interrupted)
            Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                TextButton(
                  style: TextButton.styleFrom(
                    visualDensity: VisualDensity.compact,
                    padding: const EdgeInsets.symmetric(horizontal: 8),
                  ),
                  child: const Text('继续传输'),
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
          if (p.incoming && p.status == TransferStatus.completed && p.filePath != null) ...[
            const SizedBox(height: 4),
            Align(
              alignment: Alignment.centerRight,
              child: TextButton.icon(
                icon: const Icon(Icons.open_in_new, size: 16),
                label: const Text('打开文件'),
                onPressed: () async {
                  final err = await openFileNative(p.filePath!);
                  if (err != null && mounted) {
                    ScaffoldMessenger.of(context).showSnackBar(
                      SnackBar(content: Text('打开失败: $err')),
                    );
                  }
                },
              ),
            ),
          ],
        ],
      ),
    );
  }

  Widget _kv(String k, String v) {
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        Text('$k: ', style: const TextStyle(color: Colors.grey, fontSize: 12)),
        Text(v, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 12)),
      ],
    );
  }

  /// 已接收文件列表（本机 daemon 的接收目录）
  Widget _receivedSection() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('已接收文件 (${_receivedFiles.length})',
                style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 6),
            if (_receivedFiles.isEmpty)
              const Padding(
                padding: EdgeInsets.symmetric(vertical: 6),
                child: Text('暂无已接收文件',
                    style: TextStyle(color: Colors.grey, fontSize: 13)),
              )
            else
              Column(
                children: _receivedFiles.map((name) {
                  // 已知接收目录时可直接打开文件（本机 daemon 固定 127.0.0.1）
                  final canOpen = _receiveDir != null;
                  return ListTile(
                    contentPadding: EdgeInsets.zero,
                    dense: true,
                    leading: const Icon(Icons.insert_drive_file, size: 20),
                    title: Text(name,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(fontSize: 13)),
                    subtitle: Text(
                      _receiveDir ?? '接收目录',
                      style: const TextStyle(fontSize: 11),
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                    trailing: canOpen
                        ? IconButton(
                            icon: const Icon(Icons.open_in_new, size: 18),
                            tooltip: '打开文件',
                            onPressed: () async {
                              final path = '$_receiveDir/$name';
                              final err = await openFileNative(path);
                              if (err != null && mounted) {
                                ScaffoldMessenger.of(context).showSnackBar(
                                  SnackBar(content: Text('打开失败: $err')),
                                );
                              }
                            },
                          )
                        : null,
                  );
                }).toList(),
              ),
          ],
        ),
      ),
    );
  }

  /// 设置弹窗：配对 + 守护进程地址 + 设备名称 + 接收文件保存位置
  Future<void> _showSettingsDialog() async {
    String? currentDir;
    String? currentName;
    try {
      final r = await httpGet('$_httpBase/api/config');
      final j = jsonDecode(r) as Map<String, dynamic>;
      currentDir = j['receive_dir'] as String?;
      currentName = j['device_name'] as String?;
    } catch (_) {}
    // 配对状态（开关倒计时 + 已配对列表）
    var pairOn = false;
    var pairTtl = 0;
    var peers = <Map<String, dynamic>>[];
    try {
      final r = await httpGet('$_httpBase/api/pair/mode');
      final j = jsonDecode(r) as Map<String, dynamic>;
      pairOn = j['enabled'] == true;
      pairTtl = (j['seconds_left'] as num?)?.toInt() ?? 0;
    } catch (_) {}
    try {
      final r = await httpGet('$_httpBase/api/peers');
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
                            '$_httpBase/api/pair/mode',
                            body: jsonEncode({'enabled': v}),
                          );
                          setDialogState(() {
                            pairOn = v;
                            if (v) pairTtl = 120;
                          });
                        } catch (e) {
                          if (ctx.mounted) {
                            ScaffoldMessenger.of(ctx).showSnackBar(
                              SnackBar(content: Text('切换配对模式失败: $e')),
                            );
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
                        '${p['platform'] ?? ''} · 已配对',
                        style: const TextStyle(fontSize: 12),
                      ),
                      trailing: IconButton(
                        icon: const Icon(Icons.delete_outline, size: 20),
                        tooltip: '解除配对',
                        onPressed: () async {
                          try {
                            await httpDelete(
                                '$_httpBase/api/peers/${p['device_id']}');
                            final r = await httpGet('$_httpBase/api/peers');
                            setDialogState(() {
                              peers = (jsonDecode(r) as List)
                                  .cast<Map<String, dynamic>>();
                            });
                            await _refreshDevices();
                          } catch (e) {
                            if (ctx.mounted) {
                              ScaffoldMessenger.of(ctx).showSnackBar(
                                SnackBar(content: Text('解除配对失败: $e')),
                              );
                            }
                          }
                        },
                      ),
                    ),
                ],
                const Divider(),
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
                            '$_httpBase/api/config/device-name',
                            body: jsonEncode({'device_name': name}),
                          );
                          setDialogState(() => currentName = name);
                          // 重新拉 whoami：_me 只在启动时取过一次，
                          // 不刷新的话「本机信息」会一直显示旧名字（如 device-xxxx）。
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
                const Divider(),
                const Text('接收文件保存位置', style: TextStyle(fontWeight: FontWeight.bold)),
                const SizedBox(height: 6),
                SelectableText(
                  currentDir ?? '（守护进程未运行，无法读取当前设置）',
                  style: TextStyle(
                    fontSize: 13,
                    color: currentDir == null ? Colors.orange : Colors.grey,
                  ),
                ),
                const SizedBox(height: 12),
                SizedBox(
                  width: double.infinity,
                  child: FilledButton.icon(
                    icon: const Icon(Icons.folder_open),
                    label: const Text('更改保存位置'),
                    onPressed: () => _pickReceiveDir(ctx, currentDir, (dir) {
                      setDialogState(() => currentDir = dir);
                    }),
                  ),
                ),
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
        },
      ),
    );
    nameController.dispose();
  }

  /// 系统目录选择器选保存位置 → 申请权限（公共目录）→ 调 daemon API 生效
  Future<void> _pickReceiveDir(
    BuildContext ctx,
    String? currentDir,
    void Function(String dir) onUpdated,
  ) async {
    // 1. 系统目录选择器（SAF，可浏览所有目录）
    final dir = await FilePicker.platform.getDirectoryPath(
      dialogTitle: '选择接收文件的保存目录',
    );
    if (dir == null || !ctx.mounted) return;

    // 2. 公共存储目录需要「所有文件访问」权限（应用专属目录除外）
    final isPublicStorage = dir.startsWith('/storage/') &&
        !dir.contains('/Android/data/') &&
        !dir.contains('/Android/obb/');
    if (isPublicStorage) {
      try {
        final granted = await _nativeChannel.invokeMethod<bool>('hasManageStorage') ?? false;
        if (!granted) {
          if (!ctx.mounted) return;
          ScaffoldMessenger.of(ctx).showSnackBar(
            const SnackBar(
              content: Text('需要「所有文件访问」权限才能写入所选目录，请在系统设置中授权后重试'),
              duration: Duration(seconds: 4),
            ),
          );
          await _nativeChannel.invokeMethod('requestManageStorage');
          return;
        }
      } on PlatformException {
        // 非 Android 平台无此方法，继续尝试设置
      } on MissingPluginException {
        // ignore
      }
    }

    // 3. 调 daemon API 设置新目录
    try {
      final r = await httpPost(
        '$_httpBase/api/config/receive-dir',
        body: jsonEncode({'receive_dir': dir}),
      );
      final nd = (jsonDecode(r) as Map<String, dynamic>)['receive_dir'] as String? ?? dir;
      _refreshReceiveDir();
      onUpdated(nd);
      if (ctx.mounted) {
        ScaffoldMessenger.of(ctx).showSnackBar(
          SnackBar(content: Text('保存位置已更新：$nd')),
        );
      }
    } catch (e) {
      if (ctx.mounted) {
        ScaffoldMessenger.of(ctx).showSnackBar(
          SnackBar(content: Text('设置失败: $e')),
        );
      }
    }
  }

  /// 系统文件选择器选文件（多选）→ 确认列表 → 逐个发送
  ///
  /// Android：原生 ACTION_OPEN_DOCUMENT + fd 直读（/proc/self/fd/N），
  /// 不经过 file_picker 的缓存拷贝——选 2GB 视频不再卡死 UI。
  void _showSendDialog(Device d) {
    _pickAndSend(d);
  }

  /// 选文件的统一入口，返回 null 表示用户取消。
  ///
  /// - Android：MethodChannel 调原生 ACTION_OPEN_DOCUMENT，
  ///   返回 /proc/self/fd/N 直读路径（零拷贝）；原生不可用时回退 file_picker
  /// - 其他平台：file_picker（返回真实路径）
  Future<List<PickedFileLite>?> _pickFiles() async {
    if (Platform.isAndroid) {
      try {
        final raw = await _nativeChannel.invokeMethod<List<dynamic>>('pickFiles');
        if (raw == null) return null; // 用户取消
        final picks = raw
            .map((e) => PickedFileLite.fromMap(Map<String, dynamic>.from(e as Map)))
            .where((p) => p.path.isNotEmpty)
            .toList();
        if (picks.isEmpty) {
          if (mounted) {
            ScaffoldMessenger.of(context).showSnackBar(
              const SnackBar(content: Text('无法获取所选文件的路径')),
            );
          }
          return null;
        }
        return picks;
      } on PlatformException catch (e) {
        // 原生选择器不可用/失败 → 回退 file_picker（缓存拷贝，慢但可用）
        debugPrint('[pickFiles] native failed (${e.code}), fallback to file_picker');
      } on MissingPluginException {
        debugPrint('[pickFiles] native method not available, fallback to file_picker');
      }
    }

    final result = await FilePicker.platform.pickFiles(
      type: FileType.any,
      allowMultiple: true,
      compressionQuality: 0, // 关闭压缩：传输原文件，避免低版本 Android 崩溃
      dialogTitle: '选择要发送的文件',
    );
    if (result == null || result.files.isEmpty) return null;
    final picks = result.files
        .where((f) => f.path != null)
        .map((f) => PickedFileLite(name: f.name, size: f.size, path: f.path!))
        .toList();
    if (picks.isEmpty) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('无法获取所选文件的路径')),
        );
      }
      return null;
    }
    return picks;
  }

  Future<void> _pickAndSend(Device d) async {
    // 1. 选文件（Android 原生 fd 方案，其他平台 file_picker）
    final picks = await _pickFiles();
    if (!mounted) return;

    // 用户取消
    if (picks == null || picks.isEmpty) return;

    // 3. 确认列表（多选时展示全部，点击确认逐个发送）
    showDialog<void>(
      context: context,
      builder: (_) => AlertDialog(
        title: Text('发送到 ${d.name}'),
        content: Column(
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
              // file_name 显式携带：/proc/self/fd/N 路径推断不出原始文件名
              if (picks.length > 1) {
                final batchId = 'b-${DateTime.now().microsecondsSinceEpoch}';
                for (var i = 0; i < picks.length; i++) {
                  _sendFile(d, picks[i].path,
                      fileName: picks[i].name,
                      batch: SendBatch(
                          batchId: batchId, index: i, total: picks.length));
                }
              } else {
                _sendFile(d, picks.first.path, fileName: picks.first.name);
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

/// 选中文件的轻量模型（原生选择器 / file_picker 统一）
@immutable
class PickedFileLite {
  final String name;
  final int size;
  final String path;
  const PickedFileLite({required this.name, required this.size, required this.path});

  factory PickedFileLite.fromMap(Map<String, dynamic> j) => PickedFileLite(
        name: (j['name'] as String?) ?? 'file',
        size: ((j['size'] as num?) ?? 0).toInt(),
        path: j['path'] as String? ?? '',
      );
}

/// formatBytes / formatSpeed 由共享包 kitefile_shared 提供（工作流 C）。

// ============ 简易 HTTP 客户端 ============

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
