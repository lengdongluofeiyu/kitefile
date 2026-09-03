import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import 'ffi.dart';

/// Android 原生方法通道（打开文件 / 存储权限）
const MethodChannel _nativeChannel = MethodChannel('ftcore/native');

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

/// FTCore 移动端（Android / iOS）
///
/// 架构：
/// - Android：App 启动时通过 FFI（libftcore.so）在本进程内拉起 Rust daemon，
///   Dart UI 统一走 HTTP/WS 调用 127.0.0.1:7878 —— 手机是平等的传输节点。
/// - 远程模式：设置弹窗切换到对端 IP，可当“遥控器”控制远端 daemon（开发调试用）。
/// - iOS：daemon 嵌入预留（接口一致）。

void main() {
  runApp(const FTCoreApp());
}

class FTCoreApp extends StatelessWidget {
  const FTCoreApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'FTCore',
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

@immutable
class Device {
  final String id;
  final String name;
  final String ip;
  final int transferPort;
  final int gatewayPort;
  final String platform;
  const Device({
    required this.id,
    required this.name,
    required this.ip,
    required this.transferPort,
    required this.gatewayPort,
    required this.platform,
  });

  factory Device.fromJson(Map<String, dynamic> j) => Device(
        id: j['id'] as String,
        name: j['name'] as String,
        ip: j['ip'] as String,
        transferPort: (j['transfer_port'] as num).toInt(),
        gatewayPort: (j['gateway_port'] as num).toInt(),
        platform: j['platform'] as String,
      );
}

@immutable
class WhoAmI {
  final String id;
  final String name;
  final String platform;
  final int gatewayPort;
  final int transferPort;
  const WhoAmI({
    required this.id,
    required this.name,
    required this.platform,
    required this.gatewayPort,
    required this.transferPort,
  });

  factory WhoAmI.fromJson(Map<String, dynamic> j) => WhoAmI(
        id: j['id'] as String,
        name: j['name'] as String,
        platform: j['platform'] as String,
        gatewayPort: (j['gateway_port'] as num).toInt(),
        transferPort: (j['transfer_port'] as num).toInt(),
      );
}

enum TransferStatus { pending, inProgress, completed, failed, canceled }

TransferStatus _parseStatus(String s) {
  switch (s) {
    case 'Pending':
      return TransferStatus.pending;
    case 'InProgress':
      return TransferStatus.inProgress;
    case 'Completed':
      return TransferStatus.completed;
    case 'Failed':
      return TransferStatus.failed;
    case 'Canceled':
      return TransferStatus.canceled;
    default:
      return TransferStatus.pending;
  }
}

@immutable
class TransferProgress {
  final String fileId;
  final String fileName;
  final int fileSize;
  final int bytesTransferred;
  final int chunksDone;
  final int chunksTotal;
  final int speedBps;
  final TransferStatus status;
  final String? error;

  /// true = 接收方视角；false = 发送方视角
  final bool incoming;

  /// 接收完成后的最终保存路径（仅接收方 Completed 时有值）
  final String? filePath;
  const TransferProgress({
    required this.fileId,
    required this.fileName,
    required this.fileSize,
    required this.bytesTransferred,
    required this.chunksDone,
    required this.chunksTotal,
    required this.speedBps,
    required this.status,
    required this.error,
    this.incoming = false,
    this.filePath,
  });

  factory TransferProgress.fromJson(Map<String, dynamic> j) => TransferProgress(
        fileId: j['file_id'] as String,
        fileName: j['file_name'] as String,
        fileSize: ((j['file_size'] as num?) ?? 0).toInt(),
        bytesTransferred: ((j['bytes_transferred'] as num?) ?? 0).toInt(),
        chunksDone: ((j['chunks_done'] as num?) ?? 0).toInt(),
        chunksTotal: ((j['chunks_total'] as num?) ?? 0).toInt(),
        speedBps: ((j['speed_bps'] as num?) ?? 0).toInt(),
        status: _parseStatus(j['status'] as String? ?? 'Pending'),
        error: j['error'] as String?,
        incoming: (j['incoming'] as bool?) ?? false,
        filePath: j['file_path'] as String?,
      );
}

/// 接收方待决的传入请求（WS incoming 事件 / GET /api/incoming）
@immutable
class IncomingEntry {
  final String incomingId;
  final String fileId;
  final String fileName;
  final int fileSize;
  final String? sha256;
  final String fromId;
  final String fromName;
  final String fromIp;
  const IncomingEntry({
    required this.incomingId,
    required this.fileId,
    required this.fileName,
    required this.fileSize,
    required this.sha256,
    required this.fromId,
    required this.fromName,
    required this.fromIp,
  });

  factory IncomingEntry.fromJson(Map<String, dynamic> j) => IncomingEntry(
        incomingId: j['incoming_id'] as String,
        fileId: j['file_id'] as String,
        fileName: j['file_name'] as String,
        fileSize: ((j['file_size'] as num?) ?? 0).toInt(),
        sha256: j['sha256'] as String?,
        fromId: j['from_id'] as String? ?? '',
        fromName: j['from_name'] as String? ?? '',
        fromIp: j['from_ip'] as String? ?? '',
      );
}

// ============ 主页 ============

class HomePage extends StatefulWidget {
  const HomePage({super.key});

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  // 默认指向本机；用户可改成对端 IP
  String _daemonHost = '127.0.0.1';
  final TextEditingController _hostController = TextEditingController(text: '127.0.0.1');

  String get _httpBase => 'http://$_daemonHost:7878';
  String get _wsBase => 'ws://$_daemonHost:7878/ws/progress';

  WhoAmI? _me;
  List<Device> _devices = [];
  List<String> _receivedFiles = [];
  /// 当前接收目录（设置页展示；用于“已接收文件”打开按钮）
  String? _receiveDir;
  final Map<String, TransferProgress> _progress = {};
  /// 待决定的传入请求（incoming_id → entry）
  final Map<String, IncomingEntry> _incoming = {};
  WebSocket? _ws;
  Timer? _refreshTimer;
  bool _daemonOnline = false;

  @override
  void initState() {
    super.initState();
    _bootstrap();
  }

  /// 启动序列：
  /// 1. Android：FFI 拉起本进程内 Rust daemon（监听 127.0.0.1:7878）
  /// 2. 连接 daemon（本机或远程）→ 订阅设备 / 进度 / 事件
  Future<void> _bootstrap() async {
    // Android 上拉起内嵌 daemon（幂等；已初始化则直接返回成功）
    try {
      final ok = await initFtcoreDaemon();
      if (kDebugMode) {
        // ignore: avoid_print
        print('[ftcore] embedded daemon init: $ok');
      }
    } catch (e) {
      if (kDebugMode) {
        // ignore: avoid_print
        print('[ftcore] embedded daemon failed: $e');
      }
    }
    await _initDaemon();
  }

  @override
  void dispose() {
    _ws?.close();
    _refreshTimer?.cancel();
    _hostController.dispose();
    super.dispose();
  }

  Future<void> _initDaemon() async {
    // 内嵌 daemon 的 gateway 端口绑定是异步的：FFI init 返回 ≠ 已可连接。
    // 轮询等待就绪（与桌面端行为一致）。
    await _waitDaemonReady(timeout: const Duration(seconds: 10));
    if (_daemonOnline) {
      _refreshReceiveDir();
      _refreshDevices();
      _refreshReceivedFiles();
      _refreshTimer = Timer.periodic(const Duration(seconds: 3), (_) {
        _refreshDevices();
        _refreshReceivedFiles();
      });
      _connectWs();
    }
  }

  /// 轮询 daemon 直到 /api/whoami 可达或超时
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

  Future<void> _reconnect(String host) async {
    setState(() {
      _daemonHost = host;
      _me = null;
      _devices = [];
      _receivedFiles = [];
      _receiveDir = null;
      _progress.clear();
      _incoming.clear();
      _daemonOnline = false;
    });
    await _ws?.close();
    _ws = null;
    _refreshTimer?.cancel();
    await _initDaemon();
  }

  Future<void> _fetchWhoAmI() async {
    try {
      final r = await httpGet('$_httpBase/api/whoami');
      final me = WhoAmI.fromJson(jsonDecode(r) as Map<String, dynamic>);
      setState(() {
        _me = me;
        _daemonOnline = true;
      });
    } catch (_) {
      setState(() => _daemonOnline = false);
    }
  }

  Future<void> _refreshDevices() async {
    try {
      final r = await httpGet('$_httpBase/api/devices');
      final list = (jsonDecode(r) as List).cast<Map<String, dynamic>>();
      setState(() {
        _devices = list.map(Device.fromJson).toList();
      });
    } catch (_) {}
  }

  Future<void> _connectWs() async {
    try {
      _ws = await WebSocket.connect(_wsBase);
      _ws!.listen((data) {
        if (data is! String) return;
        try {
          final j = jsonDecode(data) as Map<String, dynamic>;
          // 按 event_type 分发：progress / incoming / incoming_resolved
          switch (j['event_type'] as String? ?? 'progress') {
            case 'incoming':
              final entry = IncomingEntry.fromJson(j);
              if (mounted) {
                setState(() => _incoming[entry.incomingId] = entry);
                _showIncomingDialog(entry);
              }
              break;
            case 'incoming_resolved':
              final id = j['incoming_id'] as String?;
              if (id != null && mounted) {
                setState(() => _incoming.remove(id));
              }
              break;
            default:
              final p = TransferProgress.fromJson(j);
              if (mounted) setState(() => _progress[p.fileId] = p);
          }
        } catch (_) {
          // 忽略无法解析的消息
        }
      });
    } catch (_) {
      Future.delayed(const Duration(seconds: 5), _connectWs);
    }
  }

  /// 接收方确认弹窗：接受 / 拒绝传入请求
  void _showIncomingDialog(IncomingEntry e) {
    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (_) => AlertDialog(
        title: const Text('收到文件'),
        content: Column(
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
          ],
        ),
        actions: [
          TextButton(
            onPressed: () {
              Navigator.pop(context);
              _decideIncoming(e.incomingId, false);
            },
            child: const Text('拒绝'),
          ),
          FilledButton(
            onPressed: () {
              Navigator.pop(context);
              _decideIncoming(e.incomingId, true);
            },
            child: const Text('接受'),
          ),
        ],
      ),
    );
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

  Future<void> _sendFile(Device target, String path, {String? fileName}) async {
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
    await httpPost('$_httpBase/api/cancel/$fileId', body: '');
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('FTCore'),
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
                const Spacer(),
                Text(_daemonHost, style: const TextStyle(color: Colors.grey, fontSize: 12)),
              ],
            ),
            const SizedBox(height: 6),
            if (_me == null)
              Text(
                _daemonOnline
                    ? '加载中...'
                    : _daemonHost == '127.0.0.1'
                        ? '内嵌守护进程未就绪。请重开应用；若持续失败请反馈日志。'
                        : '未连接守护进程。检查对端 IP，或点右上角修改。',
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
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('设备列表 (${_devices.length})', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 6),
            if (_devices.isEmpty)
              const Padding(
                padding: EdgeInsets.symmetric(vertical: 6),
                child: Text('未发现设备，请确认对端已启动并处于同一局域网。',
                    style: TextStyle(color: Colors.grey, fontSize: 13)),
              )
            else
              Column(
                children: _devices.map((d) {
                  return ListTile(
                    contentPadding: EdgeInsets.zero,
                    title: Text(d.name),
                    subtitle: Text('${d.platform} · ${d.ip}:${d.transferPort}',
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
    final statusText = {
      TransferStatus.pending: '等待',
      TransferStatus.inProgress: '进行中',
      TransferStatus.completed: '已完成',
      TransferStatus.failed: '失败',
      TransferStatus.canceled: '已取消',
    }[p.status]!;
    final statusColor = {
      TransferStatus.completed: Colors.green,
      TransferStatus.failed: Colors.red,
      TransferStatus.inProgress: Colors.blue,
      TransferStatus.canceled: Colors.orange,
      TransferStatus.pending: Colors.grey,
    }[p.status]!;

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
            '${p.incoming ? '接收' : '发送'} · ${formatBytes(p.bytesTransferred)} / ${formatBytes(p.fileSize)}'
            '${p.status == TransferStatus.inProgress ? ' · ${formatSpeed(p.speedBps)}' : ''}'
            '${p.error != null ? ' · ${p.error}' : ''}',
            style: const TextStyle(fontSize: 11, color: Colors.grey),
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
                  // 本机 daemon 且已知接收目录时，可直接打开文件
                  final canOpen = _daemonHost == '127.0.0.1' && _receiveDir != null;
                  return ListTile(
                    contentPadding: EdgeInsets.zero,
                    dense: true,
                    leading: const Icon(Icons.insert_drive_file, size: 20),
                    title: Text(name,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(fontSize: 13)),
                    subtitle: Text(
                      _receiveDir ?? (_daemonHost == '127.0.0.1' ? '接收目录' : '对端接收目录'),
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

  /// 设置弹窗：守护进程地址 + 设备名称 + 接收文件保存位置
  Future<void> _showSettingsDialog() async {
    String? currentDir;
    String? currentName;
    try {
      final r = await httpGet('$_httpBase/api/config');
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
                            '$_httpBase/api/config/device-name',
                            body: jsonEncode({'device_name': name}),
                          );
                          setDialogState(() => currentName = name);
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
                const Text('守护进程地址', style: TextStyle(fontWeight: FontWeight.bold)),
                const SizedBox(height: 6),
                Text(
                  _daemonHost == '127.0.0.1' ? '本机（127.0.0.1）' : _daemonHost,
                  style: const TextStyle(fontSize: 13, color: Colors.grey),
                ),
                const SizedBox(height: 8),
                TextField(
                  controller: _hostController,
                  keyboardType: TextInputType.number,
                  decoration: const InputDecoration(
                    border: OutlineInputBorder(),
                    labelText: '切换到其他 IP（当遥控器）',
                    hintText: '127.0.0.1',
                    isDense: true,
                  ),
                ),
                const SizedBox(height: 4),
                Align(
                  alignment: Alignment.centerRight,
                  child: TextButton(
                    onPressed: () {
                      final h = _hostController.text.trim();
                      if (h.isEmpty) return;
                      Navigator.pop(ctx);
                      _reconnect(h);
                    },
                    child: const Text('连接'),
                  ),
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
        ),
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
              // 逐个发送（每个文件独立传输任务，进度列表分别展示）。
              // file_name 显式携带：/proc/self/fd/N 路径推断不出原始文件名
              for (final f in picks) {
                _sendFile(d, f.path, fileName: f.name);
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

String formatBytes(int bytes) {
  if (bytes == 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  final i = (bytes.bitLength - 1) ~/ 10;
  final idx = i < units.length ? i : units.length - 1;
  return '${(bytes / (1 << (10 * idx))).toStringAsFixed(2)} ${units[idx]}';
}

String formatSpeed(int bps) => '${formatBytes(bps)}/s';

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
