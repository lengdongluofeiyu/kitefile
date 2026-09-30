/// 双端共享数据模型（工作流 B/C）——与 core 序列化一一对应的客户端类型。
///
/// 解析纪律（工作流 B）：**缺字段 / 错类型一律降级为安全默认，禁止硬转崩溃**。
/// 历史事故：`(x as num)` / `as String` 遇到 null 或类型漂移直接抛 TypeError，
/// 把整条 WS 监听回调打死。契约测试见 desktop/test/model_contract_test.dart
/// 与 core/tests/protocol_test.rs。
library;

import 'package:flutter/foundation.dart';

// ---- JSON 安全解析助手 ----

/// JSON → int：null / 字符串数字 / 其它类型都安全降级。
int jsonInt(Object? v, [int fallback = 0]) {
  if (v is num) return v.toInt();
  if (v is String) return int.tryParse(v) ?? fallback;
  return fallback;
}

/// JSON → int?：仅有效整数保留，其余降级 null。
int? jsonIntOrNull(Object? v) {
  if (v is num) return v.toInt();
  if (v is String) return int.tryParse(v);
  return null;
}

/// JSON → String：非字符串（数字/null/对象）降级为 fallback。
String jsonStr(Object? v, [String fallback = '']) =>
    v is String ? v : fallback;

/// JSON → String?：仅字符串保留，其余降级 null。
String? jsonStrOrNull(Object? v) => v is String ? v : null;

// ---- 设备 / 本机信息 ----

@immutable
class Device {
  final String id;
  final String name;
  final String ip;
  final int transferPort;
  final int gatewayPort;
  final String platform;
  /// 对端是否处于配对模式（mDNS TXT `pair=1`；旧版对端不发 → false）。
  /// 展示规则（设计 §5.4）：已配对 ∨（未配对 ∧ pair）。
  final bool pair;
  /// 设备是否在线（发现表存活 / peers.online）。默认 false；离线不得发送。
  final bool online;
  const Device({
    required this.id,
    required this.name,
    required this.ip,
    required this.transferPort,
    required this.gatewayPort,
    required this.platform,
    this.pair = false,
    this.online = false,
  });

  factory Device.fromJson(Map<String, dynamic> j) => Device(
        id: jsonStr(j['id']),
        name: jsonStr(j['name']),
        ip: jsonStr(j['ip']),
        transferPort: jsonInt(j['transfer_port'], 7879),
        gatewayPort: jsonInt(j['gateway_port'], 7878),
        platform: jsonStr(j['platform']),
        pair: j['pair'] == true,
        online: j['online'] == true,
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
        id: jsonStr(j['id']),
        name: jsonStr(j['name']),
        platform: jsonStr(j['platform']),
        gatewayPort: jsonInt(j['gateway_port'], 7878),
        transferPort: jsonInt(j['transfer_port'], 7879),
      );
}

// ---- 传输状态与进度 ----

/// 传输状态（与 Rust `TransferStatus` 一一对应，契约测试锁定）。
/// `interrupted` = 已中断（可「继续传输」），**不等于** failed/canceled（§3.5）。
enum TransferStatus { pending, inProgress, completed, failed, canceled, interrupted }

/// 状态 wire 字符串 → 枚举（与 core 序列化契约对应，未知值降级 pending）。
TransferStatus parseTransferStatus(String s) {
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
    case 'Interrupted':
      return TransferStatus.interrupted;
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
  final bool incoming;
  final String? filePath;
  final String? retryNote;
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
    this.retryNote,
  });

  /// 安全解析（工作流 B）：缺字段/错类型一律降级为安全默认。
  factory TransferProgress.fromJson(Map<String, dynamic> j) => TransferProgress(
        fileId: jsonStr(j['file_id']),
        fileName: jsonStr(j['file_name']),
        fileSize: jsonInt(j['file_size']),
        bytesTransferred: jsonInt(j['bytes_transferred']),
        chunksDone: jsonInt(j['chunks_done']),
        chunksTotal: jsonInt(j['chunks_total']),
        speedBps: jsonInt(j['speed_bps']),
        status: parseTransferStatus(jsonStr(j['status'], 'Pending')),
        error: jsonStrOrNull(j['error']),
        incoming: j['incoming'] == true,
        filePath: jsonStrOrNull(j['file_path']),
        retryNote: jsonStrOrNull(j['retry_note']),
      );
}

// ---- 传入请求 / 批次 ----

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
  final int fromGatewayPort;
  final int fromTransferPort;
  final String? batchId;
  final int? batchIndex;
  final int? batchTotal;
  const IncomingEntry({
    required this.incomingId,
    required this.fileId,
    required this.fileName,
    required this.fileSize,
    required this.sha256,
    required this.fromId,
    required this.fromName,
    required this.fromIp,
    required this.fromGatewayPort,
    required this.fromTransferPort,
    this.batchId,
    this.batchIndex,
    this.batchTotal,
  });

  factory IncomingEntry.fromJson(Map<String, dynamic> j) => IncomingEntry(
        incomingId: jsonStr(j['incoming_id']),
        fileId: jsonStr(j['file_id']),
        fileName: jsonStr(j['file_name']),
        fileSize: jsonInt(j['file_size']),
        sha256: jsonStrOrNull(j['sha256']),
        fromId: jsonStr(j['from_id']),
        fromName: jsonStr(j['from_name']),
        fromIp: jsonStr(j['from_ip']),
        fromGatewayPort: jsonInt(j['from_gateway_port'], 7878),
        fromTransferPort: jsonInt(j['from_transfer_port'], 7879),
        batchId: jsonStrOrNull(j['batch_id']),
        batchIndex: jsonIntOrNull(j['batch_index']),
        batchTotal: jsonIntOrNull(j['batch_total']),
      );
}

/// 一次多选发送里，单个文件所属的批次信息（发给 daemon 用）
class SendBatch {
  final String batchId;
  final int index;
  final int total;
  const SendBatch(
      {required this.batchId, required this.index, required this.total});
}
