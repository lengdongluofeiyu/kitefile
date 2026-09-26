/// 双端共享展示逻辑：格式化 + §3.5 状态文案（**唯一定义处**）。
///
/// 角标 / 副文案不再各端复制——改文案只改这里，两端同时生效。
library;

import 'package:flutter/material.dart';

import 'models.dart';

/// 字节数格式化（B/KB/MB/GB/TB/PB）
String formatBytes(int bytes) {
  if (bytes == 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  final i = (bytes.bitLength - 1) ~/ 10;
  final idx = i < units.length ? i : units.length - 1;
  return '${(bytes / (1 << (10 * idx))).toStringAsFixed(2)} ${units[idx]}';
}

/// 速率格式化
String formatSpeed(int bps) => '${formatBytes(bps)}/s';

/// 是否处于自动重试中（角标显示「重试中」的条件）
bool isRetrying(TransferProgress p) =>
    p.status == TransferStatus.inProgress && p.retryNote != null;

/// 状态角标文案（修复方案 §3.5 唯一源）。
///
/// 中断 ≠ 失败 ≠ 取消；自动重试期间 inProgress 角标改「重试中」。
String statusBadgeLabel(TransferProgress p) {
  switch (p.status) {
    case TransferStatus.pending:
      return '等待';
    case TransferStatus.inProgress:
      return isRetrying(p) ? '重试中' : '传输中';
    case TransferStatus.completed:
      return '已完成';
    case TransferStatus.failed:
      return '传输失败';
    case TransferStatus.canceled:
      return '已取消';
    case TransferStatus.interrupted:
      return '已中断';
  }
}

/// 状态角标颜色（与 §3.5 文案配套）。
Color statusBadgeColor(TransferProgress p) {
  switch (p.status) {
    case TransferStatus.pending:
      return Colors.grey;
    case TransferStatus.inProgress:
      return isRetrying(p) ? Colors.amber.shade800 : Colors.blue;
    case TransferStatus.completed:
      return Colors.green;
    case TransferStatus.failed:
      return Colors.red;
    case TransferStatus.canceled:
      return Colors.orange;
    case TransferStatus.interrupted:
      return Colors.amber.shade800;
  }
}

/// 卡片副文案（修复方案 §3.5 唯一源）。
///
/// - 已中断：`{原因} · 已完成 n% · 未完成部分将重新发送`
/// - 重试中：追加 `第 2/3 次重试流 3…`
/// - 已取消：`已放弃本次传输`（无错误细节时）
/// - 真失败：具体原因
/// - [showDirection] = true 时前缀「接收/发送 ·」（手机端样式）。
String transferSubtitle(TransferProgress p, {bool showDirection = false}) {
  final dir = showDirection ? '${p.incoming ? '接收' : '发送'} · ' : '';
  final pct = p.fileSize > 0 ? (p.bytesTransferred / p.fileSize * 100).round() : 0;
  switch (p.status) {
    case TransferStatus.interrupted:
      return '$dir${p.error ?? '传输中断'} · 已完成 $pct% · 未完成部分将重新发送';
    case TransferStatus.canceled:
      return '$dir${p.error ?? '已放弃本次传输'}';
    case TransferStatus.failed:
      return '$dir${p.error ?? '传输失败'}';
    case TransferStatus.inProgress:
      // 接收方字节已收满但仍在等发送方 verify 回执/保险丝窗口（A3.8）：
      // 这期间状态确为 InProgress，但显示「传输中 · 0 B/s」会让人以为卡死，
      // 明确标出「已收满，等待发送方校验」。
      if (p.incoming && p.fileSize > 0 && p.bytesTransferred >= p.fileSize) {
        return '$dir${formatBytes(p.fileSize)} / ${formatBytes(p.fileSize)}'
            ' · 已收满，等待发送方校验';
      }
      return '$dir${formatBytes(p.bytesTransferred)} / ${formatBytes(p.fileSize)}'
          ' · ${formatSpeed(p.speedBps)}'
          '${p.retryNote != null ? ' · ${p.retryNote}' : ''}';
    default:
      return '$dir${formatBytes(p.bytesTransferred)} / ${formatBytes(p.fileSize)}'
          '${p.error != null ? ' · ${p.error}' : ''}';
  }
}
