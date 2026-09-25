/// 攒批—决策—迟到沿用状态机（工作流 C：**只实现一次**，双端共享）。
///
/// 语义（与 desktop/mobile 原复制实现一致，且与 daemon 60s 决策窗口对齐）：
/// 1. 同批 offer 由对端连续 POST，到达有先后但间隔极短；
/// 2. 攒 [`kBatchGatherWindow`]（600ms）或到齐（`length >= batch_total`）
///    后合成一张卡片（[`onShowBatch`]）；
/// 3. 用户在批窗上做出的决策经 [`recordDecision`] 记录，
///    **迟到到达的同批条目沿用同一决策**（[`onDecideSingle`]），不再弹窗；
/// 4. 记录在 [`kBatchDecisionTtl`]（70s）后回收（后端 60s 未决策即自动拒绝，
///    此后不会再有同批条目）。
///
/// 平台侧只负责：HTTP 决策、弹窗 UI、通知。状态与计时全部在本类。
library;

import 'dart:async';

import 'constants.dart';
import 'models.dart';

class BatchDecider {
  BatchDecider({
    required this.onShowSingle,
    required this.onShowBatch,
    required this.onDecideSingle,
    this.gatherWindow = kBatchGatherWindow,
    this.decisionTtl = kBatchDecisionTtl,
  });

  /// 单文件（无批次）→ 平台弹单窗
  final void Function(IncomingEntry entry) onShowSingle;

  /// 批次到齐 / 攒批超时 → 平台弹批窗（entries 已按 batchIndex 排序）
  final void Function(String batchId, List<IncomingEntry> entries) onShowBatch;

  /// 迟到沿用：同批条目按已记录决策自动处理。
  /// `quiet` = 该决策是沿用而来（平台可静默失败、不打扰用户）。
  final void Function(String incomingId, bool accept, bool quiet) onDecideSingle;

  final Duration gatherWindow;
  final Duration decisionTtl;

  final Map<String, List<IncomingEntry>> _pending = {};
  final Map<String, Timer> _gatherTimers = {};
  final Map<String, bool> _decisions = {};
  final Map<String, Timer> _decisionTimers = {};

  /// 收到一个 incoming 请求（平台侧已做同一 incoming_id 的去重）。
  void handle(IncomingEntry entry) {
    final bid = entry.batchId;
    if (bid == null) {
      onShowSingle(entry);
      return;
    }

    // 该批次已经决定过了：迟到的条目沿用同一决定，别再弹窗烦用户
    final decided = _decisions[bid];
    if (decided != null) {
      onDecideSingle(entry.incomingId, decided, true);
      return;
    }

    final list = _pending.putIfAbsent(bid, () => []);
    if (!list.any((e) => e.incomingId == entry.incomingId)) list.add(entry);

    // 到齐了就立刻弹；否则再等等（对端可能还在发剩下的文件）
    _gatherTimers[bid]?.cancel();
    if (list.length >= (entry.batchTotal ?? list.length)) {
      _fire(bid);
    } else {
      _gatherTimers[bid] = Timer(gatherWindow, () => _fire(bid));
    }
  }

  void _fire(String batchId) {
    _gatherTimers.remove(batchId)?.cancel();
    final entries = _pending.remove(batchId);
    if (entries == null || entries.isEmpty) return;
    entries.sort((a, b) => (a.batchIndex ?? 0).compareTo(b.batchIndex ?? 0));
    onShowBatch(batchId, entries);
  }

  /// 平台在用户做出批决策时调用：先记录，再发 HTTP（迟到沿用的数据源）。
  void recordDecision(String batchId, bool accept) {
    _decisions[batchId] = accept;
    _gatherTimers.remove(batchId)?.cancel();
    _decisionTimers[batchId]?.cancel();
    _decisionTimers[batchId] = Timer(decisionTtl, () {
      _decisions.remove(batchId);
      _decisionTimers.remove(batchId);
    });
  }

  /// 当前批次的已记录决策（测试 / 调试用）
  bool? decisionFor(String batchId) => _decisions[batchId];

  /// 释放全部计时器与缓存（页面 dispose 时调用）。
  void dispose() {
    for (final t in _gatherTimers.values) {
      t.cancel();
    }
    _gatherTimers.clear();
    for (final t in _decisionTimers.values) {
      t.cancel();
    }
    _decisionTimers.clear();
    _pending.clear();
    _decisions.clear();
  }
}
