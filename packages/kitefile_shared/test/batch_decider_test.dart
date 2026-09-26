// BatchDecider 单元测试（工作流 C 验收：**批决策逻辑仅一份**）
//
// 攒批窗口（600ms）、到齐即弹、迟到沿用、决策 70s 回收等时序全部用
// fake_async 虚拟时间驱动——不真等（测试闸门 E：时序可注入/缩短）。

import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:kitefile_shared/kitefile_shared.dart';

IncomingEntry _entry(
  String id, {
  String? batch,
  int? index,
  int? total,
  String name = 'f.bin',
}) =>
    IncomingEntry(
      incomingId: id,
      fileId: 'fid-$id',
      fileName: name,
      fileSize: 1,
      sha256: null,
      fromId: 'peer',
      fromName: 'peer',
      fromIp: '1.2.3.4',
      fromGatewayPort: 7878,
      fromTransferPort: 7879,
      batchId: batch,
      batchIndex: index,
      batchTotal: total,
    );

/// 测试桩：记录回调
class _Recorder {
  final singles = <IncomingEntry>[];
  final batches = <(String, List<IncomingEntry>)>[];
  final decides = <(String, bool, bool)>[];
  late final BatchDecider decider;

  _Recorder({Duration gather = kBatchGatherWindow, Duration ttl = kBatchDecisionTtl}) {
    decider = BatchDecider(
      onShowSingle: singles.add,
      onShowBatch: (id, entries) => batches.add((id, entries)),
      onDecideSingle: (id, accept, quiet) => decides.add((id, accept, quiet)),
      gatherWindow: gather,
      decisionTtl: ttl,
    );
  }
}

void main() {
  group('BatchDecider（攒批—决策—迟到沿用，双端唯一实现）', () {
    test('单文件（无批次）直接弹单窗', () {
      final r = _Recorder();
      r.decider.handle(_entry('s1'));
      expect(r.singles.map((e) => e.incomingId), ['s1']);
      expect(r.batches, isEmpty);
    });

    test('同批次到齐立即弹批窗，条目按 batchIndex 排序', () {
      fakeAsync((async) {
        final r = _Recorder();
        r.decider.handle(_entry('b1', batch: 'g1', index: 1, total: 2));
        expect(r.batches, isEmpty, reason: '未到齐不得弹窗');
        r.decider.handle(_entry('b0', batch: 'g1', index: 0, total: 2));
        expect(r.batches.length, 1, reason: '到齐立即弹');
        final (id, entries) = r.batches.single;
        expect(id, 'g1');
        expect(entries.map((e) => e.incomingId), ['b0', 'b1'],
            reason: '必须按 batchIndex 排序');
        async.elapse(const Duration(seconds: 1));
        expect(r.batches.length, 1, reason: '弹过之后不得重复弹');
      });
    });

    test('未到齐：攒批窗口 600ms 超时后弹已到达条目', () {
      fakeAsync((async) {
        final r = _Recorder();
        r.decider.handle(_entry('only', batch: 'g2', index: 0, total: 3));
        expect(r.batches, isEmpty);
        async.elapse(const Duration(milliseconds: 599));
        expect(r.batches, isEmpty, reason: '窗口未到不弹');
        async.elapse(const Duration(milliseconds: 2));
        expect(r.batches.length, 1);
        expect(r.batches.single.$2.single.incomingId, 'only');
      });
    });

    test('记录决策后：迟到的同批条目沿用决策，不弹窗', () {
      fakeAsync((async) {
        final r = _Recorder();
        r.decider.recordDecision('g3', true);
        r.decider.handle(_entry('late', batch: 'g3', index: 0, total: 1));
        expect(r.batches, isEmpty, reason: '已决策批次不再弹窗');
        expect(r.singles, isEmpty);
        expect(r.decides, [('late', true, true)], reason: '迟到条目静默沿用接受');
      });
    });

    test('拒绝决策同样沿用；决策 70s 后回收、重新攒批', () {
      fakeAsync((async) {
        final r = _Recorder();
        r.decider.recordDecision('g4', false);
        r.decider.handle(_entry('late-r', batch: 'g4', index: 0, total: 1));
        expect(r.decides, [('late-r', false, true)]);

        // 超过 TTL：记录被回收，同批条目重新进入攒批流程
        async.elapse(kBatchDecisionTtl + const Duration(seconds: 1));
        expect(r.decider.decisionFor('g4'), isNull);
        r.decider.handle(_entry('after-ttl', batch: 'g4', index: 0, total: 1));
        expect(r.decides.length, 1, reason: '回收后不得再沿用');
        async.elapse(kBatchGatherWindow + const Duration(milliseconds: 10));
        expect(r.batches.length, 1, reason: '回收后重新攒批弹窗');
      });
    });

    test('重复条目（同一 incoming_id）只入列一次', () {
      fakeAsync((async) {
        final r = _Recorder();
        r.decider.handle(_entry('dup', batch: 'g5', index: 0, total: 3));
        r.decider.handle(_entry('dup', batch: 'g5', index: 0, total: 3));
        async.elapse(kBatchGatherWindow + const Duration(milliseconds: 10));
        expect(r.batches.single.$2.length, 1);
      });
    });

    test('dispose 后计时器全部取消（页面销毁不泄漏）', () {
      fakeAsync((async) {
        final r = _Recorder();
        r.decider.handle(_entry('d1', batch: 'g6', index: 0, total: 3));
        r.decider.dispose();
        async.elapse(const Duration(seconds: 10));
        expect(r.batches, isEmpty, reason: 'dispose 后不得再弹窗');
      });
    });
  });

  group('§3.5 状态文案（唯一源）', () {
    TransferProgress p(TransferStatus s, {String? error, String? retryNote}) =>
        TransferProgress(
          fileId: 'f',
          fileName: 'a.bin',
          fileSize: 100,
          bytesTransferred: 48,
          chunksDone: 1,
          chunksTotal: 2,
          speedBps: 0,
          status: s,
          error: error,
          retryNote: retryNote,
        );

    test('角标：中断 ≠ 失败 ≠ 取消；重试中覆盖传输中', () {
      expect(statusBadgeLabel(p(TransferStatus.interrupted)), '已中断');
      expect(statusBadgeLabel(p(TransferStatus.failed)), '传输失败');
      expect(statusBadgeLabel(p(TransferStatus.canceled)), '已取消');
      expect(statusBadgeLabel(p(TransferStatus.inProgress)), '传输中');
      expect(
          statusBadgeLabel(p(TransferStatus.inProgress, retryNote: '第 2/3 次')),
          '重试中');
    });

    test('中断副文案：原因 + 完成度 + 可续说明', () {
      final s = transferSubtitle(
        p(TransferStatus.interrupted, error: '流 2/2 失败（连接超时）'),
      );
      expect(s, contains('流 2/2 失败（连接超时）'));
      expect(s, contains('已完成 48%'));
      expect(s, contains('未完成部分将重新发送'));
    });

    test('取消/失败副文案（§3.5）', () {
      expect(transferSubtitle(p(TransferStatus.canceled)), '已放弃本次传输');
      expect(transferSubtitle(p(TransferStatus.failed, error: '磁盘空间不足')),
          '磁盘空间不足');
    });

    test('方向前缀（手机端样式）', () {
      final s = transferSubtitle(
        p(TransferStatus.canceled),
        showDirection: true,
      );
      expect(s, startsWith('发送 · '));
    });

    test('接收方收满但仍在等 verify：标「已收满，等待发送方校验」', () {
      // 真机不一致场景：字节 100% 但接收方还没 finalize——
      // 不能显示「传输中 · 0 B/s」让人以为卡死
      final waiting = TransferProgress(
        fileId: 'f',
        fileName: 'a.jpg',
        fileSize: 100,
        bytesTransferred: 100,
        chunksDone: 0,
        chunksTotal: 1,
        speedBps: 0,
        status: TransferStatus.inProgress,
        error: null,
        incoming: true,
      );
      expect(transferSubtitle(waiting), contains('已收满，等待发送方校验'));
      expect(transferSubtitle(waiting), isNot(contains('0 B/s')));

      // 发送方（非 incoming）同状态不误伤：仍显示正常速度行
      final sender = TransferProgress(
        fileId: 'f',
        fileName: 'a.jpg',
        fileSize: 100,
        bytesTransferred: 100,
        chunksDone: 0,
        chunksTotal: 1,
        speedBps: 1024,
        status: TransferStatus.inProgress,
        error: null,
        incoming: false,
      );
      expect(transferSubtitle(sender), isNot(contains('已收满')));
    });
  });
}
