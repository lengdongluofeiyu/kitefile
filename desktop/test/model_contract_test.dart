// KiteFile 桌面端模型契约测试（工作流 B）
//
// 锁定 B 验收：**缺字段 / 错类型一律降级，桌面不崩**。
// 历史事故：`(x as num)` / `as String` 硬转在字段缺失或类型漂移时抛
// TypeError，把整条 WS 消息（乃至监听回调）打死。

import 'package:flutter_test/flutter_test.dart';

import 'package:kitefile_desktop/main.dart';

void main() {
  group('TransferProgress.fromJson', () {
    test('完整样例消息（与 core 契约测试同一份字段集）', () {
      final p = TransferProgress.fromJson({
        'file_id': 'fid',
        'file_name': 'a.bin',
        'file_size': 10,
        'bytes_transferred': 4,
        'chunks_done': 1,
        'chunks_total': 2,
        'speed_bps': 100,
        'status': 'Interrupted',
        'error': '流 2/2 失败（连接超时）',
        'incoming': true,
        'file_path': null,
        'retry_note': '第 2/3 次重试流 2…',
      });
      expect(p.fileId, 'fid');
      expect(p.fileSize, 10);
      expect(p.status, TransferStatus.interrupted);
      expect(p.error, '流 2/2 失败（连接超时）');
      expect(p.incoming, isTrue);
      expect(p.retryNote, '第 2/3 次重试流 2…');
    });

    test('缺全部字段：降级默认值，不抛异常', () {
      final p = TransferProgress.fromJson({});
      expect(p.fileId, '');
      expect(p.fileName, '');
      expect(p.fileSize, 0);
      expect(p.status, TransferStatus.pending);
      expect(p.incoming, isFalse);
      expect(p.error, isNull);
      expect(p.retryNote, isNull);
    });

    test('错类型：数字冒充字符串 / 字符串冒充数字 / 非法状态', () {
      final p = TransferProgress.fromJson({
        'file_id': 42, // 数字
        'file_size': 'abc', // 非法字符串
        'bytes_transferred': '99', // 合法字符串数字 → 99
        'chunks_done': 1.9, // 浮点 → 1
        'status': 123, // 数字
        'error': 7, // 数字
        'incoming': 'yes', // 非 bool
        'retry_note': ['x'], // 列表
      });
      expect(p.fileId, '');
      expect(p.fileSize, 0);
      expect(p.bytesTransferred, 99);
      expect(p.chunksDone, 1);
      expect(p.status, TransferStatus.pending);
      expect(p.error, isNull);
      expect(p.incoming, isFalse);
      expect(p.retryNote, isNull);
    });

    test('未知状态字符串降级 pending（前向兼容）', () {
      final p = TransferProgress.fromJson({'status': 'SomethingNew'});
      expect(p.status, TransferStatus.pending);
    });

    test('全部六种状态 wire 字符串可解析（契约）', () {
      const cases = {
        'Pending': TransferStatus.pending,
        'InProgress': TransferStatus.inProgress,
        'Completed': TransferStatus.completed,
        'Failed': TransferStatus.failed,
        'Canceled': TransferStatus.canceled,
        'Interrupted': TransferStatus.interrupted,
      };
      cases.forEach((wire, want) {
        expect(TransferProgress.fromJson({'status': wire}).status, want,
            reason: '状态 $wire');
      });
    });
  });

  group('Device / IncomingEntry / WhoAmI', () {
    test('Device 缺字段/错类型不崩', () {
      final d = Device.fromJson({'id': 1, 'transfer_port': 'x'});
      expect(d.id, '');
      expect(d.transferPort, 7879); // 安全默认
    });

    test('IncomingEntry 空对象不崩', () {
      final e = IncomingEntry.fromJson({});
      expect(e.incomingId, '');
      expect(e.fileSize, 0);
      expect(e.batchIndex, isNull);
    });

    test('WhoAmI 错类型端口降级默认', () {
      final w = WhoAmI.fromJson({'gateway_port': 'oops'});
      expect(w.gatewayPort, 7878);
    });
  });
}
