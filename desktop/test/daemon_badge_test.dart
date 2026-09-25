// A 验收 5：daemon 被杀 → 在线徽标不再显示「已连接」。
//
// 手法：测试内起一个真实本地 HTTP server 充当 daemon（flutter test 默认把
// dart:io HttpClient 拦成 400，本用例显式 HttpOverrides.global = null 恢复
// 真实网络）。关键约束：widget 测试跑在 FakeAsync 区里，**真实 socket 的
// accept/响应必须在 tester.runAsync 的真实区里创建与探测**，否则事件被
// 假区吞掉、互相等死。先让 whoami 通（徽标变绿），再关掉 server 模拟
// daemon 被杀，等 3s 周期轮询把真值回写为离线（A3.6：轮询失败必须掉线）。

import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:kitefile_desktop/main.dart';

void main() {
  testWidgets('daemon 被杀后徽标从已连接切换为未运行（A验收5）', (tester) async {
    // 恢复真实网络：本用例自带本地 server，不吃 flutter test 的 400 拦截
    HttpOverrides.global = null;

    var hits = 0;
    // server 与首次探测都在真实区完成（见文件头注释）
    final result = await tester.runAsync(() async {
      final srv = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      srv.listen((req) async {
        hits++;
        if (req.uri.path == '/api/whoami') {
          final body = jsonEncode({
            'id': 'test-daemon',
            'name': 'Test Daemon',
            'platform': 'windows',
            'gateway_port': srv.port,
            'transfer_port': 7879,
          });
          req.response.statusCode = 200;
          req.response.headers.contentType = ContentType.json;
          req.response.write(body);
          await req.response.close();
        } else {
          req.response.statusCode = 404;
          await req.response.close();
        }
      });
      String probe;
      try {
        final body =
            await httpGet('http://127.0.0.1:${srv.port}/api/whoami');
        probe = 'ok:$body';
      } catch (e) {
        probe = 'err:$e';
      }
      return (srv, probe);
    });
    final (server, probe) = result!;
    expect(probe, startsWith('ok:'), reason: '测试 server 应可达（$probe）');

    // 桌面端走全局 daemonPort（kDaemonHttp 读它）——指到本测试 server
    daemonPort = server.port;

    await tester.pumpWidget(const KiteFileApp());
    // widget 的 whoami 在假区发起、真实事件在 runAsync 里落地，两者交替推进：
    // 多轮「runAsync 等真实 IO + pump 冲微任务/重建帧」，直到徽标变绿。
    var green = 0;
    for (var i = 0; i < 8 && green == 0; i++) {
      await tester.runAsync(
          () => Future<void>.delayed(const Duration(milliseconds: 300)));
      await tester.pump();
      green = find.text('守护进程已连接').evaluate().length;
    }
    expect(find.text('守护进程已连接'), findsWidgets,
        reason: 'whoami 可达时应显示已连接（probe=$probe, hits=$hits）');
    expect(find.text('守护进程未运行'), findsNothing);

    // 模拟 daemon 被杀：关掉 server（close 是真实 IO，必须在真实区 await）
    await tester.runAsync(() => server.close(force: true));

    // 3s 周期轮询（假时间）触发 → 连接失败（真实事件）→ 回写离线；
    // 与阶段 1 相同的交替推进，直到徽标掉线。
    await tester.pump(const Duration(seconds: 4));
    var offline = 0;
    for (var i = 0; i < 8 && offline == 0; i++) {
      await tester.runAsync(
          () => Future<void>.delayed(const Duration(milliseconds: 300)));
      await tester.pump();
      offline = find.text('守护进程未运行').evaluate().length;
    }
    expect(offline, greaterThan(0),
        reason: 'daemon 死后徽标必须掉线，不得停留在已连接');
    expect(find.text('守护进程已连接'), findsNothing,
        reason: '掉线后不得再出现已连接');

    // 收尾：销毁页面取消周期定时器；推进假时间消化 WS 重连的 5s 延迟
    //（mounted 保护使其不再排新定时器）
    await tester.pumpWidget(const SizedBox.shrink());
    await tester.pump(const Duration(seconds: 7));
  });
}
