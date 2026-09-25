// KiteFile 移动端 Widget 冒烟测试：验证主界面正常构建
//
// 收尾约定：dispose 页面后推进假时间，把 `_waitDaemonReady` 轮询里
// 尚未触发的 Future.delayed 消化掉（挂起 Timer 会触发测试框架断言）。

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:kitefile_mobile/main.dart';

void main() {
  testWidgets('HomePage 构建冒烟测试', (WidgetTester tester) async {
    await tester.pumpWidget(const KiteFileApp());

    // 标题与三个分区标题存在
    expect(find.text('KiteFile'), findsOneWidget);
    expect(find.text('本机 / 守护进程'), findsOneWidget);
    expect(find.text('设备列表 (0)'), findsOneWidget);
    expect(find.text('传输进度 (0)'), findsOneWidget);

    // 销毁页面（触发 dispose：取消周期定时器、移除生命周期观察者），
    // 再推进假时间让启动轮询的在途延迟落地——其 while 条件依赖 mounted，
    // 销毁后自然退出，不会再排新定时器。
    await tester.pumpWidget(const SizedBox.shrink());
    await tester.pump(const Duration(seconds: 1));
  });
}
