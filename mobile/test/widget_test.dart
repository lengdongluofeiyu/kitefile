// FTCore 移动端 Widget 冒烟测试：验证主界面正常构建

import 'package:flutter_test/flutter_test.dart';

import 'package:ftcore_mobile/main.dart';

void main() {
  testWidgets('HomePage 构建冒烟测试', (WidgetTester tester) async {
    await tester.pumpWidget(const FTCoreApp());

    // 标题与三个分区标题存在
    expect(find.text('FTCore'), findsOneWidget);
    expect(find.text('本机 / 守护进程'), findsOneWidget);
    expect(find.text('设备列表 (0)'), findsOneWidget);
    expect(find.text('传输进度 (0)'), findsOneWidget);
  });
}
