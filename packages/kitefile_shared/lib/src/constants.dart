/// 双端共享常量——与 core 对应定义保持一致（注释标明唯一源）。

/// daemon 网关端口候选。
/// **Dart 侧唯一定义处**；与 Rust `GATEWAY_PORT_CANDIDATES`（core/src/lib.rs）
/// 一一对应。默认端口可能被系统保留（Hyper-V/WSL/Docker 动态保留 TCP 端口），
/// daemon 会退避到备选，UI 必须按候选逐个探测。
const List<int> kGatewayPortCandidates = [7878, 17878, 27878];

/// whoami 探测端口序列：**当前已知端口优先**（命中过就记住），随后是全部候选。
///
/// 为什么必须扫候选：daemon 启动时若 7878 被系统保留区间占用会退避到
/// 17878，只认默认端口的 UI 会让守护进程「看起来从来没启动」（真机踩过）。
/// 当前端口放首位：既保证已发现端口零延迟命中，也方便测试注入临时端口
/// （`candidates` 可传空列表隔离本机真实 daemon）。
List<int> whoamiScanPorts(int current,
        [List<int> candidates = kGatewayPortCandidates]) =>
    <int>{current, ...candidates}.toList(growable: false);

/// incoming 请求决策超时（秒）。
/// 与 Rust `INCOMING_DECISION_TIMEOUT_SECS`（core/src/transfer.rs）一致：
/// 60s 内未接受 daemon 自动拒绝，UI 倒计时与此对齐。
const int kDecisionTimeoutSecs = 60;

/// 攒批窗口：同批 offer 由对端连续 POST，到达有先后但间隔极短；
/// 攒 600ms（或到齐）后再合成一张卡片弹出。
const Duration kBatchGatherWindow = Duration(milliseconds: 600);

/// 批决策记录回收窗口：晚于后端 60s 自动拒绝窗口 10s 即可丢弃
/// （此后不会再有同批条目到达，记录失去意义）。
const Duration kBatchDecisionTtl = Duration(seconds: 70);
