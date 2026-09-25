/// 双端共享常量——与 core 对应定义保持一致（注释标明唯一源）。

/// daemon 网关端口候选。
/// **Dart 侧唯一定义处**；与 Rust `GATEWAY_PORT_CANDIDATES`（core/src/lib.rs）
/// 一一对应。默认端口可能被系统保留（Hyper-V/WSL/Docker 动态保留 TCP 端口），
/// daemon 会退避到备选，UI 必须按候选逐个探测。
const List<int> kGatewayPortCandidates = [7878, 17878, 27878];

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
