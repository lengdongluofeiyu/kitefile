/// KiteFile 双端共享业务层（工作流 C）。
///
/// 收敛原则：**业务只写一份**——数据模型、§3.5 状态文案、攒批决策状态机
/// 全部放在本包；desktop / mobile 只保留平台壳（选文件、托盘/通知、布局）。
/// 双端 pubspec 以 path 依赖引用本包。
library;

export 'src/batch_decider.dart';
export 'src/constants.dart';
export 'src/models.dart';
export 'src/presentation.dart';
