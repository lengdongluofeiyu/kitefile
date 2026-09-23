//! 平台抽象：用于跨平台构建（Windows / Android / iOS / macOS）
//!
//! 不同平台的差异点：
//! - 文件路径根目录（接收目录、文档目录）
//! - 权限申请（Android 的 MANAGE_EXTERNAL_STORAGE、iOS 的 Photos）
//! - 零拷贝优化（Windows: TransmitFile、Linux/macOS: sendfile）
//! - 后台服务（Android: Foreground Service、iOS: Background Tasks）
//!
//! 每个平台实现一个 `platform_name()` 返回字符串标识。

/// 返回当前平台名称
pub fn platform_name() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        "windows"
    }
    #[cfg(target_os = "android")]
    {
        "android"
    }
    #[cfg(target_os = "ios")]
    {
        "ios"
    }
    #[cfg(target_os = "macos")]
    {
        "macos"
    }
    #[cfg(target_os = "linux")]
    {
        "linux"
    }
    #[cfg(not(any(
        target_os = "windows",
        target_os = "android",
        target_os = "ios",
        target_os = "macos",
        target_os = "linux"
    )))]
    {
        "unknown"
    }
}

/// 平台默认接收目录
pub fn default_receive_dir() -> std::path::PathBuf {
    #[cfg(target_os = "android")]
    {
        // Android：使用应用专属外部存储
        std::path::PathBuf::from("/sdcard/Download/kitefile")
    }
    #[cfg(target_os = "ios")]
    {
        // iOS：使用 Documents（sandboxed）
        std::path::PathBuf::from("Documents/kitefile")
    }
    #[cfg(target_os = "macos")]
    {
        std::env::var("HOME")
            .map(|h| std::path::PathBuf::from(h).join("Downloads/kitefile"))
            .unwrap_or_else(|_| std::path::PathBuf::from("kitefile"))
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var("USERPROFILE")
            .map(|h| std::path::PathBuf::from(h).join("Downloads/kitefile"))
            .unwrap_or_else(|_| std::path::PathBuf::from("kitefile"))
    }
    #[cfg(target_os = "linux")]
    {
        std::env::var("HOME")
            .map(|h| std::path::PathBuf::from(h).join("Downloads/kitefile"))
            .unwrap_or_else(|_| std::path::PathBuf::from("kitefile"))
    }
    #[cfg(not(any(
        target_os = "windows",
        target_os = "android",
        target_os = "ios",
        target_os = "macos",
        target_os = "linux"
    )))]
    {
        std::path::PathBuf::from("kitefile")
    }
}

/// 平台初始化钩子（如 Android 申请权限、iOS 注册 background task）
pub fn platform_init() {
    #[cfg(target_os = "android")]
    {
        // 在 Android 上申请 MANAGE_EXTERNAL_STORAGE 等权限由上层 Flutter 完成
    }
    #[cfg(target_os = "ios")]
    {
        // iOS 上注册 background task identifier 由上层 Flutter 完成
    }
    #[allow(unused)]
    {
        // no-op for other platforms
    }
}
