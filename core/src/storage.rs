//! 接收端文件落盘管理
//!
//! 设计：
//! - 每个传输任务在 receive_dir 下创建 `<file_name>.<id8>.part` 临时文件（预分配大小）
//! - N 条 TCP 流各写一段连续字节区间（`stream_layout`），流内顺序写、无 ACK
//! - 全部流完成 + 校验通过后，原子重命名为最终文件名
//! - 中止（abort，中断/失败排查）时保留 .part；**确定取消**时删除 .part
//!
//! TODO: 保留 .part **不等于**支持断点续传。槽位与流完成位图只存在于
//! 内存，进程重启即丢失。要真正续传，得先把槽位元数据落盘（含分段布局、
//! 位图、原 file_id），详见方案 N2 / N3。在此之前 .part 只作为失败排查的现场。

use crate::protocol::stream_layout;
use crate::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// 计算文件 sha256（hex 小写）
fn sha256_of_file(path: &Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    let mut f = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut f, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
}

/// 把对端给的文件名收敛成"纯文件名"，挡住路径穿越。
///
/// 文件名来自 offer，是**可信度为零的输入**。`Path::join` 有两个坑：
///   1. 遇到绝对路径（`/etc/passwd`、`C:\x`）会直接丢弃前面的基目录
///   2. `..` 成分会往上跳目录
/// 两者合起来意味着对端可以指定接收目录之外的任意写入位置。
///
/// `Path::file_name()` 天然去掉了目录成分和 `.` / `..`，
/// 对 `..`、空串、Windows 盘符前缀（`C:`）返回 None，正好兜底成 "unnamed"。
fn safe_file_name(file_name: &str) -> String {
    let cleaned = Path::new(file_name)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    if cleaned.is_empty() {
        "unnamed".to_string()
    } else {
        cleaned
    }
}

/// 第 n 个候选文件名：`n=0` 是原名，`n>=1` 是 `stem (n).ext`
///
/// 拆成纯函数是为了能零成本单测——冲突改名最容易写错的就是扩展名切分
/// （`archive.tar.gz` 该变成 `archive.tar (1).gz` 而不是 `archive (1).tar.gz`）。
fn conflict_name(file_name: &str, n: u32) -> String {
    if n == 0 {
        return file_name.to_string();
    }
    let p = Path::new(file_name);
    let stem = p.file_stem().unwrap_or_default().to_string_lossy();
    match p.extension() {
        Some(ext) => format!("{} ({}).{}", stem, n, ext.to_string_lossy()),
        // 无扩展名（含 `.bashrc` 这类隐藏文件）：整体当 stem
        None => format!("{} ({})", stem, n),
    }
}

/// 找目录下第一个不存在的候选路径（`a.mp4` → `a (1).mp4` → `a (2).mp4` …）
fn unique_final_path(dir: &Path, file_name: &str) -> Option<PathBuf> {
    const MAX_CANDIDATES: u32 = 1000;
    for n in 0..MAX_CANDIDATES {
        let candidate = dir.join(conflict_name(file_name, n));
        if !candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

/// 单个接收任务的状态
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReceiveSlot {
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
    /// 发送方声明的并行流数（分段布局由它决定，两端必须一致）
    pub stream_count: u32,
    /// 各流的 `(start_offset, len)`，与 [`crate::protocol::stream_layout`] 一致
    pub segments: Vec<(u64, u64)>,
    /// 各流是否已收完
    pub streams_done: Vec<bool>,
    /// 已落盘字节（进度用；可能领先/落后于已完成流的段长之和）
    pub bytes_received: u64,
    /// 发送方在 offer 中声明的整文件 sha256（校验用；可空）
    pub sha256: Option<String>,
    /// true = 发送方声明 sha256 会延后补发（POST /api/verify）：
    /// 全部流收齐后若哈希未到，暂不 finalize，等哈希或超时保险丝
    #[serde(default)]
    pub await_sha256: bool,
    pub temp_path: PathBuf,
    pub final_path: PathBuf,
}

impl ReceiveSlot {
    pub fn is_complete(&self) -> bool {
        !self.streams_done.is_empty() && self.streams_done.iter().all(|ok| *ok)
    }
}

/// 接收目录持久化标记文件名（存放在启动时的默认目录下）
const SAVE_DIR_MARKER: &str = ".kitefile-save-dir";

/// 接收管理器：维护所有正在接收的文件
pub struct StorageManager {
    /// 启动时的默认目录（持久化标记存这里，与当前目录解耦）
    base_dir: PathBuf,
    /// 当前接收目录（可在运行期通过设置修改，读多写少用 RwLock）
    receive_dir: parking_lot::RwLock<PathBuf>,
    slots: Arc<Mutex<HashMap<String, ReceiveSlot>>>,
}

/// 读取持久化的接收目录设置（返回 None 表示从未设置过 / 已失效）
fn load_persisted_dir(base_dir: &Path) -> Option<PathBuf> {
    let marker = base_dir.join(SAVE_DIR_MARKER);
    let content = std::fs::read_to_string(&marker).ok()?;
    let dir = PathBuf::from(content.trim());
    if dir.as_os_str().is_empty() {
        return None;
    }
    // 目录必须可创建（如已被删除/卸载则忽略旧设置）
    if std::fs::create_dir_all(&dir).is_err() {
        warn!(?dir, "persisted receive dir invalid, fallback to default");
        return None;
    }
    Some(dir)
}

impl StorageManager {
    pub fn new(receive_dir: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&receive_dir);
        // 恢复上次设置的保存位置（marker 存放在默认目录下）
        let effective = load_persisted_dir(&receive_dir).unwrap_or_else(|| receive_dir.clone());
        Self {
            base_dir: receive_dir,
            receive_dir: parking_lot::RwLock::new(effective),
            slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 当前接收目录
    pub fn receive_dir(&self) -> PathBuf {
        self.receive_dir.read().clone()
    }

    /// 修改接收目录（设置页调用）。
    /// 尝试创建目录，失败则返回错误且不生效；进行中的任务仍写入原路径。
    /// 成功后写入持久化标记，daemon 重启后仍生效。
    pub fn set_receive_dir(&self, dir: PathBuf) -> Result<()> {
        std::fs::create_dir_all(&dir)
            .map_err(|e| crate::CoreError::Transfer(format!("invalid receive dir: {}", e)))?;
        // 持久化（best-effort：写失败不影响本次生效，只是重启后回默认）
        if let Err(e) = std::fs::write(self.base_dir.join(SAVE_DIR_MARKER), dir.to_string_lossy().as_bytes()) {
            warn!(error = %e, "persist receive dir failed");
        }
        *self.receive_dir.write() = dir;
        Ok(())
    }

    /// 创建新接收槽位。`stream_count` 必须来自发送方 offer。
    #[allow(clippy::too_many_arguments)]
    pub async fn create_slot(
        &self,
        file_id: String,
        file_name: String,
        file_size: u64,
        stream_count: u32,
        sha256: Option<String>,
        await_sha256: bool,
    ) -> Result<()> {
        let segments = stream_layout(file_size, stream_count);
        let receive_dir = self.receive_dir();
        // 先清洗再拼路径：直接 join 对端给的名字等于允许它指定任意写入位置
        let file_name = safe_file_name(&file_name);
        let id_short = file_id.get(..8).unwrap_or(&file_id);
        let temp_path = receive_dir.join(format!("{}.{}.part", file_name, id_short));
        let final_path = receive_dir.join(&file_name);

        // 预分配文件大小
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&temp_path)
            .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
        if file_size > 0 {
            let _ = file.set_len(file_size);
        }

        let slot = ReceiveSlot {
            file_id: file_id.clone(),
            file_name,
            file_size,
            stream_count: stream_count.max(1),
            streams_done: vec![false; segments.len()],
            segments,
            bytes_received: 0,
            sha256,
            await_sha256,
            temp_path,
            final_path,
        };

        self.slots.lock().await.insert(file_id, slot);
        Ok(())
    }

    /// 槽位快照（流式写入时取 temp_path / 校验 stream_id）
    pub async fn get_slot(&self, file_id: &str) -> Option<ReceiveSlot> {
        self.slots.lock().await.get(file_id).cloned()
    }

    /// 补发最终 sha256（POST /api/verify/:file_id 调用）。
    ///
    /// - 槽位已不存在（已 finalize / 重复通知）：返回 None（幂等）
    /// - None 不覆盖已有哈希（超时保险丝先到时让真哈希优先）
    /// 返回更新后的槽位快照，调用方据此判断是否可以 finalize
    pub async fn set_final_sha256(
        &self,
        file_id: &str,
        sha256: Option<String>,
    ) -> Option<ReceiveSlot> {
        let mut slots = self.slots.lock().await;
        let slot = slots.get_mut(file_id)?;
        match (&slot.sha256, &sha256) {
            (Some(_), None) => {}
            _ => slot.sha256 = sha256,
        }
        Some(slot.clone())
    }

    /// 在指定偏移写入一段数据（测试与简单路径用；生产路径由调用方持句柄顺序写）。
    pub async fn write_at(&self, file_id: &str, offset: u64, data: &[u8]) -> Result<()> {
        let temp_path = {
            let slots = self.slots.lock().await;
            let Some(slot) = slots.get(file_id) else {
                return Err(crate::CoreError::Transfer(format!(
                    "no receive slot for file_id {}",
                    file_id
                )));
            };
            if offset + data.len() as u64 > slot.file_size {
                return Err(crate::CoreError::Transfer(format!(
                    "write_at out of range: offset={} len={} file_size={}",
                    offset,
                    data.len(),
                    slot.file_size
                )));
            }
            slot.temp_path.clone()
        };

        let data = data.to_vec();
        let n = data.len() as u64;
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(&temp_path)?;
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(&data)?;
            f.flush()?;
            Ok(())
        })
        .await
        .map_err(|e| crate::CoreError::Transfer(e.to_string()))?
        .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
        self.add_bytes(file_id, n).await;
        Ok(())
    }

    /// 累计已收字节（进度）
    pub async fn add_bytes(&self, file_id: &str, n: u64) {
        if let Some(slot) = self.slots.lock().await.get_mut(file_id) {
            slot.bytes_received = slot.bytes_received.saturating_add(n);
        }
    }

    /// 标记一条流收完。返回更新后的槽位快照（None = 槽位已不在）。
    ///
    /// 段长会补进 `bytes_received`（若调用方未用 `add_bytes` 逐段累计，
    /// 完成时至少保证总量正确）。重复 mark 幂等。
    pub async fn finish_stream(&self, file_id: &str, stream_id: u32) -> Option<ReceiveSlot> {
        let mut slots = self.slots.lock().await;
        let slot = slots.get_mut(file_id)?;
        let idx = stream_id as usize;
        if idx < slot.streams_done.len() {
            if !slot.streams_done[idx] {
                slot.streams_done[idx] = true;
            }
        }
        // 用已完成流的段长之和校正 bytes_received，避免双计或漏计
        let done_bytes: u64 = slot
            .segments
            .iter()
            .zip(&slot.streams_done)
            .filter(|(_, done)| **done)
            .map(|((_, len), _)| *len)
            .sum();
        if done_bytes > slot.bytes_received {
            slot.bytes_received = done_bytes;
        }
        Some(slot.clone())
    }

    /// 完成时校验 sha256 并将 .part 重命名为最终文件
    ///
    /// 校验失败返回 `ChecksumMismatch`，槽位移除、.part 保留供排查。
    /// 先原子领取（remove）槽位：并发的多个流同时判定完成时只有一个 finalize 生效，
    /// 避免二次校验 / 二次 rename 报错把 Completed 覆盖成 Failed。
    ///
    /// 返回**实际落盘路径**——目标已存在时会改名成 `name (1).ext`，
    /// 调用方必须用这个返回值通知 UI，否则"打开文件"会指向错误的路径。
    pub async fn finalize(&self, file_id: &str, expected_sha256: Option<&str>) -> Result<PathBuf> {
        let slot = {
            let mut slots = self.slots.lock().await;
            slots.remove(file_id)
        };

        if let Some(slot) = slot {
            if !slot.is_complete() {
                warn!(file_id, "finalize called but streams incomplete");
            }

            // 整文件 sha256 校验（offer 声明了才校验）
            if let Some(expected) = expected_sha256 {
                let temp_path = slot.temp_path.clone();
                let actual = tokio::task::spawn_blocking(move || sha256_of_file(&temp_path))
                    .await
                    .map_err(|e| crate::CoreError::Transfer(e.to_string()))?
                    .map_err(|e| crate::CoreError::Transfer(e.to_string()))?;
                if !actual.eq_ignore_ascii_case(expected) {
                    warn!(file_id, expected, actual, "checksum mismatch");
                    return Err(crate::CoreError::ChecksumMismatch {
                        expected: expected.to_string(),
                        actual,
                    });
                }
            }

            // 目标已存在就顺延改名（a.mp4 → a (1).mp4），而不是覆盖或报错。
            //
            // 循环是为了处理"检查和改名之间名字被抢"：两个传输同时收完同名文件时，
            // 可能都看到 a.mp4 不存在，然后 Windows 上后一个 rename 会失败
            // （Unix 的 rename 是覆盖语义，这里统一按最严的情况处理）。
            // 重挑一次名字即可——被抢的那个现在 exists 了，会自动跳过。
            const MAX_RENAME_ATTEMPTS: u32 = 8;
            let dir = self.receive_dir();
            let mut last_err: Option<std::io::Error> = None;
            for _ in 0..MAX_RENAME_ATTEMPTS {
                let candidate = unique_final_path(&dir, &slot.file_name).ok_or_else(|| {
                    crate::CoreError::Transfer(format!(
                        "no available name for {} (tried 1000 suffixes)",
                        slot.file_name
                    ))
                })?;
                match tokio::fs::rename(&slot.temp_path, &candidate).await {
                    Ok(()) => {
                        if candidate.file_name() != Some(std::ffi::OsStr::new(&slot.file_name)) {
                            info!(?candidate, "final name taken, renamed to avoid overwrite");
                        }
                        info!(?candidate, "file finalized");
                        return Ok(candidate);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        warn!(?candidate, "rename target taken concurrently, retrying");
                        last_err = Some(e);
                    }
                    Err(e) => return Err(crate::CoreError::Transfer(e.to_string())),
                }
            }
            Err(crate::CoreError::Transfer(format!(
                "rename failed after {} attempts: {}",
                MAX_RENAME_ATTEMPTS,
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown".into())
            )))
        } else {
            // 槽位不存在：已被别的路径 finalize 过（幂等）
            Ok(PathBuf::new())
        }
    }

    /// 中止接收：移除槽位（不再接受该文件的流），**保留 .part 文件**。
    ///
    /// 用于中断（可续传）与不可重试失败的排查现场——不是断点续传的基础，
    /// 位图随槽位一起没了，重启后无从续起，见文件头 TODO。
    pub async fn abort(&self, file_id: &str) -> Option<ReceiveSlot> {
        self.slots.lock().await.remove(file_id)
    }

    /// **确定取消**时中止接收并删除 `.part` 临时文件。
    ///
    /// 与 [`Self::abort`] 的区别：取消是用户明确放弃，临时文件没有留存价值，
    /// 必须主动清掉，避免接收目录被半成品占满。中断/失败仍走 abort 保留现场。
    pub async fn abort_and_remove_temp(&self, file_id: &str) -> Option<ReceiveSlot> {
        let slot = self.slots.lock().await.remove(file_id);
        if let Some(s) = &slot {
            match tokio::fs::remove_file(&s.temp_path).await {
                Ok(()) => {
                    info!(
                        %file_id,
                        path = %s.temp_path.display(),
                        "removed canceled transfer temp file"
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(
                        %file_id,
                        path = %s.temp_path.display(),
                        error = %e,
                        "remove canceled transfer temp file failed"
                    );
                }
            }
        }
        slot
    }

    pub async fn list_in_progress(&self) -> Vec<ReceiveSlot> {
        self.slots.lock().await.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    //! 文件名相关的纯函数走单元测试：不碰磁盘、不需要临时目录，
    //! 而这两处的边界（多段扩展名、路径穿越）恰恰最容易写错又不常被调用。
    use super::*;

    #[test]
    fn conflict_name_keeps_extension() {
        assert_eq!(conflict_name("a.mp4", 0), "a.mp4");
        assert_eq!(conflict_name("a.mp4", 1), "a (1).mp4");
        assert_eq!(conflict_name("a.mp4", 2), "a (2).mp4");
    }

    #[test]
    fn conflict_name_multi_ext_treats_last_as_ext() {
        assert_eq!(conflict_name("archive.tar.gz", 1), "archive.tar (1).gz");
    }

    #[test]
    fn conflict_name_hidden_file_has_no_ext() {
        assert_eq!(conflict_name(".bashrc", 1), ".bashrc (1)");
    }

    #[test]
    fn safe_file_name_strips_directories() {
        assert_eq!(safe_file_name("../../etc/passwd"), "passwd");
        assert_eq!(safe_file_name("/etc/passwd"), "passwd");
        assert_eq!(safe_file_name("C:\\Windows\\System32\\evil.exe"), "evil.exe");
        assert_eq!(safe_file_name(".."), "unnamed");
        assert_eq!(safe_file_name(""), "unnamed");
    }
}
