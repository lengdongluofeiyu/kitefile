//! 接收端文件落盘管理
//!
//! 设计：
//! - 每个传输任务在 receive_dir 下创建 `<file_id>.part` 临时文件
//! - 各 chunk 通过定位写（seek + write）写入正确偏移
//! - 全部 chunk 完成 + 校验通过后，原子重命名为最终文件名
//! - 中止（abort）时保留 .part 文件
//!
//! TODO: 保留 .part **不等于**支持断点续传。槽位与 chunk 完成位图只存在于
//! 内存，进程重启即丢失，重启后这些 .part 无法被识别、也无人认领。
//! 要真正续传，得先把槽位元数据落盘（含 chunk_size、位图、原 file_id），
//! 详见方案 N2 / N3。在此之前 .part 只作为失败排查的现场。

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
    pub chunk_size: usize,
    pub chunk_count: u64,
    pub received_chunks: Vec<bool>,
    /// 发送方在 offer 中声明的整文件 sha256（校验用；可空）
    pub sha256: Option<String>,
    /// true = 发送方声明 sha256 会延后补发（POST /api/verify）：
    /// 全部 chunk 收齐后若哈希未到，暂不 finalize，等哈希或超时保险丝
    #[serde(default)]
    pub await_sha256: bool,
    pub temp_path: PathBuf,
    pub final_path: PathBuf,
}

impl ReceiveSlot {
    pub fn next_missing_chunk(&self) -> Option<u64> {
        self.received_chunks
            .iter()
            .position(|ok| !ok)
            .map(|i| i as u64)
    }

    pub fn is_complete(&self) -> bool {
        self.received_chunks.iter().all(|ok| *ok)
    }
}

/// 接收目录持久化标记文件名（存放在启动时的默认目录下）
const SAVE_DIR_MARKER: &str = ".ftcore-save-dir";

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

    /// 创建新接收槽位
    #[allow(clippy::too_many_arguments)]
    pub async fn create_slot(
        &self,
        file_id: String,
        file_name: String,
        file_size: u64,
        chunk_size: usize,
        sha256: Option<String>,
        await_sha256: bool,
    ) -> Result<()> {
        let chunk_count = (file_size + chunk_size as u64 - 1) / chunk_size as u64;
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
            chunk_size,
            chunk_count,
            received_chunks: vec![false; chunk_count as usize],
            sha256,
            await_sha256,
            temp_path,
            final_path,
        };

        self.slots.lock().await.insert(file_id, slot);
        Ok(())
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

    /// 将一个 chunk 写入对应槽位
    ///
    /// 校验失败 / 槽位不存在均返回 Err：接收方只在落盘成功后才回 ACK，
    /// 让发送方能感知到丢块，而不是误认为已发送完成。
    pub async fn write_chunk(&self, file_id: &str, chunk_id: u64, data: &[u8]) -> Result<()> {
        let slot_ref = {
            let slots = self.slots.lock().await;
            slots.get(file_id).cloned()
        };

        let Some(slot) = slot_ref else {
            return Err(crate::CoreError::Transfer(format!(
                "no receive slot for file_id {}",
                file_id
            )));
        };

        // 长度校验：chunk 数据必须与声明的偏移/大小严格一致
        if chunk_id >= slot.chunk_count {
            return Err(crate::CoreError::Transfer(format!(
                "chunk_id {} out of range (chunk_count={})",
                chunk_id, slot.chunk_count
            )));
        }
        let offset = chunk_id * slot.chunk_size as u64;
        let expected_len =
            std::cmp::min(slot.file_size.saturating_sub(offset), slot.chunk_size as u64) as usize;
        if data.len() != expected_len {
            return Err(crate::CoreError::Transfer(format!(
                "chunk {} length mismatch: expected {}, got {}",
                chunk_id,
                expected_len,
                data.len()
            )));
        }

        let temp_path = slot.temp_path.clone();
        let data = data.to_vec();

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

        let mut slots = self.slots.lock().await;
        if let Some(slot) = slots.get_mut(file_id) {
            if (chunk_id as usize) < slot.received_chunks.len() {
                slot.received_chunks[chunk_id as usize] = true;
            }
        }
        Ok(())
    }

    /// 完成时校验 sha256 并将 .part 重命名为最终文件
    ///
    /// 校验失败返回 `ChecksumMismatch`，槽位移除、.part 保留供排查。
    /// 先原子领取（remove）槽位：并发的多个 chunk 同时判定完成时只有一个 finalize 生效，
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
                warn!(file_id, "finalize called but chunks incomplete");
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

    /// 中止接收：移除槽位（不再接受该文件的 chunk），**保留 .part 文件**。
    ///
    /// 注意这里只移除内存里的槽位，不删磁盘文件。保留下来是为了事后排查
    /// 传输失败的原因（落盘内容、偏移都对不对）。
    /// 它不是断点续传的基础——位图随槽位一起没了，重启后无从续起，见文件头 TODO。
    pub async fn abort(&self, file_id: &str) -> Option<ReceiveSlot> {
        self.slots.lock().await.remove(file_id)
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
        // 多段扩展名只认最后一段：`archive.tar (1).gz` 而不是 `archive (1).tar.gz`
        assert_eq!(conflict_name("archive.tar.gz", 1), "archive.tar (1).gz");
        assert_eq!(conflict_name("noext", 1), "noext (1)");
        // 隐藏文件：Rust 的 extension() 对 `.bashrc` 返回 None，整体当 stem
        assert_eq!(conflict_name(".bashrc", 1), ".bashrc (1)");
    }

    /// 文件名来自对端的 offer，是可信度为零的输入
    #[test]
    fn safe_file_name_blocks_traversal() {
        assert_eq!(safe_file_name("a.txt"), "a.txt");
        assert_eq!(safe_file_name("../../evil.exe"), "evil.exe");
        assert_eq!(safe_file_name("/etc/passwd"), "passwd");
        assert_eq!(safe_file_name("C:/Windows/evil.exe"), "evil.exe");
        // 只剩目录成分 / 空串 / 纯 `..` → 兜底名，绝不能退化成目录
        assert_eq!(safe_file_name(".."), "unnamed");
        assert_eq!(safe_file_name(""), "unnamed");
        assert_eq!(safe_file_name("a/b/"), "b");
    }
}
