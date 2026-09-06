//! 接收槽位的持久化元数据（`<file_id>.meta`）
//!
//! **本阶段只落类型和序列化，不写文件。**
//!
//! 为什么现在就写：格式先定死并锁进测试，将来做跨重启续传（N3）时
//! 就能"只填数据、不改格式"。等到真要做续传时再设计，会发现当初
//! 随手定的结构有坑（比如位图用布尔数组，640 块要 3KB+）。
//! 格式定义见 docs/design-phase4-error-recovery.md §4.1。
//!
//! 字段只增不改：新增字段必须给默认值，保证旧 meta 仍能被读出。

use base64::Engine;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// 当前元数据格式版本
pub const SLOT_META_VERSION: u32 = 1;

/// 位打包的接收位图 → base64。
///
/// **LSB-first**：第 n 块对应 `bytes[n / 8]` 的第 `n % 8` 位（从低位算起）。
/// 用位打包而不是布尔数组：640 个块 = 80 字节，
/// 布尔 JSON 数组（`[true,false,...]`）要 3KB 以上。
pub fn pack_bitmap(received: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; received.len().div_ceil(8)];
    for (i, ok) in received.iter().enumerate() {
        if *ok {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

/// [`pack_bitmap`] 的逆运算。`count` 是块总数（不是字节数）。
///
/// 位图字节不足时按 0 补齐——旧 meta 的块数比现在少也算正常，
/// 总比 panic 好；多出来的块位都是 false，会被重新传输。
pub fn unpack_bitmap(bytes: &[u8], count: usize) -> Vec<bool> {
    (0..count)
        .map(|i| {
            let byte = bytes.get(i / 8).copied().unwrap_or(0);
            byte & (1 << (i % 8)) != 0
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotMeta {
    pub version: u32,
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub chunk_size: usize,
    pub chunk_count: u64,
    /// 整文件 sha256；deferred 补发尚未到达时为 null
    pub sha256: Option<String>,
    /// [`pack_bitmap`] 的结果再过一层 base64
    pub bitmap: String,
    /// 槽位创建时间（Unix 秒），用于将来的 .part 过期清理
    pub created_at: u64,
    pub peer_id: String,
    /// 发送方源文件的 mtime（秒）；对端没给就是 null
    pub source_mtime: Option<i64>,
}

impl SlotMeta {
    /// 从内存槽位生成元数据
    pub fn from_slot(
        slot: &crate::storage::ReceiveSlot,
        peer_id: &str,
        source_mtime: Option<i64>,
    ) -> Self {
        Self {
            version: SLOT_META_VERSION,
            file_id: slot.file_id.clone(),
            file_name: slot.file_name.clone(),
            file_size: slot.file_size,
            chunk_size: slot.chunk_size,
            chunk_count: slot.chunk_count,
            sha256: slot.sha256.clone(),
            bitmap: base64::engine::general_purpose::STANDARD
                .encode(pack_bitmap(&slot.received_chunks)),
            created_at: now_secs(),
            peer_id: peer_id.to_string(),
            source_mtime,
        }
    }

    pub fn to_json(&self) -> crate::Result<String> {
        serde_json::to_string(self).map_err(|e| crate::CoreError::Transfer(e.to_string()))
    }

    /// 解析元数据。**版本不认识就返回 None（丢弃）**——
    /// 让调用方走全新传输，好过按错误格式解析出一堆错位数据。
    pub fn from_json(s: &str) -> Option<Self> {
        let meta: SlotMeta = match serde_json::from_str(s) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "unparsable slot meta, discarding");
                return None;
            }
        };
        if meta.version != SLOT_META_VERSION {
            warn!(version = meta.version, "unknown slot meta version, discarding");
            return None;
        }
        Some(meta)
    }

    /// 位图解码回布尔数组
    pub fn received_chunks(&self) -> Option<Vec<bool>> {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(&self.bitmap)
            .ok()?;
        Some(unpack_bitmap(&raw, self.chunk_count as usize))
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// LSB-first 的方向性。
    ///
    /// 故意选**非对称**的位置：如果同时置位 `v[0]` 和 `v[7]`，
    /// LSB-first 与 MSB-first 的结果都是 `0b1000_0001`，这条断言就白写了，
    /// 锁不住方向——这是写完第一版后发现的问题。
    #[test]
    fn bitmap_packs_lsb_first() {
        let mut v = vec![false; 16];
        v[1] = true; // LSB-first → 0b0000_0010；若写成 MSB-first 会是 0b0100_0000
        v[8] = true; // 跨字节：byte1 的最低位
        let packed = pack_bitmap(&v);
        assert_eq!(packed.len(), 2);
        assert_eq!(packed[0], 0b0000_0010, "第 1 块必须在 byte0 的 bit1");
        assert_eq!(packed[1], 0b0000_0001, "第 8 块必须在 byte1 的 bit0");
        assert_eq!(unpack_bitmap(&packed, 16), v);
    }

    /// 文档里算的那笔账：640 块 = 80 字节（布尔数组要 3KB+）
    #[test]
    fn bitmap_is_bit_packed() {
        assert_eq!(pack_bitmap(&vec![false; 640]).len(), 80);
        // 不满 8 的余数要单独占一个字节
        assert_eq!(pack_bitmap(&vec![false; 9]).len(), 2);
        assert_eq!(pack_bitmap(&vec![false; 8]).len(), 1);
        assert_eq!(pack_bitmap(&vec![false; 0]).len(), 0);
    }

    /// 位图字节比块数少时不 panic，缺的按未接收处理
    #[test]
    fn unpack_tolerates_short_bitmap() {
        let got = unpack_bitmap(&[0b0000_0001], 16);
        assert_eq!(got.len(), 16);
        assert!(got[0]);
        assert!(!got[8], "越界的块位应为 false");
    }

    #[test]
    fn json_has_documented_fields() {
        let meta = SlotMeta {
            version: SLOT_META_VERSION,
            file_id: "uuid-1".into(),
            file_name: "a.mp4".into(),
            file_size: 10737418240,
            chunk_size: 16777216,
            chunk_count: 640,
            sha256: None,
            bitmap: base64::engine::general_purpose::STANDARD.encode(vec![0u8; 80]),
            created_at: 1757000000,
            peer_id: "dev-1".into(),
            source_mtime: None,
        };
        let json: serde_json::Value = serde_json::from_str(&meta.to_json().unwrap()).unwrap();
        for key in [
            "version",
            "file_id",
            "file_name",
            "file_size",
            "chunk_size",
            "chunk_count",
            "sha256",
            "bitmap",
            "created_at",
            "peer_id",
            "source_mtime",
        ] {
            assert!(json.get(key).is_some(), "缺少字段 {key}");
        }
        // 允许为 null 的两个字段，JSON 里必须是 null 而不是缺失
        assert!(json["sha256"].is_null());
        assert!(json["source_mtime"].is_null());

        // 往返一致
        let back = SlotMeta::from_json(&meta.to_json().unwrap()).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn unknown_version_is_discarded() {
        let base = SlotMeta {
            version: SLOT_META_VERSION,
            file_id: "u".into(),
            file_name: "a".into(),
            file_size: 4,
            chunk_size: 4,
            chunk_count: 1,
            sha256: None,
            bitmap: String::new(),
            created_at: 1,
            peer_id: "p".into(),
            source_mtime: None,
        };
        assert!(SlotMeta::from_json(&base.to_json().unwrap()).is_some());

        // 把 version 改成 2：必须返回 None，而不是解析出一个字段错位的结构
        let v2 = base.to_json().unwrap().replace("\"version\":1", "\"version\":2");
        assert!(
            SlotMeta::from_json(&v2).is_none(),
            "不认识的 version 必须丢弃"
        );
        // 烂 JSON 也不能 panic
        assert!(SlotMeta::from_json("{not json").is_none());
    }

    #[test]
    fn bitmap_roundtrips_through_base64() {
        let mut chunks = vec![false; 100];
        for i in (0..100).step_by(3) {
            chunks[i] = true;
        }
        let meta = SlotMeta {
            version: SLOT_META_VERSION,
            file_id: "u".into(),
            file_name: "a".into(),
            file_size: 400,
            chunk_size: 4,
            chunk_count: 100,
            sha256: None,
            bitmap: base64::engine::general_purpose::STANDARD.encode(pack_bitmap(&chunks)),
            created_at: 1,
            peer_id: "p".into(),
            source_mtime: None,
        };
        assert_eq!(meta.received_chunks(), Some(chunks));
    }
}
