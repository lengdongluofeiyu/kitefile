//! Storage 层测试：chunk 落盘、乱序写、finalize 校验、abort

use ftcore::storage::StorageManager;

/// 测试用的接收目录。
///
/// 走 `FTCORE_TEST_TMP` 是为了能把它指到 E 盘——系统 temp 在 C 盘，
/// 空间紧张时测试会以很莫名其妙的方式失败（写一半的文件、链接器被杀）。
/// `scripts/rust-env.sh` 已经把这个变量设好了。
fn temp_recv_dir(tag: &str) -> std::path::PathBuf {
    let base = std::env::var("FTCORE_TEST_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let dir = base.join(format!(
        "ftcore-storage-test-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn sha256_of(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::new().chain_update(data).finalize())
}

#[tokio::test]
async fn test_create_slot_and_chunk_bitmap() {
    let dir = temp_recv_dir("bitmap");
    let mgr = StorageManager::new(dir.clone());

    // 10 字节 / chunk_size 4 → 3 chunks
    mgr.create_slot(
        "file-uuid-1234".into(),
        "hello.bin".into(),
        10,
        4,
        None,
        false,
    )
    .await
    .unwrap();

    let slots = mgr.list_in_progress().await;
    assert_eq!(slots.len(), 1);
    let s = &slots[0];
    assert_eq!(s.chunk_count, 3);
    assert_eq!(s.received_chunks.len(), 3);
    assert!(!s.is_complete());
    assert_eq!(s.next_missing_chunk(), Some(0));
    assert!(s
        .temp_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap()
        .ends_with(".part"));
    assert_eq!(s.final_path, dir.join("hello.bin"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_write_chunks_out_of_order_and_finalize() {
    let dir = temp_recv_dir("outoforder");
    let mgr = StorageManager::new(dir.clone());

    // 内容 "AAAABBBBCC"（10 字节，chunk_size 4 → 3 chunks）
    let content = b"AAAABBBBCC";
    mgr.create_slot(
        "file-uuid-5678".into(),
        "out.bin".into(),
        content.len() as u64,
        4,
        None,
        false,
    )
    .await
    .unwrap();

    // 乱序写：chunk 2 → chunk 0 → chunk 1
    mgr.write_chunk("file-uuid-5678", 2, &content[8..]).await.unwrap();
    mgr.write_chunk("file-uuid-5678", 0, &content[0..4]).await.unwrap();

    let slots = mgr.list_in_progress().await;
    assert!(!slots[0].is_complete());

    mgr.write_chunk("file-uuid-5678", 1, &content[4..8]).await.unwrap();

    let slots = mgr.list_in_progress().await;
    assert!(slots[0].is_complete());

    // finalize（无 sha256 声明 → 不校验，直接重命名）
    mgr.finalize("file-uuid-5678", None).await.unwrap();

    // 最终文件内容正确（定位写保证乱序写入拼回原序）
    let got = std::fs::read(dir.join("out.bin")).unwrap();
    assert_eq!(got, content);

    // 槽位已清理
    assert!(mgr.list_in_progress().await.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_finalize_checksum_mismatch() {
    let dir = temp_recv_dir("mismatch");
    let mgr = StorageManager::new(dir.clone());

    let content = b"correct-data";
    // 声明“另一份内容”的 sha256 → 必然 mismatch
    let wrong_hash = sha256_of(b"other-data");
    mgr.create_slot(
        "file-uuid-9012".into(),
        "bad.bin".into(),
        content.len() as u64,
        4,
        Some(wrong_hash.clone()),
        false,
    )
    .await
    .unwrap();

    mgr.write_chunk("file-uuid-9012", 0, &content[0..4]).await.unwrap();
    mgr.write_chunk("file-uuid-9012", 1, &content[4..8]).await.unwrap();
    mgr.write_chunk("file-uuid-9012", 2, &content[8..]).await.unwrap();

    let slots = mgr.list_in_progress().await;
    assert!(slots[0].is_complete());
    let temp_path = slots[0].temp_path.clone();

    // finalize 应返回 ChecksumMismatch，且不生成最终文件
    let err = mgr.finalize("file-uuid-9012", Some(&wrong_hash)).await;
    match err {
        Err(ftcore::CoreError::ChecksumMismatch { .. }) => {}
        other => panic!("expected ChecksumMismatch, got {:?}", other.map(|_| ())),
    }

    // 最终文件不存在，.part 保留
    assert!(!dir.join("bad.bin").exists());
    assert!(temp_path.exists());
    // 槽位已清理
    assert!(mgr.list_in_progress().await.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_finalize_checksum_ok() {
    let dir = temp_recv_dir("ok-hash");
    let mgr = StorageManager::new(dir.clone());

    let content = b"integrity-data";
    let good_hash = sha256_of(content);
    mgr.create_slot(
        "file-uuid-3456".into(),
        "good.bin".into(),
        content.len() as u64,
        8,
        Some(good_hash),
        false,
    )
    .await
    .unwrap();
    mgr.write_chunk("file-uuid-3456", 0, &content[0..8]).await.unwrap();
    mgr.write_chunk("file-uuid-3456", 1, &content[8..]).await.unwrap();
    mgr.finalize("file-uuid-3456", Some(&sha256_of(content))).await.unwrap();

    let got = std::fs::read(dir.join("good.bin")).unwrap();
    assert_eq!(got, content);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_abort_keeps_part_file() {
    let dir = temp_recv_dir("abort");
    let mgr = StorageManager::new(dir.clone());

    mgr.create_slot("file-uuid-7890".into(), "x.bin".into(), 8, 4, None, false)
        .await
        .unwrap();
    mgr.write_chunk("file-uuid-7890", 0, b"AAAA").await.unwrap();

    let temp_path = mgr.list_in_progress().await[0].temp_path.clone();
    let slot = mgr.abort("file-uuid-7890").await;
    assert!(slot.is_some());
    assert!(mgr.list_in_progress().await.is_empty());
    // .part 保留（断点续传预留）
    assert!(temp_path.exists());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_write_chunk_unknown_file_id_errors() {
    let dir = temp_recv_dir("noop");
    let mgr = StorageManager::new(dir.clone());

    // 未创建槽位 → 必须报错：接收方只应在落盘成功后回 ACK，
    // 静默忽略会让发送方误认为已发送完成（丢块）
    let err = mgr.write_chunk("nonexistent", 0, b"data").await;
    assert!(err.is_err());
    assert!(mgr.list_in_progress().await.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_write_chunk_length_mismatch_errors() {
    let dir = temp_recv_dir("lenmismatch");
    let mgr = StorageManager::new(dir.clone());

    // 10 字节 / chunk_size 4：chunk 1 期望 4 字节，传 3 字节 → 必须报错
    mgr.create_slot("file-uuid-len".into(), "len.bin".into(), 10, 4, None, false)
        .await
        .unwrap();
    assert!(mgr.write_chunk("file-uuid-len", 1, b"abc").await.is_err());
    // chunk_id 越界也报错
    assert!(mgr.write_chunk("file-uuid-len", 3, b"abcd").await.is_err());
    assert!(!mgr.list_in_progress().await[0].is_complete());

    let _ = std::fs::remove_dir_all(&dir);
}

/// 目标文件已存在时必须改名，而不是覆盖。
///
/// 覆盖是**静默丢数据**：用户原来那个同名文件没了，界面还显示"传输成功"。
#[tokio::test]
async fn test_finalize_renames_when_target_exists() {
    let dir = temp_recv_dir("conflict");
    let mgr = StorageManager::new(dir.clone());

    std::fs::write(dir.join("dup.bin"), b"OLD-CONTENT").unwrap();

    let content = b"NEW-CONTENT";
    mgr.create_slot(
        "file-uuid-conflict".into(),
        "dup.bin".into(),
        content.len() as u64,
        4,
        None,
        false,
    )
    .await
    .unwrap();
    mgr.write_chunk("file-uuid-conflict", 0, &content[0..4])
        .await
        .unwrap();
    mgr.write_chunk("file-uuid-conflict", 1, &content[4..8])
        .await
        .unwrap();
    mgr.write_chunk("file-uuid-conflict", 2, &content[8..])
        .await
        .unwrap();

    let final_path = mgr.finalize("file-uuid-conflict", None).await.unwrap();

    // 新文件落在 `dup (1).bin`，且返回值就是这个路径——UI 靠它做"打开文件"
    assert_eq!(final_path, dir.join("dup (1).bin"));
    assert_eq!(std::fs::read(dir.join("dup (1).bin")).unwrap(), content);
    // 原文件一个字节都不能动
    assert_eq!(std::fs::read(dir.join("dup.bin")).unwrap(), b"OLD-CONTENT");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 已存在原名与 (1) 时，应顺延到 (2)
#[tokio::test]
async fn test_finalize_picks_next_free_name() {
    let dir = temp_recv_dir("conflict3");
    let mgr = StorageManager::new(dir.clone());
    std::fs::write(dir.join("dup.bin"), b"OLD").unwrap();
    std::fs::write(dir.join("dup (1).bin"), b"OLD1").unwrap();

    let content = b"NEWCONTENT!";
    mgr.create_slot("f3".into(), "dup.bin".into(), content.len() as u64, 4, None, false)
        .await
        .unwrap();
    for (i, c) in content.chunks(4).enumerate() {
        mgr.write_chunk("f3", i as u64, c).await.unwrap();
    }
    let p = mgr.finalize("f3", None).await.unwrap();
    assert_eq!(p, dir.join("dup (2).bin"));

    let _ = std::fs::remove_dir_all(&dir);
}

/// 对端给的文件名带路径成分时，落点必须仍在接收目录内
#[tokio::test]
async fn test_create_slot_blocks_path_traversal() {
    let dir = temp_recv_dir("traversal");
    let mgr = StorageManager::new(dir.clone());

    mgr.create_slot("f4".into(), "../../evil.exe".into(), 4, 4, None, false)
        .await
        .unwrap();
    let slot = &mgr.list_in_progress().await[0];
    assert_eq!(slot.final_path, dir.join("evil.exe"));
    assert!(slot.temp_path.starts_with(&dir));

    let _ = std::fs::remove_dir_all(&dir);
}
