//! Storage 层测试：按偏移流式写入、段完成、finalize 校验、abort

use ftcore::storage::StorageManager;

/// 测试用的接收目录。
///
/// 走 `FTCORE_TEST_TMP` 是为了能把它指到 E 盘——系统 temp 在 C 盘，
/// 空间紧张时测试会以很莫名其妙的方式失败（写一半的文件、链接器被杀）。
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
async fn test_create_slot_and_stream_bitmap() {
    let dir = temp_recv_dir("bitmap");
    let mgr = StorageManager::new(dir.clone());

    // 10 字节 / 3 流 → 段长 4+3+3
    mgr.create_slot(
        "file-uuid-1234".into(),
        "hello.bin".into(),
        10,
        3,
        None,
        false,
    )
    .await
    .unwrap();

    let slots = mgr.list_in_progress().await;
    assert_eq!(slots.len(), 1);
    let s = &slots[0];
    assert_eq!(s.stream_count, 3);
    assert_eq!(s.streams_done.len(), 3);
    assert_eq!(s.segments, vec![(0, 4), (4, 3), (7, 3)]);
    assert!(!s.is_complete());
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
async fn test_write_streams_out_of_order_and_finalize() {
    let dir = temp_recv_dir("outoforder");
    let mgr = StorageManager::new(dir.clone());

    // 内容 "AAAABBBBCC"（10 字节，3 流）
    let content = b"AAAABBBBCC";
    mgr.create_slot(
        "file-uuid-5678".into(),
        "out.bin".into(),
        content.len() as u64,
        3,
        None,
        false,
    )
    .await
    .unwrap();

    // 乱序写：流 2 → 流 0 → 流 1
    mgr.write_at("file-uuid-5678", 7, &content[7..]).await.unwrap();
    mgr.write_at("file-uuid-5678", 0, &content[0..4]).await.unwrap();

    let slot = mgr.finish_stream("file-uuid-5678", 2).await.unwrap();
    assert!(!slot.is_complete());
    let slot = mgr.finish_stream("file-uuid-5678", 0).await.unwrap();
    assert!(!slot.is_complete());

    mgr.write_at("file-uuid-5678", 4, &content[4..7]).await.unwrap();
    let slot = mgr.finish_stream("file-uuid-5678", 1).await.unwrap();
    assert!(slot.is_complete());

    mgr.finalize("file-uuid-5678", None).await.unwrap();

    let got = std::fs::read(dir.join("out.bin")).unwrap();
    assert_eq!(got, content);

    assert!(mgr.list_in_progress().await.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_finalize_checksum_mismatch() {
    let dir = temp_recv_dir("mismatch");
    let mgr = StorageManager::new(dir.clone());

    let content = b"correct-data";
    let wrong_hash = sha256_of(b"other-data");
    mgr.create_slot(
        "file-uuid-9012".into(),
        "bad.bin".into(),
        content.len() as u64,
        3,
        Some(wrong_hash.clone()),
        false,
    )
    .await
    .unwrap();

    mgr.write_at("file-uuid-9012", 0, &content[0..4]).await.unwrap();
    mgr.write_at("file-uuid-9012", 4, &content[4..8]).await.unwrap();
    mgr.write_at("file-uuid-9012", 8, &content[8..]).await.unwrap();
    for id in 0..3 {
        mgr.finish_stream("file-uuid-9012", id).await.unwrap();
    }

    let err = mgr.finalize("file-uuid-9012", Some(&wrong_hash)).await;
    assert!(err.is_err());
    // .part 保留供排查
    assert!(mgr.list_in_progress().await.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_finalize_checksum_ok() {
    let dir = temp_recv_dir("ok");
    let mgr = StorageManager::new(dir.clone());

    let content = b"correct-data";
    let hash = sha256_of(content);
    mgr.create_slot(
        "file-uuid-3456".into(),
        "good.bin".into(),
        content.len() as u64,
        2,
        Some(hash.clone()),
        false,
    )
    .await
    .unwrap();

    mgr.write_at("file-uuid-3456", 0, &content[0..8]).await.unwrap();
    mgr.write_at("file-uuid-3456", 8, &content[8..]).await.unwrap();
    mgr.finish_stream("file-uuid-3456", 0).await.unwrap();
    mgr.finish_stream("file-uuid-3456", 1).await.unwrap();

    let path = mgr.finalize("file-uuid-3456", Some(&hash)).await.unwrap();
    assert_eq!(std::fs::read(path).unwrap(), content);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_abort_keeps_part_file() {
    let dir = temp_recv_dir("abort");
    let mgr = StorageManager::new(dir.clone());

    mgr.create_slot("file-uuid-7890".into(), "keep.bin".into(), 8, 1, None, false)
        .await
        .unwrap();
    mgr.write_at("file-uuid-7890", 0, b"AAAA").await.unwrap();

    let slot = mgr.abort("file-uuid-7890").await.unwrap();
    assert!(mgr.list_in_progress().await.is_empty());
    assert!(slot.temp_path.exists(), "abort 必须保留 .part");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_write_at_unknown_file_id_errors() {
    let dir = temp_recv_dir("unknown");
    let mgr = StorageManager::new(dir.clone());

    let err = mgr.write_at("nonexistent", 0, b"data").await;
    assert!(err.is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_write_at_out_of_range_errors() {
    let dir = temp_recv_dir("range");
    let mgr = StorageManager::new(dir.clone());

    mgr.create_slot("file-uuid-len".into(), "len.bin".into(), 10, 2, None, false)
        .await
        .unwrap();
    assert!(mgr.write_at("file-uuid-len", 8, b"abcd").await.is_err());
    assert!(mgr.write_at("file-uuid-len", 0, b"0123456789").await.is_ok());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_finalize_picks_next_free_name() {
    let dir = temp_recv_dir("conflict");
    let mgr = StorageManager::new(dir.clone());
    std::fs::write(dir.join("out.bin"), b"old").unwrap();

    let content = b"AAAABBBBCC";
    mgr.create_slot(
        "file-uuid-conflict".into(),
        "out.bin".into(),
        content.len() as u64,
        3,
        None,
        false,
    )
    .await
    .unwrap();
    mgr.write_at("file-uuid-conflict", 0, &content[0..4])
        .await
        .unwrap();
    mgr.write_at("file-uuid-conflict", 4, &content[4..7])
        .await
        .unwrap();
    mgr.write_at("file-uuid-conflict", 7, &content[7..])
        .await
        .unwrap();
    for id in 0..3 {
        mgr.finish_stream("file-uuid-conflict", id).await.unwrap();
    }

    let path = mgr.finalize("file-uuid-conflict", None).await.unwrap();
    assert_eq!(path, dir.join("out (1).bin"));
    assert_eq!(std::fs::read(&path).unwrap(), content);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_finalize_renames_when_target_exists() {
    let dir = temp_recv_dir("rename");
    let mgr = StorageManager::new(dir.clone());
    std::fs::write(dir.join("a.bin"), b"occupied").unwrap();

    mgr.create_slot("f3".into(), "a.bin".into(), 4, 1, None, false)
        .await
        .unwrap();
    mgr.write_at("f3", 0, b"data").await.unwrap();
    mgr.finish_stream("f3", 0).await.unwrap();

    let path = mgr.finalize("f3", None).await.unwrap();
    assert_eq!(path, dir.join("a (1).bin"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_empty_file_single_empty_stream() {
    let dir = temp_recv_dir("empty");
    let mgr = StorageManager::new(dir.clone());

    mgr.create_slot("empty-1".into(), "empty.bin".into(), 0, 1, None, false)
        .await
        .unwrap();
    let slot = mgr.finish_stream("empty-1", 0).await.unwrap();
    assert!(slot.is_complete());
    let path = mgr.finalize("empty-1", None).await.unwrap();
    assert_eq!(std::fs::read(path).unwrap(), b"");

    let _ = std::fs::remove_dir_all(&dir);
}
