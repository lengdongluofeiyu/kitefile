//! Storage 层测试：chunk 落盘、乱序写、finalize 校验、abort

use ftcore::storage::StorageManager;

fn temp_recv_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
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
