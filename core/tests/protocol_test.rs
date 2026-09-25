//! 协议层测试：StreamHeader / WsEvent 序列化 + 双端契约（工作流 B）
//!
//! 契约测试锁住 **core 序列化 ↔ Dart 解析** 的样例消息：
//! 字段名、状态字符串、JSON 类型任一漂移，这里先红。

use kitefile::protocol::{HttpOffer, StreamHeader, WsEvent};
use kitefile::transfer::{TransferProgress, TransferStatus};

#[test]
fn test_stream_header_roundtrip() {
    let h = StreamHeader {
        file_id_prefix: 0xdead_beef_cafe_babe,
        stream_id: 2,
        _reserved: 0,
        start_offset: 1 << 33,
        data_len: 3 * 1024 * 1024 * 1024,
    };
    let bytes = h.to_bytes();
    assert_eq!(bytes.len(), StreamHeader::SIZE);

    let back = StreamHeader::from_bytes(&bytes).unwrap();
    assert_eq!(back.file_id_prefix, h.file_id_prefix);
    assert_eq!(back.stream_id, h.stream_id);
    assert_eq!(back.start_offset, h.start_offset);
    assert_eq!(back.data_len, h.data_len);
}

#[test]
fn test_stream_header_too_short() {
    let short = [0u8; StreamHeader::SIZE - 1];
    assert!(StreamHeader::from_bytes(&short).is_err());
}

#[test]
fn test_ws_event_progress_flatten() {
    let p = kitefile::transfer::TransferProgress {
        file_id: "fid".into(),
        file_name: "a.bin".into(),
        file_size: 10,
        bytes_transferred: 4,
        chunks_done: 1,
        chunks_total: 2,
        speed_bps: 100,
        status: kitefile::transfer::TransferStatus::InProgress,
        error: None,
        incoming: false,
        file_path: None,
        retry_note: None,
    };
    let ev = WsEvent::Progress { progress: p };
    let s = serde_json::to_string(&ev).unwrap();
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["event_type"], "progress");
    assert_eq!(v["file_id"], "fid");
    assert_eq!(v["bytes_transferred"], 4);
}

#[test]
fn test_ws_event_incoming_shape() {
    let entry = kitefile::protocol::IncomingEntry {
        incoming_id: "inc-1".into(),
        file_id: "fid".into(),
        file_name: "a.bin".into(),
        file_size: 10,
        stream_count: Some(2),
        sha256: None,
        sha256_deferred: true,
        from_id: "d1".into(),
        from_name: "peer".into(),
        from_ip: "1.2.3.4".into(),
        from_gateway_port: 7878,
        from_transfer_port: 7879,
        batch_id: None,
        batch_index: None,
        batch_total: None,
        created_at: 1,
        decision: None,
    };
    let ev = WsEvent::Incoming { entry };
    let s = serde_json::to_string(&ev).unwrap();
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["event_type"], "incoming");
    assert_eq!(v["incoming_id"], "inc-1");
    assert_eq!(v["stream_count"], 2);
}

#[test]
fn test_ws_event_incoming_resolved_shape() {
    let ev = WsEvent::IncomingResolved {
        incoming_id: "inc-1".into(),
        accepted: true,
    };
    let s = serde_json::to_string(&ev).unwrap();
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["event_type"], "incoming_resolved");
    assert_eq!(v["accepted"], true);
}

// ============ 工作流 B：双端契约测试 ============

/// 状态机的线上字符串：与 desktop/mobile `_parseStatus` 的 switch 分支逐一对应。
/// 改任一侧（加状态/改名）必须同步另一侧，否则 UI 会把状态降级成「等待」。
#[test]
fn status_wire_strings_match_dart_contract() {
    use TransferStatus::*;
    let cases = [
        (Pending, "Pending"),
        (InProgress, "InProgress"),
        (Completed, "Completed"),
        (Failed, "Failed"),
        (Canceled, "Canceled"),
        (Interrupted, "Interrupted"),
    ];
    for (status, wire) in cases {
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, format!("\"{wire}\""), "状态序列化漂移");
        let back: TransferStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status, "状态反序列化漂移");
    }
}

/// `TransferProgress` 的 JSON 字段集合：Dart `fromJson` 依赖这些键与类型。
/// 新增进度字段 → 这里补一行 + 双端模型各加一个可选字段（可选即不破坏旧端）。
#[test]
fn progress_json_keys_match_dart_contract() {
    let p = TransferProgress {
        file_id: "fid".into(),
        file_name: "a.bin".into(),
        file_size: 10,
        bytes_transferred: 4,
        chunks_done: 1,
        chunks_total: 2,
        speed_bps: 100,
        status: TransferStatus::Interrupted,
        error: Some("流 2/2 失败（连接超时）".into()),
        incoming: true,
        file_path: Some("C:/tmp/a.bin".into()),
        retry_note: Some("第 2/3 次重试流 2…".into()),
    };
    let v = serde_json::to_value(&p).unwrap();
    for key in [
        "file_id",
        "file_name",
        "file_size",
        "bytes_transferred",
        "chunks_done",
        "chunks_total",
        "speed_bps",
        "status",
        "error",
        "incoming",
        "file_path",
        "retry_note",
    ] {
        assert!(v.get(key).is_some(), "缺少字段 {key}（Dart fromJson 依赖）");
    }
    // 类型契约：数字必须是 JSON number，状态必须是字符串
    assert!(v["file_size"].is_number());
    assert!(v["speed_bps"].is_number());
    assert_eq!(v["status"], "Interrupted");
    assert!(v["incoming"].is_boolean());
}

/// `HttpOffer` 的 JSON 字段集合（含 `version` 门槛字段）。
#[test]
fn offer_json_keys_match_contract() {
    let offer = HttpOffer {
        file_id: "fid".into(),
        file_name: "a.bin".into(),
        file_size: 10,
        stream_count: Some(2),
        sha256: None,
        sha256_deferred: true,
        from_id: "d1".into(),
        from_name: "peer".into(),
        from_ip: "1.2.3.4".into(),
        from_gateway_port: 7878,
        from_transfer_port: 7879,
        batch_id: Some("b1".into()),
        batch_index: Some(0),
        batch_total: Some(3),
        version: Some(kitefile::protocol::PROTOCOL_VERSION),
    };
    let v = serde_json::to_value(&offer).unwrap();
    for key in [
        "file_id",
        "file_name",
        "file_size",
        "stream_count",
        "sha256",
        "sha256_deferred",
        "from_id",
        "from_name",
        "from_ip",
        "from_gateway_port",
        "from_transfer_port",
        "batch_id",
        "batch_index",
        "batch_total",
        "version",
    ] {
        assert!(v.get(key).is_some(), "缺少字段 {key}");
    }
    assert_eq!(
        v["version"],
        kitefile::protocol::PROTOCOL_VERSION,
        "发送方必须携带当前协议版本"
    );
}

/// 旧版本 offer（无 version / 无 stream_count）必须还能反序列化（兼容降级），
/// 而不是解析失败把整条链路打死。
#[test]
fn legacy_offer_without_new_fields_deserializes() {
    let json = r#"{
        "file_id": "x", "file_name": "a", "file_size": 10,
        "sha256": null, "sha256_deferred": true,
        "from_id": "d", "from_name": "n", "from_ip": "1.2.3.4",
        "from_gateway_port": 7878, "from_transfer_port": 7879
    }"#;
    let offer: HttpOffer = serde_json::from_str(json).unwrap();
    assert_eq!(offer.version, None, "旧对端不携带版本 → 按 legacy 放行");
    assert_eq!(offer.stream_count, None);
}
