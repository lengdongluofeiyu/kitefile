//! 协议层测试：StreamHeader / WsEvent 序列化

use ftcore::protocol::{StreamHeader, WsEvent};

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
    let p = ftcore::transfer::TransferProgress {
        file_id: "fid".into(),
        file_name: "a.bin".into(),
        file_size: 10,
        bytes_transferred: 4,
        chunks_done: 1,
        chunks_total: 2,
        speed_bps: 100,
        status: ftcore::transfer::TransferStatus::InProgress,
        error: None,
        incoming: false,
        file_path: None,
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
    let entry = ftcore::protocol::IncomingEntry {
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
