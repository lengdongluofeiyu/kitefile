//! 协议层测试：DataFrameHeader / ControlMessage / WsEvent 序列化

use ftcore::protocol::{ControlMessage, DataFrameHeader, WsEvent};
use ftcore::transfer::{TransferProgress, TransferStatus};

#[test]
fn test_dataframe_header_roundtrip() {
    let h = DataFrameHeader {
        file_id_prefix: 0x1234_5678_9abc_def0,
        chunk_id: 42,
        data_len: 1024,
        _reserved: 0,
    };
    let bytes = h.to_bytes();
    assert_eq!(bytes.len(), DataFrameHeader::SIZE);
    assert_eq!(bytes.len(), 24);

    let back = DataFrameHeader::from_bytes(&bytes).unwrap();
    assert_eq!(back.file_id_prefix, h.file_id_prefix);
    assert_eq!(back.chunk_id, h.chunk_id);
    assert_eq!(back.data_len, h.data_len);
    assert_eq!(back._reserved, h._reserved);
}

#[test]
fn test_dataframe_header_too_short() {
    let short = [0u8; 16];
    assert!(DataFrameHeader::from_bytes(&short).is_err());
}

#[test]
fn test_control_message_roundtrip() {
    let msgs = vec![
        ControlMessage::Offer {
            version: 1,
            file_name: "a.mp4".into(),
            file_size: 1024,
            file_id: "uuid-1".into(),
            chunk_size: 16 * 1024 * 1024,
            chunk_count: 1,
            sha256: "abc".into(),
            resume_token: None,
        },
        ControlMessage::ChunkAck {
            file_id: "uuid-1".into(),
            chunk_id: 7,
            ok: true,
        },
        ControlMessage::Cancel {
            file_id: "uuid-1".into(),
            reason: "user canceled".into(),
        },
    ];
    for m in &msgs {
        let line = m.to_line().unwrap();
        assert!(line.ends_with('\n'));
        let back = ControlMessage::from_line(&line).unwrap();
        match (m, back) {
            (ControlMessage::Offer { file_id: a, .. }, ControlMessage::Offer { file_id: b, .. })
            | (
                ControlMessage::ChunkAck { file_id: a, .. },
                ControlMessage::ChunkAck { file_id: b, .. },
            )
            | (
                ControlMessage::Cancel { file_id: a, .. },
                ControlMessage::Cancel { file_id: b, .. },
            ) => assert_eq!(a, &b),
            _ => panic!("variant mismatch"),
        }
    }
}

#[test]
fn test_ws_event_progress_flatten() {
    let ev = WsEvent::Progress {
        progress: TransferProgress {
            file_id: "f1".into(),
            file_name: "test.bin".into(),
            file_size: 100,
            bytes_transferred: 50,
            chunks_done: 1,
            chunks_total: 2,
            speed_bps: 1024,
            status: TransferStatus::InProgress,
            error: None,
            incoming: false,
            file_path: None,
        },
    };
    let s = serde_json::to_string(&ev).unwrap();
    // tag 字段 + flatten 的进度字段应平铺在同一层
    assert!(s.contains(r#""event_type":"progress""#));
    assert!(s.contains(r#""file_id":"f1""#));
    assert!(s.contains(r#""bytes_transferred":50"#));

    // 反序列化回枚举
    let back: WsEvent = serde_json::from_str(&s).unwrap();
    match back {
        WsEvent::Progress { progress } => {
            assert_eq!(progress.file_id, "f1");
            assert_eq!(progress.status, TransferStatus::InProgress);
            assert!(!progress.incoming);
        }
        _ => panic!("expected Progress variant"),
    }
}

#[test]
fn test_ws_event_incoming_shape() {
    let s = r#"{
        "event_type": "incoming",
        "incoming_id": "inc-1",
        "file_id": "f1",
        "file_name": "test.bin",
        "file_size": 100,
        "sha256": null,
        "from_id": "dev-1",
        "from_name": "sender",
        "from_ip": "192.168.1.2",
        "from_gateway_port": 7878,
        "from_transfer_port": 7879,
        "created_at": 1700000000000,
        "decision": null
    }"#;
    let ev: WsEvent = serde_json::from_str(s).unwrap();
    match ev {
        WsEvent::Incoming { entry } => {
            assert_eq!(entry.incoming_id, "inc-1");
            assert_eq!(entry.file_id, "f1");
            assert_eq!(entry.from_ip, "192.168.1.2");
            assert_eq!(entry.decision, None);
        }
        _ => panic!("expected Incoming variant"),
    }
}

#[test]
fn test_ws_event_incoming_resolved_shape() {
    let s = r#"{"event_type":"incoming_resolved","incoming_id":"inc-1","accepted":true}"#;
    let ev: WsEvent = serde_json::from_str(s).unwrap();
    match ev {
        WsEvent::IncomingResolved {
            incoming_id,
            accepted,
        } => {
            assert_eq!(incoming_id, "inc-1");
            assert!(accepted);
        }
        _ => panic!("expected IncomingResolved variant"),
    }
}
