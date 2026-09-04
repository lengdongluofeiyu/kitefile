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
    // 数据通道上目前只有 ChunkAck 一种消息，握手全部走 HTTP。
    let acks = vec![
        ControlMessage::ChunkAck {
            file_id: "uuid-1".into(),
            chunk_id: 7,
            ok: true,
        },
        // ok=false：接收方告知这一块没写成功，发送方据此判失败
        ControlMessage::ChunkAck {
            file_id: "uuid-2".into(),
            chunk_id: 0,
            ok: false,
        },
    ];
    for m in &acks {
        let line = m.to_line().unwrap();
        assert!(line.ends_with('\n'), "行协议必须以换行结尾");
        let back = ControlMessage::from_line(&line).unwrap();
        // 往返要连 chunk_id、ok 一起对上——只比 file_id 的话，
        // 这两个字段的序列化 bug 会溜过去。
        match (m, back) {
            (
                ControlMessage::ChunkAck {
                    file_id: a,
                    chunk_id: ca,
                    ok: oa,
                },
                ControlMessage::ChunkAck {
                    file_id: b,
                    chunk_id: cb,
                    ok: ob,
                },
            ) => {
                assert_eq!(a, &b);
                assert_eq!(ca, &cb);
                assert_eq!(oa, &ob);
            }
        }
    }
}

/// 线格式锁：`{"type":"chunk_ack",...}` 是跨版本契约，
/// 改了 serde 的 tag / rename 配置就会让新旧版本互相解析不了。
#[test]
fn test_chunk_ack_wire_format() {
    let line = ControlMessage::ChunkAck {
        file_id: "f".into(),
        chunk_id: 3,
        ok: true,
    }
    .to_line()
    .unwrap();
    assert!(
        line.contains("\"type\":\"chunk_ack\""),
        "线格式变了会影响跨版本兼容：{line}"
    );
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
