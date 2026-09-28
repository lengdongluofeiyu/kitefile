//! P2 回归：配对模式的 mDNS TXT `pair=1` 必须传播到对端设备表。
//!
//! 背景（真实反馈）：两个设备都开启了配对模式，设备列表却互相看不见。
//! 本测试在同机起两个 daemon，走真实组播链路验证「开启 → 对端可见 pair=1
//! → 关闭 → 对端回落 pair=0」的完整传播，把「逻辑错误」与「用户环境
//! 组播不通」区分开。
//!
//! 环境策略：无非环回网卡、或同机组播本身不通时 SKIP（与 gateway_test
//! 的 require_sockets 同思路），不阻塞无网络的 CI。

use kitefile::discovery::DiscoveryService;
use std::sync::Arc;
use std::time::Duration;

async fn wait_for<T>(dur: Duration, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + dur;
    loop {
        if let Some(v) = f() {
            return Some(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test]
async fn pairing_mode_txt_propagates_to_peer() {
    if kitefile::discovery::my_ipv4_addrs().is_empty() {
        eprintln!("SKIP pairing_mode_txt_propagates_to_peer: 无非环回 IPv4");
        return;
    }

    let a = match DiscoveryService::new(
        "pair-a".into(),
        "pair-test-a".into(),
        18091,
        18191,
        None,
    ) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            eprintln!("SKIP pairing_mode_txt_propagates_to_peer: daemon A 启动失败 {e}");
            return;
        }
    };
    let b = match DiscoveryService::new(
        "pair-b".into(),
        "pair-test-b".into(),
        18092,
        18192,
        None,
    ) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            eprintln!("SKIP pairing_mode_txt_propagates_to_peer: daemon B 启动失败 {e}");
            return;
        }
    };
    b.clone().spawn_event_loop(tokio::runtime::Handle::current());

    // ---- 第一阶段：基础组播必须通（B 能发现 A），否则环境无法验证 ----
    let found = wait_for(Duration::from_secs(8), || {
        b.list_devices()
            .iter()
            .find(|d| d.id == "pair-test-a")
            .cloned()
    })
    .await;
    let Some(dev) = found else {
        eprintln!("SKIP pairing_mode_txt_propagates_to_peer: 同机组播不通（B 未发现 A）");
        return;
    };
    assert!(!dev.pair, "初始注册应为 pair=0，实际 {:?}", dev.pair);

    // ---- 第二阶段：A 开启配对模式 → B 必须看到 pair=1 ----
    a.clone()
        .set_pairing_mode(true)
        .expect("A 开启配对模式失败");
    let updated = wait_for(Duration::from_secs(8), || {
        let d = b
            .list_devices()
            .into_iter()
            .find(|d| d.id == "pair-test-a")?;
        d.pair.then_some(())
    })
    .await;
    assert!(
        updated.is_some(),
        "A 开启配对模式 8 秒后 B 仍看不到 pair=1：mDNS TXT 更新未传播到对端设备表"
    );

    // ---- 第三阶段：关闭 → 对端回落 pair=0（防“卡在开启”） ----
    a.clone()
        .set_pairing_mode(false)
        .expect("A 关闭配对模式失败");
    let cleared = wait_for(Duration::from_secs(8), || {
        let d = b
            .list_devices()
            .into_iter()
            .find(|d| d.id == "pair-test-a")?;
        (!d.pair).then_some(())
    })
    .await;
    assert!(
        cleared.is_some(),
        "A 关闭配对模式 8 秒后 B 仍显示 pair=1：关闭路径未传播"
    );
}
