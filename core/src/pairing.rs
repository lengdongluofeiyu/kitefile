//! 配对模式状态（阶段 5 · P2）
//!
//! 「进入配对模式」是设备的显式动作（默认关闭，不跨重启复活）：
//!
//! - 开启期间 mDNS TXT 广播 `pair=1`，对端设备表把本机标为「可配对」；
//!   关闭 / 120s 超时后改回 `pair=0`（见 `discovery` 的 re-register）。
//! - P3 起，`pair/hello` 等配对接口只在开启期间受理（配对接口的门）。
//!
//! 过期由 `discovery::set_pairing_mode` 里 spawn 的定时任务驱动，
//! 用**代数（generation）**做失效判断：重复开启 / 手动关闭都会让代数 +1，
//! 旧定时任务醒来时发现代数对不上就静默作废，不会把新状态误关。

use std::time::Duration;

use parking_lot::RwLock;
use tokio::time::Instant;

/// 配对模式默认时长（蓝牙可发现模式同款量级）
pub const PAIRING_TTL: Duration = Duration::from_secs(120);

#[derive(Debug)]
pub struct PairingState {
    /// (代数, 到期时刻)；`None` = 未开启
    inner: RwLock<(u64, Option<Instant>)>,
}

impl PairingState {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new((0, None)),
        }
    }

    /// 开启（或刷新倒计时）。返回新代数，供过期任务绑定。
    pub fn activate(&self) -> u64 {
        let mut g = self.inner.write();
        g.0 = g.0.wrapping_add(1);
        g.1 = Some(Instant::now() + PAIRING_TTL);
        g.0
    }

    /// 手动关闭。返回新代数（作废所有在途过期任务）。
    pub fn deactivate(&self) -> u64 {
        let mut g = self.inner.write();
        g.0 = g.0.wrapping_add(1);
        g.1 = None;
        g.0
    }

    /// 过期任务回调：仅当代数仍是 `gen` 且已到期时清掉状态并返回 true。
    pub fn expire_if_current(&self, gen: u64) -> bool {
        let mut g = self.inner.write();
        if g.0 != gen {
            return false;
        }
        let expired = g.1.map(|t| Instant::now() >= t).unwrap_or(false);
        if expired {
            g.1 = None;
            true
        } else {
            false
        }
    }

    /// 当前是否处于配对模式（惰性判断：即使没人在跑过期任务也正确）
    pub fn is_active(&self) -> bool {
        let g = self.inner.read();
        g.1.map(|t| Instant::now() < t).unwrap_or(false)
    }

    /// 剩余秒数（未开启 = 0）
    pub fn seconds_left(&self) -> u32 {
        let g = self.inner.read();
        g.1
            .map(|t| t.saturating_duration_since(Instant::now()).as_secs().min(u32::MAX as u64) as u32)
            .unwrap_or(0)
    }
}

impl Default for PairingState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_inactive() {
        let p = PairingState::new();
        assert!(!p.is_active());
        assert_eq!(p.seconds_left(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn activate_then_expire_with_correct_gen() {
        let p = PairingState::new();
        let gen = p.activate();
        assert!(p.is_active());
        assert!(p.seconds_left() > 0 && p.seconds_left() <= 120);

        // 未到期：不许清
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(!p.expire_if_current(gen));
        assert!(p.is_active());

        // 到期：清掉
        tokio::time::sleep(PAIRING_TTL).await;
        assert!(p.expire_if_current(gen));
        assert!(!p.is_active());
        assert_eq!(p.seconds_left(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_gen_cannot_expire_new_state() {
        let p = PairingState::new();
        let old_gen = p.activate();
        tokio::time::sleep(Duration::from_secs(5)).await;
        // 重复开启（刷新）→ 代数前进，旧过期任务作废
        let new_gen = p.activate();
        assert_ne!(old_gen, new_gen);

        // 只推进到刷新后的到期点之前（虚拟时间精确落点会让 now==expires）
        tokio::time::sleep(Duration::from_secs(100)).await;
        assert!(!p.expire_if_current(old_gen), "旧代数不得关闭新状态");
        assert!(p.is_active(), "刷新后的配对模式必须仍在倒计时");

        // 手动关闭同样推进代数
        let off_gen = p.deactivate();
        assert!(!p.is_active());
        assert!(!p.expire_if_current(new_gen), "已手动关闭，旧任务不得复活状态");
        let _ = off_gen;
    }

    #[tokio::test(start_paused = true)]
    async fn manual_deactivate_immediate() {
        let p = PairingState::new();
        p.activate();
        assert!(p.is_active());
        p.deactivate();
        assert!(!p.is_active());
    }
}
