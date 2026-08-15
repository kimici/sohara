//! Cooperative pause gate for serve-mode admin control (S6)

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// A shared pause flag: while paused, the executor holds pulled source
/// records unprocessed (stopping further intake, so back pressure
/// propagates upstream) until resumed.
pub struct PauseGate {
    paused: AtomicBool,
    notify: Notify,
}

impl Default for PauseGate {
    fn default() -> Self {
        Self {
            paused: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}

impl PauseGate {
    #[must_use]
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Set the paused state; resuming wakes every waiter.
    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
        if !paused {
            self.notify.notify_waiters();
        }
    }

    /// Wait until the gate is unpaused.
    pub async fn wait_unpaused(&self) {
        while self.paused.load(Ordering::Relaxed) {
            self.notify.notified().await;
        }
    }
}
