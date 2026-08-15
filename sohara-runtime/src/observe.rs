//! Runtime observability helpers (D1): error ring buffer

use std::collections::VecDeque;

use serde::Serialize;

/// Maximum number of retained error events.
pub const ERROR_RING_CAP: usize = 100;

/// One recorded runtime error, for the dashboard error stream.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorEvent {
    pub time: String,
    pub step: String,
    /// Error category: `when` / `transform` / `sink` / `source` / `edge`.
    pub kind: String,
    pub message: String,
}

/// A bounded ring of recent errors (newest first when read).
#[derive(Default)]
pub struct ErrorRing {
    events: VecDeque<ErrorEvent>,
}

impl ErrorRing {
    /// Push one event, dropping the oldest when the ring is full.
    pub fn record(&mut self, step: &str, kind: &str, message: String) {
        if self.events.len() >= ERROR_RING_CAP {
            self.events.pop_front();
        }
        self.events.push_back(ErrorEvent {
            time: chrono::Utc::now().to_rfc3339(),
            step: step.to_owned(),
            kind: kind.to_owned(),
            message,
        });
    }

    /// The retained events, newest first.
    #[must_use]
    pub fn events(&self) -> Vec<ErrorEvent> {
        self.events.iter().rev().cloned().collect()
    }
}
