//! Plane event history (D6): lifecycle and state-transition events

use serde_json::Value;

use crate::registry::{Inner, Registry};

/// Maximum retained plane events (D6 event history).
pub(crate) const EVENT_CAP: usize = 200;

impl Registry {
    /// Recent plane events, newest first.
    pub async fn list_events(&self) -> Vec<Value> {
        let inner = self.inner.lock().await;
        inner.events.iter().rev().cloned().collect()
    }

    /// Record one event.
    pub(crate) async fn record_event(&self, kind: &str, message: &str) {
        record_event_locked(&mut *self.inner.lock().await, kind, message);
    }

    /// Look up a declared instance (admin proxying).
    pub async fn instance_decl(&self, id: &str) -> Option<crate::types::InstanceDecl> {
        self.inner.lock().await.instances.get(id).cloned()
    }
}

pub(crate) fn record_event_locked(inner: &mut Inner, kind: &str, message: &str) {
    let event = serde_json::json!({
        "time": chrono::Utc::now().to_rfc3339(),
        "kind": kind,
        "message": message,
    });
    inner.events.push_back(event);
    if inner.events.len() > EVENT_CAP {
        inner.events.pop_front();
    }
}
