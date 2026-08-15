//! In-process pub/sub event bus with bounded channels

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::Value;
use sohara_core::{EventBus, Result};
use tokio::sync::mpsc;

/// A bounded in-process topic bus shared across trigger and sink steps.
pub struct InProcessBus {
    capacity: usize,
    topics: Mutex<HashMap<String, Vec<mpsc::Sender<Value>>>>,
}

impl InProcessBus {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            topics: Mutex::new(HashMap::new()),
        }
    }

    /// Subscribe to a topic, returning the receiver side of a bounded channel.
    #[must_use]
    pub fn subscribe(&self, topic: &str) -> mpsc::Receiver<Value> {
        let (sender, receiver) = mpsc::channel(self.capacity);
        self.topics
            .lock()
            .expect("bus lock poisoned")
            .entry(topic.to_owned())
            .or_default()
            .push(sender);
        receiver
    }
}

impl EventBus for InProcessBus {
    fn publish(&self, topic: &str, payload: Value) -> Result<()> {
        let senders = self
            .topics
            .lock()
            .expect("bus lock poisoned")
            .get(topic)
            .cloned()
            .unwrap_or_default();
        if senders.is_empty() {
            tracing::warn!("no subscribers for topic '{topic}', dropping payload");
            return Ok(());
        }
        for sender in senders {
            if let Err(mpsc::error::TrySendError::Full(_)) = sender.try_send(payload.clone()) {
                tracing::warn!("subscriber for topic '{topic}' is full, dropping payload");
            }
        }
        Ok(())
    }
}
