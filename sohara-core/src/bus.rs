//! In-process event bus abstraction

use serde_json::Value;

use crate::Result;

/// A publish-only event bus used by queue sinks and shared across steps.
pub trait EventBus: Send + Sync {
    /// Publish a payload to a topic, delivering it to every subscriber.
    ///
    /// # Errors
    /// Returns an error when the bus is unavailable; full subscribers are
    /// dropped with a warning (bounded backpressure).
    fn publish(&self, topic: &str, payload: Value) -> Result<()>;
}
