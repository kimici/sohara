//! Key-value state store abstraction (single-machine persistence)

use serde_json::Value;

use crate::Result;

/// A durable key-value store for step states, checkpoints, delivered-record
/// markers, and parked approvals.
pub trait StateStore: Send + Sync {
    /// Load the value stored under `key`, if any.
    fn load(&self, key: &str) -> Result<Option<Value>>;

    /// Store `value` under `key`.
    fn save(&self, key: &str, value: Value) -> Result<()>;

    /// Remove the entry stored under `key`.
    fn delete(&self, key: &str) -> Result<()>;
}
