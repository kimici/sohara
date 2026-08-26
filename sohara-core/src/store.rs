//! Key-value state store abstraction (single-machine persistence)

use serde_json::Value;

use crate::Result;

/// Synchronous access bridge for script-hosted per-step state.
pub trait StateAccessor: Send + Sync {
    /// Load the current state for one step id.
    fn get(&self, step_id: &str) -> Value;

    /// Replace the current state for one step id.
    fn set(&self, step_id: &str, value: Value);

    /// Persist every pending in-memory state snapshot.
    fn checkpoint(&self) -> Result<()>;
}

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
