use serde_json::Value;

use crate::Result;

/// Synchronous bridge used by scripts to start and stop sub-flows.
pub trait FlowRunner: Send + Sync {
    /// Start one sub-flow defined by its JSON config and return an opaque
    /// handle id. Finite flows may block until completion before returning.
    fn run(&self, flow: Value) -> Result<String>;

    /// Stop a previously started sub-flow. Stopping an unknown or already
    /// finished handle is a no-op.
    fn stop(&self, handle_id: &str) -> Result<()>;
}
