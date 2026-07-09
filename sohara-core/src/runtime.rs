//! Runtime - manages the execution of pipelines and triggers

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::error::Result;
use crate::pipeline::Pipeline;

/// The main runtime that manages pipelines and triggers
pub struct Runtime {
    name: String,
    pipelines: Arc<RwLock<HashMap<String, Pipeline>>>,
}

impl Runtime {
    /// Create a new runtime
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pipelines: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the runtime name
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Register a pipeline
    pub async fn register_pipeline(&self, pipeline: Pipeline) {
        let mut pipelines = self.pipelines.write().await;
        pipelines.insert(pipeline.name().to_string(), pipeline);
    }

    /// Start the runtime (placeholder for actual implementation)
    #[allow(clippy::unused_async)]
    pub async fn start(&self) -> Result<()> {
        tracing::info!("[{}] Runtime starting...", self.name);
        // TODO: Implement actual runtime logic
        // - Start all registered pipelines
        // - Start all triggers
        // - Handle shutdown signals
        Ok(())
    }

    /// Stop the runtime
    #[allow(clippy::unused_async)]
    pub async fn stop(&self) -> Result<()> {
        tracing::info!("[{}] Runtime stopping...", self.name);
        // TODO: Implement graceful shutdown
        Ok(())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new("default")
    }
}
