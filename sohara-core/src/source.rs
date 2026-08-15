//! Source trait - data producers that emit records

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::record::Record;
use crate::Result;

/// A source that produces records
#[async_trait]
pub trait Source: Send + Sync {
    /// Start streaming records
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>>;

    /// Name of this source for logging/debugging
    fn name(&self) -> &str;
}

/// A source that yields records from a vector (useful for testing)
pub struct VecSource {
    name: String,
    records: Vec<Record>,
}

impl VecSource {
    pub fn new(name: impl Into<String>, records: Vec<Record>) -> Self {
        Self {
            name: name.into(),
            records,
        }
    }
}

#[async_trait]
impl Source for VecSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let records = self.records.clone();
        Ok(Box::pin(futures::stream::iter(records.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Source for Box<dyn Source> {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        (**self).stream().await
    }

    fn name(&self) -> &str {
        (**self).name()
    }
}

/// A source with a lifecycle, used by `serve` mode.
///
/// `start` runs before the graph executes; `stop` is called on graceful
/// shutdown and must cause `stream()` to end.
#[async_trait]
pub trait Trigger: Source {
    /// Start the trigger (e.g. bind the HTTP listener).
    async fn start(&self) -> Result<()> {
        Ok(())
    }

    /// Stop the trigger so its stream ends.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}
