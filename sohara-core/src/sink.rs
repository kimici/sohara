//! Sink trait - data consumers that receive records

use async_trait::async_trait;

use crate::record::Record;
use crate::Result;

/// A sink that consumes records
#[async_trait]
pub trait Sink: Send + Sync {
    /// Send a record to this sink
    async fn send(&self, record: Record) -> Result<()>;

    /// Flush any buffered records
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Name of this sink for logging/debugging
    fn name(&self) -> &str;
}

/// A sink that collects records into a vector (useful for testing)
pub struct VecSink {
    name: String,
    records: tokio::sync::Mutex<Vec<Record>>,
}

impl VecSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            records: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn into_records(self) -> Vec<Record> {
        self.records.into_inner()
    }
}

#[async_trait]
impl Sink for VecSink {
    async fn send(&self, record: Record) -> Result<()> {
        self.records.lock().await.push(record);
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A sink that logs records (for debugging)
pub struct LogSink {
    name: String,
}

impl LogSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait]
impl Sink for LogSink {
    async fn send(&self, record: Record) -> Result<()> {
        tracing::info!("[{}] Received record: {:?}", self.name, record.id);
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Sink for Box<dyn Sink> {
    async fn send(&self, record: Record) -> Result<()> {
        (**self).send(record).await
    }

    async fn flush(&self) -> Result<()> {
        (**self).flush().await
    }

    fn name(&self) -> &str {
        (**self).name()
    }
}
