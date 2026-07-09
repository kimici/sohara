//! Pipeline - combines source, transforms, and sink

use futures::StreamExt;

use crate::error::Result;
use crate::record::Record;
use crate::sink::Sink;
use crate::source::Source;
use crate::transform::Transform;

/// A processing pipeline that connects a source to a sink through transforms
pub struct Pipeline {
    name: String,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Get the pipeline name
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run a pipeline with the given source, transforms, and sink
    pub async fn run<S, K>(
        &self,
        source: &S,
        transforms: &[Box<dyn Transform>],
        sink: &K,
    ) -> Result<PipelineStats>
    where
        S: Source,
        K: Sink,
    {
        let mut stats = PipelineStats::new();
        let mut stream = source.stream().await?;

        while let Some(record) = stream.next().await {
            let record = match record {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("[{}] Source error: {}", self.name, e);
                    stats.errors += 1;
                    continue;
                }
            };
            self.process_record(record, transforms, sink, &mut stats)
                .await;
        }

        sink.flush().await?;
        Ok(stats)
    }

    /// Run a pipeline and collect all records into a vector
    pub async fn run_collect<S>(
        &self,
        source: &S,
        transforms: &[Box<dyn Transform>],
    ) -> Result<Vec<Record>>
    where
        S: Source,
    {
        let mut results = Vec::new();
        let mut stream = source.stream().await?;

        while let Some(record) = stream.next().await {
            let record = match record {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Source error: {}", e);
                    continue;
                }
            };
            if let Some(transformed) = self.apply_transforms(record, transforms).await {
                results.push(transformed);
            }
        }

        Ok(results)
    }

    /// Process a single record through transforms and send to sink
    async fn process_record(
        &self,
        record: Record,
        transforms: &[Box<dyn Transform>],
        sink: &impl Sink,
        stats: &mut PipelineStats,
    ) {
        let Some(transformed) = self.apply_transforms(record, transforms).await else {
            return;
        };
        match sink.send(transformed).await {
            Ok(()) => stats.processed += 1,
            Err(e) => {
                tracing::error!("[{}] Sink failed: {}", self.name, e);
                stats.errors += 1;
            }
        }
    }

    /// Apply transforms sequentially, returning `None` if filtered
    async fn apply_transforms(
        &self,
        mut record: Record,
        transforms: &[Box<dyn Transform>],
    ) -> Option<Record> {
        for transform in transforms {
            match transform.transform(record.clone()).await {
                Ok(transformed) => record = transformed,
                Err(crate::Error::Transform(_)) => return None,
                Err(e) => {
                    tracing::error!(
                        "[{}] Transform '{}' failed: {}",
                        self.name,
                        transform.name(),
                        e
                    );
                    return None;
                }
            }
        }
        Some(record)
    }
}

/// Statistics from a pipeline run
#[derive(Debug, Default)]
pub struct PipelineStats {
    pub processed: usize,
    pub filtered: usize,
    pub errors: usize,
}

impl PipelineStats {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}
