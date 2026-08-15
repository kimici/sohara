//! Pipeline - combines source, transforms, and sink

use futures::StreamExt;

use crate::error::Result;
use crate::record::Record;
use crate::sink::Sink;
use crate::source::Source;
use crate::transform::{Transform, TransformOutcome};

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
            match record {
                Ok(record) => {
                    self.process_record(record, transforms, sink, &mut stats)
                        .await;
                }
                Err(error) => {
                    tracing::error!("[{}] Source error: {}", self.name, error);
                    stats.errors += 1;
                }
            }
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
        let mut stats = PipelineStats::new();
        let mut results = Vec::new();
        let mut stream = source.stream().await?;

        while let Some(record) = stream.next().await {
            match record {
                Ok(record) => {
                    results.extend(self.apply_transforms(record, transforms, &mut stats).await);
                }
                Err(error) => {
                    tracing::error!("Source error: {}", error);
                    stats.errors += 1;
                }
            }
        }

        Ok(results)
    }

    /// Process a single record through transforms and send results to the sink
    async fn process_record(
        &self,
        record: Record,
        transforms: &[Box<dyn Transform>],
        sink: &impl Sink,
        stats: &mut PipelineStats,
    ) {
        for record in self.apply_transforms(record, transforms, stats).await {
            match sink.send(record).await {
                Ok(()) => stats.processed += 1,
                Err(error) => {
                    tracing::error!("[{}] Sink failed: {}", self.name, error);
                    stats.errors += 1;
                }
            }
        }
    }

    /// Apply transforms sequentially to a record, returning surviving records
    async fn apply_transforms(
        &self,
        record: Record,
        transforms: &[Box<dyn Transform>],
        stats: &mut PipelineStats,
    ) -> Vec<Record> {
        let mut pending = vec![record];
        for transform in transforms {
            let mut next = Vec::new();
            for record in pending {
                next.extend(self.apply_one(transform.as_ref(), record, stats).await);
            }
            pending = next;
            if pending.is_empty() {
                break;
            }
        }
        pending
    }

    /// Apply a single transform to one record, updating stats
    async fn apply_one(
        &self,
        transform: &dyn Transform,
        record: Record,
        stats: &mut PipelineStats,
    ) -> Vec<Record> {
        match transform.transform(record).await {
            Ok(TransformOutcome::Pass(record)) => vec![record],
            Ok(TransformOutcome::Filtered) => {
                stats.filtered += 1;
                Vec::new()
            }
            Ok(TransformOutcome::Expand(records)) => records,
            Ok(TransformOutcome::Fail(error)) | Err(error) => {
                self.log_failure(transform, &error);
                stats.errors += 1;
                Vec::new()
            }
        }
    }

    fn log_failure(&self, transform: &dyn Transform, error: &crate::Error) {
        tracing::error!(
            "[{}] Transform '{}' failed: {}",
            self.name,
            transform.name(),
            error
        );
    }
}

/// Statistics from a pipeline run
#[derive(Debug, Default)]
pub struct PipelineStats {
    /// Records successfully sent to the sink
    pub processed: usize,
    /// Records filtered out by a transform
    pub filtered: usize,
    /// Source/transform/sink failures encountered
    pub errors: usize,
}

impl PipelineStats {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}
