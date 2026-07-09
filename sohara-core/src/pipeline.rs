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
                    // Apply transforms sequentially
                    let mut current = record;
                    let mut skip = false;

                    for transform in transforms {
                        match transform.transform(current.clone()).await {
                            Ok(transformed) => current = transformed,
                            Err(crate::Error::Transform(_)) => {
                                // Transform filtered out the record
                                skip = true;
                                stats.filtered += 1;
                                break;
                            }
                            Err(e) => {
                                tracing::error!(
                                    "[{}] Transform '{}' failed: {}",
                                    self.name,
                                    transform.name(),
                                    e
                                );
                                stats.errors += 1;
                                skip = true;
                                break;
                            }
                        }
                    }

                    if !skip {
                        match sink.send(current).await {
                            Ok(()) => stats.processed += 1,
                            Err(e) => {
                                tracing::error!("[{}] Sink failed: {}", self.name, e);
                                stats.errors += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("[{}] Source error: {}", self.name, e);
                    stats.errors += 1;
                }
            }
        }

        // Flush the sink
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
            match record {
                Ok(record) => {
                    let mut current = record;
                    let mut skip = false;

                    for transform in transforms {
                        match transform.transform(current.clone()).await {
                            Ok(transformed) => current = transformed,
                            Err(crate::Error::Transform(_)) => {
                                skip = true;
                                break;
                            }
                            Err(e) => {
                                tracing::error!("Transform failed: {}", e);
                                skip = true;
                                break;
                            }
                        }
                    }

                    if !skip {
                        results.push(current);
                    }
                }
                Err(e) => {
                    tracing::error!("Source error: {}", e);
                }
            }
        }

        Ok(results)
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
    pub fn new() -> Self {
        Self::default()
    }
}
