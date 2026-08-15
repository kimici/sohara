//! Source pumping and flush lifecycle of the executor

use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::StreamExt;

use sohara_core::{Record, Result};

use std::collections::HashMap;

use serde_json::{Map, Value};
use tokio::sync::Mutex;

use crate::buffers::{BatchBuffer, JoinBuffer};
use crate::executor::Executor;
use crate::graph::{FlowGraph, Node, NodeStep};
use crate::stats::{Counters, StatsSnapshot, StepStat};

use sohara_core::EvalContext;

impl Executor {
    /// Run every source to completion, flush batch buffers and sinks, and
    /// return the final statistics.
    pub async fn run(self: Arc<Self>) -> Result<StatsSnapshot> {
        let mut tasks: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::new();
        for root in &self.graph().roots {
            let this = self.clone();
            let root = root.clone();
            tasks.push(tokio::spawn(async move { this.run_source(&root).await }));
        }
        for task in tasks {
            task.await.map_err(|error| {
                sohara_core::Error::Runtime(format!("source task failed: {error}"))
            })??;
        }
        self.flush_all().await?;
        Ok(self.snapshot())
    }

    async fn run_source(self: &Arc<Self>, root: &str) -> Result<()> {
        let Some(node) = self.graph().node(root) else {
            return Ok(());
        };
        let NodeStep::Source(source) = &node.step else {
            return Ok(());
        };
        let mut stream = source.stream().await?;
        while let Some(item) = stream.next().await {
            // Pause gate (S6): hold the pulled record without processing it
            // while paused; not pulling further propagates back pressure.
            if let Some(gate) = &self.pause {
                gate.wait_unpaused().await;
            }
            match item {
                Ok(record) => self.walk(record, root.to_owned()).await?,
                Err(error) => {
                    tracing::error!("[source {root}] {error}");
                    self.counters().errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    /// Inject a record at a node (used by `approve`), then flush everything.
    pub async fn inject_and_flush(
        self: &Arc<Self>,
        record: Record,
        node: &Arc<Node>,
    ) -> Result<()> {
        self.route(record, node).await?;
        self.flush_all().await
    }

    pub async fn flush_all(self: &Arc<Self>) -> Result<()> {
        self.flush_batches().await?;
        self.flush_sinks().await?;
        self.checkpoint().await;
        Ok(())
    }

    pub(crate) async fn flush_batches(self: &Arc<Self>) -> Result<()> {
        for (id, buffer) in &self.batches {
            let records = std::mem::take(&mut buffer.lock().await.records);
            if records.is_empty() {
                continue;
            }
            if let Some(node) = self.graph().node(id).cloned() {
                crate::control::emit_batch(self, &node, records).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn flush_sinks(&self) -> Result<()> {
        for id in &self.sinks {
            if let Some(node) = self.graph().node(id) {
                if let NodeStep::Sink(sink) = &node.step {
                    sink.flush().await?;
                }
            }
        }
        Ok(())
    }

    /// Apply a closure to the per-step statistic of `id`.
    pub(crate) async fn tick_step(&self, id: &str, f: impl FnOnce(&mut StepStat)) {
        let mut stats = self.step_stats.lock().await;
        f(stats.entry(id.to_owned()).or_default());
    }

    pub(crate) fn ectx(&self) -> EvalContext<'_> {
        EvalContext { vars: &self.vars }
    }

    pub(crate) fn graph(&self) -> &Arc<FlowGraph> {
        &self.graph
    }

    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }

    pub(crate) fn counters(&self) -> &Counters {
        &self.counters
    }

    pub(crate) fn vars(&self) -> &Map<String, Value> {
        &self.vars
    }

    pub(crate) fn joins(&self) -> &HashMap<String, Arc<Mutex<JoinBuffer>>> {
        &self.joins
    }

    pub(crate) fn batches(&self) -> &HashMap<String, Arc<Mutex<BatchBuffer>>> {
        &self.batches
    }
}
