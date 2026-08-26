//! Graph executor: walks records through the flow graph

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use serde_json::{Map, Value};
use tokio::sync::Mutex;

use crate::pause::PauseGate;
use crate::stats::{Counters, ExecutorConfig, RunReport, StatsSnapshot, StepStat};

use sohara_core::{Record, Result, TransformOutcome};

use crate::buffers::{BatchBuffer, JoinBuffer};
use crate::graph::{ErrorPolicy, FlowGraph, Node};
use crate::observe::{ErrorEvent, ErrorRing};

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Executes a [`FlowGraph`].
pub struct Executor {
    pub(crate) graph: Arc<FlowGraph>,
    pub(crate) vars: Map<String, Value>,
    pub(crate) counters: Counters,
    pub(crate) joins: HashMap<String, Arc<Mutex<JoinBuffer>>>,
    pub(crate) batches: HashMap<String, Arc<Mutex<BatchBuffer>>>,
    pub(crate) sinks: Vec<String>,
    pub(crate) step_stats: Mutex<BTreeMap<String, StepStat>>,
    pub(crate) error_ring: Mutex<ErrorRing>,
    started_at: String,
    pub(crate) pause: Option<Arc<PauseGate>>,
    pub(crate) runtime: Arc<crate::persist::RuntimeState>,
    pub(crate) flow_name: String,
    pub(crate) checkpoint_every: Option<u64>,
}

impl Executor {
    #[must_use]
    pub(crate) fn new(
        graph: Arc<FlowGraph>,
        vars: Map<String, Value>,
        config: ExecutorConfig,
        runtime: Arc<crate::persist::RuntimeState>,
    ) -> Self {
        let (joins, batches, sinks) = crate::buffers::scan_nodes(&graph);
        let flow_name = graph.name.clone();
        Self {
            graph,
            vars,
            counters: Counters::default(),
            joins,
            batches,
            sinks,
            step_stats: Mutex::new(BTreeMap::new()),
            error_ring: Mutex::new(ErrorRing::default()),
            started_at: chrono::Utc::now().to_rfc3339(),
            pause: config.pause,
            runtime,
            flow_name,
            checkpoint_every: config.checkpoint_every,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            processed: self.counters.processed.load(Ordering::Relaxed),
            filtered: self.counters.filtered.load(Ordering::Relaxed),
            errors: self.counters.errors.load(Ordering::Relaxed),
            waiting: self.counters.waiting.load(Ordering::Relaxed),
            duplicates: self.counters.duplicates.load(Ordering::Relaxed),
        }
    }

    /// Full run report including per-step statistics (S6).
    pub async fn report(&self) -> RunReport {
        RunReport {
            run_id: self.runtime.run_id.clone(),
            flow: self.flow_name.clone(),
            started_at: self.started_at.clone(),
            stats: self.snapshot(),
            steps: self.step_stats.lock().await.clone(),
        }
    }

    /// Pause intake (serve mode); in-flight records drain naturally.
    pub fn pause(&self) {
        if let Some(gate) = &self.pause {
            gate.set_paused(true);
        }
    }

    /// Resume intake.
    pub fn resume(&self) {
        if let Some(gate) = &self.pause {
            gate.set_paused(false);
        }
    }

    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.pause.as_ref().is_some_and(|gate| gate.paused())
    }

    /// Record a runtime error into the dashboard ring (D1).
    pub(crate) async fn record_error(&self, step: &str, kind: &str, message: String) {
        self.error_ring.lock().await.record(step, kind, message);
    }

    /// Recent errors, newest first (D1 dashboard).
    pub async fn error_events(&self) -> Vec<ErrorEvent> {
        self.error_ring.lock().await.events()
    }

    pub(crate) fn apply_transform<'a>(
        self: &Arc<Self>,
        node: &'a Arc<Node>,
        transform: &'a dyn sohara_core::Transform,
        record: Record,
    ) -> BoxFuture<'a, Result<()>> {
        let this = self.clone();
        let node = node.clone();
        Box::pin(async move {
            let mut attempt = 0u32;
            loop {
                let started = Instant::now();
                let outcome = transform.transform(record.clone()).await;
                let nanos = started.elapsed().as_nanos() as u64;
                let failure = match outcome {
                    Ok(TransformOutcome::Pass(record)) => {
                        this.tick_step(&node.id, |stat| {
                            stat.processed += 1;
                            stat.nanos += nanos;
                        })
                        .await;
                        return this.route(record, &node).await;
                    }
                    Ok(TransformOutcome::Filtered) => {
                        this.counters.filtered.fetch_add(1, Ordering::Relaxed);
                        this.tick_step(&node.id, |stat| {
                            stat.filtered += 1;
                            stat.nanos += nanos;
                        })
                        .await;
                        return Ok(());
                    }
                    Ok(TransformOutcome::Expand(records)) => {
                        this.tick_step(&node.id, |stat| {
                            stat.processed += 1;
                            stat.nanos += nanos;
                        })
                        .await;
                        for record in records {
                            this.route(record, &node).await?;
                        }
                        return Ok(());
                    }
                    Ok(TransformOutcome::Fail(error)) => error,
                    Err(error) => error,
                };
                match node.policy {
                    ErrorPolicy::Retry { max, backoff } if attempt < max => {
                        attempt += 1;
                        tokio::time::sleep(backoff).await;
                        tracing::warn!("[{}] failed (attempt {attempt}/{max}): {failure}", node.id);
                    }
                    ErrorPolicy::Continue => {
                        this.counters.errors.fetch_add(1, Ordering::Relaxed);
                        this.tick_step(&node.id, |stat| {
                            stat.errors += 1;
                            stat.nanos += nanos;
                        })
                        .await;
                        this.record_error(&node.id, "transform", failure.to_string())
                            .await;
                        tracing::warn!("[{}] failed (continue): {failure}", node.id);
                        return Ok(());
                    }
                    _ => {
                        this.counters.errors.fetch_add(1, Ordering::Relaxed);
                        this.tick_step(&node.id, |stat| {
                            stat.errors += 1;
                            stat.nanos += nanos;
                        })
                        .await;
                        this.record_error(&node.id, "transform", failure.to_string())
                            .await;
                        tracing::error!("[{}] failed: {failure}", node.id);
                        return Err(failure);
                    }
                }
            }
        })
    }
}
