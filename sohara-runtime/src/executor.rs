//! Graph executor: walks records through the flow graph

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde_json::{Map, Value};
use tokio::sync::Mutex;

use crate::pause::PauseGate;
use crate::stats::{Counters, ExecutorConfig, RunReport, StatsSnapshot, StepStat};

use sohara_core::{eval, is_truthy, Record, Result, StateStore, TransformOutcome};

use crate::buffers::{BatchBuffer, JoinBuffer};
use crate::control::apply_control;
use crate::graph::{ErrorPolicy, FlowGraph, Node, NodeStep};

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
    started_at: String,
    pub(crate) pause: Option<Arc<PauseGate>>,
    pub(crate) store: Option<Arc<dyn StateStore>>,
    pub(crate) run_id: String,
    pub(crate) flow_name: String,
    pub(crate) checkpoint_every: Option<u64>,
    pub(crate) since_checkpoint: AtomicUsize,
    pub(crate) states: Mutex<HashMap<String, Value>>,
}

impl Executor {
    #[must_use]
    pub fn new(graph: Arc<FlowGraph>, vars: Map<String, Value>, config: ExecutorConfig) -> Self {
        let (joins, batches, sinks) = crate::buffers::scan_nodes(&graph);
        let flow_name = graph.name.clone();
        let run_id =
            crate::persist::resolve_run_id(&flow_name, config.store.as_ref(), config.resume);
        let states = Mutex::new(crate::persist::initial_states(&graph, &run_id, &config));
        Self {
            graph,
            vars,
            counters: Counters::default(),
            joins,
            batches,
            sinks,
            step_stats: Mutex::new(BTreeMap::new()),
            started_at: chrono::Utc::now().to_rfc3339(),
            pause: config.pause,
            store: config.store,
            run_id,
            flow_name,
            checkpoint_every: config.checkpoint_every,
            since_checkpoint: AtomicUsize::new(0),
            states,
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
            run_id: self.run_id.clone(),
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

    pub(crate) fn walk(
        self: &Arc<Self>,
        record: Record,
        node_id: String,
    ) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        Box::pin(async move {
            let Some(node) = this.graph.node(&node_id).cloned() else {
                tracing::warn!("route to unknown node '{node_id}', dropping record");
                return Ok(());
            };
            if let Some(when) = &node.when {
                match eval(when, &record.payload, &this.ectx()) {
                    Ok(value) if is_truthy(&value) => {}
                    Ok(_) => return Ok(()),
                    Err(error) => {
                        tracing::warn!("[{}] 'when' failed: {error}", node.id);
                        this.counters.errors.fetch_add(1, Ordering::Relaxed);
                        this.tick_step(&node.id, |stat| stat.errors += 1).await;
                        return Ok(());
                    }
                }
            }
            match &node.step {
                NodeStep::Sink(sink) => {
                    if this.is_delivered(&record) {
                        this.counters.duplicates.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    let started = Instant::now();
                    let result = sink.send(record.clone()).await;
                    let nanos = started.elapsed().as_nanos() as u64;
                    match result {
                        Ok(()) => {
                            this.mark_delivered(&record);
                            this.counters.processed.fetch_add(1, Ordering::Relaxed);
                            this.tick_step(&node.id, |stat| {
                                stat.processed += 1;
                                stat.nanos += nanos;
                            })
                            .await;
                            this.maybe_checkpoint().await;
                        }
                        Err(error) => {
                            tracing::error!("[{}] sink failed: {error}", node.id);
                            this.counters.errors.fetch_add(1, Ordering::Relaxed);
                            this.tick_step(&node.id, |stat| {
                                stat.errors += 1;
                                stat.nanos += nanos;
                            })
                            .await;
                        }
                    }
                }
                NodeStep::Transform(transform) => {
                    this.apply_transform(&node, transform.as_ref(), record)
                        .await?;
                }
                NodeStep::Control(control) => {
                    apply_control(&this, &node, control, record).await?;
                }
                NodeStep::Source(_) => {
                    this.route(record, &node).await?;
                }
            }
            Ok(())
        })
    }

    pub(crate) fn route(
        self: &Arc<Self>,
        record: Record,
        node: &Arc<Node>,
    ) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        let routes = node.routes.clone();
        let node_id = node.id.clone();
        Box::pin(async move {
            for route in &routes {
                if let Some(when) = &route.when {
                    match eval(when, &record.payload, &this.ectx()) {
                        Ok(value) if is_truthy(&value) => {}
                        Ok(_) => continue,
                        Err(error) => {
                            tracing::warn!("[{node_id}] edge 'when' failed: {error}");
                            this.counters.errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    }
                }
                this.walk(record.clone(), route.to.clone()).await?;
            }
            Ok(())
        })
    }

    fn apply_transform<'a>(
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
                        tracing::error!("[{}] failed: {failure}", node.id);
                        return Err(failure);
                    }
                }
            }
        })
    }
}
