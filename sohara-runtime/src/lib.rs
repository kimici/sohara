//! Sohara Runtime: flow graph builder and executor (S2), serve mode (S3),
//! persistence and recovery (S4), observability and admin API (S6)

mod admin;
mod buffers;
pub mod control;
pub mod executor;
pub mod graph;
mod history;
pub mod node;
mod observe;
mod pause;
mod persist;
mod routes;
mod runner;
pub mod serve;
mod stats;
mod walker;
mod warnings;

pub use admin::TriggerInfo;
pub use executor::Executor;
pub use graph::{ErrorPolicy, FlowGraph, Node, NodeStep, Route};
pub use observe::{ErrorEvent, ErrorRing};
pub use pause::PauseGate;
pub use serve::{
    approve_pending, serve, serve_with_shutdown, serve_with_shutdown_opts, ServeOptions,
};
pub use stats::{ExecutorConfig, RunReport, StatsSnapshot, StepStat};

use serde_json::{Map, Value};
use sohara_core::{BuildContext, Result, StateStore};
use std::sync::Arc;

/// Build and execute a flow graph in one call (run mode; triggers ignored).
///
/// # Errors
/// Fails when the graph cannot be built or a step fails with
/// `on_error: fail` (the default).
pub async fn run_flow(
    flow: &sohara_config::FlowConfig,
    registry: &sohara_core::ComponentRegistry,
) -> Result<StatsSnapshot> {
    run_flow_with_store(flow, registry, None, false).await
}

/// Run mode with an optional state store and resume flag (S4).
pub async fn run_flow_with_store(
    flow: &sohara_config::FlowConfig,
    registry: &sohara_core::ComponentRegistry,
    store: Option<Arc<dyn StateStore>>,
    resume: bool,
) -> Result<StatsSnapshot> {
    run_flow_with_store_report(flow, registry, store, resume)
        .await
        .map(|report| report.stats)
}

/// Run mode returning the full report (run id, per-step stats; S6).
pub async fn run_flow_with_store_report(
    flow: &sohara_config::FlowConfig,
    registry: &sohara_core::ComponentRegistry,
    store: Option<Arc<dyn StateStore>>,
    resume: bool,
) -> Result<RunReport> {
    if !flow.triggers.is_empty() {
        tracing::warn!("flow declares triggers; they are ignored in 'run' mode (use 'serve')");
    }
    let ctx = BuildContext {
        vars: flow.vars.clone().into_iter().collect(),
        bus: None,
    };
    let graph = Arc::new(FlowGraph::build(flow, registry, &ctx)?);
    let vars: Map<String, Value> = ctx.vars.clone();
    let config = ExecutorConfig {
        store,
        resume,
        checkpoint_every: flow
            .checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.every),
        pause: None,
    };
    let executor = Arc::new(Executor::new(graph, vars, config));
    let runner = executor.clone();
    runner.run().await?;
    Ok(executor.report().await)
}
