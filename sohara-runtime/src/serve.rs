//! Serve mode: long-running triggers + graph execution + graceful shutdown,
//! plus the approve-pending recovery entry point (S4)

use std::future::Future;
use std::sync::Arc;

use sohara_core::{
    BuildContext, ComponentRegistry, ControlNode, Error, Result, StateStore, Trigger,
};
use sohara_triggers::{build_trigger, InProcessBus};

use crate::executor::Executor;
use crate::graph::{FlowGraph, NodeStep};
use crate::persist::take_approve_queue;
use crate::stats::{ExecutorConfig, StatsSnapshot};

/// Extra serve-mode options (S4 state store, S6 admin API).
#[derive(Default)]
pub struct ServeOptions {
    pub store: Option<Arc<dyn StateStore>>,
    /// Bind the admin API (health / pause / resume / metrics) to this address.
    pub admin: Option<std::net::SocketAddr>,
}

/// Run a flow in serve mode until the shutdown future resolves, then stop
/// triggers, drain in-flight work, and flush batch buffers and sinks.
pub async fn serve_with_shutdown(
    flow: &sohara_config::FlowConfig,
    registry: &ComponentRegistry,
    bus: Arc<InProcessBus>,
    store: Option<Arc<dyn StateStore>>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<StatsSnapshot> {
    let options = ServeOptions { store, admin: None };
    serve_with_shutdown_opts(flow, registry, bus, shutdown, options).await
}

/// Serve mode with extended options (state store, admin API; S4/S6).
pub async fn serve_with_shutdown_opts(
    flow: &sohara_config::FlowConfig,
    registry: &ComponentRegistry,
    bus: Arc<InProcessBus>,
    shutdown: impl Future<Output = ()> + Send,
    options: ServeOptions,
) -> Result<StatsSnapshot> {
    let triggers = build_triggers(flow, &bus)?;
    let ctx = BuildContext {
        vars: flow.vars.clone().into_iter().collect(),
        bus: Some(bus),
    };
    let graph = Arc::new(FlowGraph::build_with_triggers(
        flow, registry, &ctx, &triggers,
    )?);
    let config = ExecutorConfig {
        store: options.store,
        resume: false,
        checkpoint_every: flow
            .checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.every),
        pause: Some(Arc::new(crate::pause::PauseGate::default())),
    };
    let executor = Arc::new(Executor::new(graph, ctx.vars.clone(), config));
    let admin_task = match options.admin {
        Some(addr) => Some(crate::admin::spawn(addr, executor.clone()).await?),
        None => None,
    };
    for trigger in &triggers {
        trigger.start().await?;
    }
    let mut run_task = tokio::spawn({
        let executor = executor.clone();
        async move { executor.run().await }
    });
    tokio::select! {
        result = &mut run_task => {
            result.map_err(|error| Error::Runtime(format!("executor task failed: {error}")))??;
        }
        _ = shutdown => {
            tracing::info!("graceful shutdown requested");
            for trigger in &triggers {
                trigger.stop().await?;
            }
            run_task
                .await
                .map_err(|error| Error::Runtime(format!("executor task failed: {error}")))??;
        }
    }
    if let Some(task) = admin_task {
        task.abort();
    }
    Ok(executor.snapshot())
}

/// Run a flow in serve mode until SIGINT/SIGTERM.
pub async fn serve(
    flow: &sohara_config::FlowConfig,
    registry: &ComponentRegistry,
    bus: Arc<InProcessBus>,
) -> Result<StatsSnapshot> {
    serve_with_shutdown(flow, registry, bus, None, shutdown_signal()).await
}

/// Approve every parked record of the flow's `approve` steps (optionally a
/// single step), re-injecting them after the gate and flushing.
///
/// Returns the number of records approved.
pub async fn approve_pending(
    flow: &sohara_config::FlowConfig,
    registry: &ComponentRegistry,
    store: Arc<dyn StateStore>,
    step: Option<&str>,
) -> Result<usize> {
    let ctx = BuildContext {
        vars: flow.vars.clone().into_iter().collect(),
        bus: None,
    };
    let graph = Arc::new(FlowGraph::build(flow, registry, &ctx)?);
    let config = ExecutorConfig {
        store: Some(store),
        resume: true,
        checkpoint_every: None,
        pause: None,
    };
    let executor = Arc::new(Executor::new(graph, ctx.vars.clone(), config));
    let mut approved = 0usize;
    for (id, node) in &executor.graph().nodes {
        if !matches!(node.step, NodeStep::Control(ControlNode::Approve { .. })) {
            continue;
        }
        if step.is_some_and(|filter| filter != id.as_str()) {
            continue;
        }
        let parked = take_approve_queue(&executor, id)?;
        approved += parked.len();
        for record in parked {
            executor.inject_and_flush(record, node).await?;
        }
    }
    Ok(approved)
}

fn build_triggers(
    flow: &sohara_config::FlowConfig,
    bus: &Arc<InProcessBus>,
) -> Result<Vec<Arc<dyn Trigger>>> {
    flow.triggers
        .iter()
        .map(|config| build_trigger(config, Some(bus.clone())))
        .collect()
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("signal handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.expect("ctrl-c handler");
    }
}
