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

/// Extra serve-mode options (S4 state store, S6 admin API, D1 dashboard).
#[derive(Default)]
pub struct ServeOptions {
    pub store: Option<Arc<dyn StateStore>>,
    /// Bind the admin API (health / pause / resume / metrics / dashboard) here.
    pub admin: Option<std::net::SocketAddr>,
    /// Require `Authorization: Bearer <token>` on admin endpoints.
    pub admin_token: Option<String>,
    /// History file: served by `/admin/history`; serve mode appends an entry
    /// when it stops.
    pub history: Option<std::path::PathBuf>,
    /// Reuse the stored run id on start (D1/D2 restart contract).
    pub resume: bool,
    /// Plane relay URL: bridge the local event bus to the plane (D5a).
    pub relay: Option<String>,
    /// Bearer token for the relay endpoints.
    pub relay_token: Option<String>,
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
    let options = ServeOptions {
        store,
        admin: None,
        ..ServeOptions::default()
    };
    serve_with_shutdown_opts(flow, registry, bus, shutdown, options).await
}

/// Serve mode with extended options (state store, admin API, history).
pub async fn serve_with_shutdown_opts(
    flow: &sohara_config::FlowConfig,
    registry: &ComponentRegistry,
    bus: Arc<InProcessBus>,
    shutdown: impl Future<Output = ()> + Send,
    options: ServeOptions,
) -> Result<StatsSnapshot> {
    let triggers = build_triggers(flow, &bus)?;
    let relay_bus = options.relay.as_ref().map(|plane| {
        sohara_triggers::RelayBus::spawn(
            bus.clone(),
            plane.clone(),
            options.relay_token.clone(),
            queue_topics(flow),
        )
    });
    let ctx = BuildContext {
        vars: flow.vars.clone().into_iter().collect(),
        bus: Some(relay_bus.clone().map_or_else(
            || bus as std::sync::Arc<dyn sohara_core::EventBus>,
            |bridge| bridge as std::sync::Arc<dyn sohara_core::EventBus>,
        )),
    };
    let graph = Arc::new(FlowGraph::build_with_triggers(
        flow, registry, &ctx, &triggers,
    )?);
    let config = ExecutorConfig {
        store: options.store,
        resume: options.resume,
        checkpoint_every: flow
            .checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.every),
        pause: Some(Arc::new(crate::pause::PauseGate::default())),
    };
    let executor = Arc::new(Executor::new(graph, ctx.vars.clone(), config));
    let admin_task = match options.admin {
        Some(addr) => {
            let state = Arc::new(crate::admin::AdminState {
                executor: executor.clone(),
                triggers: flow
                    .triggers
                    .iter()
                    .map(|trigger| crate::admin::TriggerInfo {
                        id: trigger.id.clone(),
                        kind: trigger.trigger_type.clone(),
                    })
                    .collect(),
                token: options.admin_token,
                history: options.history.clone(),
            });
            Some(crate::admin::spawn(addr, state).await?)
        }
        None => None,
    };
    for trigger in &triggers {
        trigger.start().await?;
    }
    let mut run_task = tokio::spawn({
        let executor = executor.clone();
        async move { executor.run().await }
    });
    let outcome: Result<StatsSnapshot> = tokio::select! {
        result = &mut run_task => {
            result.map_err(|error| Error::Runtime(format!("executor task failed: {error}")))?
        }
        _ = shutdown => {
            tracing::info!("graceful shutdown requested");
            for trigger in &triggers {
                trigger.stop().await?;
            }
            run_task
                .await
                .map_err(|error| Error::Runtime(format!("executor task failed: {error}")))?
        }
    };
    if let Some(task) = admin_task {
        task.abort();
    }
    if let Some(bridge) = &relay_bus {
        bridge.stop();
    }
    if let Some(path) = &options.history {
        let report = executor.report().await;
        let (status, error) = match &outcome {
            Ok(_) => ("ok", None),
            Err(error) => ("error", Some(error.to_string())),
        };
        if let Err(error) = crate::history::append(path, &report, status, error.as_deref()) {
            tracing::error!("failed to append run history: {error}");
        }
    }
    outcome
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

/// Topics subscribed by the flow's queue triggers (relay subscriptions).
fn queue_topics(flow: &sohara_config::FlowConfig) -> Vec<String> {
    flow.triggers
        .iter()
        .filter(|trigger| trigger.trigger_type == "queue")
        .filter_map(|trigger| {
            trigger
                .config()
                .ok()
                .and_then(|config| config.get("topic").cloned())
                .and_then(|value| value.as_str().map(str::to_owned))
        })
        .collect()
}
