//! Implementations of the `sohara` CLI subcommands.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use sohara_core::{ComponentRegistry, StateStore};
use sohara_persistence::JsonFileStore;

use crate::history::{append_history, default_history_path, failed_report};
use crate::report::{print_stats, print_steps};
use crate::{ApproveFlags, RunFlags, ServeFlags, ServerFlags};

pub(crate) fn init_logging() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

pub(crate) async fn run_flow(flags: RunFlags) -> Result<()> {
    let RunFlags {
        flow,
        extensions,
        resume,
        verbose,
        history,
    } = flags;
    let started = chrono::Utc::now().to_rfc3339();
    let config = load_flow(&flow)?;
    let (registry, host) = registry_and_extensions(&extensions)?;
    let store = store_for(&config, &host)?;
    let history = history.as_deref().unwrap_or(default_history_path());
    match sohara_runtime::run_flow_with_store_report(&config, &registry, store, resume).await {
        Ok(report) => finish_run(&config, &report, history, verbose),
        Err(error) => fail_run(&config, started, history, error),
    }
}

fn finish_run(
    config: &sohara_config::FlowConfig,
    report: &sohara_runtime::RunReport,
    history: &Path,
    verbose: bool,
) -> Result<()> {
    print_stats("finished", &config.name, &report.stats);
    if verbose {
        print_steps(report);
    }
    append_history(history, report, None)?;
    if report.stats.errors > 0 {
        std::process::exit(1);
    }
    Ok(())
}

fn fail_run<E: std::fmt::Display>(
    config: &sohara_config::FlowConfig,
    started: String,
    history: &Path,
    error: E,
) -> Result<()> {
    append_history(
        history,
        &failed_report(&config.name, started),
        Some(error.to_string()),
    )?;
    eprintln!("Flow '{}' failed: {error}", config.name);
    std::process::exit(1)
}

pub(crate) async fn serve_flow(flags: ServeFlags) -> Result<()> {
    let verbose = flags.verbose;
    let ServeFlags {
        flow, extensions, ..
    } = &flags;
    let config = load_flow(flow)?;
    let (registry, host) = registry_and_extensions(extensions)?;
    reject_external_bus_with_queue(&config)?;
    let bus = Arc::new(sohara_triggers::InProcessBus::new(128));
    let store = store_for(&config, &host)?;
    let shared_bus = shared_bus_for(&config, &host)?;
    let options = serve_options(flags, store, shared_bus, host);
    let stats =
        sohara_runtime::serve_with_shutdown_opts(&config, &registry, bus, ctrl_c(), options)
            .await?;
    print_stats("stopped", &config.name, &stats);
    if verbose {
        println!("(step statistics are available on /admin/metrics)");
    }
    Ok(())
}

fn serve_options(
    flags: ServeFlags,
    store: Option<Arc<dyn StateStore>>,
    shared_bus: Option<Arc<dyn sohara_core::EventBus>>,
    host: sohara_runtime::StdioExtensionHost,
) -> sohara_runtime::ServeOptions {
    let ServeFlags {
        admin,
        admin_token,
        history,
        resume,
        relay,
        relay_token,
        ..
    } = flags;
    sohara_runtime::ServeOptions {
        store,
        admin,
        admin_token,
        history: Some(history.unwrap_or_else(|| default_history_path().to_owned())),
        resume,
        relay,
        relay_token,
        shared_bus,
        extension_host: Some(Arc::new(host)),
    }
}

fn reject_external_bus_with_queue(config: &sohara_config::FlowConfig) -> Result<()> {
    if config.event_bus.is_some()
        && config
            .triggers
            .iter()
            .any(|trigger| trigger.trigger_type == "queue")
    {
        return Err(anyhow!(
            "queue triggers require the built-in in-process bus; external event_bus is not supported with trigger.type=queue"
        ));
    }
    Ok(())
}

fn registry_and_extensions(
    extensions: &[PathBuf],
) -> Result<(ComponentRegistry, sohara_runtime::StdioExtensionHost)> {
    let mut registry = ComponentRegistry::new();
    sohara_builtins::register_all(&mut registry);
    sohara_io::register_all(&mut registry);
    sohara_js::register_all(&mut registry);
    let builtin_paths = builtin_extension_paths();
    let host = sohara_runtime::load_stdio_extensions_with_trusted(
        &mut registry,
        &builtin_paths,
        extensions,
    )?;
    for extension in host.loaded() {
        tracing::info!(
            "loaded stdio extension: {} v{} [{}]",
            extension.name,
            extension.version,
            extension.registrations.join(", ")
        );
    }
    Ok((registry, host))
}

fn load_flow(path: &Path) -> Result<sohara_config::FlowConfig> {
    match path.extension().and_then(std::ffi::OsStr::to_str) {
        Some("js") => {
            let value = sohara_js::evaluate_flow_file(path).map_err(|error| {
                anyhow!("failed to evaluate JS flow '{}': {error}", path.display())
            })?;
            sohara_config::FlowConfig::from_json_value(value, path.parent())
                .map_err(|error| anyhow!("invalid JS flow '{}': {error}", path.display()))
        }
        _ => sohara_config::FlowConfig::load(path).map_err(Into::into),
    }
}

fn builtin_extension_paths() -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../extensions/builtin");
    if root.exists() {
        vec![root]
    } else {
        Vec::new()
    }
}

/// Build the flow's state store from its checkpoint config (paths relative
/// to the flow file directory).
fn store_for(
    config: &sohara_config::FlowConfig,
    host: &sohara_runtime::StdioExtensionHost,
) -> Result<Option<Arc<dyn StateStore>>> {
    let Some(store) = config
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.store.as_ref())
    else {
        return Ok(None);
    };
    match store {
        sohara_config::StoreConfig::Path(path) => Ok(Some(Arc::new(
            JsonFileStore::new(PathBuf::from(path)).expect("open state store"),
        ))),
        sohara_config::StoreConfig::Component(component) => host
            .build_state_store(&sohara_config::StoreConfig::Component(component.clone()))?
            .map_or_else(
                || {
                    Err(anyhow!(
                        "unknown external state store type '{}'",
                        component.component_type
                    ))
                },
                |store| Ok(Some(store)),
            ),
    }
}

fn shared_bus_for(
    config: &sohara_config::FlowConfig,
    host: &sohara_runtime::StdioExtensionHost,
) -> Result<Option<Arc<dyn sohara_core::EventBus>>> {
    let Some(event_bus) = &config.event_bus else {
        return Ok(None);
    };
    host.build_event_bus(event_bus)?.map_or_else(
        || {
            Err(anyhow!(
                "unknown external event_bus type '{}'",
                event_bus.component_type
            ))
        },
        |bus| Ok(Some(bus)),
    )
}

/// Start the zero-config server: load extensions, bind the admin API, load no flows.
pub(crate) async fn run_server(flags: ServerFlags) -> Result<()> {
    let ServerFlags {
        addr,
        admin_token,
        data_dir,
        extensions,
    } = flags;
    // Validate the token before touching the filesystem or binding a port.
    let state = Arc::new(sohara_runtime::ServerState::new(admin_token)?);
    // Extensions are available for flows created later via the management API (C2).
    let (_registry, _host) = registry_and_extensions(&extensions)?;
    // Prepare the data layout without loading any flow (C1 contract).
    let flows_dir = data_dir.join("flows");
    std::fs::create_dir_all(&flows_dir)?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (bound, handle) = sohara_runtime::spawn_server(addr, state, async move {
        let _ = shutdown_rx.await;
    })
    .await?;
    tracing::info!(
        "sohara server listening on {bound} (no flows loaded, data-dir={})",
        data_dir.display()
    );

    ctrl_c().await;
    // Signal graceful shutdown, then wait for in-flight requests to drain.
    let _ = shutdown_tx.send(());
    if let Err(error) = handle.await {
        tracing::error!("sohara server task failed to join: {error}");
    }
    Ok(())
}

pub(crate) async fn approve_flow(flags: ApproveFlags) -> Result<()> {
    let ApproveFlags {
        flow,
        extensions,
        step,
    } = flags;
    let config = load_flow(&flow)?;
    let (registry, host) = registry_and_extensions(&extensions)?;
    let store = store_for(&config, &host)?.ok_or_else(|| {
        anyhow!("flow has no checkpoint store; declare checkpoint.store to use approve")
    })?;
    let approved =
        sohara_runtime::approve_pending(&config, &registry, store, step.as_deref()).await?;
    println!(
        "Approved {approved} pending record(s) of flow '{}'",
        config.name
    );
    Ok(())
}

async fn ctrl_c() {
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
