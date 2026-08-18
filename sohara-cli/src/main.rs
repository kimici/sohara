//! `sohara` command-line interface: run / serve / approve / history / init

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use sohara_core::{ComponentRegistry, StateStore};
use sohara_persistence::JsonFileStore;

mod history;
mod report;
mod scaffold;
use history::{append_history, default_history_path, failed_report, show_history};
use report::{print_stats, print_steps};
use scaffold::init_project;

#[derive(Parser)]
#[command(
    name = "sohara",
    version,
    about = "Lightweight single-machine automation framework"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a flow once and print statistics
    Run {
        /// Path to the flow YAML file
        flow: PathBuf,
        /// Extension manifest file or directory (repeatable)
        #[arg(long, value_name = "PATH")]
        extensions: Vec<PathBuf>,
        /// Resume the previous run (idempotent re-delivery + state restore)
        #[arg(long)]
        resume: bool,
        /// Print the per-step statistics table
        #[arg(long)]
        verbose: bool,
        /// Run history file (default: .sohara/history.jsonl)
        #[arg(long)]
        history: Option<PathBuf>,
    },
    /// Serve a flow with its triggers until Ctrl+C
    Serve {
        /// Path to the flow YAML file
        flow: PathBuf,
        /// Extension manifest file or directory (repeatable)
        #[arg(long, value_name = "PATH")]
        extensions: Vec<PathBuf>,
        /// Enable the admin API + dashboard on this address
        #[arg(long, value_name = "ADDR")]
        admin: Option<SocketAddr>,
        /// Require this bearer token on admin endpoints
        #[arg(long, value_name = "TOKEN")]
        admin_token: Option<String>,
        /// Run history file (default: .sohara/history.jsonl; serve mode appends
        /// an entry on shutdown)
        #[arg(long)]
        history: Option<PathBuf>,
        /// Reuse the stored run id on start (restart contract)
        #[arg(long)]
        resume: bool,
        /// Bridge the event bus to a plane relay (D5a)
        #[arg(long, value_name = "URL")]
        relay: Option<String>,
        /// Bearer token for the relay endpoints
        #[arg(long, value_name = "TOKEN")]
        relay_token: Option<String>,
        /// Print the per-step statistics table on shutdown
        #[arg(long)]
        verbose: bool,
    },
    /// Approve parked records of a flow's approve steps
    Approve {
        /// Path to the flow YAML file
        flow: PathBuf,
        /// Extension manifest file or directory (repeatable)
        #[arg(long, value_name = "PATH")]
        extensions: Vec<PathBuf>,
        /// Only approve records parked at this step id
        #[arg(long)]
        step: Option<String>,
    },
    /// Show recent run history
    History {
        /// Run history file (default: .sohara/history.jsonl)
        #[arg(long)]
        history: Option<PathBuf>,
        /// Show only the last N entries
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Scaffold a new flow project (flow.yaml + data/)
    Init {
        /// Target directory (defaults to the current directory)
        #[arg(default_value = ".")]
        dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    dispatch(Cli::parse()).await
}

async fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Run {
            flow,
            extensions,
            resume,
            verbose,
            history,
        } => run_flow(&flow, &extensions, resume, verbose, history.as_deref()).await,
        Command::Serve {
            flow,
            extensions,
            admin,
            admin_token,
            history,
            resume,
            relay,
            relay_token,
            verbose,
        } => {
            serve_flow(
                &flow,
                serve_flags(
                    admin,
                    admin_token,
                    history,
                    resume,
                    relay,
                    relay_token,
                    extensions,
                ),
                verbose,
            )
            .await
        }
        Command::Approve {
            flow,
            extensions,
            step,
        } => approve_flow(&flow, &extensions, step.as_deref()).await,
        Command::History { history, limit } => show_history(history.as_deref(), limit),
        Command::Init { dir } => init_project(&dir),
    }
}

/// Pack the serve flags (one call site; arg-count lint is fine here).
#[allow(clippy::too_many_arguments)]
fn serve_flags(
    admin: Option<SocketAddr>,
    admin_token: Option<String>,
    history: Option<PathBuf>,
    resume: bool,
    relay: Option<String>,
    relay_token: Option<String>,
    extensions: Vec<PathBuf>,
) -> ServeFlags {
    ServeFlags {
        admin,
        admin_token,
        history,
        resume,
        relay,
        relay_token,
        extensions,
    }
}

/// Serve-mode command flags (D1 dashboard).
struct ServeFlags {
    admin: Option<SocketAddr>,
    admin_token: Option<String>,
    history: Option<PathBuf>,
    resume: bool,
    relay: Option<String>,
    relay_token: Option<String>,
    extensions: Vec<PathBuf>,
}

fn init_logging() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
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

async fn run_flow(
    flow: &Path,
    extensions: &[PathBuf],
    resume: bool,
    verbose: bool,
    history: Option<&Path>,
) -> Result<()> {
    let started = chrono::Utc::now().to_rfc3339();
    let config = sohara_config::FlowConfig::load(flow)?;
    let (registry, host) = registry_and_extensions(extensions)?;
    let store = store_for(&config, &host)?;
    let history = history.unwrap_or(default_history_path());
    match sohara_runtime::run_flow_with_store_report(&config, &registry, store, resume).await {
        Ok(report) => {
            print_stats("finished", &config.name, &report.stats);
            if verbose {
                print_steps(&report);
            }
            append_history(history, &report, None)?;
            if report.stats.errors > 0 {
                std::process::exit(1);
            }
            Ok(())
        }
        Err(error) => {
            append_history(
                history,
                &failed_report(&config.name, started),
                Some(error.to_string()),
            )?;
            eprintln!("Flow '{}' failed: {error}", config.name);
            std::process::exit(1);
        }
    }
}

async fn serve_flow(flow: &Path, flags: ServeFlags, verbose: bool) -> Result<()> {
    let config = sohara_config::FlowConfig::load(flow)?;
    let (registry, host) = registry_and_extensions(&flags.extensions)?;
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
    let bus = Arc::new(sohara_triggers::InProcessBus::new(128));
    let store = store_for(&config, &host)?;
    let shared_bus = shared_bus_for(&config, &host)?;
    let options = sohara_runtime::ServeOptions {
        store,
        admin: flags.admin,
        admin_token: flags.admin_token,
        history: Some(
            flags
                .history
                .unwrap_or_else(|| default_history_path().to_owned()),
        ),
        resume: flags.resume,
        relay: flags.relay,
        relay_token: flags.relay_token,
        shared_bus,
        extension_host: Some(Arc::new(host)),
    };
    let stats =
        sohara_runtime::serve_with_shutdown_opts(&config, &registry, bus, ctrl_c(), options)
            .await?;
    print_stats("stopped", &config.name, &stats);
    if verbose {
        println!("(step statistics are available on /admin/metrics)");
    }
    Ok(())
}

async fn approve_flow(flow: &Path, extensions: &[PathBuf], step: Option<&str>) -> Result<()> {
    let config = sohara_config::FlowConfig::load(flow)?;
    let (registry, host) = registry_and_extensions(extensions)?;
    let store = store_for(&config, &host)?.ok_or_else(|| {
        anyhow!("flow has no checkpoint store; declare checkpoint.store to use approve")
    })?;
    let approved = sohara_runtime::approve_pending(&config, &registry, store, step).await?;
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
