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
use history::{append_history, default_history_path, failed_report, show_history};
use report::{print_stats, print_steps};

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
        /// Print the per-step statistics table on shutdown
        #[arg(long)]
        verbose: bool,
    },
    /// Approve parked records of a flow's approve steps
    Approve {
        /// Path to the flow YAML file
        flow: PathBuf,
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
            resume,
            verbose,
            history,
        } => run_flow(&flow, resume, verbose, history.as_deref()).await,
        Command::Serve {
            flow,
            admin,
            admin_token,
            history,
            resume,
            verbose,
        } => {
            let flags = ServeFlags {
                admin,
                admin_token,
                history,
                resume,
            };
            serve_flow(&flow, flags, verbose).await
        }
        Command::Approve { flow, step } => approve_flow(&flow, step.as_deref()).await,
        Command::History { history, limit } => show_history(history.as_deref(), limit),
        Command::Init { dir } => init_project(&dir),
    }
}

/// Serve-mode command flags (D1 dashboard).
struct ServeFlags {
    admin: Option<SocketAddr>,
    admin_token: Option<String>,
    history: Option<PathBuf>,
    resume: bool,
}

fn init_logging() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn registry() -> ComponentRegistry {
    let mut registry = ComponentRegistry::new();
    sohara_builtins::register_all(&mut registry);
    sohara_io::register_all(&mut registry);
    sohara_js::register_all(&mut registry);
    registry
}

/// Build the flow's state store from its checkpoint config (paths relative
/// to the flow file directory).
fn store_for(flow_path: &Path, config: &sohara_config::FlowConfig) -> Option<Arc<dyn StateStore>> {
    let path = config.checkpoint.as_ref()?.store.as_ref()?;
    let full = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        flow_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(path)
    };
    Some(Arc::new(
        JsonFileStore::new(full).expect("open state store"),
    ))
}

async fn run_flow(flow: &Path, resume: bool, verbose: bool, history: Option<&Path>) -> Result<()> {
    let started = chrono::Utc::now().to_rfc3339();
    let config = sohara_config::FlowConfig::load(flow)?;
    let registry = registry();
    let store = store_for(flow, &config);
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
    let registry = registry();
    let bus = Arc::new(sohara_triggers::InProcessBus::new(128));
    let store = store_for(flow, &config);
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

async fn approve_flow(flow: &Path, step: Option<&str>) -> Result<()> {
    let config = sohara_config::FlowConfig::load(flow)?;
    let registry = registry();
    let store = store_for(flow, &config).ok_or_else(|| {
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

fn init_project(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir.join("data"))?;
    std::fs::write(dir.join("flow.yaml"), FLOW_YAML)?;
    std::fs::write(dir.join("data").join("input.csv"), INPUT_CSV)?;
    println!("Created {}/flow.yaml and data/input.csv", dir.display());
    Ok(())
}

const FLOW_YAML: &str = r#"name: basic
version: "1"
steps:
  - id: in
    kind: source
    type: file
    config: { path: data/input.csv, format: csv }
  - id: adult
    kind: transform
    type: filter
    config: { where: "age >= 18" }
  - id: enrich
    kind: transform
    type: map
    config: { expr: { processed_at: "now()" } }
  - id: out
    kind: sink
    type: file
    config: { path: output/result.jsonl, format: jsonl }
"#;

const INPUT_CSV: &str = "name,age\nAlice,30\nBob,15\nCarol,40\n";
