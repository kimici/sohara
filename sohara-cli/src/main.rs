//! `sohara` command-line interface: run / serve / server / approve / history / init

mod commands;
mod history;
mod report;
mod scaffold;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use commands::{approve_flow, init_logging, run_flow, run_server, serve_flow};
use history::show_history;
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
    Run(RunFlags),
    /// Serve a flow with its triggers until Ctrl+C
    Serve(ServeFlags),
    /// Start an empty server with the admin API (no flows loaded)
    Server(ServerFlags),
    /// Approve parked records of a flow's approve steps
    Approve(ApproveFlags),
    /// Show recent run history
    History(HistoryFlags),
    /// Scaffold a new flow project (flow.yaml + data/)
    Init(InitFlags),
}

#[derive(Args)]
struct RunFlags {
    /// Path to the flow YAML or JS file
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
}

/// Serve-mode command flags (D1 dashboard).
#[derive(Args)]
struct ServeFlags {
    /// Path to the flow YAML or JS file
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
}

#[derive(Args)]
struct ServerFlags {
    /// Address for the admin API
    #[arg(long, value_name = "ADDR")]
    addr: SocketAddr,
    /// Bearer token required on every endpoint (mandatory)
    #[arg(long, value_name = "TOKEN")]
    admin_token: String,
    /// Data directory for flow definitions and runtime state
    #[arg(long, value_name = "PATH", default_value = ".sohara/server")]
    data_dir: PathBuf,
    /// Extension manifest file or directory (repeatable)
    #[arg(long, value_name = "PATH")]
    extensions: Vec<PathBuf>,
}

#[derive(Args)]
struct ApproveFlags {
    /// Path to the flow YAML or JS file
    flow: PathBuf,
    /// Extension manifest file or directory (repeatable)
    #[arg(long, value_name = "PATH")]
    extensions: Vec<PathBuf>,
    /// Only approve records parked at this step id
    #[arg(long)]
    step: Option<String>,
}

#[derive(Args)]
struct HistoryFlags {
    /// Run history file (default: .sohara/history.jsonl)
    #[arg(long)]
    history: Option<PathBuf>,
    /// Show only the last N entries
    #[arg(long, default_value_t = 20)]
    limit: usize,
}

#[derive(Args)]
struct InitFlags {
    /// Target directory (defaults to the current directory)
    #[arg(default_value = ".")]
    dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    dispatch(Cli::parse()).await
}

async fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Run(flags) => run_flow(flags).await,
        Command::Serve(flags) => serve_flow(flags).await,
        Command::Server(flags) => run_server(flags).await,
        Command::Approve(flags) => approve_flow(flags).await,
        Command::History(flags) => show_history(flags.history.as_deref(), flags.limit),
        Command::Init(flags) => init_project(&flags.dir),
    }
}
