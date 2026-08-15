//! `sohara-plane`: control plane for sohara agents (D3)

use std::path::PathBuf;

use anyhow::Result;
use sohara_plane::Plane;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = parse_args();
    let state_label = args
        .state
        .as_deref()
        .map_or("<memory>", |p| p.to_str().unwrap_or("<memory>"))
        .to_owned();
    let plane = Plane::open(args.state, args.token);
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    tracing::info!(
        "sohara-plane listening on {} (state: {state_label})",
        args.addr
    );
    axum::serve(listener, plane.router()).await?;
    Ok(())
}

struct Args {
    addr: String,
    state: Option<PathBuf>,
    token: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        addr: "127.0.0.1:9600".to_owned(),
        state: None,
        token: None,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--addr" => args.addr = iter.next().expect("--addr value"),
            "--state" => args.state = Some(iter.next().expect("--state value").into()),
            "--token" => args.token = Some(iter.next().expect("--token value")),
            other => {
                eprintln!("unknown argument '{other}' (--addr/--state/--token)");
                std::process::exit(2);
            }
        }
    }
    args
}

fn init_logging() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
