//! `sohara-agent`: supervise local sohara instances for a control plane

use std::sync::Arc;

use anyhow::Result;
use sohara_agent::{Agent, AgentConfig, HttpTransport};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "agent.yaml".to_owned());
    let text = std::fs::read_to_string(&path)?;
    let config: AgentConfig = serde_yaml::from_str(&text)?;
    let transport = Arc::new(HttpTransport::new(
        config.plane.url.clone(),
        config.plane.token.clone(),
    ));
    tracing::info!(
        "agent '{}' starting with {} instance(s), plane {}",
        config.node.id,
        config.instances.len(),
        config.plane.url
    );
    Agent::new(&config, transport).run(ctrl_c()).await
}

fn init_logging() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
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
