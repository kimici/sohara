//! Agent orchestration: heartbeat loop + command dispatch (D2)

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use reqwest::Client;

use crate::config::AgentConfig;
use crate::instance::{InstanceCommand, InstanceManager};
use crate::transport::{Command, CommandAck, ControlTransport, Heartbeat, InstanceReport};

/// One node agent: supervises local instances and talks to the plane.
pub struct Agent {
    node_id: String,
    heartbeat_ms: u64,
    managers: Vec<Arc<InstanceManager>>,
    transport: Arc<dyn ControlTransport>,
}

impl Agent {
    /// Build an agent from config, starting every declared instance.
    #[must_use]
    pub fn new(config: &AgentConfig, transport: Arc<dyn ControlTransport>) -> Self {
        let client = Client::new();
        let managers = config
            .instances
            .iter()
            .map(|spec| InstanceManager::spawn(spec.clone(), client.clone()))
            .collect();
        Self {
            node_id: config.node.id.clone(),
            heartbeat_ms: config.heartbeat_ms,
            managers,
            transport,
        }
    }

    /// Run heartbeats and command dispatch until `shutdown` resolves, then
    /// stop every instance.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(self.heartbeat_ms));
        let mut last_seq = 0u64;
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let heartbeat = self.build_heartbeat().await;
                    match self.transport.heartbeat(&heartbeat).await {
                        Ok(commands) => last_seq = self.dispatch(commands, last_seq).await,
                        Err(error) => tracing::warn!("heartbeat failed: {error}"),
                    }
                }
                _ = &mut shutdown => break,
            }
        }
        for manager in &self.managers {
            manager.shutdown().await;
        }
        Ok(())
    }

    async fn build_heartbeat(&self) -> Heartbeat {
        let mut instances = Vec::new();
        for manager in &self.managers {
            let snapshot = manager.snapshot().await;
            instances.push(InstanceReport {
                id: snapshot.id,
                state: snapshot.state,
                paused: snapshot.paused,
                healthy: snapshot.healthy,
                restarts: snapshot.restarts,
                admin: snapshot.admin,
            });
        }
        Heartbeat {
            node_id: self.node_id.clone(),
            time: chrono::Utc::now().to_rfc3339(),
            instances,
        }
    }

    /// Execute queued commands in order, acking each one (seq dedup).
    async fn dispatch(&self, commands: Vec<Command>, last_seq: u64) -> u64 {
        let mut seq = last_seq;
        for command in commands {
            if command.seq <= last_seq {
                continue;
            }
            let ack = self.execute(&command).await;
            seq = seq.max(command.seq);
            if let Err(error) = self.transport.ack(&ack).await {
                tracing::warn!("ack {} failed: {error}", command.seq);
            }
        }
        seq
    }

    async fn execute(&self, command: &Command) -> CommandAck {
        let Some(op) = parse_op(&command.op) else {
            return CommandAck {
                seq: command.seq,
                ok: false,
                error: Some(format!("unknown op '{}'", command.op)),
            };
        };
        match self.managers.iter().find(|m| m.id() == command.instance) {
            Some(manager) => {
                manager.send(op);
                CommandAck {
                    seq: command.seq,
                    ok: true,
                    error: None,
                }
            }
            None => CommandAck {
                seq: command.seq,
                ok: false,
                error: Some(format!("unknown instance '{}'", command.instance)),
            },
        }
    }
}

fn parse_op(op: &str) -> Option<InstanceCommand> {
    match op {
        "start" => Some(InstanceCommand::Start),
        "stop" => Some(InstanceCommand::Stop),
        "restart" => Some(InstanceCommand::Restart),
        "pause" => Some(InstanceCommand::Pause),
        "resume" => Some(InstanceCommand::Resume),
        _ => None,
    }
}
