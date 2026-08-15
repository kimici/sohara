//! Agent orchestration: heartbeat loop + command dispatch (D2)

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use reqwest::Client;

use crate::config::AgentConfig;
use crate::instance::{InstanceCommand, InstanceManager};
use crate::transport::{
    Command, CommandAck, ControlTransport, DesiredInstance, Heartbeat, InstanceReport,
};

/// One node agent: supervises local instances and talks to the plane.
pub struct Agent {
    node_id: String,
    heartbeat_ms: u64,
    managers: HashMap<String, Arc<InstanceManager>>,
    transport: Arc<dyn ControlTransport>,
    client: Client,
}

impl Agent {
    /// Build an agent from config, starting every declared instance.
    #[must_use]
    pub fn new(config: &AgentConfig, transport: Arc<dyn ControlTransport>) -> Self {
        let client = crate::http_client();
        let managers = config
            .instances
            .iter()
            .map(|spec| {
                let manager = InstanceManager::spawn(spec.clone(), client.clone());
                (spec.id.clone(), manager)
            })
            .collect();
        Self {
            node_id: config.node.id.clone(),
            heartbeat_ms: config.heartbeat_ms,
            managers,
            transport,
            client,
        }
    }

    /// Run heartbeats and command dispatch until `shutdown` resolves, then
    /// stop every instance.
    pub async fn run(mut self, shutdown: impl Future<Output = ()> + Send) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(self.heartbeat_ms));
        let mut last_seq = 0u64;
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let heartbeat = self.build_heartbeat().await;
                    let response = self.transport.heartbeat(&heartbeat).await;
                    match response {
                        Ok(response) => {
                            last_seq = self.dispatch(response.commands, last_seq).await;
                            self.reconcile(response.desired).await;
                        }
                        Err(error) => tracing::warn!("heartbeat failed: {error}"),
                    }
                }
                _ = &mut shutdown => break,
            }
        }
        for manager in self.managers.values() {
            manager.shutdown().await;
        }
        Ok(())
    }

    async fn build_heartbeat(&self) -> Heartbeat {
        let mut instances = Vec::new();
        for manager in self.managers.values() {
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

    /// Reconcile the local manager set with the plane's desired list (D3):
    /// spawn missing instances (honoring the desired state) and stop extras.
    async fn reconcile(&mut self, desired: Vec<DesiredInstance>) {
        let client = self.client.clone();
        let desired_ids: std::collections::HashSet<String> =
            desired.iter().map(|item| item.spec.id.clone()).collect();
        for item in desired {
            let id = item.spec.id.clone();
            if self.managers.contains_key(&id) {
                continue;
            }
            let manager =
                InstanceManager::spawn_with_desired(item.spec, &item.desired, client.clone());
            self.managers.insert(id, manager);
        }
        let stale: Vec<String> = self
            .managers
            .keys()
            .filter(|id| !desired_ids.contains(*id))
            .cloned()
            .collect();
        for id in stale {
            if let Some(manager) = self.managers.remove(&id) {
                manager.shutdown().await;
            }
        }
    }

    async fn execute(&self, command: &Command) -> CommandAck {
        let Some(op) = parse_op(&command.op) else {
            return CommandAck {
                seq: command.seq,
                ok: false,
                error: Some(format!("unknown op '{}'", command.op)),
            };
        };
        match self.managers.get(&command.instance) {
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
