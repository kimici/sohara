//! Instance supervision: process lifecycle, health probing, restarts (D2)

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde::Serialize;
use tokio::process::Child;
use tokio::sync::{mpsc, RwLock};

use crate::config::InstanceSpec;
use crate::process::{handle_command, spawn_child, supervise_tick, terminate_child};

/// Lifecycle state of one managed instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    Starting,
    Running,
    Paused,
    Restarting,
    Stopping,
    Stopped,
    Failed,
}

impl InstanceState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Restarting => "restarting",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

/// Commands accepted from the agent loop (plane operations).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceCommand {
    Start,
    Stop,
    Restart,
    Pause,
    Resume,
}

/// Mutable supervisor state shared with the agent loop.
#[derive(Debug, Clone)]
pub(crate) struct Shared {
    pub(crate) id: String,
    pub(crate) state: InstanceState,
    pub(crate) paused: bool,
    pub(crate) healthy: bool,
    pub(crate) restarts: u32,
    pub(crate) admin: Option<String>,
    pub(crate) trigger: Option<String>,
}

/// Read-only instance state snapshot (heartbeat payloads, tests).
#[derive(Debug, Clone, Serialize)]
pub struct InstanceSnapshot {
    pub id: String,
    pub state: InstanceState,
    pub paused: bool,
    pub healthy: bool,
    pub restarts: u32,
    pub admin: Option<String>,
    pub trigger: Option<String>,
}

/// Supervises one instance: spawns the process, probes health, honors
/// lifecycle commands, and restarts according to policy.
pub struct InstanceManager {
    id: String,
    spec: InstanceSpec,
    snapshot: Arc<RwLock<Shared>>,
    cmd_tx: mpsc::UnboundedSender<InstanceCommand>,
}

impl InstanceManager {
    /// Spawn the supervisor task and the initial process.
    #[must_use]
    pub fn spawn(spec: InstanceSpec, client: Client) -> Arc<Self> {
        Self::spawn_with_desired(spec, "running", client)
    }

    /// Spawn with an initial desired state: `stopped` starts without a
    /// process, `paused` starts the process and pauses it (D3).
    #[must_use]
    pub fn spawn_with_desired(spec: InstanceSpec, desired: &str, client: Client) -> Arc<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(RwLock::new(Shared {
            id: spec.id.clone(),
            state: InstanceState::Starting,
            paused: false,
            healthy: false,
            restarts: 0,
            admin: spec.admin.clone(),
            trigger: spec.trigger.clone(),
        }));
        let manager = Arc::new(Self {
            id: spec.id.clone(),
            spec: spec.clone(),
            snapshot: shared,
            cmd_tx,
        });
        tokio::spawn(supervise(
            spec,
            client,
            cmd_rx,
            manager.snapshot.clone(),
            desired.to_owned(),
        ));
        manager
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The launch spec this manager was created with (D4 spec updates).
    #[must_use]
    pub fn spec(&self) -> &InstanceSpec {
        &self.spec
    }

    /// Send a lifecycle command (fire-and-forget).
    pub fn send(&self, command: InstanceCommand) {
        let _ = self.cmd_tx.send(command);
    }

    /// Current snapshot.
    pub async fn snapshot(&self) -> InstanceSnapshot {
        let shared = self.snapshot.read().await;
        InstanceSnapshot {
            id: shared.id.clone(),
            state: shared.state,
            paused: shared.paused,
            healthy: shared.healthy,
            restarts: shared.restarts,
            admin: shared.admin.clone(),
            trigger: shared.trigger.clone(),
        }
    }

    /// Stop the instance and wait for the supervisor to drain.
    pub async fn shutdown(&self) {
        self.send(InstanceCommand::Stop);
        for _ in 0..100 {
            if matches!(
                self.snapshot.read().await.state,
                InstanceState::Stopped | InstanceState::Failed
            ) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// The per-instance supervision loop: commands, exit detection, health ticks.
async fn supervise(
    spec: InstanceSpec,
    client: Client,
    mut rx: mpsc::UnboundedReceiver<InstanceCommand>,
    shared: Arc<RwLock<Shared>>,
    initial_desired: String,
) {
    let mut child: Option<Child> = None;
    let mut counters = Counters::default();
    if initial_desired != "stopped" {
        spawn_child(&spec, &shared, &mut child).await;
        if initial_desired == "paused" {
            crate::process::pause_now(&client, &shared, &spec).await;
        }
    } else {
        set_state(&shared, InstanceState::Stopped).await;
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else { break };
                handle_command(command, &spec, &client, &shared, &mut child).await;
            }
            _ = interval.tick() => {
                supervise_tick(&spec, &client, &shared, &mut child, &mut counters).await;
            }
        }
    }
    terminate_child(&mut child).await;
}

/// Restart-attempt and health-failure counters.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) attempts: u32,
    pub(crate) failures: u32,
}

/// Shared-state helpers used by the process module.
pub(crate) async fn set_state(shared: &RwLock<Shared>, state: InstanceState) {
    shared.write().await.state = state;
}

pub(crate) async fn state(shared: &RwLock<Shared>) -> InstanceState {
    shared.read().await.state
}
