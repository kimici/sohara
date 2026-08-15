//! Process lifecycle helpers: spawn, terminate, restart policy (D2)

use std::time::Duration;

use reqwest::Client;
use tokio::process::{Child, Command};
use tokio::sync::RwLock;

use crate::config::InstanceSpec;
use crate::instance::{set_state, state, Counters, InstanceCommand, InstanceState, Shared};
use crate::policy::restart_backoff;
use crate::probe::{admin_post, probe_health};

/// Build the launch command for one instance.
fn launch_command(spec: &InstanceSpec, admin: &str) -> Command {
    let mut command = Command::new(&spec.bin);
    if spec.args.is_empty() {
        command.args(["serve", &spec.flow, "--admin", admin]);
        if spec.resume {
            command.arg("--resume");
        }
        if let Some(token) = &spec.admin_token {
            command.args(["--admin-token", token]);
        }
    } else {
        command.args(&spec.args);
    }
    command
}

/// Pick a free loopback port (best-effort).
fn free_addr() -> Option<String> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .ok()?
        .local_addr()
        .ok()
        .map(|addr| addr.to_string())
}

/// Resolve the admin address once, then spawn the process.
pub(crate) async fn spawn_child(
    spec: &InstanceSpec,
    shared: &RwLock<Shared>,
    child: &mut Option<Child>,
) {
    let admin = {
        let mut shared = shared.write().await;
        if shared.admin.is_none() {
            shared.admin = free_addr();
        }
        shared.state = InstanceState::Starting;
        shared.admin.clone().unwrap_or_default()
    };
    match launch_command(spec, &admin).spawn() {
        Ok(process) => {
            *child = Some(process);
            let mut shared = shared.write().await;
            shared.state = InstanceState::Running;
            shared.healthy = true;
        }
        Err(error) => {
            tracing::error!("[{}] spawn failed: {error}", spec.id);
            set_state(shared, InstanceState::Failed).await;
            shared.write().await.healthy = false;
        }
    }
}

/// Send SIGTERM (grace) then SIGKILL, and reap the process.
pub(crate) async fn terminate_child(child: &mut Option<Child>) {
    let Some(mut child) = child.take() else {
        return;
    };
    if let Some(pid) = child.id() {
        #[cfg(unix)]
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
        #[cfg(not(unix))]
        let _ = child.kill().await;
    }
    let result = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    if result.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

/// Handle one lifecycle command.
pub(crate) async fn handle_command(
    command: InstanceCommand,
    spec: &InstanceSpec,
    client: &Client,
    shared: &RwLock<Shared>,
    child: &mut Option<Child>,
) {
    match command {
        InstanceCommand::Start => {
            if matches!(
                state(shared).await,
                InstanceState::Stopped | InstanceState::Failed
            ) {
                spawn_child(spec, shared, child).await;
            }
        }
        InstanceCommand::Stop => {
            set_state(shared, InstanceState::Stopping).await;
            terminate_child(child).await;
            set_state(shared, InstanceState::Stopped).await;
        }
        InstanceCommand::Restart => {
            set_state(shared, InstanceState::Restarting).await;
            terminate_child(child).await;
            spawn_child(spec, shared, child).await;
        }
        InstanceCommand::Pause => pause_instance(client, shared, spec).await,
        InstanceCommand::Resume => resume_instance(client, shared, spec).await,
    }
}

/// Pause without a command round-trip (initial desired state; D3).
pub(crate) async fn pause_now(client: &Client, shared: &RwLock<Shared>, spec: &InstanceSpec) {
    pause_instance(client, shared, spec).await;
}

async fn pause_instance(client: &Client, shared: &RwLock<Shared>, spec: &InstanceSpec) {
    let admin = shared.read().await.admin.clone();
    let Some(admin) = admin else {
        return;
    };
    if admin_post(client, &admin, spec.admin_token.as_deref(), "/admin/pause")
        .await
        .is_ok()
    {
        let mut shared = shared.write().await;
        shared.paused = true;
        shared.state = InstanceState::Paused;
    }
}

async fn resume_instance(client: &Client, shared: &RwLock<Shared>, spec: &InstanceSpec) {
    let admin = shared.read().await.admin.clone();
    let Some(admin) = admin else {
        return;
    };
    if admin_post(client, &admin, spec.admin_token.as_deref(), "/admin/resume")
        .await
        .is_ok()
    {
        let mut shared = shared.write().await;
        shared.paused = false;
        shared.state = InstanceState::Running;
    }
}

/// One supervision tick: detect exits, probe health, apply restart policy.
pub(crate) async fn supervise_tick(
    spec: &InstanceSpec,
    client: &Client,
    shared: &RwLock<Shared>,
    child: &mut Option<Child>,
    counters: &mut Counters,
) {
    if child_exited(child).await {
        *child = None;
        if state(shared).await == InstanceState::Stopping {
            set_state(shared, InstanceState::Stopped).await;
            return;
        }
        restart_or_fail(spec, shared, child, counters).await;
        return;
    }
    if !spec.health_enabled || child.is_none() {
        return;
    }
    let admin = shared.read().await.admin.clone();
    let Some(admin) = admin else {
        return;
    };
    match probe_health(client, &admin, spec.admin_token.as_deref()).await {
        Ok(health) => apply_health(spec, shared, child, counters, health).await,
        Err(error) => {
            tracing::debug!("[{}] health probe failed: {error}", spec.id);
            counters.failures += 1;
            check_health_restart(spec, shared, child, counters).await;
        }
    }
}

async fn child_exited(child: &mut Option<Child>) -> bool {
    match child.as_mut() {
        Some(child) => match child.try_wait() {
            Ok(Some(status)) => {
                tracing::info!("instance process exited: {status}");
                true
            }
            Ok(None) => false,
            Err(_) => true,
        },
        None => false,
    }
}

async fn apply_health(
    spec: &InstanceSpec,
    shared: &RwLock<Shared>,
    child: &mut Option<Child>,
    counters: &mut Counters,
    health: crate::probe::Health,
) {
    if !health.healthy {
        counters.failures += 1;
        check_health_restart(spec, shared, child, counters).await;
        return;
    }
    counters.failures = 0;
    let mut shared = shared.write().await;
    shared.healthy = true;
    shared.paused = health.paused;
    shared.state = if health.paused {
        InstanceState::Paused
    } else if shared.state == InstanceState::Paused {
        InstanceState::Running
    } else {
        shared.state
    };
}

async fn check_health_restart(
    spec: &InstanceSpec,
    shared: &RwLock<Shared>,
    child: &mut Option<Child>,
    counters: &mut Counters,
) {
    if counters.failures >= spec.policy.health_failures {
        restart_or_fail(spec, shared, child, counters).await;
    }
}

/// Restart according to policy, or fail when the budget is spent.
async fn restart_or_fail(
    spec: &InstanceSpec,
    shared: &RwLock<Shared>,
    child: &mut Option<Child>,
    counters: &mut Counters,
) {
    counters.attempts += 1;
    shared.write().await.restarts = counters.attempts;
    match restart_backoff(&spec.policy, counters.attempts) {
        Some(backoff) => {
            tracing::warn!(
                "[{}] restarting (attempt {}) after {backoff:?}",
                spec.id,
                counters.attempts
            );
            set_state(shared, InstanceState::Restarting).await;
            terminate_child(child).await;
            tokio::time::sleep(backoff).await;
            spawn_child(spec, shared, child).await;
            counters.failures = 0;
        }
        None => {
            tracing::error!("[{}] restart budget spent, marking failed", spec.id);
            terminate_child(child).await;
            set_state(shared, InstanceState::Failed).await;
            shared.write().await.healthy = false;
        }
    }
}
