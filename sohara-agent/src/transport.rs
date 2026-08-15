//! Control-plane transport (D2): heartbeat + command queue + acks

use anyhow::{bail, Result};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::instance::InstanceState;

/// One instance's state as reported in a heartbeat.
#[derive(Debug, Clone, Serialize)]
pub struct InstanceReport {
    pub id: String,
    pub state: InstanceState,
    pub paused: bool,
    pub healthy: bool,
    pub restarts: u32,
    pub admin: Option<String>,
}

/// Periodic heartbeat payload.
#[derive(Debug, Clone, Serialize)]
pub struct Heartbeat {
    pub node_id: String,
    pub time: String,
    pub instances: Vec<InstanceReport>,
}

/// A command queued by the plane for this agent.
#[derive(Debug, Clone, Deserialize)]
pub struct Command {
    pub seq: u64,
    pub op: String,
    pub instance: String,
}

/// Execution result for one command.
#[derive(Debug, Clone, Serialize)]
pub struct CommandAck {
    pub seq: u64,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Agent-to-plane control transport abstraction.
#[async_trait]
pub trait ControlTransport: Send + Sync {
    /// Report state and pull queued commands.
    async fn heartbeat(&self, heartbeat: &Heartbeat) -> Result<Vec<Command>>;
    /// Acknowledge one command.
    async fn ack(&self, ack: &CommandAck) -> Result<()>;
}

/// HTTP/JSON transport against the plane's `/agent/*` API.
pub struct HttpTransport {
    client: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl HttpTransport {
    #[must_use]
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base: base.into(),
            token,
        }
    }

    async fn send<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        let mut request = self.client.post(format!("{}{path}", self.base));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.json(body).send().await?;
        if !response.status().is_success() {
            bail!("plane {path} -> {}", response.status());
        }
        Ok(response.json().await?)
    }
}

#[async_trait]
impl ControlTransport for HttpTransport {
    async fn heartbeat(&self, heartbeat: &Heartbeat) -> Result<Vec<Command>> {
        self.send("/agent/heartbeat", heartbeat).await
    }

    async fn ack(&self, ack: &CommandAck) -> Result<()> {
        self.send("/agent/ack", ack).await
    }
}
