//! Agent configuration (D2)

use serde::{Deserialize, Serialize};

/// Full agent configuration file (YAML).
#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    pub node: NodeConfig,
    pub plane: PlaneConfig,
    #[serde(default)]
    pub instances: Vec<InstanceSpec>,
    /// Heartbeat interval in milliseconds.
    #[serde(default = "default_heartbeat")]
    pub heartbeat_ms: u64,
}

fn default_heartbeat() -> u64 {
    5000
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlaneConfig {
    pub url: String,
    #[serde(default)]
    pub token: Option<String>,
}

/// One managed sohara instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceSpec {
    pub id: String,
    /// Path to the flow YAML passed to `sohara serve`.
    pub flow: String,
    /// Binary to launch (defaults to `sohara`).
    #[serde(default = "default_bin")]
    pub bin: String,
    /// Instance admin address (host:port); auto-picked when omitted.
    #[serde(default)]
    pub admin: Option<String>,
    /// The flow's http trigger address (host:port) for gateway routing (D4).
    #[serde(default)]
    pub trigger: Option<String>,
    /// Bearer token for the instance admin API.
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Restart with the stored run id (`serve --resume`).
    #[serde(default)]
    pub resume: bool,
    /// Custom launch arguments (override the serve invocation; tests).
    #[serde(default)]
    pub args: Vec<String>,
    /// Enable health probing (default true).
    #[serde(default = "default_true")]
    pub health_enabled: bool,
    #[serde(default)]
    pub policy: Policy,
}

fn default_bin() -> String {
    "sohara".to_owned()
}

fn default_true() -> bool {
    true
}

impl Default for InstanceSpec {
    fn default() -> Self {
        Self {
            id: String::new(),
            flow: String::new(),
            bin: default_bin(),
            admin: None,
            trigger: None,
            admin_token: None,
            resume: false,
            args: Vec::new(),
            health_enabled: true,
            policy: Policy::default(),
        }
    }
}

/// Restart / health policy for one instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    /// Restart on crash / health failure (false = mark failed only).
    #[serde(default = "default_true")]
    pub restart: bool,
    /// Maximum restart attempts before giving up.
    #[serde(default = "default_max_restarts")]
    pub max_restarts: u32,
    /// Base backoff between restarts, milliseconds (doubles each attempt).
    #[serde(default = "default_backoff")]
    pub backoff_ms: u64,
    /// Consecutive failed health probes before a restart.
    #[serde(default = "default_health_failures")]
    pub health_failures: u32,
}

fn default_max_restarts() -> u32 {
    5
}

fn default_backoff() -> u64 {
    2000
}

fn default_health_failures() -> u32 {
    3
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            restart: true,
            max_restarts: default_max_restarts(),
            backoff_ms: default_backoff(),
            health_failures: default_health_failures(),
        }
    }
}
