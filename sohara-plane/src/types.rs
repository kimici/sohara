//! Registry types (D3)

use serde::{Deserialize, Serialize};

use sohara_agent::InstanceSpec;

/// Desired instance state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Desired {
    #[default]
    Running,
    Paused,
    Stopped,
}

impl Desired {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
        }
    }
}

/// A declared instance: where it runs, its launch spec, and the desired state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceDecl {
    pub id: String,
    pub node: String,
    /// Routing group id used by gateway routes (D4).
    #[serde(default)]
    pub flow_id: Option<String>,
    #[serde(default)]
    pub desired: Desired,
    pub spec: InstanceSpec,
}

/// A flow fragment stored for the manager UI / future deployment (D3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDecl {
    pub id: String,
    pub name: String,
    pub yaml: String,
}

/// Instance view merged from desired + last reported actual state.
#[derive(Debug, Clone, Serialize)]
pub struct InstanceView {
    pub id: String,
    pub node: String,
    pub flow_id: Option<String>,
    pub desired: String,
    pub actual: Option<String>,
    pub healthy: Option<bool>,
    pub paused: Option<bool>,
    pub restarts: Option<u32>,
    pub admin: Option<String>,
    pub trigger: Option<String>,
}

/// Node view from heartbeats.
#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    pub id: String,
    pub last_seen: Option<String>,
}

/// Gateway routing mode (D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RouteMode {
    #[default]
    Proxy,
    Bus,
}

/// Candidate selection strategy (D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    #[default]
    RoundRobin,
    Hash,
}

/// A gateway route: path prefix → instance group (D4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteDecl {
    pub id: String,
    /// Gateway path prefix (served under `/gw<path>`).
    pub path: String,
    /// Matches instances whose `flow_id` equals this value.
    pub flow_id: String,
    #[serde(default)]
    pub mode: RouteMode,
    #[serde(default)]
    pub strategy: Strategy,
    /// Header used as the hash key when `strategy: hash`.
    #[serde(default)]
    pub sticky_key: Option<String>,
    /// Topic for `mode: bus` (arrives with D5).
    #[serde(default)]
    pub topic: Option<String>,
}
