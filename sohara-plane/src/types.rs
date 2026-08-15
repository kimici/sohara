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
    pub desired: String,
    pub actual: Option<String>,
    pub healthy: Option<bool>,
    pub paused: Option<bool>,
    pub restarts: Option<u32>,
    pub admin: Option<String>,
}

/// Node view from heartbeats.
#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    pub id: String,
    pub last_seen: Option<String>,
}
