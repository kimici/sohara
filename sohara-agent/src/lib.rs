//! Sohara node agent: supervises local sohara instances under a control plane

mod agent;
mod config;
mod instance;
mod policy;
mod probe;
mod process;
mod transport;

pub use agent::Agent;
pub use config::{AgentConfig, InstanceSpec, NodeConfig, PlaneConfig, Policy};
pub use instance::{InstanceCommand, InstanceManager, InstanceSnapshot, InstanceState};
pub use transport::{
    Command, CommandAck, ControlTransport, DesiredInstance, Heartbeat, HeartbeatResponse,
    HttpTransport, InstanceReport,
};

/// A reqwest client that ignores `HTTP_PROXY` (agents talk to localhost).
#[must_use]
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client")
}
