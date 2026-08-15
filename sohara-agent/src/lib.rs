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
    Command, CommandAck, ControlTransport, Heartbeat, HttpTransport, InstanceReport,
};
