//! Graph node types

use std::time::Duration;

use serde_json::Value;
use sohara_core::{ControlNode, Expr, Sink, Source, Transform};

/// A routing rule from one node to the next.
#[derive(Debug, Clone)]
pub struct Route {
    pub to: String,
    pub when: Option<Expr>,
}

/// Per-node error policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorPolicy {
    Fail,
    Continue,
    Retry { max: u32, backoff: Duration },
}

/// What a node executes.
pub enum NodeStep {
    Source(Box<dyn Source>),
    Transform(Box<dyn Transform>),
    Sink(Box<dyn Sink>),
    Control(ControlNode),
}

/// One node of the flow graph.
pub struct Node {
    pub id: String,
    pub when: Option<Expr>,
    pub policy: ErrorPolicy,
    pub step: NodeStep,
    pub routes: Vec<Route>,
    /// Number of routes pointing at this node (used by `join.all`).
    pub incoming: usize,
    /// Initial accumulated state (S4; `state`/`loop.while` steps).
    pub state: Option<Value>,
}
