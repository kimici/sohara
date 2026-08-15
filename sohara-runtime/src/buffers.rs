//! Shared buffers for join and batch nodes

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use sohara_core::{JoinMode, Record};
use tokio::sync::Mutex;

/// Buffer for a `join` node, keyed by correlation id.
#[derive(Default)]
pub(crate) struct JoinBuffer {
    pub(crate) mode: JoinMode,
    pub(crate) n: usize,
    pub(crate) expected: usize,
    pub(crate) groups: HashMap<String, Vec<Record>>,
}

impl JoinBuffer {
    pub(crate) fn new(mode: JoinMode, n: usize, expected: usize) -> Self {
        Self {
            mode,
            n,
            expected,
            groups: HashMap::new(),
        }
    }
}

/// Buffer for a `batch` node.
#[derive(Default)]
pub(crate) struct BatchBuffer {
    pub(crate) records: Vec<Record>,
    pub(crate) since: Option<Instant>,
}

/// Join buffers, batch buffers, and sink ids discovered by [`scan_nodes`].
pub(crate) type NodeScan = (
    HashMap<String, Arc<Mutex<JoinBuffer>>>,
    HashMap<String, Arc<Mutex<BatchBuffer>>>,
    Vec<String>,
);

/// Scan the graph for join buffers, batch buffers, and sink ids.
pub(crate) fn scan_nodes(graph: &crate::graph::FlowGraph) -> NodeScan {
    use crate::graph::NodeStep;

    let mut joins = HashMap::new();
    let mut batches = HashMap::new();
    let mut sinks = Vec::new();
    for (id, node) in &graph.nodes {
        match &node.step {
            NodeStep::Control(sohara_core::ControlNode::Join { mode, n }) => {
                joins.insert(
                    id.clone(),
                    Arc::new(Mutex::new(JoinBuffer::new(*mode, *n, node.incoming))),
                );
            }
            NodeStep::Control(sohara_core::ControlNode::Batch { .. }) => {
                batches.insert(id.clone(), Arc::new(Mutex::new(BatchBuffer::default())));
            }
            NodeStep::Sink(_) => sinks.push(id.clone()),
            _ => {}
        }
    }
    (joins, batches, sinks)
}
