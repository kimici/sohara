//! Structural reachability warnings over the built graph

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::node::{Node, NodeStep};

type Adjacency<'a> = HashMap<&'a str, Vec<&'a str>>;

/// Warn about nodes unreachable from a source or unable to reach a sink
/// (structural smell, not an error).
pub fn warn_unreachable(nodes: &BTreeMap<String, Arc<Node>>, roots: &[String]) {
    let (forward, backward) = adjacencies(nodes);
    let start_ids = roots.iter().map(String::as_str).collect::<Vec<_>>();
    let from_source = reach(&forward, &start_ids);
    let sinks = nodes
        .values()
        .filter(|node| matches!(node.step, NodeStep::Sink(_)))
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    let to_sink = reach(&backward, &sinks);
    for (id, node) in nodes {
        if !from_source.contains(id.as_str()) {
            tracing::warn!("step '{id}' is unreachable from any source");
        }
        if !to_sink.contains(id.as_str()) && !matches!(node.step, NodeStep::Sink(_)) {
            tracing::warn!("step '{id}' cannot reach any sink");
        }
    }
}

fn adjacencies(nodes: &BTreeMap<String, Arc<Node>>) -> (Adjacency<'_>, Adjacency<'_>) {
    let mut forward: Adjacency<'_> = HashMap::new();
    let mut backward: Adjacency<'_> = HashMap::new();
    for node in nodes.values() {
        for route in &node.routes {
            forward
                .entry(node.id.as_str())
                .or_default()
                .push(route.to.as_str());
            backward
                .entry(route.to.as_str())
                .or_default()
                .push(node.id.as_str());
        }
    }
    (forward, backward)
}

fn reach(adjacency: &Adjacency<'_>, starts: &[&str]) -> HashSet<String> {
    let mut seen = HashSet::new();
    let mut stack: Vec<&str> = starts.to_vec();
    while let Some(id) = stack.pop() {
        if !seen.insert(id.to_owned()) {
            continue;
        }
        if let Some(targets) = adjacency.get(id) {
            for target in targets {
                stack.push(target);
            }
        }
    }
    seen
}
