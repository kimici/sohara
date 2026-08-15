//! Route validation helpers: targets, incoming counts, and cycle detection

use std::collections::{BTreeMap, HashMap, HashSet};

use sohara_core::{Error, Result};

use crate::node::Route;

/// Count routes pointing at each node (used by `join.all`).
pub fn count_incoming(routes: &BTreeMap<String, Vec<Route>>) -> BTreeMap<String, usize> {
    let mut incoming = BTreeMap::new();
    for targets in routes.values() {
        for route in targets {
            *incoming.entry(route.to.clone()).or_insert(0) += 1;
        }
    }
    incoming
}

/// Ensure every route points at an existing node and no self-loops exist.
pub fn validate_targets(routes: &BTreeMap<String, Vec<Route>>, ids: &HashSet<&str>) -> Result<()> {
    for (from, targets) in routes {
        for route in targets {
            if !ids.contains(route.to.as_str()) {
                return Err(Error::Config(format!(
                    "step '{from}' routes to unknown step '{}'",
                    route.to
                )));
            }
            if from == &route.to {
                return Err(Error::Config(format!(
                    "self-loop on step '{from}' is not allowed"
                )));
            }
        }
    }
    Ok(())
}

/// Kahn's algorithm over all routes; errors when a cycle exists.
pub fn check_acyclic(routes: &BTreeMap<String, Vec<Route>>, ids: &HashSet<&str>) -> Result<()> {
    let mut indegree = indegree_map(routes, ids);
    let mut ready: Vec<&str> = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(id, _)| *id)
        .collect();
    let mut visited = 0usize;
    while let Some(id) = ready.pop() {
        visited += 1;
        for route in routes.get(id).into_iter().flatten() {
            let degree = indegree.get_mut(route.to.as_str()).expect("known id");
            *degree -= 1;
            if *degree == 0 {
                ready.push(route.to.as_str());
            }
        }
    }
    if visited != ids.len() {
        return Err(Error::Config(
            "flow contains a cycle; loops must use control steps (foreach/loop)".to_owned(),
        ));
    }
    Ok(())
}

fn indegree_map<'a>(
    routes: &'a BTreeMap<String, Vec<Route>>,
    ids: &'a HashSet<&'a str>,
) -> HashMap<&'a str, usize> {
    let mut indegree: HashMap<&str, usize> = ids.iter().map(|id| (*id, 0)).collect();
    for targets in routes.values() {
        for route in targets {
            if let Some(degree) = indegree.get_mut(route.to.as_str()) {
                *degree += 1;
            }
        }
    }
    indegree
}
