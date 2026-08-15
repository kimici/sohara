//! S2 structural validation: version, ids, expressions, and DAG shape

use std::collections::{HashMap, HashSet};

use sohara_core::StepKind;

use crate::{ConfigError, FlowConfig, StepConfig};

/// Validate a flow: version, unique ids, parseable expressions, and a DAG
/// shape (at least one source, at least one sink, no cycles, valid edges).
pub fn validate(flow: &FlowConfig) -> Result<(), ConfigError> {
    validate_version(flow)?;
    validate_triggers(flow)?;
    validate_steps(flow)?;
    validate_edges(flow)?;
    for step in &flow.steps {
        let _ = step.config()?;
    }
    Ok(())
}

fn validate_triggers(flow: &FlowConfig) -> Result<(), ConfigError> {
    let mut ids: HashSet<&str> = flow.steps.iter().map(|s| s.id.as_str()).collect();
    for trigger in &flow.triggers {
        if trigger.id.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "trigger id must not be empty".to_owned(),
            ));
        }
        if !ids.insert(trigger.id.as_str()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate id '{}' (trigger ids share the step id namespace)",
                trigger.id
            )));
        }
        let _ = trigger.config()?;
    }
    Ok(())
}

fn validate_version(flow: &FlowConfig) -> Result<(), ConfigError> {
    if flow.version != "1" {
        return Err(ConfigError::Invalid(format!(
            "unsupported schema version '{}' (supported: \"1\")",
            flow.version
        )));
    }
    Ok(())
}

fn validate_steps(flow: &FlowConfig) -> Result<(), ConfigError> {
    if flow.steps.is_empty() {
        return Err(ConfigError::Invalid(
            "flow must declare at least one step".to_owned(),
        ));
    }
    let mut ids = HashSet::new();
    for step in &flow.steps {
        validate_step(step, &mut ids)?;
    }
    validate_counts(flow)?;
    Ok(())
}

fn validate_step(step: &StepConfig, ids: &mut HashSet<String>) -> Result<(), ConfigError> {
    if step.id.trim().is_empty() {
        return Err(ConfigError::Invalid("step id must not be empty".to_owned()));
    }
    if !ids.insert(step.id.clone()) {
        return Err(ConfigError::Invalid(format!(
            "duplicate step id '{}'",
            step.id
        )));
    }
    if step.kind.is_none() || step.step_type.is_none() {
        return Err(ConfigError::Invalid(format!(
            "step '{}' is missing 'kind'/'type' (and no template provided them)",
            step.id
        )));
    }
    if let Some(when) = &step.when {
        sohara_core::parse(when).map_err(|error| {
            ConfigError::Invalid(format!("step '{}': invalid 'when': {error}", step.id))
        })?;
    }
    Ok(())
}

fn validate_counts(flow: &FlowConfig) -> Result<(), ConfigError> {
    let sources = count_kind(flow, StepKind::Source) + flow.triggers.len();
    if sources == 0 {
        return Err(ConfigError::Invalid(
            "flow needs at least one source or trigger".to_owned(),
        ));
    }
    let sinks = count_kind(flow, StepKind::Sink);
    if sinks == 0 {
        return Err(ConfigError::Invalid(
            "flow needs at least one sink".to_owned(),
        ));
    }
    Ok(())
}

fn count_kind(flow: &FlowConfig, kind: StepKind) -> usize {
    flow.steps.iter().filter(|s| s.kind == Some(kind)).count()
}

fn validate_edges(flow: &FlowConfig) -> Result<(), ConfigError> {
    let mut ids: HashSet<&str> = flow.steps.iter().map(|s| s.id.as_str()).collect();
    for trigger in &flow.triggers {
        ids.insert(trigger.id.as_str());
    }
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &flow.edges {
        let (from, to) = (edge.from(), edge.to());
        if !ids.contains(from) {
            return Err(ConfigError::Invalid(format!(
                "edge from unknown step '{from}'"
            )));
        }
        if !ids.contains(to) {
            return Err(ConfigError::Invalid(format!("edge to unknown step '{to}'")));
        }
        if from == to {
            return Err(ConfigError::Invalid(format!(
                "self-loop on step '{from}' is not allowed"
            )));
        }
        if let Some(when) = edge.when() {
            sohara_core::parse(when).map_err(|error| {
                ConfigError::Invalid(format!("edge '{from}' -> '{to}': invalid 'when': {error}"))
            })?;
        }
        adjacency.entry(from).or_default().push(to);
    }
    check_acyclic(&adjacency)
}

/// Kahn's algorithm; errors when the edge graph contains a cycle.
fn check_acyclic(adjacency: &HashMap<&str, Vec<&str>>) -> Result<(), ConfigError> {
    let mut indegree = initial_indegree(adjacency);
    let mut ready: Vec<&str> = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(node, _)| *node)
        .collect();
    let mut visited = 0usize;
    while let Some(node) = ready.pop() {
        visited += 1;
        for to in adjacency.get(node).into_iter().flatten() {
            let degree = indegree.get_mut(to).expect("indegree entry");
            *degree -= 1;
            if *degree == 0 {
                ready.push(to);
            }
        }
    }
    if visited != indegree.len() {
        return Err(ConfigError::Invalid(
            "flow contains a cycle; loops must use control steps (foreach/loop)".to_owned(),
        ));
    }
    Ok(())
}

fn initial_indegree<'a>(adjacency: &HashMap<&'a str, Vec<&'a str>>) -> HashMap<&'a str, usize> {
    let mut indegree: HashMap<&str, usize> = adjacency
        .keys()
        .map(|node| (*node, 0))
        .chain(adjacency.values().flatten().map(|to| (*to, 0)))
        .collect();
    for targets in adjacency.values() {
        for to in targets {
            *indegree.entry(to).or_insert(0) += 1;
        }
    }
    indegree
}
