//! Flow graph: construction from config, routing, and structural validation

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use sohara_config::{FlowConfig, OnError, StepConfig};
use sohara_core::{
    parse, parse_duration, BuildContext, BuiltStep, ComponentRegistry, ControlNode, Error, Expr,
    Record, Result, Source, Trigger,
};

pub use crate::node::{ErrorPolicy, Node, NodeStep, Route};
use crate::routes::{check_acyclic, count_incoming, validate_targets};
use crate::warnings::warn_unreachable;

/// The flow graph with id lookup.
pub struct FlowGraph {
    pub name: String,
    pub nodes: BTreeMap<String, Arc<Node>>,
    pub roots: Vec<String>,
}

impl FlowGraph {
    #[must_use]
    pub fn node(&self, id: &str) -> Option<&Arc<Node>> {
        self.nodes.get(id)
    }

    /// Build the graph from a validated flow config.
    ///
    /// # Errors
    /// Fails when a route points at an unknown step, a sink has outgoing
    /// routes, or the graph contains a cycle.
    pub fn build(
        flow: &FlowConfig,
        registry: &ComponentRegistry,
        ctx: &BuildContext,
    ) -> Result<Self> {
        let (partials, roots) = build_partials(flow, registry, ctx)?;
        Self::assemble(flow, partials, roots)
    }

    /// Build the graph with pre-built trigger instances as source roots.
    ///
    /// # Errors
    /// Same as [`FlowGraph::build`]; trigger order must match `flow.triggers`.
    pub fn build_with_triggers(
        flow: &FlowConfig,
        registry: &ComponentRegistry,
        ctx: &BuildContext,
        triggers: &[Arc<dyn Trigger>],
    ) -> Result<Self> {
        let (mut partials, mut roots) = build_partials(flow, registry, ctx)?;
        for (config, trigger) in flow.triggers.iter().zip(triggers) {
            partials.insert(
                config.id.clone(),
                PartialNode {
                    step: NodeStep::Source(Box::new(TriggerAdapter(trigger.clone()))),
                    when: None,
                    policy: ErrorPolicy::Fail,
                    state: None,
                },
            );
            roots.push(config.id.clone());
        }
        Self::assemble(flow, partials, roots)
    }

    fn assemble(
        flow: &FlowConfig,
        partials: BTreeMap<String, PartialNode>,
        roots: Vec<String>,
    ) -> Result<Self> {
        let routes = wire_routes(flow, &partials)?;
        let incoming = count_incoming(&routes);
        let ids: HashSet<&str> = partials.keys().map(String::as_str).collect();
        validate_targets(&routes, &ids)?;
        check_acyclic(&routes, &ids)?;
        let nodes = assemble_nodes(partials, &routes, &incoming)?;
        warn_unreachable(&nodes, &roots);
        Ok(Self {
            name: flow.name.clone(),
            nodes,
            roots,
        })
    }
}

/// Wraps a trigger so it can be stored as a plain source node.
struct TriggerAdapter(Arc<dyn Trigger>);

#[async_trait::async_trait]
impl Source for TriggerAdapter {
    async fn stream(&self) -> Result<futures::stream::BoxStream<'static, Result<Record>>> {
        self.0.stream().await
    }

    fn name(&self) -> &str {
        self.0.name()
    }
}

fn build_partials(
    flow: &FlowConfig,
    registry: &ComponentRegistry,
    ctx: &BuildContext,
) -> Result<(BTreeMap<String, PartialNode>, Vec<String>)> {
    let mut partials: BTreeMap<String, PartialNode> = BTreeMap::new();
    let mut roots = Vec::new();
    for step in &flow.steps {
        let (step_node, is_source) = build_step_node(step, registry, ctx)?;
        let when = step
            .when
            .as_deref()
            .map(parse)
            .transpose()
            .map_err(|error| {
                Error::Config(format!("step '{}': invalid 'when': {error}", step.id))
            })?;
        partials.insert(
            step.id.clone(),
            PartialNode {
                step: step_node,
                when,
                policy: resolve_policy(step)?,
                state: step.state.clone(),
            },
        );
        if is_source {
            roots.push(step.id.clone());
        }
    }
    Ok((partials, roots))
}

fn build_step_node(
    step: &StepConfig,
    registry: &ComponentRegistry,
    ctx: &BuildContext,
) -> Result<(NodeStep, bool)> {
    let config = step
        .config()
        .map_err(|error| Error::Config(error.to_string()))?;
    let built = registry
        .build(step.kind(), step.step_type(), &Value::Object(config), ctx)
        .map_err(|error| Error::Config(format!("step '{}': {error}", step.id)))?;
    Ok(match built {
        BuiltStep::Source(source) => (NodeStep::Source(source), true),
        BuiltStep::Transform(transform) => (NodeStep::Transform(transform), false),
        BuiltStep::Sink(sink) => (NodeStep::Sink(sink), false),
        BuiltStep::Control(control) => (NodeStep::Control(control), false),
    })
}

fn assemble_nodes(
    partials: BTreeMap<String, PartialNode>,
    routes: &BTreeMap<String, Vec<Route>>,
    incoming: &BTreeMap<String, usize>,
) -> Result<BTreeMap<String, Arc<Node>>> {
    let mut nodes = BTreeMap::new();
    for (id, partial) in partials {
        let node_routes = routes.get(&id).cloned().unwrap_or_default();
        if matches!(partial.step, NodeStep::Sink(_)) && !node_routes.is_empty() {
            return Err(Error::Config(format!(
                "sink '{id}' cannot have outgoing edges"
            )));
        }
        nodes.insert(
            id.clone(),
            Arc::new(Node {
                id: id.clone(),
                when: partial.when,
                policy: partial.policy,
                step: partial.step,
                routes: node_routes,
                incoming: incoming.get(&id).copied().unwrap_or(0),
                state: partial.state,
            }),
        );
    }
    Ok(nodes)
}

struct PartialNode {
    step: NodeStep,
    when: Option<Expr>,
    policy: ErrorPolicy,
    state: Option<Value>,
}

fn resolve_policy(step: &StepConfig) -> Result<ErrorPolicy> {
    match step.on_error.unwrap_or_default() {
        OnError::Fail => Ok(ErrorPolicy::Fail),
        OnError::Continue => Ok(ErrorPolicy::Continue),
        OnError::Retry => {
            let retry = step.retry.as_ref().ok_or_else(|| {
                Error::Config(format!(
                    "step '{}': on_error: retry needs a 'retry' block",
                    step.id
                ))
            })?;
            let backoff = parse_duration(&retry.backoff).map_err(|error| {
                Error::Config(format!("step '{}': retry backoff: {error}", step.id))
            })?;
            Ok(ErrorPolicy::Retry {
                max: retry.max,
                backoff,
            })
        }
    }
}

fn wire_routes(
    flow: &FlowConfig,
    partials: &BTreeMap<String, PartialNode>,
) -> Result<BTreeMap<String, Vec<Route>>> {
    let mut routes: BTreeMap<String, Vec<Route>> = BTreeMap::new();
    if flow.edges.is_empty() {
        // Implicit linear chaining: consecutive steps in declaration order,
        // except nodes that already route explicitly (switch/parallel/loop).
        for pair in flow.steps.windows(2) {
            let (prev, next) = (&pair[0], &pair[1]);
            if has_explicit_routes(partials.get(&prev.id)) {
                continue;
            }
            routes.entry(prev.id.clone()).or_default().push(Route {
                to: next.id.clone(),
                when: None,
            });
        }
    } else {
        for edge in &flow.edges {
            let when = edge.when().map(parse).transpose().map_err(|error| {
                Error::Config(format!(
                    "edge '{}' -> '{}': invalid 'when': {error}",
                    edge.from(),
                    edge.to()
                ))
            })?;
            routes
                .entry(edge.from().to_owned())
                .or_default()
                .push(Route {
                    to: edge.to().to_owned(),
                    when,
                });
        }
    }
    for (id, partial) in partials {
        let NodeStep::Control(control) = &partial.step else {
            continue;
        };
        let extra = match control {
            ControlNode::Switch { cases, default } => {
                let mut routes = cases
                    .iter()
                    .map(|case| Route {
                        to: case.to.clone(),
                        when: None,
                    })
                    .collect::<Vec<_>>();
                routes.push(Route {
                    to: default.clone(),
                    when: None,
                });
                routes
            }
            ControlNode::Parallel { branches } => branches
                .iter()
                .map(|branch| Route {
                    to: branch.clone(),
                    when: None,
                })
                .collect(),
            ControlNode::Loop { body: Some(to), .. } => {
                vec![Route {
                    to: to.clone(),
                    when: None,
                }]
            }
            _ => Vec::new(),
        };
        routes.entry(id.clone()).or_default().extend(extra);
    }
    Ok(routes)
}

fn has_explicit_routes(partial: Option<&PartialNode>) -> bool {
    matches!(
        partial.map(|p| &p.step),
        Some(NodeStep::Control(ControlNode::Switch { .. }))
            | Some(NodeStep::Control(ControlNode::Parallel { .. }))
            | Some(NodeStep::Control(ControlNode::Loop { body: Some(_), .. }))
    )
}
