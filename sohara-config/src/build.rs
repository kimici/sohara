//! Build a runnable pipeline from a validated flow

use std::sync::{Arc, Mutex};

use serde_json::Value;
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, Error, Result, Sink, Source, Transform,
};

use crate::{FlowConfig, StepConfig};

/// A built flow, ready for `Pipeline::run`.
pub struct BuiltFlow {
    pub source: Box<dyn Source>,
    pub transforms: Vec<Box<dyn Transform>>,
    pub sinks: Vec<Box<dyn Sink>>,
}

/// Build every step through the registry, prefixing errors with the step id.
///
/// # Errors
/// Returns a config error naming the failing step when a factory rejects the
/// config, the `(kind, type)` pair is unknown, or a control step appears.
pub fn build_flow(flow: &FlowConfig, registry: &ComponentRegistry) -> Result<BuiltFlow> {
    let base = BuildContext {
        vars: flow.vars.clone().into_iter().collect(),
        bus: None,
        state_accessor: None,
        flow_runner: None,
        registered_steps: None,
        flow: flow.name.clone(),
        step: None,
    };
    let registry = prepare_registry(flow, registry, &base)?;
    let mut source = None;
    let mut transforms = Vec::new();
    let mut sinks = Vec::new();
    for step in &flow.steps {
        let ctx = step_context(&base, step);
        match build_step(step, &registry, &ctx)? {
            BuiltStep::Source(step_source) => source = Some(step_source),
            BuiltStep::Transform(step_transform) => transforms.push(step_transform),
            BuiltStep::Sink(step_sink) => sinks.push(step_sink),
            BuiltStep::Control(_) => {
                return Err(Error::Config(format!(
                    "step '{}': control steps need the graph executor (sohara-runtime)",
                    step.id
                )));
            }
        }
    }
    Ok(BuiltFlow {
        source: source.ok_or_else(|| Error::Config("flow has no source step".to_owned()))?,
        transforms,
        sinks,
    })
}

/// Clone the shared registry and extend it with any flow-local script
/// registrations discovered in a script-only pre-pass.
pub fn prepare_registry(
    flow: &FlowConfig,
    registry: &ComponentRegistry,
    base: &BuildContext,
) -> Result<ComponentRegistry> {
    let mut registry = registry.clone();
    let registered_steps = Arc::new(Mutex::new(Vec::new()));
    for step in &flow.steps {
        if step.step_type() != "script" {
            continue;
        }
        let mut ctx = step_context(base, step);
        ctx.registered_steps = Some(registered_steps.clone());
        let _ = build_step(step, &registry, &ctx)?;
    }
    let registered_steps = registered_steps
        .lock()
        .map_err(|_| Error::Runtime("registered steps collector poisoned".to_owned()))?
        .clone();
    for registered in registered_steps {
        let step_type = registered.step_type.clone();
        if registry.contains(registered.kind, &registered.step_type) {
            return Err(Error::Config(format!(
                "script-registered step conflicts with existing kind='{}' type='{}'",
                registered.kind.as_str(),
                registered.step_type
            )));
        }
        registry.register(
            registered.kind,
            &step_type,
            sohara_js::registered_factory(registered),
        );
    }
    Ok(registry)
}

/// A per-step build context carrying the step's identity (script `ctx.step`).
pub fn step_context(base: &BuildContext, step: &StepConfig) -> BuildContext {
    let mut ctx = base.clone();
    ctx.step = Some(sohara_core::StepMeta {
        id: step.id.clone(),
        name: step.name.clone().unwrap_or_else(|| step.id.clone()),
        kind: step.kind().as_str().to_owned(),
        step_type: step.step_type().to_owned(),
    });
    ctx
}

fn build_step(
    step: &StepConfig,
    registry: &ComponentRegistry,
    ctx: &BuildContext,
) -> Result<BuiltStep> {
    let config = step
        .config()
        .map_err(|error| Error::Config(error.to_string()))?;
    registry
        .build(step.kind(), step.step_type(), &Value::Object(config), ctx)
        .map_err(|error| Error::Config(format!("step '{}': {error}", step.id)))
}
