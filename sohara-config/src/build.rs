//! Build a runnable pipeline from a validated flow

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
    let ctx = BuildContext {
        vars: flow.vars.clone().into_iter().collect(),
        bus: None,
    };
    let mut source = None;
    let mut transforms = Vec::new();
    let mut sinks = Vec::new();
    for step in &flow.steps {
        match build_step(step, registry, &ctx)? {
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
