//! QuickJS scripting bridge and script steps (S5/S6)
//!
//! Synchronous host calls (per the design's §7 baseline): each script step
//! creates an isolated QuickJS context per invocation, injects the `sohara`
//! host object, and calls the entry function with `(record, ctx)` / `(ctx)`.
//!
//! Module layout:
//! - [`env`]: per-step environment (permissions, state, emit buffer)
//! - [`host`]: host callback implementations (`sohara.*` and `ctx.*`)
//! - [`callbacks`]: host callback registration on a script context
//! - [`bridge`]: context setup and `__call1` / `__call2` entry invocation
//! - [`step`]: the `script` source / transform / sink step types
//! - [`convert`]: `serde_json::Value` <-> QuickJS `JsValue`

mod bridge;
mod callbacks;
mod convert;
mod env;
mod flow;
mod host;
mod step;

use std::path::PathBuf;
use std::sync::Arc;

use quick_js::JsValue;
use serde::Deserialize;
use serde_json::Value;
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, Error, Record, RegisteredStep, Result, StepFactory,
    StepKind, TransformOutcome,
};

use crate::convert::js_to_value;
use crate::env::StepEnv;

/// Config object for `script` steps.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptConfig {
    /// Script file path (module resolution is relative to its directory).
    #[serde(default)]
    script: Option<String>,
    /// Inline script source (mutually exclusive with `script`).
    #[serde(default)]
    inline: Option<String>,
    /// Entry function name (`transform` / `consume` / `generate` by default).
    #[serde(default)]
    entry: Option<String>,
    /// Permissions granted to the script (e.g. `file.write`, `db`, `http`, `notify`).
    #[serde(default)]
    allow: Vec<String>,
    /// SQLite database path for `sohara.db.query` (needs the `db` permission).
    #[serde(default)]
    db: Option<String>,
}

fn load_source(cfg: &ScriptConfig, what: &str) -> Result<String> {
    match (&cfg.script, &cfg.inline) {
        (Some(path), None) => std::fs::read_to_string(path).map_err(|error| {
            Error::Config(format!("{what}: cannot read script '{path}': {error}"))
        }),
        (None, Some(code)) => Ok(code.clone()),
        (None, None) => Err(Error::Config(format!(
            "{what}: needs a 'script' file or 'inline' code"
        ))),
        (Some(_), Some(_)) => Err(Error::Config(format!(
            "{what}: 'script' and 'inline' are mutually exclusive"
        ))),
    }
}

/// Build the per-step environment from the build context.
fn step_env(cfg: &ScriptConfig, ctx: &BuildContext, source: &str) -> StepEnv {
    let step = ctx.step.clone().unwrap_or_default();
    let script_dir = cfg
        .script
        .as_deref()
        .map(PathBuf::from)
        .and_then(|path| path.parent().map(std::path::Path::to_path_buf));
    StepEnv {
        name: if step.name.is_empty() {
            "script".to_owned()
        } else {
            step.name.clone()
        },
        script_dir,
        vars: ctx.vars.clone(),
        flow: ctx.flow.clone(),
        step,
        bus: ctx.bus.clone().map(crate::env::BusHandle),
        state_accessor: ctx.state_accessor.clone().map(crate::env::AccessorHandle),
        flow_runner: ctx.flow_runner.clone().map(crate::env::FlowRunnerHandle),
        permissions: cfg.allow.clone(),
        db: cfg.db.clone(),
        registration_source: Some(Arc::new(source.to_owned())),
        registered_steps: ctx.registered_steps.clone(),
        subflow_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        emit: Arc::new(std::sync::Mutex::new(Vec::new())),
        correlation_id: String::new(),
    }
}

fn registered_env(registered: &RegisteredStep, ctx: &BuildContext) -> StepEnv {
    let step = ctx.step.clone().unwrap_or_default();
    StepEnv {
        name: if step.name.is_empty() {
            "script".to_owned()
        } else {
            step.name.clone()
        },
        script_dir: registered.script_dir.clone(),
        vars: ctx.vars.clone(),
        flow: ctx.flow.clone(),
        step,
        bus: ctx.bus.clone().map(crate::env::BusHandle),
        permissions: registered.permissions.clone(),
        db: registered.db.clone(),
        state_accessor: ctx.state_accessor.clone().map(crate::env::AccessorHandle),
        flow_runner: ctx.flow_runner.clone().map(crate::env::FlowRunnerHandle),
        registration_source: Some(Arc::new(registered.source.clone())),
        registered_steps: None,
        subflow_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        emit: Arc::new(std::sync::Mutex::new(Vec::new())),
        correlation_id: String::new(),
    }
}

fn scan_registered_steps(env: &StepEnv, source: &str) -> Result<()> {
    if env.registered_steps.is_none() {
        return Ok(());
    }
    let context = crate::bridge::setup_context(env)?;
    crate::bridge::run_script(&context, source)
}

/// Translate a script return value into a transform outcome.
fn outcome_from_js(result: JsValue) -> Result<TransformOutcome> {
    if matches!(result, JsValue::Undefined | JsValue::Null) {
        return Ok(TransformOutcome::Filtered);
    }
    match js_to_value(&result) {
        Value::Array(items) => Ok(TransformOutcome::Expand(
            items.into_iter().map(Record::new).collect(),
        )),
        value => Ok(TransformOutcome::Pass(Record::new(value))),
    }
}

struct FactoryFn<F>(F);

impl<F> StepFactory for FactoryFn<F>
where
    F: Fn(&Value, &BuildContext) -> Result<BuiltStep> + Send + Sync,
{
    fn build(&self, config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        (self.0)(config, ctx)
    }
}

fn factory<F>(build: F) -> Arc<dyn StepFactory>
where
    F: Fn(&Value, &BuildContext) -> Result<BuiltStep> + Send + Sync + 'static,
{
    Arc::new(FactoryFn(build))
}

/// Parse a config object into a typed struct with strict unknown-field
/// rejection, wrapped in a readable config error.
fn parse_config<C: serde::de::DeserializeOwned>(config: &Value, what: &str) -> Result<C> {
    serde_json::from_value(config.clone())
        .map_err(|error| Error::Config(format!("{what}: {error}")))
}

/// Register the script steps into a registry.
pub fn register_all(registry: &mut ComponentRegistry) {
    registry.register(
        StepKind::Transform,
        "script",
        factory(step::ScriptStep::build),
    );
    registry.register(
        StepKind::Source,
        "script",
        factory(step::ScriptSource::build),
    );
    registry.register(StepKind::Sink, "script", factory(step::ScriptSink::build));
}

/// Create a factory for one flow-local step registered from a script.
pub fn registered_factory(registered: RegisteredStep) -> Arc<dyn StepFactory> {
    factory(move |_config, ctx| step::build_registered(&registered, ctx))
}

pub use flow::evaluate_flow_file;
