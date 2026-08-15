//! QuickJS scripting bridge and script steps (S5)
//!
//! Synchronous host calls (per the S5 design): each script step creates an
//! isolated QuickJS context per invocation, injects the `sohara` host object,
//! and calls the entry function with `(record, ctx)` / `(ctx)`.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use quick_js::{Context, JsValue};
use serde::Deserialize;
use serde_json::{Map, Value};
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, Error, Record, Result, Sink, Source, StepFactory,
    StepKind, Transform, TransformOutcome,
};

mod convert;
use convert::{js_to_value, value_to_js};

/// Preamble exposing the host bridge as the global `sohara` object.
const PREAMBLE: &str = r#"
var sohara = {
  log: function(level, msg) { return __log(String(level), String(msg)); },
  env: function(name) { return __env(String(name)); },
  var: function(name) { return __var(String(name)); },
  now: function() { return __now(); },
  uuid: function() { return __uuid(); },
  fail: function(msg) { throw new Error(String(msg)); },
  json: JSON
};
"#;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptConfig {
    #[serde(default)]
    script: Option<String>,
    #[serde(default)]
    inline: Option<String>,
    #[serde(default)]
    entry: Option<String>,
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

fn js_error(context: &str, error: &quick_js::ExecutionError) -> Error {
    Error::Transform(format!("{context}: {error}"))
}

fn setup_context(vars: &Map<String, Value>, step: &str) -> Result<Context> {
    let context =
        Context::new().map_err(|error| Error::Transform(format!("quickjs init: {error}")))?;
    register_log_callback(&context)?;
    register_env_callback(&context)?;
    register_var_callback(&context, vars)?;
    register_util_callbacks(&context)?;
    context
        .eval(PREAMBLE)
        .map_err(|error| js_error("quickjs preamble", &error))?;
    context
        .set_global(
            "__ctx",
            value_to_js(&serde_json::json!({ "step": { "id": step } })),
        )
        .map_err(|error| js_error("quickjs ctx binding", &error))?;
    Ok(context)
}

fn register_log_callback(context: &Context) -> Result<()> {
    context
        .add_callback("__log", |level: String, message: String| {
            match level.as_str() {
                "error" => tracing::error!("{message}"),
                "warn" => tracing::warn!("{message}"),
                "debug" => tracing::debug!("{message}"),
                _ => tracing::info!("{message}"),
            }
            JsValue::Undefined
        })
        .map_err(|error| js_error("quickjs log callback", &error))
}

fn register_env_callback(context: &Context) -> Result<()> {
    context
        .add_callback("__env", |name: String| match std::env::var(name) {
            Ok(value) => JsValue::String(value),
            Err(_) => JsValue::Undefined,
        })
        .map_err(|error| js_error("quickjs env callback", &error))
}

fn register_var_callback(context: &Context, vars: &Map<String, Value>) -> Result<()> {
    let flow_vars = vars.clone();
    context
        .add_callback("__var", move |name: String| {
            flow_vars
                .get(&name)
                .map(|value| JsValue::String(value.to_string()))
                .unwrap_or(JsValue::Undefined)
        })
        .map_err(|error| js_error("quickjs var callback", &error))
}

fn register_util_callbacks(context: &Context) -> Result<()> {
    context
        .add_callback("__now", || JsValue::String(chrono::Utc::now().to_rfc3339()))
        .map_err(|error| js_error("quickjs now callback", &error))?;
    context
        .add_callback("__uuid", || {
            JsValue::String(uuid::Uuid::new_v4().to_string())
        })
        .map_err(|error| js_error("quickjs uuid callback", &error))
}

fn run_script(context: &Context, source: &str) -> Result<()> {
    context
        .eval(source)
        .map(|_| ())
        .map_err(|error| js_error("script eval", &error))
}

/// A transform that runs a QuickJS script per record.
pub struct ScriptStep {
    name: String,
    source: String,
    entry: String,
    vars: Map<String, Value>,
}

impl ScriptStep {
    /// Build the step from config.
    pub fn build(config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: ScriptConfig = parse_config(config, "script step")?;
        let step = Self {
            name: "script".to_owned(),
            source: load_source(&cfg, "script step")?,
            entry: cfg.entry.unwrap_or_else(|| "transform".to_owned()),
            vars: ctx.vars.clone(),
        };
        Ok(BuiltStep::Transform(Box::new(step)))
    }
}

#[async_trait]
impl Transform for ScriptStep {
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        let context = setup_context(&self.vars, &self.name)?;
        run_script(&context, &self.source)?;
        let record_js = value_to_js(&record.payload);
        let result = context
            .call_function(&self.entry, vec![record_js])
            .map_err(|error| js_error(&format!("script entry '{}'", self.entry), &error))?;
        outcome_from_js(result)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A source that generates records from a QuickJS script.
pub struct ScriptSource {
    name: String,
    source: String,
    vars: Map<String, Value>,
}

impl ScriptSource {
    /// Build the step from config.
    pub fn build(config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: ScriptConfig = parse_config(config, "script source")?;
        let step = Self {
            name: "script".to_owned(),
            source: load_source(&cfg, "script source")?,
            vars: ctx.vars.clone(),
        };
        Ok(BuiltStep::Source(Box::new(step)))
    }
}

#[async_trait]
impl Source for ScriptSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let context = setup_context(&self.vars, &self.name)?;
        run_script(&context, &self.source)?;
        let result = context
            .call_function("generate", Vec::<JsValue>::new())
            .map_err(|error| js_error("script generate", &error))?;
        let records: Vec<Record> = match js_to_value(&result) {
            Value::Array(items) => items.into_iter().map(Record::new).collect(),
            other => {
                return Err(Error::Transform(format!(
                    "script generate must return an array, got {other}"
                )));
            }
        };
        Ok(Box::pin(futures::stream::iter(records.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A sink that consumes records via a QuickJS script.
pub struct ScriptSink {
    name: String,
    source: String,
    vars: Map<String, Value>,
}

impl ScriptSink {
    /// Build the step from config.
    pub fn build(config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: ScriptConfig = parse_config(config, "script sink")?;
        let step = Self {
            name: "script".to_owned(),
            source: load_source(&cfg, "script sink")?,
            vars: ctx.vars.clone(),
        };
        Ok(BuiltStep::Sink(Box::new(step)))
    }
}

#[async_trait]
impl Sink for ScriptSink {
    async fn send(&self, record: Record) -> Result<()> {
        let context = setup_context(&self.vars, &self.name)?;
        run_script(&context, &self.source)?;
        let record_js = value_to_js(&record.payload);
        context
            .call_function("consume", vec![record_js])
            .map_err(|error| js_error("script consume", &error))?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

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
    registry.register(StepKind::Transform, "script", factory(ScriptStep::build));
    registry.register(StepKind::Source, "script", factory(ScriptSource::build));
    registry.register(StepKind::Sink, "script", factory(ScriptSink::build));
}
