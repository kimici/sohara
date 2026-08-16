//! Context setup and entry invocation for script steps (S6 host API)

use quick_js::{Context, JsValue};
use serde_json::{json, Value};
use sohara_core::{Error, Record, Result, TransformOutcome};

use crate::callbacks::register_callbacks;
use crate::convert::value_to_js;
use crate::env::StepEnv;

/// Create a fresh context for one invocation and register the host bridge.
pub fn setup_context(env: &StepEnv) -> Result<Context> {
    let context =
        Context::new().map_err(|error| Error::Transform(format!("quickjs init: {error}")))?;
    register_callbacks(&context, env)?;
    context
        .eval(include_str!("../assets/preamble.js"))
        .map_err(|error| js_error("quickjs preamble", &error))?;
    Ok(context)
}

/// Evaluate the user script (defines the entry function).
pub fn run_script(context: &Context, source: &str) -> Result<()> {
    context
        .eval(source)
        .map(|_| ())
        .map_err(|error| js_error("script eval", &error))
}

/// Call `transform(record, ctx)` via the preamble wrapper.
pub fn call_transform(
    context: &Context,
    env: &StepEnv,
    entry: &str,
    record: &Record,
) -> Result<JsValue> {
    let meta = json!({
        "id": record.id,
        "timestamp": record.timestamp.to_rfc3339(),
        "metadata": record.metadata,
    });
    let record_js = value_to_js(&record.payload);
    call(
        context,
        "__call2",
        vec![
            JsValue::String(entry.to_owned()),
            record_js,
            step_js(env),
            flow_js(env),
            state_js(env),
            correlation_js(env),
            value_to_js(&meta),
        ],
    )
}

/// Call `consume(record, ctx)` via the preamble wrapper.
pub fn call_consume(
    context: &Context,
    env: &StepEnv,
    entry: &str,
    record: &Record,
) -> Result<JsValue> {
    call_transform(context, env, entry, record)
}

/// Call `generate(ctx)` via the preamble wrapper.
pub fn call_source(context: &Context, env: &StepEnv, entry: &str) -> Result<JsValue> {
    call(
        context,
        "__call1",
        vec![
            JsValue::String(entry.to_owned()),
            step_js(env),
            flow_js(env),
            state_js(env),
            correlation_js(env),
        ],
    )
}

/// Build a fresh invocation environment: fresh correlation id + emit buffer.
pub fn invocation(env: &StepEnv) -> StepEnv {
    let mut env = env.clone();
    env.correlation_id = uuid::Uuid::new_v4().to_string();
    env.emit = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    env
}

/// Default initial script state (`{}`).
pub fn initial_state() -> std::sync::Arc<std::sync::Mutex<Value>> {
    std::sync::Arc::new(std::sync::Mutex::new(Value::Object(serde_json::Map::new())))
}

/// Merge `ctx.emit` records into the outcome of an entry call.
pub fn merge_emitted(env: &StepEnv, outcome: TransformOutcome) -> Result<TransformOutcome> {
    let emitted = std::mem::take(
        &mut *env
            .emit
            .lock()
            .map_err(|_| Error::Runtime("emit buffer poisoned".into()))?,
    );
    if emitted.is_empty() {
        return Ok(outcome);
    }
    let records = emitted.into_iter().map(Record::new).collect::<Vec<_>>();
    Ok(match outcome {
        TransformOutcome::Expand(items) => {
            let mut items = items;
            items.extend(records);
            TransformOutcome::Expand(items)
        }
        TransformOutcome::Pass(record) => {
            let mut items = vec![record];
            items.extend(records);
            TransformOutcome::Expand(items)
        }
        TransformOutcome::Filtered => TransformOutcome::Expand(records),
        other => other,
    })
}

fn call(context: &Context, wrapper: &str, args: Vec<JsValue>) -> Result<JsValue> {
    context
        .call_function(wrapper, args)
        .map_err(|error| js_error(&format!("script entry ({wrapper})"), &error))
}

fn step_js(env: &StepEnv) -> JsValue {
    value_to_js(&json!({
        "id": env.step.id,
        "name": env.step.name,
        "kind": env.step.kind,
        "type": env.step.step_type,
    }))
}

fn flow_js(env: &StepEnv) -> JsValue {
    value_to_js(&json!({ "name": env.flow, "version": "1" }))
}

fn state_js(env: &StepEnv) -> JsValue {
    let state = env
        .state
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default();
    value_to_js(&state)
}

fn correlation_js(env: &StepEnv) -> JsValue {
    JsValue::String(env.correlation_id.clone())
}

pub(crate) fn js_error(context: &str, error: &quick_js::ExecutionError) -> Error {
    Error::Transform(format!("{context}: {error}"))
}
