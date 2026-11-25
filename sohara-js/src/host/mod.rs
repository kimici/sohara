//! Host callback implementations for the script bridge (S6 host API).
//!
//! All callbacks are synchronous (the design's §7 baseline): the QuickJS
//! worker thread blocks while the host operation completes.

mod db;
mod http;

pub use db::db_query;
pub use http::http_request;

use std::time::Duration;

use quick_js::JsValue;

use crate::convert::js_to_value;
use crate::env::StepEnv;

pub fn log(env: &StepEnv, level: String, message: String) -> Result<JsValue, String> {
    match level.as_str() {
        "error" => tracing::error!("[{}] {message}", env.name),
        "warn" => tracing::warn!("[{}] {message}", env.name),
        "debug" => tracing::debug!("[{}] {message}", env.name),
        _ => tracing::info!("[{}] {message}", env.name),
    }
    Ok(JsValue::Undefined)
}

pub fn env(_env: &StepEnv, name: String) -> Result<JsValue, String> {
    Ok(match std::env::var(&name) {
        Ok(value) => JsValue::String(value),
        Err(_) => JsValue::Undefined,
    })
}

pub fn var(env: &StepEnv, name: String) -> Result<JsValue, String> {
    Ok(env
        .vars
        .get(&name)
        .map(crate::convert::value_to_js)
        .unwrap_or(JsValue::Undefined))
}

pub fn now() -> JsValue {
    JsValue::String(chrono::Utc::now().to_rfc3339())
}

pub fn uuid() -> JsValue {
    JsValue::String(uuid::Uuid::new_v4().to_string())
}

pub fn sleep(ms: f64) -> Result<JsValue, String> {
    let millis = ms.clamp(0.0, 60_000.0) as u64;
    std::thread::sleep(Duration::from_millis(millis));
    Ok(JsValue::Undefined)
}

pub fn notify(env: &StepEnv, topic: String, payload: JsValue) -> Result<JsValue, String> {
    env.require(crate::env::PERM_NOTIFY)?;
    let bus = env
        .bus
        .as_ref()
        .map(|handle| &handle.0)
        .ok_or_else(|| "sohara.notify: no event bus in run mode (use serve)".to_owned())?;
    bus.publish(&topic, js_to_value(&payload))
        .map_err(|error| format!("sohara.notify({topic}): {error}"))?;
    Ok(JsValue::Undefined)
}

pub fn file_read(_env: &StepEnv, path: String) -> Result<JsValue, String> {
    std::fs::read_to_string(&path)
        .map(JsValue::String)
        .map_err(|error| format!("sohara.file.read({path}): {error}"))
}

pub fn file_write(env: &StepEnv, path: String, content: String) -> Result<JsValue, String> {
    env.require(crate::env::PERM_FILE_WRITE)?;
    std::fs::write(&path, content)
        .map(|_| JsValue::Undefined)
        .map_err(|error| format!("sohara.file.write({path}): {error}"))
}

pub fn require_source(env: &StepEnv, path: String) -> Result<JsValue, String> {
    let full = env.resolve_module(&path)?;
    std::fs::read_to_string(&full)
        .map(JsValue::String)
        .map_err(|error| format!("sohara.require({path}): {error}"))
}

pub fn emit(env: &StepEnv, record: JsValue) -> Result<JsValue, String> {
    env.emit
        .lock()
        .map_err(|_| "emit buffer poisoned".to_owned())?
        .push(js_to_value(&record));
    Ok(JsValue::Undefined)
}

pub fn register_step(
    env: &StepEnv,
    kind: String,
    step_type: String,
    handler_name: String,
) -> Result<JsValue, String> {
    let Some(registered_steps) = &env.registered_steps else {
        return Ok(JsValue::Undefined);
    };
    let source = env
        .registration_source
        .as_ref()
        .ok_or_else(|| "registerStep: missing script source".to_owned())?;
    let kind = match kind.as_str() {
        "source" => sohara_core::StepKind::Source,
        "transform" => sohara_core::StepKind::Transform,
        "sink" => sohara_core::StepKind::Sink,
        "control" => sohara_core::StepKind::Control,
        other => return Err(format!("registerStep: unsupported kind '{other}'")),
    };
    registered_steps
        .lock()
        .map_err(|_| "registered steps collector poisoned".to_owned())?
        .push(sohara_core::RegisteredStep {
            kind,
            step_type,
            handler_name,
            source: (**source).clone(),
            script_dir: env.script_dir.clone(),
            permissions: env.permissions.clone(),
            db: env.db.clone(),
        });
    Ok(JsValue::Undefined)
}

pub fn flow_run(env: &StepEnv, config: JsValue) -> Result<JsValue, String> {
    let runner = env.flow_runner.as_ref().ok_or_else(|| {
        "sohara.flow.run(...) is unavailable outside runtime execution".to_owned()
    })?;
    let handle = runner
        .0
        .run(js_to_value(&config))
        .map_err(|error| format!("sohara.flow.run(...): {error:?}"))?;
    env.subflow_handles
        .lock()
        .map_err(|_| "subflow handle list poisoned".to_owned())?
        .push(handle.clone());
    Ok(JsValue::String(handle))
}

pub fn flow_stop(env: &StepEnv, handle_id: String) -> Result<JsValue, String> {
    let runner = env.flow_runner.as_ref().ok_or_else(|| {
        "sohara.flow.stop(...) is unavailable outside runtime execution".to_owned()
    })?;
    runner
        .0
        .stop(&handle_id)
        .map_err(|error| format!("sohara.flow.stop({handle_id}): {error:?}"))?;
    if let Ok(mut handles) = env.subflow_handles.lock() {
        handles.retain(|handle| handle != &handle_id);
    }
    Ok(JsValue::Undefined)
}

pub fn flow_save(env: &StepEnv, config: JsValue, path: String) -> Result<JsValue, String> {
    let full = if std::path::Path::new(&path).is_absolute() {
        std::path::PathBuf::from(&path)
    } else {
        env.script_dir
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join(&path)
    };
    let yaml = serde_yaml::to_string(&js_to_value(&config))
        .map_err(|error| format!("sohara.flow.save({path}): {error}"))?;
    if let Some(parent) = full.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("sohara.flow.save({path}): {error}"))?;
    }
    std::fs::write(&full, yaml).map_err(|error| format!("sohara.flow.save({path}): {error}"))?;
    Ok(JsValue::Undefined)
}

pub fn state_sync(env: &StepEnv, state: JsValue) -> Result<JsValue, String> {
    if let Some(accessor) = &env.state_accessor {
        accessor.0.set(&env.step.id, js_to_value(&state));
    }
    Ok(JsValue::Undefined)
}

pub fn checkpoint(env: &StepEnv) -> Result<JsValue, String> {
    if let Some(accessor) = &env.state_accessor {
        accessor
            .0
            .checkpoint()
            .map_err(|error| format!("ctx.checkpoint(): {error}"))?;
    } else {
        tracing::debug!("script ctx.checkpoint() requested without a runtime state accessor");
    }
    Ok(JsValue::Undefined)
}
