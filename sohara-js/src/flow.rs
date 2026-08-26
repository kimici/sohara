use std::path::{Path, PathBuf};

use quick_js::{Arguments, Context, JsValue};
use serde_json::Value;
use sohara_core::{Error, Result};

use crate::bridge::js_error;
use crate::convert::js_to_value;

pub fn evaluate_flow_file(path: &Path) -> Result<Value> {
    let source = std::fs::read_to_string(path).map_err(|error| {
        Error::Config(format!(
            "js flow: cannot read script '{}': {error}",
            path.display()
        ))
    })?;
    let script_dir = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let context =
        Context::new().map_err(|error| Error::Config(format!("quickjs init: {error}")))?;
    register_flow_callbacks(&context, script_dir)?;
    context
        .eval(include_str!("../assets/preamble.js"))
        .map_err(|error| js_error("quickjs preamble", &error))?;
    let wrapped = format!(
        r#"(function() {{
            var __unset = {{}};
            var module = {{ exports: __unset }};
            var exports = {{}};
            globalThis.__lastBuiltFlow = undefined;
            {}
            return module.exports !== __unset ? module.exports : globalThis.__lastBuiltFlow;
        }})()"#,
        source
    );
    let result = context
        .eval(&wrapped)
        .map_err(|error| js_error("js flow eval", &error))?;
    match result {
        JsValue::Null | JsValue::Undefined => Err(Error::Config(
            "js flow must export a flow object (e.g. `module.exports = sohara.flow(...).build()`)"
                .to_owned(),
        )),
        other => Ok(js_to_value(&other)),
    }
}

fn register_flow_callbacks(context: &Context, script_dir: PathBuf) -> Result<()> {
    context
        .add_callback("__require_source", {
            let script_dir = script_dir.clone();
            move |path: String| flow_require_source(&script_dir, &path)
        })
        .map_err(|error| js_error("flow require callback", &error))?;
    context
        .add_callback("__flow_save", {
            let script_dir = script_dir.clone();
            move |args: Arguments| -> std::result::Result<JsValue, String> {
                let mut args = args.into_vec();
                let config = args.get_mut(0).cloned().unwrap_or(JsValue::Undefined);
                let path = args
                    .get(1)
                    .cloned()
                    .map(|value| crate::convert::js_to_value(&value))
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .ok_or_else(|| "sohara.flow.save(path): missing path".to_owned())?;
                flow_save(&script_dir, config, &path)
            }
        })
        .map_err(|error| js_error("flow save callback", &error))?;
    Ok(())
}

fn flow_require_source(script_dir: &Path, path: &str) -> std::result::Result<JsValue, String> {
    let path = resolve_module(script_dir, path)?;
    std::fs::read_to_string(&path)
        .map(JsValue::String)
        .map_err(|error| format!("sohara.flow require '{}': {error}", path.display()))
}

fn flow_save(
    script_dir: &Path,
    config: JsValue,
    path: &str,
) -> std::result::Result<JsValue, String> {
    let full = resolve_path(script_dir, path);
    let yaml = serde_yaml::to_string(&js_to_value(&config))
        .map_err(|error| format!("sohara.flow.save({path}): {error}"))?;
    if let Some(parent) = full.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("sohara.flow.save({path}): {error}"))?;
    }
    std::fs::write(&full, yaml).map_err(|error| format!("sohara.flow.save({path}): {error}"))?;
    Ok(JsValue::Undefined)
}

fn resolve_module(script_dir: &Path, path: &str) -> std::result::Result<PathBuf, String> {
    let candidate = resolve_path(script_dir, path);
    if candidate.is_file() {
        return Ok(candidate);
    }
    let with_extension = if candidate.extension().is_none() {
        candidate.with_extension("js")
    } else {
        candidate
    };
    if with_extension.is_file() {
        Ok(with_extension)
    } else {
        Err(format!("require: module '{path}' not found"))
    }
}

fn resolve_path(script_dir: &Path, path: &str) -> PathBuf {
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        script_dir.join(candidate)
    }
}
