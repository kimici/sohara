//! Host callback registration for script contexts (S6 host API).

use quick_js::{Arguments, Context, JsValue};
use sohara_core::Result;
use std::result::Result as StdResult;

use crate::bridge::js_error;
use crate::convert::js_to_value;
use crate::env::StepEnv;
use crate::host;

/// Register every host bridge callback on a fresh context.
pub fn register_callbacks(context: &Context, env: &StepEnv) -> Result<()> {
    register_log_callback(context, env)?;
    register_meta_callbacks(context, env)?;
    register_util_callbacks(context)?;
    register_io_callbacks(context, env)?;
    register_script_callbacks(context, env)?;
    Ok(())
}

fn register_log_callback(context: &Context, env: &StepEnv) -> Result<()> {
    context
        .add_callback("__log", {
            let env = env.clone();
            move |level: String, message: String| host::log(&env, level, message)
        })
        .map_err(|error| js_error("log callback", &error))
}

fn register_meta_callbacks(context: &Context, env: &StepEnv) -> Result<()> {
    context
        .add_callback("__env", {
            let env = env.clone();
            move |name: String| host::env(&env, name)
        })
        .map_err(|error| js_error("env callback", &error))?;
    context
        .add_callback("__var", {
            let env = env.clone();
            move |name: String| host::var(&env, name)
        })
        .map_err(|error| js_error("var callback", &error))?;
    Ok(())
}

fn register_util_callbacks(context: &Context) -> Result<()> {
    context
        .add_callback("__now", host::now)
        .map_err(|error| js_error("now callback", &error))?;
    context
        .add_callback("__uuid", host::uuid)
        .map_err(|error| js_error("uuid callback", &error))?;
    context
        .add_callback("__sleep", |ms: f64| host::sleep(ms))
        .map_err(|error| js_error("sleep callback", &error))?;
    Ok(())
}

fn register_io_callbacks(context: &Context, env: &StepEnv) -> Result<()> {
    register_notify_callback(context, env)?;
    register_file_callbacks(context, env)?;
    register_http_callback(context, env)?;
    register_db_callback(context, env)?;
    Ok(())
}

fn register_notify_callback(context: &Context, env: &StepEnv) -> Result<()> {
    context
        .add_callback("__notify", {
            let env = env.clone();
            move |args: Arguments| -> StdResult<JsValue, String> {
                let mut args = args.into_vec();
                let topic = arg_string(&args, 0)?;
                let payload = arg_js(&mut args, 1).unwrap_or(JsValue::Undefined);
                host::notify(&env, topic, payload)
            }
        })
        .map_err(|error| js_error("notify callback", &error))
}

fn register_file_callbacks(context: &Context, env: &StepEnv) -> Result<()> {
    context
        .add_callback("__file_read", {
            let env = env.clone();
            move |path: String| host::file_read(&env, path)
        })
        .map_err(|error| js_error("file.read callback", &error))?;
    context
        .add_callback("__file_write", {
            let env = env.clone();
            move |path: String, content: String| host::file_write(&env, path, content)
        })
        .map_err(|error| js_error("file.write callback", &error))?;
    Ok(())
}

fn register_http_callback(context: &Context, env: &StepEnv) -> Result<()> {
    context
        .add_callback("__http_request", {
            let env = env.clone();
            move |args: Arguments| -> StdResult<JsValue, String> {
                let mut args = args.into_vec();
                let opts = arg_js(&mut args, 0).unwrap_or(JsValue::Undefined);
                host::http_request(&env, opts)
            }
        })
        .map_err(|error| js_error("http callback", &error))
}

fn register_db_callback(context: &Context, env: &StepEnv) -> Result<()> {
    context
        .add_callback("__db_query", {
            let env = env.clone();
            move |args: Arguments| -> StdResult<JsValue, String> {
                let mut args = args.into_vec();
                let sql = arg_string(&args, 0)?;
                let params = arg_js(&mut args, 1).unwrap_or(JsValue::Undefined);
                host::db_query(&env, sql, params)
            }
        })
        .map_err(|error| js_error("db callback", &error))
}

fn register_script_callbacks(context: &Context, env: &StepEnv) -> Result<()> {
    context
        .add_callback("__require_source", {
            let env = env.clone();
            move |path: String| host::require_source(&env, path)
        })
        .map_err(|error| js_error("require callback", &error))?;
    register_state_callbacks(context, env)?;
    context
        .add_callback("__checkpoint", host::checkpoint)
        .map_err(|error| js_error("checkpoint callback", &error))?;
    Ok(())
}

fn register_state_callbacks(context: &Context, env: &StepEnv) -> Result<()> {
    context
        .add_callback("__emit", {
            let env = env.clone();
            move |args: Arguments| -> StdResult<JsValue, String> {
                let mut args = args.into_vec();
                let record = arg_js(&mut args, 0).unwrap_or(JsValue::Undefined);
                host::emit(&env, record)
            }
        })
        .map_err(|error| js_error("emit callback", &error))?;
    context
        .add_callback("__state_sync", {
            let env = env.clone();
            move |args: Arguments| -> StdResult<JsValue, String> {
                let mut args = args.into_vec();
                let state = arg_js(&mut args, 0).unwrap_or(JsValue::Undefined);
                host::state_sync(&env, state)
            }
        })
        .map_err(|error| js_error("state callback", &error))?;
    Ok(())
}

fn arg_string(args: &[JsValue], index: usize) -> StdResult<String, String> {
    args.get(index)
        .map(js_to_value)
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| format!("expected string argument at position {index}"))
}

fn arg_js(args: &mut [JsValue], index: usize) -> StdResult<JsValue, String> {
    args.get(index)
        .cloned()
        .ok_or_else(|| format!("missing argument at position {index}"))
}
