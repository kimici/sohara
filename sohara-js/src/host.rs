//! Host callback implementations for the script bridge (S6 host API).
//!
//! All callbacks are synchronous (the design's §7 baseline): the QuickJS
//! worker thread blocks while the host operation completes.

use std::time::Duration;

use quick_js::JsValue;
use serde_json::Value;

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

/// Parsed `sohara.http.request` options.
struct HttpOptions {
    url: String,
    method: String,
    headers: Vec<(String, String)>,
    timeout_ms: u64,
    body: Option<Value>,
}

fn parse_http_opts(opts: &Value) -> Result<HttpOptions, String> {
    let url = opts
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| "sohara.http.request: opts.url is required".to_owned())?
        .to_owned();
    let method = opts
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("GET")
        .to_ascii_uppercase();
    let headers = parse_http_headers(opts);
    let timeout_ms = opts
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(10_000);
    let body = opts.get("body").cloned().filter(|v| !v.is_null());
    Ok(HttpOptions {
        url,
        method,
        headers,
        timeout_ms,
        body,
    })
}

fn parse_http_headers(opts: &Value) -> Vec<(String, String)> {
    opts.get("headers")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        value
                            .as_str()
                            .map_or_else(|| value.to_string(), str::to_owned),
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub fn http_request(env: &StepEnv, opts: JsValue) -> Result<JsValue, String> {
    env.require(crate::env::PERM_HTTP)?;
    let opts = parse_http_opts(&js_to_value(&opts))?;
    // `reqwest::blocking` refuses to run inside a tokio runtime (its wait
    // helper enters a debug shell runtime); run the request on a plain OS
    // thread so callbacks stay valid inside async contexts.
    std::thread::scope(|scope| {
        scope
            .spawn(move || http_blocking(&opts))
            .join()
            .unwrap_or_else(|_| Err("sohara.http.request: worker panicked".to_owned()))
    })
}

fn http_blocking(opts: &HttpOptions) -> Result<JsValue, String> {
    let client = http_client();
    let mut request = client.request(http_method(&opts.method)?, &opts.url);
    request = request.timeout(Duration::from_millis(opts.timeout_ms.clamp(1_000, 120_000)));
    for (key, value) in &opts.headers {
        request = request.header(key, value);
    }
    if let Some(body) = &opts.body {
        request = request.json(body);
    }
    let response = request
        .send()
        .map_err(|error| format!("sohara.http.request({}): {error}", opts.url))?;
    response_to_js(response)
}

fn response_to_js(response: reqwest::blocking::Response) -> Result<JsValue, String> {
    let status = response.status().as_u16();
    let response_headers = response
        .headers()
        .iter()
        .map(|(key, value)| {
            (
                key.to_string(),
                value.to_str().unwrap_or_default().to_owned(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let text = response
        .text()
        .map_err(|error| format!("sohara.http.request: read body: {error}"))?;
    let json: Option<Value> = serde_json::from_str(&text).ok();
    Ok(crate::convert::value_to_js(&serde_json::json!({
        "status": status,
        "ok": (200..300).contains(&status),
        "headers": response_headers,
        "text": text,
        "json": json,
    })))
}

/// Process-wide blocking client, kept alive forever: dropping a
/// `reqwest::blocking::Client` inside an async context panics, and QuickJS
/// callbacks run inside the runtime's async context.
fn http_client() -> &'static reqwest::blocking::Client {
    static CLIENT: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .expect("reqwest blocking client")
    })
}

fn http_method(method: &str) -> Result<reqwest::Method, String> {
    match method {
        "GET" => Ok(reqwest::Method::GET),
        "POST" => Ok(reqwest::Method::POST),
        "PUT" => Ok(reqwest::Method::PUT),
        "DELETE" => Ok(reqwest::Method::DELETE),
        "PATCH" => Ok(reqwest::Method::PATCH),
        "HEAD" => Ok(reqwest::Method::HEAD),
        other => Err(format!("sohara.http.request: unsupported method '{other}'")),
    }
}

pub fn db_query(env: &StepEnv, sql: String, params: JsValue) -> Result<JsValue, String> {
    env.require(crate::env::PERM_DB)?;
    let path = env
        .db
        .as_ref()
        .ok_or_else(|| "sohara.db.query: no 'db' path in the script step config".to_owned())?;
    let connection = rusqlite::Connection::open(path)
        .map_err(|error| format!("sohara.db.query: open '{path}': {error}"))?;
    let params = js_to_value(&params);
    let params = sql_params(&params);
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("sohara.db.query: prepare: {error}"))?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(params.iter()), row_to_object)
        .map_err(|error| format!("sohara.db.query: {error}"))?;
    let mut result = Vec::new();
    for row in rows {
        let row = row.map_err(|error| format!("sohara.db.query: row: {error}"))?;
        result.push(row);
    }
    Ok(crate::convert::value_to_js(&Value::Array(result)))
}

fn sql_params(params: &Value) -> Vec<rusqlite::types::Value> {
    params
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|value| match value {
            Value::Null => rusqlite::types::Value::Null,
            Value::Bool(flag) => rusqlite::types::Value::Integer(i64::from(flag)),
            Value::Number(number) => {
                if let Some(integer) = number.as_i64() {
                    rusqlite::types::Value::Integer(integer)
                } else {
                    rusqlite::types::Value::Real(number.as_f64().unwrap_or(0.0))
                }
            }
            Value::String(text) => rusqlite::types::Value::Text(text),
            other => rusqlite::types::Value::Text(other.to_string()),
        })
        .collect()
}

fn row_to_object(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    let mut object = serde_json::Map::new();
    for index in 0..row.as_ref().column_count() {
        let name = row.as_ref().column_name(index)?.to_owned();
        let value = match row.get_ref(index)? {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(number) => Value::from(number),
            rusqlite::types::ValueRef::Real(number) => Value::from(number),
            rusqlite::types::ValueRef::Text(text) => {
                Value::String(String::from_utf8_lossy(text).into_owned())
            }
            rusqlite::types::ValueRef::Blob(blob) => {
                Value::String(format!("blob:{}bytes", blob.len()))
            }
        };
        object.insert(name, value);
    }
    Ok(Value::Object(object))
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

pub fn state_sync(env: &StepEnv, state: JsValue) -> Result<JsValue, String> {
    *env.state.lock().map_err(|_| "state poisoned".to_owned())? = js_to_value(&state);
    Ok(JsValue::Undefined)
}

pub fn checkpoint() -> Result<JsValue, String> {
    tracing::debug!("script ctx.checkpoint() requested (no-op in the current runtime)");
    Ok(JsValue::Undefined)
}
