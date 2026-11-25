//! Blocking HTTP host callback (`sohara.http.request`).

use std::time::Duration;

use quick_js::JsValue;
use serde_json::Value;

use crate::convert::js_to_value;
use crate::env::StepEnv;

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
