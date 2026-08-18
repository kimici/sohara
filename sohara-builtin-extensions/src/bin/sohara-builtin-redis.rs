use anyhow::{anyhow, Result};
use redis::Commands;
use serde_json::{json, Value};
use sohara_builtin_extensions::{initialize_result, run_loop, Capabilities, JsonRpcRequest};

fn main() -> Result<()> {
    run_loop(handle)
}

fn handle(request: JsonRpcRequest) -> Result<Value> {
    match request.method.as_str() {
        "initialize" => Ok(initialize_result(Capabilities {
            state_store: true,
            event_bus: true,
            ..Capabilities::default()
        })),
        "state.load" => {
            let config = config(&request.params)?;
            let key = key(config, request.params.get("key"))?;
            let mut conn = connection(config)?;
            let value: Option<String> = conn.get(key)?;
            match value {
                Some(value) => {
                    Ok(json!({ "found": true, "value": serde_json::from_str::<Value>(&value)? }))
                }
                None => Ok(json!({ "found": false, "value": Value::Null })),
            }
        }
        "state.save" => {
            let config = config(&request.params)?;
            let key = key(config, request.params.get("key"))?;
            let value = request
                .params
                .get("value")
                .ok_or_else(|| anyhow!("missing value"))?;
            let mut conn = connection(config)?;
            let _: () = conn.set(key, serde_json::to_string(value)?)?;
            Ok(json!({}))
        }
        "state.delete" => {
            let config = config(&request.params)?;
            let key = key(config, request.params.get("key"))?;
            let mut conn = connection(config)?;
            let _: usize = conn.del(key)?;
            Ok(json!({}))
        }
        "bus.publish" => {
            let config = config(&request.params)?;
            let topic = topic(config, request.params.get("topic"))?;
            let payload = request
                .params
                .get("payload")
                .ok_or_else(|| anyhow!("missing payload"))?;
            let mut conn = connection(config)?;
            let _: i64 = conn.publish(topic, serde_json::to_string(payload)?)?;
            Ok(json!({}))
        }
        other => Err(anyhow!("unknown method: {other}")),
    }
}

fn config(params: &Value) -> Result<&serde_json::Map<String, Value>> {
    params
        .get("config")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("missing config"))
}

fn connection(config: &serde_json::Map<String, Value>) -> Result<redis::Connection> {
    let url = if let Some(url) = config.get("url").and_then(Value::as_str) {
        url.to_owned()
    } else {
        let host = config
            .get("host")
            .and_then(Value::as_str)
            .unwrap_or("127.0.0.1");
        let port = config.get("port").and_then(Value::as_u64).unwrap_or(6379);
        let db = config.get("db").and_then(Value::as_u64).unwrap_or(0);
        format!("redis://{host}:{port}/{db}")
    };
    Ok(redis::Client::open(url)?.get_connection()?)
}

fn key(config: &serde_json::Map<String, Value>, key: Option<&Value>) -> Result<String> {
    let prefix = config
        .get("prefix")
        .and_then(Value::as_str)
        .unwrap_or("sohara:");
    let key = key
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    Ok(format!("{prefix}{key}"))
}

fn topic(config: &serde_json::Map<String, Value>, topic: Option<&Value>) -> Result<String> {
    let prefix = config
        .get("channel_prefix")
        .and_then(Value::as_str)
        .unwrap_or("");
    let topic = topic
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing topic"))?;
    Ok(format!("{prefix}{topic}"))
}
