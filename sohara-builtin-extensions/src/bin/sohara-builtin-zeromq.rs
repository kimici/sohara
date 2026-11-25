use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use sohara_builtin_extensions::{initialize_result, run_loop, Capabilities, JsonRpcRequest};
use std::time::Duration;
use tokio::runtime::Runtime;
use zeromq::{PubSocket, Socket, SocketRecv, SocketSend, SubSocket};

fn main() -> Result<()> {
    let mut state = State::new()?;
    run_loop(move |request| handle(&mut state, request))
}

struct State {
    runtime: Runtime,
    pub_socket: Option<PubSocket>,
    sub_socket: Option<SubSocket>,
    pub_ready: bool,
    sub_ready: bool,
    stopped: bool,
}

impl State {
    fn new() -> Result<Self> {
        Ok(Self {
            runtime: Runtime::new()?,
            pub_socket: None,
            sub_socket: None,
            pub_ready: false,
            sub_ready: false,
            stopped: false,
        })
    }
}

fn handle(state: &mut State, request: JsonRpcRequest) -> Result<Value> {
    match request.method.as_str() {
        "initialize" => Ok(initialize_result(Capabilities {
            trigger: true,
            event_bus: true,
            ..Capabilities::default()
        })),
        "bus.publish" => handle_publish(state, &request.params),
        "trigger.start" => handle_start(state, &request.params),
        "trigger.pull" => handle_pull(state, &request.params),
        "trigger.stop" => handle_stop(state),
        other => Err(anyhow!("unknown method: {other}")),
    }
}

fn handle_publish(state: &mut State, params: &Value) -> Result<Value> {
    let config = config(params)?;
    let topic = params
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let payload = params
        .get("payload")
        .ok_or_else(|| anyhow!("missing payload"))?;
    ensure_pub_socket(state, config)?;
    let message = json!({ "topic": topic, "payload": payload }).to_string();
    let runtime = &state.runtime;
    let socket = state
        .pub_socket
        .as_mut()
        .ok_or_else(|| anyhow!("failed to initialize zeromq pub socket"))?;
    if !state.pub_ready {
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(300)).await });
        state.pub_ready = true;
    }
    runtime.block_on(async { socket.send(message.into()).await })?;
    Ok(json!({}))
}

fn handle_start(state: &mut State, params: &Value) -> Result<Value> {
    let config = config(params)?;
    ensure_sub_socket(state, config)?;
    let runtime = &state.runtime;
    let socket = state
        .sub_socket
        .as_mut()
        .ok_or_else(|| anyhow!("failed to initialize zeromq sub socket"))?;
    runtime.block_on(async { socket.subscribe("").await })?;
    if !state.sub_ready {
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(300)).await });
        state.sub_ready = true;
    }
    state.stopped = false;
    Ok(json!({}))
}

fn handle_pull(state: &mut State, params: &Value) -> Result<Value> {
    if state.stopped {
        return Ok(json!({ "records": [], "done": true }));
    }
    let config = config(params)?;
    let timeout = config
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(100);
    let topic_filter = config.get("topic").and_then(Value::as_str).unwrap_or("");
    let payload = match recv_payload(state, config, timeout)? {
        Some(payload) => payload,
        None => return Ok(json!({ "records": [], "done": false })),
    };
    if !topic_filter.is_empty()
        && payload.get("topic").and_then(Value::as_str) != Some(topic_filter)
    {
        return Ok(json!({ "records": [], "done": false }));
    }
    let record_payload = payload.get("payload").cloned().unwrap_or(payload.clone());
    Ok(make_record(record_payload))
}

fn make_record(payload: Value) -> Value {
    json!({
        "records": [{
            "id": format!("zeromq-{}", uuid::Uuid::new_v4()),
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": payload,
            "metadata": {}
        }],
        "done": false
    })
}

fn recv_payload(
    state: &mut State,
    config: &serde_json::Map<String, Value>,
    timeout: u64,
) -> Result<Option<Value>> {
    ensure_sub_socket(state, config)?;
    let runtime = &state.runtime;
    let socket = state
        .sub_socket
        .as_mut()
        .ok_or_else(|| anyhow!("failed to initialize zeromq sub socket"))?;
    let maybe_message = runtime.block_on(async {
        tokio::time::timeout(Duration::from_millis(timeout), socket.recv()).await
    });
    let message = match maybe_message {
        Ok(Ok(message)) => message,
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => return Ok(None),
    };
    let frame = message
        .get(0)
        .ok_or_else(|| anyhow!("zeromq message had no frames"))?;
    let raw = String::from_utf8(frame.to_vec())?;
    Ok(Some(
        serde_json::from_str::<Value>(&raw).unwrap_or_else(|_| json!({ "raw": raw })),
    ))
}

fn handle_stop(state: &mut State) -> Result<Value> {
    state.stopped = true;
    state.sub_socket.take();
    state.sub_ready = false;
    Ok(json!({}))
}

fn config(params: &Value) -> Result<&serde_json::Map<String, Value>> {
    params
        .get("config")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("missing config"))
}

fn ensure_pub_socket(state: &mut State, config: &serde_json::Map<String, Value>) -> Result<()> {
    if state.pub_socket.is_none() {
        let endpoint = endpoint(config)?;
        let mode = mode(config);
        let mut socket = PubSocket::new();
        state.runtime.block_on(async {
            if mode == "bind" {
                socket.bind(endpoint).await.map(|_| ())
            } else {
                socket.connect(endpoint).await
            }
        })?;
        state.pub_socket = Some(socket);
        state.pub_ready = false;
    }
    Ok(())
}

fn ensure_sub_socket(state: &mut State, config: &serde_json::Map<String, Value>) -> Result<()> {
    if state.sub_socket.is_none() {
        let endpoint = endpoint(config)?;
        let mode = mode(config);
        let mut socket = SubSocket::new();
        state.runtime.block_on(async {
            if mode == "bind" {
                socket.bind(endpoint).await.map(|_| ())
            } else {
                socket.connect(endpoint).await
            }
        })?;
        state.sub_socket = Some(socket);
        state.sub_ready = false;
    }
    Ok(())
}

fn endpoint(config: &serde_json::Map<String, Value>) -> Result<&str> {
    config
        .get("endpoint")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("config.endpoint is required"))
}

fn mode(config: &serde_json::Map<String, Value>) -> &str {
    config
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("connect")
}
