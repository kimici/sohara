use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Write};

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub id: u64,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcSuccess<'a> {
    jsonrpc: &'static str,
    id: u64,
    result: &'a Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcFailure<'a> {
    jsonrpc: &'static str,
    id: u64,
    error: JsonRpcError<'a>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError<'a> {
    code: i64,
    message: &'a str,
}

#[derive(Debug, Default, Serialize)]
pub struct Capabilities {
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub source: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub transform: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub sink: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub trigger: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub state_store: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub event_bus: bool,
}

pub fn initialize_result(capabilities: Capabilities) -> Value {
    serde_json::json!({
        "protocol": "sohara.stdio/v1",
        "capabilities": capabilities,
    })
}

pub fn run_loop(mut handle: impl FnMut(JsonRpcRequest) -> Result<Value>) -> Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        let request: JsonRpcRequest = serde_json::from_str(&line)?;
        let id = request.id;
        match handle(request) {
            Ok(result) => {
                let response = JsonRpcSuccess {
                    jsonrpc: "2.0",
                    id,
                    result: &result,
                };
                serde_json::to_writer(&mut stdout, &response)?;
            }
            Err(error) => {
                let message = error.to_string();
                let response = JsonRpcFailure {
                    jsonrpc: "2.0",
                    id,
                    error: JsonRpcError {
                        code: -32000,
                        message: &message,
                    },
                };
                serde_json::to_writer(&mut stdout, &response)?;
            }
        }
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}
