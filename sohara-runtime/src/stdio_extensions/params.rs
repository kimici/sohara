//! JSON-RPC wire types for the stdio protocol.

use super::*;

#[derive(Debug, Serialize)]
pub(crate) struct JsonRpcRequest<'a, T> {
    pub(crate) jsonrpc: &'static str,
    pub(crate) id: u64,
    pub(crate) method: &'a str,
    pub(crate) params: T,
}

#[derive(Debug, Deserialize)]
pub(crate) struct JsonRpcResponse<T> {
    pub(crate) jsonrpc: String,
    pub(crate) id: u64,
    pub(crate) result: Option<T>,
    pub(crate) error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct JsonRpcError {
    pub(crate) code: i64,
    pub(crate) message: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct InitializeParams<'a> {
    pub(crate) protocol: &'a str,
    pub(crate) extension: HostExtensionInfo<'a>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HostExtensionInfo<'a> {
    pub(crate) name: &'a str,
    pub(crate) version: &'a str,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InitializeResult {
    pub(crate) protocol: String,
    #[serde(default)]
    pub(crate) capabilities: ExtensionCapabilities,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ExtensionCapabilities {
    #[serde(default)]
    pub(crate) source: bool,
    #[serde(default)]
    pub(crate) transform: bool,
    #[serde(default)]
    pub(crate) sink: bool,
    #[serde(default)]
    pub(crate) trigger: bool,
    #[serde(default)]
    pub(crate) state_store: bool,
    #[serde(default)]
    pub(crate) event_bus: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct StepParams<'a> {
    pub(crate) step: &'a StepMeta,
    pub(crate) config: &'a Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct TransformParams<'a> {
    pub(crate) step: &'a StepMeta,
    pub(crate) config: &'a Value,
    pub(crate) record: &'a Record,
}

#[derive(Debug, Serialize)]
pub(crate) struct SinkParams<'a> {
    pub(crate) step: &'a StepMeta,
    pub(crate) config: &'a Value,
    pub(crate) record: &'a Record,
}

#[derive(Debug, Serialize)]
pub(crate) struct TriggerParams<'a> {
    pub(crate) trigger: &'a TriggerMeta,
    pub(crate) config: &'a Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct StoreKeyParams<'a> {
    pub(crate) store: TypeName<'a>,
    pub(crate) config: &'a Value,
    pub(crate) key: &'a str,
}

#[derive(Debug, Serialize)]
pub(crate) struct StoreSaveParams<'a> {
    pub(crate) store: TypeName<'a>,
    pub(crate) config: &'a Value,
    pub(crate) key: &'a str,
    pub(crate) value: &'a Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct EventBusPublishParams<'a> {
    pub(crate) bus: TypeName<'a>,
    pub(crate) config: &'a Value,
    pub(crate) topic: &'a str,
    pub(crate) payload: &'a Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct TypeName<'a> {
    #[serde(rename = "type")]
    pub(crate) type_name: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", rename_all = "lowercase")]
pub(crate) enum TransformResult {
    Pass { record: Record },
    Filtered,
    Expand { records: Vec<Record> },
}

#[derive(Debug, Deserialize)]
pub(crate) struct PullResult {
    #[serde(default)]
    pub(crate) records: Vec<Record>,
    #[serde(default)]
    pub(crate) done: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EmptyResult {}

#[derive(Debug, Deserialize)]
pub(crate) struct StateLoadResult {
    pub(crate) found: bool,
    #[serde(default)]
    pub(crate) value: Option<Value>,
}
