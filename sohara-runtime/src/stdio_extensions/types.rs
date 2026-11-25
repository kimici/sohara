//! Shared data types for the stdio extension host.

use super::*;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StdioExtensionManifest {
    pub(crate) name: String,
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) builtin: bool,
    #[serde(default = "default_protocol")]
    pub(crate) protocol: String,
    pub(crate) command: String,
    #[serde(default)]
    pub(crate) cwd: Option<String>,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    #[serde(default)]
    pub(crate) steps: Vec<StdioStepRegistration>,
    #[serde(default)]
    pub(crate) triggers: Vec<NamedRegistration>,
    #[serde(default)]
    pub(crate) state_stores: Vec<NamedRegistration>,
    #[serde(default)]
    pub(crate) event_buses: Vec<NamedRegistration>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StdioStepRegistration {
    pub(crate) kind: StepKind,
    #[serde(rename = "type")]
    pub(crate) step_type: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamedRegistration {
    #[serde(rename = "type")]
    pub(crate) type_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AsyncKind {
    Source,
    Transform,
    Sink,
    Trigger,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlockingKind {
    StateStore,
    EventBus,
}

#[derive(Debug, Clone)]
pub(crate) struct AsyncSpec {
    pub(crate) extension_name: String,
    pub(crate) extension_version: String,
    pub(crate) protocol: String,
    pub(crate) command: String,
    pub(crate) cwd: Option<String>,
    pub(crate) args: Vec<String>,
    pub(crate) kind: AsyncKind,
    pub(crate) type_name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct BlockingSpec {
    pub(crate) extension_name: String,
    pub(crate) extension_version: String,
    pub(crate) protocol: String,
    pub(crate) command: String,
    pub(crate) cwd: Option<String>,
    pub(crate) args: Vec<String>,
    pub(crate) kind: BlockingKind,
    pub(crate) type_name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct StdioStepFactory {
    pub(crate) spec: AsyncSpec,
}

#[derive(Debug)]
pub(crate) struct StdioSourceStep {
    pub(crate) name: String,
    pub(crate) spec: AsyncSpec,
    pub(crate) config: Value,
    pub(crate) step: StepMeta,
    pub(crate) client: SharedAsyncClient,
}

#[derive(Debug)]
pub(crate) struct StdioTransformStep {
    pub(crate) name: String,
    pub(crate) spec: AsyncSpec,
    pub(crate) config: Value,
    pub(crate) step: StepMeta,
    pub(crate) client: SharedAsyncClient,
}

#[derive(Debug)]
pub(crate) struct StdioSinkStep {
    pub(crate) name: String,
    pub(crate) spec: AsyncSpec,
    pub(crate) config: Value,
    pub(crate) step: StepMeta,
    pub(crate) client: SharedAsyncClient,
}

#[derive(Debug)]
pub(crate) struct StdioTriggerSource {
    pub(crate) name: String,
    pub(crate) spec: AsyncSpec,
    pub(crate) config: Value,
    pub(crate) trigger: TriggerMeta,
    pub(crate) client: SharedAsyncClient,
}

#[derive(Debug)]
pub(crate) struct SourceState {
    pub(crate) spec: AsyncSpec,
    pub(crate) config: Value,
    pub(crate) step: StepMeta,
    pub(crate) client: SharedAsyncClient,
    pub(crate) buffered: VecDeque<Record>,
    pub(crate) finished: bool,
}

#[derive(Debug)]
pub(crate) struct TriggerState {
    pub(crate) spec: AsyncSpec,
    pub(crate) config: Value,
    pub(crate) trigger: TriggerMeta,
    pub(crate) client: SharedAsyncClient,
    pub(crate) buffered: VecDeque<Record>,
    pub(crate) finished: bool,
}

#[derive(Debug)]
pub(crate) struct AsyncStdioClient {
    pub(crate) child: Child,
    pub(crate) stdin: ChildStdin,
    pub(crate) stdout: BufReader<ChildStdout>,
    pub(crate) next_id: u64,
    pub(crate) capabilities: ExtensionCapabilities,
}

#[derive(Debug)]
pub(crate) struct StdioStateStore {
    pub(crate) spec: BlockingSpec,
    pub(crate) config: Value,
    pub(crate) client: StdMutex<Option<BlockingStdioClient>>,
}

#[derive(Debug)]
pub(crate) struct StdioEventBus {
    pub(crate) spec: BlockingSpec,
    pub(crate) config: Value,
    pub(crate) client: StdMutex<Option<BlockingStdioClient>>,
}

#[derive(Debug)]
pub(crate) struct BlockingStdioClient {
    pub(crate) child: StdChild,
    pub(crate) stdin: StdChildStdin,
    pub(crate) stdout: StdBufReader<StdChildStdout>,
    pub(crate) next_id: u64,
    pub(crate) capabilities: ExtensionCapabilities,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TriggerMeta {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) trigger_type: String,
}
