//! External extensions over subprocess stdio + JSON-RPC.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::io::{BufRead, BufReader as StdBufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{
    Child as StdChild, ChildStdin as StdChildStdin, ChildStdout as StdChildStdout,
    Command as StdCommand, Stdio as StdStdio,
};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use futures::stream::{self, BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sohara_config::{ComponentConfig, StoreConfig, TriggerConfig};
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, Error, EventBus, Record, Result, Sink, Source,
    StateStore, StepFactory, StepKind, StepMeta, Transform, TransformOutcome, Trigger,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

const PROTOCOL: &str = "sohara.stdio/v1";

type SharedAsyncClient = Arc<Mutex<Option<AsyncStdioClient>>>;

/// One loaded extension manifest.
#[derive(Debug, Clone)]
pub struct LoadedExtension {
    pub name: String,
    pub version: String,
    pub registrations: Vec<String>,
}

/// Host-side registry of loaded stdio extensions beyond step factories.
#[derive(Debug, Default, Clone)]
pub struct StdioExtensionHost {
    loaded: Vec<LoadedExtension>,
    triggers: HashMap<String, AsyncSpec>,
    state_stores: HashMap<String, BlockingSpec>,
    event_buses: HashMap<String, BlockingSpec>,
}

impl StdioExtensionHost {
    #[must_use]
    pub fn loaded(&self) -> &[LoadedExtension] {
        &self.loaded
    }

    pub fn build_trigger(&self, config: &TriggerConfig) -> Result<Option<Arc<dyn Trigger>>> {
        let Some(spec) = self.triggers.get(&config.trigger_type) else {
            return Ok(None);
        };
        let raw = Value::Object(
            config
                .config()
                .map_err(|error| Error::Config(error.to_string()))?,
        );
        Ok(Some(Arc::new(StdioTriggerSource {
            name: format!("{}::{}", spec.extension_name, spec.type_name),
            spec: spec.clone(),
            config: raw,
            trigger: TriggerMeta {
                id: config.id.clone(),
                trigger_type: config.trigger_type.clone(),
            },
            client: Arc::new(Mutex::new(None)),
        })))
    }

    pub fn build_state_store(&self, config: &StoreConfig) -> Result<Option<Arc<dyn StateStore>>> {
        let StoreConfig::Component(component) = config else {
            return Ok(None);
        };
        let Some(spec) = self.state_stores.get(&component.component_type) else {
            return Ok(None);
        };
        Ok(Some(Arc::new(StdioStateStore {
            spec: spec.clone(),
            config: Value::Object(
                component
                    .config()
                    .map_err(|error| Error::Config(error.to_string()))?,
            ),
            client: StdMutex::new(None),
        })))
    }

    pub fn build_event_bus(&self, config: &ComponentConfig) -> Result<Option<Arc<dyn EventBus>>> {
        let Some(spec) = self.event_buses.get(&config.component_type) else {
            return Ok(None);
        };
        Ok(Some(Arc::new(StdioEventBus {
            spec: spec.clone(),
            config: Value::Object(
                config
                    .config()
                    .map_err(|error| Error::Config(error.to_string()))?,
            ),
            client: StdMutex::new(None),
        })))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StdioExtensionManifest {
    name: String,
    version: String,
    #[serde(default)]
    builtin: bool,
    #[serde(default = "default_protocol")]
    protocol: String,
    command: String,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    steps: Vec<StdioStepRegistration>,
    #[serde(default)]
    triggers: Vec<NamedRegistration>,
    #[serde(default)]
    state_stores: Vec<NamedRegistration>,
    #[serde(default)]
    event_buses: Vec<NamedRegistration>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StdioStepRegistration {
    kind: StepKind,
    #[serde(rename = "type")]
    step_type: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedRegistration {
    #[serde(rename = "type")]
    type_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AsyncKind {
    Source,
    Transform,
    Sink,
    Trigger,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BlockingKind {
    StateStore,
    EventBus,
}

#[derive(Debug, Clone)]
struct AsyncSpec {
    extension_name: String,
    extension_version: String,
    protocol: String,
    command: String,
    cwd: Option<String>,
    args: Vec<String>,
    kind: AsyncKind,
    type_name: String,
}

#[derive(Debug, Clone)]
struct BlockingSpec {
    extension_name: String,
    extension_version: String,
    protocol: String,
    command: String,
    cwd: Option<String>,
    args: Vec<String>,
    kind: BlockingKind,
    type_name: String,
}

#[derive(Debug, Clone)]
struct StdioStepFactory {
    spec: AsyncSpec,
}

#[derive(Debug)]
struct StdioSourceStep {
    name: String,
    spec: AsyncSpec,
    config: Value,
    step: StepMeta,
    client: SharedAsyncClient,
}

#[derive(Debug)]
struct StdioTransformStep {
    name: String,
    spec: AsyncSpec,
    config: Value,
    step: StepMeta,
    client: SharedAsyncClient,
}

#[derive(Debug)]
struct StdioSinkStep {
    name: String,
    spec: AsyncSpec,
    config: Value,
    step: StepMeta,
    client: SharedAsyncClient,
}

#[derive(Debug)]
struct StdioTriggerSource {
    name: String,
    spec: AsyncSpec,
    config: Value,
    trigger: TriggerMeta,
    client: SharedAsyncClient,
}

#[derive(Debug)]
struct SourceState {
    spec: AsyncSpec,
    config: Value,
    step: StepMeta,
    client: SharedAsyncClient,
    buffered: VecDeque<Record>,
    finished: bool,
}

#[derive(Debug)]
struct TriggerState {
    spec: AsyncSpec,
    config: Value,
    trigger: TriggerMeta,
    client: SharedAsyncClient,
    buffered: VecDeque<Record>,
    finished: bool,
}

#[derive(Debug)]
struct AsyncStdioClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    capabilities: ExtensionCapabilities,
}

#[derive(Debug)]
struct StdioStateStore {
    spec: BlockingSpec,
    config: Value,
    client: StdMutex<Option<BlockingStdioClient>>,
}

#[derive(Debug)]
struct StdioEventBus {
    spec: BlockingSpec,
    config: Value,
    client: StdMutex<Option<BlockingStdioClient>>,
}

#[derive(Debug)]
struct BlockingStdioClient {
    child: StdChild,
    stdin: StdChildStdin,
    stdout: StdBufReader<StdChildStdout>,
    next_id: u64,
    capabilities: ExtensionCapabilities,
}

#[derive(Debug, Clone, Serialize)]
struct TriggerMeta {
    id: String,
    #[serde(rename = "type")]
    trigger_type: String,
}

#[derive(Debug, Serialize)]
struct JsonRpcRequest<'a, T> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: T,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    jsonrpc: String,
    id: u64,
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Serialize)]
struct InitializeParams<'a> {
    protocol: &'a str,
    extension: HostExtensionInfo<'a>,
}

#[derive(Debug, Serialize)]
struct HostExtensionInfo<'a> {
    name: &'a str,
    version: &'a str,
}

#[derive(Debug, Deserialize)]
struct InitializeResult {
    protocol: String,
    #[serde(default)]
    capabilities: ExtensionCapabilities,
}

#[derive(Debug, Default, Deserialize)]
struct ExtensionCapabilities {
    #[serde(default)]
    source: bool,
    #[serde(default)]
    transform: bool,
    #[serde(default)]
    sink: bool,
    #[serde(default)]
    trigger: bool,
    #[serde(default)]
    state_store: bool,
    #[serde(default)]
    event_bus: bool,
}

#[derive(Debug, Serialize)]
struct StepParams<'a> {
    step: &'a StepMeta,
    config: &'a Value,
}

#[derive(Debug, Serialize)]
struct TransformParams<'a> {
    step: &'a StepMeta,
    config: &'a Value,
    record: &'a Record,
}

#[derive(Debug, Serialize)]
struct SinkParams<'a> {
    step: &'a StepMeta,
    config: &'a Value,
    record: &'a Record,
}

#[derive(Debug, Serialize)]
struct TriggerParams<'a> {
    trigger: &'a TriggerMeta,
    config: &'a Value,
}

#[derive(Debug, Serialize)]
struct StoreKeyParams<'a> {
    store: TypeName<'a>,
    config: &'a Value,
    key: &'a str,
}

#[derive(Debug, Serialize)]
struct StoreSaveParams<'a> {
    store: TypeName<'a>,
    config: &'a Value,
    key: &'a str,
    value: &'a Value,
}

#[derive(Debug, Serialize)]
struct EventBusPublishParams<'a> {
    bus: TypeName<'a>,
    config: &'a Value,
    topic: &'a str,
    payload: &'a Value,
}

#[derive(Debug, Serialize)]
struct TypeName<'a> {
    #[serde(rename = "type")]
    type_name: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", rename_all = "lowercase")]
enum TransformResult {
    Pass { record: Record },
    Filtered,
    Expand { records: Vec<Record> },
}

#[derive(Debug, Deserialize)]
struct PullResult {
    #[serde(default)]
    records: Vec<Record>,
    #[serde(default)]
    done: bool,
}

#[derive(Debug, Deserialize)]
struct EmptyResult {}

#[derive(Debug, Deserialize)]
struct StateLoadResult {
    found: bool,
    #[serde(default)]
    value: Option<Value>,
}

impl Drop for AsyncStdioClient {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl Drop for BlockingStdioClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

impl StepFactory for StdioStepFactory {
    fn build(&self, config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        let step = ctx.step.clone().ok_or_else(|| {
            Error::Config(format!(
                "stdio extension '{}' requires step metadata during build",
                self.spec.type_name
            ))
        })?;
        let client = Arc::new(Mutex::new(None));
        let name = format!("{}::{}", self.spec.extension_name, self.spec.type_name);
        Ok(match self.spec.kind {
            AsyncKind::Source => BuiltStep::Source(Box::new(StdioSourceStep {
                name,
                spec: self.spec.clone(),
                config: config.clone(),
                step,
                client,
            })),
            AsyncKind::Transform => BuiltStep::Transform(Box::new(StdioTransformStep {
                name,
                spec: self.spec.clone(),
                config: config.clone(),
                step,
                client,
            })),
            AsyncKind::Sink => BuiltStep::Sink(Box::new(StdioSinkStep {
                name,
                spec: self.spec.clone(),
                config: config.clone(),
                step,
                client,
            })),
            AsyncKind::Trigger => {
                return Err(Error::Config(format!(
                    "trigger '{}' must be declared under flow.triggers, not steps",
                    self.spec.type_name
                )));
            }
        })
    }
}

#[async_trait::async_trait]
impl Source for StdioSourceStep {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let state = SourceState {
            spec: self.spec.clone(),
            config: self.config.clone(),
            step: self.step.clone(),
            client: self.client.clone(),
            buffered: VecDeque::new(),
            finished: false,
        };
        Ok(Box::pin(stream::try_unfold(state, next_source_record)))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Transform for StdioTransformStep {
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        let mut guard = self.client.lock().await;
        let client = ensure_async_client(&mut guard, &self.spec).await?;
        match client
            .transform(&self.spec, &self.step, &self.config, &record)
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => {
                reset_async_client(&mut guard);
                Err(error)
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Sink for StdioSinkStep {
    async fn send(&self, record: Record) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = ensure_async_client(&mut guard, &self.spec).await?;
        if let Err(error) = client
            .sink_send(&self.spec, &self.step, &self.config, &record)
            .await
        {
            reset_async_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = ensure_async_client(&mut guard, &self.spec).await?;
        if let Err(error) = client
            .sink_flush(&self.spec, &self.step, &self.config)
            .await
        {
            reset_async_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Source for StdioTriggerSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let state = TriggerState {
            spec: self.spec.clone(),
            config: self.config.clone(),
            trigger: self.trigger.clone(),
            client: self.client.clone(),
            buffered: VecDeque::new(),
            finished: false,
        };
        Ok(Box::pin(stream::try_unfold(state, next_trigger_record)))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Trigger for StdioTriggerSource {
    async fn start(&self) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = ensure_async_client(&mut guard, &self.spec).await?;
        if let Err(error) = client
            .trigger_start(&self.spec, &self.trigger, &self.config)
            .await
        {
            reset_async_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut guard = self.client.lock().await;
        let Some(client) = guard.as_mut() else {
            return Ok(());
        };
        if let Err(error) = client
            .trigger_stop(&self.spec, &self.trigger, &self.config)
            .await
        {
            reset_async_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }
}

impl StateStore for StdioStateStore {
    fn load(&self, key: &str) -> Result<Option<Value>> {
        let mut guard = self.client.lock().expect("state store lock poisoned");
        let result = {
            let client = ensure_blocking_client(&mut guard, &self.spec)?;
            client.state_load(&self.spec, &self.config, key)
        };
        match result {
            Ok(result) => Ok(if result.found { result.value } else { None }),
            Err(error) => {
                reset_blocking_client(&mut guard);
                Err(error)
            }
        }
    }

    fn save(&self, key: &str, value: Value) -> Result<()> {
        let mut guard = self.client.lock().expect("state store lock poisoned");
        let result = {
            let client = ensure_blocking_client(&mut guard, &self.spec)?;
            client.state_save(&self.spec, &self.config, key, &value)
        };
        if let Err(error) = result {
            reset_blocking_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let mut guard = self.client.lock().expect("state store lock poisoned");
        let result = {
            let client = ensure_blocking_client(&mut guard, &self.spec)?;
            client.state_delete(&self.spec, &self.config, key)
        };
        if let Err(error) = result {
            reset_blocking_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }
}

impl EventBus for StdioEventBus {
    fn publish(&self, topic: &str, payload: Value) -> Result<()> {
        let mut guard = self.client.lock().expect("event bus lock poisoned");
        let result = {
            let client = ensure_blocking_client(&mut guard, &self.spec)?;
            client.bus_publish(&self.spec, &self.config, topic, &payload)
        };
        if let Err(error) = result {
            reset_blocking_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }
}

impl AsyncStdioClient {
    async fn spawn(spec: &AsyncSpec) -> Result<Self> {
        let mut command = Command::new(&spec.command);
        command
            .args(&spec.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }
        let mut child = command
            .spawn()
            .map_err(|error| async_error(spec, spawn_async_error(spec, &command, error)))?;
        let stdin = child.stdin.take().ok_or_else(|| {
            async_error(
                spec,
                format!("extension '{}' did not expose stdin", spec.extension_name),
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            async_error(
                spec,
                format!("extension '{}' did not expose stdout", spec.extension_name),
            )
        })?;
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
            capabilities: ExtensionCapabilities::default(),
        };
        client.initialize(spec).await?;
        Ok(client)
    }

    async fn initialize(&mut self, spec: &AsyncSpec) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "initialize",
            params: InitializeParams {
                protocol: PROTOCOL,
                extension: HostExtensionInfo {
                    name: &spec.extension_name,
                    version: &spec.extension_version,
                },
            },
        };
        let response: JsonRpcResponse<InitializeResult> = self.request(spec, request).await?;
        let result = take_async_result(spec, response, "initialize", &spec.extension_name)?;
        if result.protocol != spec.protocol {
            return Err(async_error(
                spec,
                format!(
                    "extension '{}' protocol mismatch: host={} extension={}",
                    spec.extension_name, spec.protocol, result.protocol
                ),
            ));
        }
        self.capabilities = result.capabilities;
        self.ensure_capability(spec)
    }

    async fn source_pull(
        &mut self,
        spec: &AsyncSpec,
        step: &StepMeta,
        config: &Value,
    ) -> Result<PullResult> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "source.pull",
            params: StepParams { step, config },
        };
        let response: JsonRpcResponse<PullResult> = self.request(spec, request).await?;
        take_async_result(spec, response, "source.pull", &step.step_type)
    }

    async fn transform(
        &mut self,
        spec: &AsyncSpec,
        step: &StepMeta,
        config: &Value,
        record: &Record,
    ) -> Result<TransformOutcome> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "transform",
            params: TransformParams {
                step,
                config,
                record,
            },
        };
        let response: JsonRpcResponse<TransformResult> = self.request(spec, request).await?;
        let result = take_async_result(spec, response, "transform", &step.step_type)?;
        Ok(match result {
            TransformResult::Pass { record } => TransformOutcome::Pass(record),
            TransformResult::Filtered => TransformOutcome::Filtered,
            TransformResult::Expand { records } => TransformOutcome::Expand(records),
        })
    }

    async fn sink_send(
        &mut self,
        spec: &AsyncSpec,
        step: &StepMeta,
        config: &Value,
        record: &Record,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "sink.send",
            params: SinkParams {
                step,
                config,
                record,
            },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request).await?;
        let _ = take_async_result(spec, response, "sink.send", &step.step_type)?;
        Ok(())
    }

    async fn sink_flush(
        &mut self,
        spec: &AsyncSpec,
        step: &StepMeta,
        config: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "sink.flush",
            params: StepParams { step, config },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request).await?;
        let _ = take_async_result(spec, response, "sink.flush", &step.step_type)?;
        Ok(())
    }

    async fn trigger_start(
        &mut self,
        spec: &AsyncSpec,
        trigger: &TriggerMeta,
        config: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "trigger.start",
            params: TriggerParams { trigger, config },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request).await?;
        let _ = take_async_result(spec, response, "trigger.start", &trigger.trigger_type)?;
        Ok(())
    }

    async fn trigger_pull(
        &mut self,
        spec: &AsyncSpec,
        trigger: &TriggerMeta,
        config: &Value,
    ) -> Result<PullResult> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "trigger.pull",
            params: TriggerParams { trigger, config },
        };
        let response: JsonRpcResponse<PullResult> = self.request(spec, request).await?;
        take_async_result(spec, response, "trigger.pull", &trigger.trigger_type)
    }

    async fn trigger_stop(
        &mut self,
        spec: &AsyncSpec,
        trigger: &TriggerMeta,
        config: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "trigger.stop",
            params: TriggerParams { trigger, config },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request).await?;
        let _ = take_async_result(spec, response, "trigger.stop", &trigger.trigger_type)?;
        Ok(())
    }

    async fn request<T, R>(
        &mut self,
        spec: &AsyncSpec,
        request: JsonRpcRequest<'_, T>,
    ) -> Result<JsonRpcResponse<R>>
    where
        T: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let mut line = serde_json::to_vec(&request)?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .await
            .map_err(|error| async_error(spec, write_error(request.method, error)))?;
        self.stdin
            .flush()
            .await
            .map_err(|error| async_error(spec, flush_error(request.method, error)))?;

        let mut response = String::new();
        let read = self
            .stdout
            .read_line(&mut response)
            .await
            .map_err(|error| async_error(spec, read_error(request.method, error)))?;
        if read == 0 {
            return Err(async_error(
                spec,
                format!("extension closed stdout during {}", request.method),
            ));
        }
        let parsed: JsonRpcResponse<R> = serde_json::from_str(&response).map_err(|error| {
            async_error(
                spec,
                format!(
                    "invalid {} JSON-RPC response: {error}; raw={}",
                    request.method,
                    response.trim()
                ),
            )
        })?;
        validate_jsonrpc(spec, request.method, request.id, &parsed)?;
        Ok(parsed)
    }

    fn ensure_capability(&self, spec: &AsyncSpec) -> Result<()> {
        if self.capabilities.supports_async(spec.kind) {
            return Ok(());
        }
        Err(async_error(
            spec,
            format!(
                "extension '{}' does not advertise {} capability",
                spec.extension_name,
                async_kind_name(spec.kind)
            ),
        ))
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

impl BlockingStdioClient {
    fn spawn(spec: &BlockingSpec) -> Result<Self> {
        let mut command = StdCommand::new(&spec.command);
        command
            .args(&spec.args)
            .stdin(StdStdio::piped())
            .stdout(StdStdio::piped())
            .stderr(StdStdio::inherit());
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }
        let mut child = command
            .spawn()
            .map_err(|error| blocking_error(spec, spawn_blocking_error(spec, &command, error)))?;
        let stdin = child.stdin.take().ok_or_else(|| {
            blocking_error(
                spec,
                format!("extension '{}' did not expose stdin", spec.extension_name),
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            blocking_error(
                spec,
                format!("extension '{}' did not expose stdout", spec.extension_name),
            )
        })?;
        let mut client = Self {
            child,
            stdin,
            stdout: StdBufReader::new(stdout),
            next_id: 1,
            capabilities: ExtensionCapabilities::default(),
        };
        client.initialize(spec)?;
        Ok(client)
    }

    fn initialize(&mut self, spec: &BlockingSpec) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "initialize",
            params: InitializeParams {
                protocol: PROTOCOL,
                extension: HostExtensionInfo {
                    name: &spec.extension_name,
                    version: &spec.extension_version,
                },
            },
        };
        let response: JsonRpcResponse<InitializeResult> = self.request(spec, request)?;
        let result = take_blocking_result(spec, response, "initialize", &spec.extension_name)?;
        if result.protocol != spec.protocol {
            return Err(blocking_error(
                spec,
                format!(
                    "extension '{}' protocol mismatch: host={} extension={}",
                    spec.extension_name, spec.protocol, result.protocol
                ),
            ));
        }
        self.capabilities = result.capabilities;
        self.ensure_capability(spec)
    }

    fn state_load(
        &mut self,
        spec: &BlockingSpec,
        config: &Value,
        key: &str,
    ) -> Result<StateLoadResult> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "state.load",
            params: StoreKeyParams {
                store: TypeName {
                    type_name: &spec.type_name,
                },
                config,
                key,
            },
        };
        let response: JsonRpcResponse<StateLoadResult> = self.request(spec, request)?;
        take_blocking_result(spec, response, "state.load", &spec.type_name)
    }

    fn state_save(
        &mut self,
        spec: &BlockingSpec,
        config: &Value,
        key: &str,
        value: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "state.save",
            params: StoreSaveParams {
                store: TypeName {
                    type_name: &spec.type_name,
                },
                config,
                key,
                value,
            },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request)?;
        let _ = take_blocking_result(spec, response, "state.save", &spec.type_name)?;
        Ok(())
    }

    fn state_delete(&mut self, spec: &BlockingSpec, config: &Value, key: &str) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "state.delete",
            params: StoreKeyParams {
                store: TypeName {
                    type_name: &spec.type_name,
                },
                config,
                key,
            },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request)?;
        let _ = take_blocking_result(spec, response, "state.delete", &spec.type_name)?;
        Ok(())
    }

    fn bus_publish(
        &mut self,
        spec: &BlockingSpec,
        config: &Value,
        topic: &str,
        payload: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "bus.publish",
            params: EventBusPublishParams {
                bus: TypeName {
                    type_name: &spec.type_name,
                },
                config,
                topic,
                payload,
            },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request)?;
        let _ = take_blocking_result(spec, response, "bus.publish", &spec.type_name)?;
        Ok(())
    }

    fn request<T, R>(
        &mut self,
        spec: &BlockingSpec,
        request: JsonRpcRequest<'_, T>,
    ) -> Result<JsonRpcResponse<R>>
    where
        T: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let mut line = serde_json::to_vec(&request)?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .map_err(|error| blocking_error(spec, write_error(request.method, error)))?;
        self.stdin
            .flush()
            .map_err(|error| blocking_error(spec, flush_error(request.method, error)))?;

        let mut response = String::new();
        let read = self
            .stdout
            .read_line(&mut response)
            .map_err(|error| blocking_error(spec, read_error(request.method, error)))?;
        if read == 0 {
            return Err(blocking_error(
                spec,
                format!("extension closed stdout during {}", request.method),
            ));
        }
        let parsed: JsonRpcResponse<R> = serde_json::from_str(&response).map_err(|error| {
            blocking_error(
                spec,
                format!(
                    "invalid {} JSON-RPC response: {error}; raw={}",
                    request.method,
                    response.trim()
                ),
            )
        })?;
        validate_jsonrpc(spec, request.method, request.id, &parsed)?;
        Ok(parsed)
    }

    fn ensure_capability(&self, spec: &BlockingSpec) -> Result<()> {
        if self.capabilities.supports_blocking(spec.kind) {
            return Ok(());
        }
        Err(blocking_error(
            spec,
            format!(
                "extension '{}' does not advertise {} capability",
                spec.extension_name,
                blocking_kind_name(spec.kind)
            ),
        ))
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

impl ExtensionCapabilities {
    const fn supports_async(&self, kind: AsyncKind) -> bool {
        match kind {
            AsyncKind::Source => self.source,
            AsyncKind::Transform => self.transform,
            AsyncKind::Sink => self.sink,
            AsyncKind::Trigger => self.trigger,
        }
    }

    const fn supports_blocking(&self, kind: BlockingKind) -> bool {
        match kind {
            BlockingKind::StateStore => self.state_store,
            BlockingKind::EventBus => self.event_bus,
        }
    }
}

async fn next_source_record(mut state: SourceState) -> Result<Option<(Record, SourceState)>> {
    loop {
        if let Some(record) = state.buffered.pop_front() {
            return Ok(Some((record, state)));
        }
        if state.finished {
            return Ok(None);
        }
        let mut guard = state.client.lock().await;
        let client = ensure_async_client(&mut guard, &state.spec).await?;
        let response = client
            .source_pull(&state.spec, &state.step, &state.config)
            .await;
        let pulled = match response {
            Ok(result) => result,
            Err(error) => {
                reset_async_client(&mut guard);
                return Err(error);
            }
        };
        if !pulled.done && pulled.records.is_empty() {
            reset_async_client(&mut guard);
            return Err(async_error(
                &state.spec,
                format!(
                    "extension '{}' returned no records and done=false for source.pull",
                    state.spec.extension_name
                ),
            ));
        }
        state.finished = pulled.done;
        state.buffered = pulled.records.into();
    }
}

async fn next_trigger_record(mut state: TriggerState) -> Result<Option<(Record, TriggerState)>> {
    loop {
        if let Some(record) = state.buffered.pop_front() {
            return Ok(Some((record, state)));
        }
        if state.finished {
            return Ok(None);
        }
        let mut guard = state.client.lock().await;
        let client = ensure_async_client(&mut guard, &state.spec).await?;
        let response = client
            .trigger_pull(&state.spec, &state.trigger, &state.config)
            .await;
        let pulled = match response {
            Ok(result) => result,
            Err(error) => {
                reset_async_client(&mut guard);
                return Err(error);
            }
        };
        if !pulled.done && pulled.records.is_empty() {
            drop(guard);
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        state.finished = pulled.done;
        state.buffered = pulled.records.into();
    }
}

async fn ensure_async_client<'a>(
    guard: &'a mut Option<AsyncStdioClient>,
    spec: &AsyncSpec,
) -> Result<&'a mut AsyncStdioClient> {
    if guard.is_none() {
        *guard = Some(AsyncStdioClient::spawn(spec).await?);
    }
    guard.as_mut().ok_or_else(|| {
        async_error(
            spec,
            format!("extension '{}' failed to initialize", spec.extension_name),
        )
    })
}

fn ensure_blocking_client<'a>(
    guard: &'a mut Option<BlockingStdioClient>,
    spec: &BlockingSpec,
) -> Result<&'a mut BlockingStdioClient> {
    if guard.is_none() {
        *guard = Some(BlockingStdioClient::spawn(spec)?);
    }
    guard.as_mut().ok_or_else(|| {
        blocking_error(
            spec,
            format!("extension '{}' failed to initialize", spec.extension_name),
        )
    })
}

fn reset_async_client(client: &mut Option<AsyncStdioClient>) {
    if let Some(client) = client.as_mut() {
        let _ = client.child.start_kill();
    }
    *client = None;
}

fn reset_blocking_client(client: &mut Option<BlockingStdioClient>) {
    if let Some(client) = client.as_mut() {
        let _ = client.child.kill();
    }
    *client = None;
}

fn validate_jsonrpc<S>(
    spec: &S,
    method: &str,
    id: u64,
    response: &JsonRpcResponse<impl Sized>,
) -> Result<()>
where
    S: SpecError,
{
    if response.jsonrpc != "2.0" {
        return Err(spec_error(
            spec,
            format!("invalid {method} JSON-RPC version: {}", response.jsonrpc),
        ));
    }
    if response.id != id {
        return Err(spec_error(
            spec,
            format!(
                "mismatched {method} response id: expected {id}, got {}",
                response.id
            ),
        ));
    }
    Ok(())
}

fn take_async_result<T>(
    spec: &AsyncSpec,
    response: JsonRpcResponse<T>,
    method: &str,
    name: &str,
) -> Result<T> {
    if let Some(error) = response.error {
        return Err(async_error(
            spec,
            format!(
                "extension '{name}' {method} failed: {} ({})",
                error.message, error.code
            ),
        ));
    }
    response.result.ok_or_else(|| {
        async_error(
            spec,
            format!("extension '{name}' returned no result for {method}"),
        )
    })
}

fn take_blocking_result<T>(
    spec: &BlockingSpec,
    response: JsonRpcResponse<T>,
    method: &str,
    name: &str,
) -> Result<T> {
    if let Some(error) = response.error {
        return Err(blocking_error(
            spec,
            format!(
                "extension '{name}' {method} failed: {} ({})",
                error.message, error.code
            ),
        ));
    }
    response.result.ok_or_else(|| {
        blocking_error(
            spec,
            format!("extension '{name}' returned no result for {method}"),
        )
    })
}

trait SpecError {
    fn into_error(&self, message: String) -> Error;
}

impl SpecError for AsyncSpec {
    fn into_error(&self, message: String) -> Error {
        async_error(self, message)
    }
}

impl SpecError for BlockingSpec {
    fn into_error(&self, message: String) -> Error {
        blocking_error(self, message)
    }
}

fn spec_error(spec: &impl SpecError, message: String) -> Error {
    spec.into_error(message)
}

fn async_error(spec: &AsyncSpec, message: String) -> Error {
    match spec.kind {
        AsyncKind::Source | AsyncKind::Trigger => Error::Source(message),
        AsyncKind::Transform => Error::Transform(message),
        AsyncKind::Sink => Error::Sink(message),
    }
}

fn blocking_error(_spec: &BlockingSpec, message: String) -> Error {
    Error::Runtime(message)
}

fn spawn_async_error(spec: &AsyncSpec, command: &Command, error: std::io::Error) -> String {
    format!(
        "failed to spawn extension '{}' ({:?}): {error}",
        spec.extension_name,
        command.as_std().get_args().collect::<Vec<&OsStr>>()
    )
}

fn spawn_blocking_error(
    spec: &BlockingSpec,
    command: &StdCommand,
    error: std::io::Error,
) -> String {
    format!(
        "failed to spawn extension '{}' ({:?}): {error}",
        spec.extension_name,
        command.get_args().collect::<Vec<&OsStr>>()
    )
}

fn write_error(method: &str, error: std::io::Error) -> String {
    format!("failed to write {method} request to extension: {error}")
}

fn flush_error(method: &str, error: std::io::Error) -> String {
    format!("failed to flush {method} request to extension: {error}")
}

fn read_error(method: &str, error: std::io::Error) -> String {
    format!("failed to read {method} response from extension: {error}")
}

fn async_kind_name(kind: AsyncKind) -> &'static str {
    match kind {
        AsyncKind::Source => "source",
        AsyncKind::Transform => "transform",
        AsyncKind::Sink => "sink",
        AsyncKind::Trigger => "trigger",
    }
}

fn blocking_kind_name(kind: BlockingKind) -> &'static str {
    match kind {
        BlockingKind::StateStore => "state_store",
        BlockingKind::EventBus => "event_bus",
    }
}

/// Load stdio extension manifests from the given files or directories and
/// register their step factories into the provided registry.
pub fn load_stdio_extensions(
    registry: &mut ComponentRegistry,
    paths: &[PathBuf],
) -> Result<StdioExtensionHost> {
    load_stdio_extensions_with_trusted(registry, &[], paths)
}

/// Load trusted builtin manifests first, then user-provided manifests.
pub fn load_stdio_extensions_with_trusted(
    registry: &mut ComponentRegistry,
    trusted_paths: &[PathBuf],
    paths: &[PathBuf],
) -> Result<StdioExtensionHost> {
    let mut host = StdioExtensionHost::default();
    for manifest_path in manifest_paths(trusted_paths)? {
        let manifest = parse_manifest(&manifest_path)?;
        validate_manifest(&manifest, registry, &host, true)?;
        register_manifest(registry, &mut host, &manifest, &manifest_path)?;
    }
    for manifest_path in manifest_paths(paths)? {
        let manifest = parse_manifest(&manifest_path)?;
        validate_manifest(&manifest, registry, &host, false)?;
        register_manifest(registry, &mut host, &manifest, &manifest_path)?;
    }
    Ok(host)
}

fn register_manifest(
    registry: &mut ComponentRegistry,
    host: &mut StdioExtensionHost,
    manifest: &StdioExtensionManifest,
    manifest_path: &Path,
) -> Result<()> {
    let command = resolve_command(manifest_path, &manifest.command);
    let cwd = manifest
        .cwd
        .as_deref()
        .map(|cwd| resolve_command(manifest_path, cwd));
    for step in &manifest.steps {
        if step.kind == StepKind::Control {
            return Err(Error::Config(format!(
                "extension '{}' step '{}': control steps are not supported in the stdio host",
                manifest.name, step.step_type
            )));
        }
        let spec = AsyncSpec {
            extension_name: manifest.name.clone(),
            extension_version: manifest.version.clone(),
            protocol: manifest.protocol.clone(),
            command: command.clone(),
            cwd: cwd.clone(),
            args: manifest.args.clone(),
            kind: match step.kind {
                StepKind::Source => AsyncKind::Source,
                StepKind::Transform => AsyncKind::Transform,
                StepKind::Sink => AsyncKind::Sink,
                StepKind::Control => unreachable!(),
            },
            type_name: step.step_type.clone(),
        };
        registry.register(
            step.kind,
            &step.step_type,
            Arc::new(StdioStepFactory { spec }),
        );
    }
    for trigger in &manifest.triggers {
        host.triggers.insert(
            trigger.type_name.clone(),
            AsyncSpec {
                extension_name: manifest.name.clone(),
                extension_version: manifest.version.clone(),
                protocol: manifest.protocol.clone(),
                command: command.clone(),
                cwd: cwd.clone(),
                args: manifest.args.clone(),
                kind: AsyncKind::Trigger,
                type_name: trigger.type_name.clone(),
            },
        );
    }
    for store in &manifest.state_stores {
        host.state_stores.insert(
            store.type_name.clone(),
            BlockingSpec {
                extension_name: manifest.name.clone(),
                extension_version: manifest.version.clone(),
                protocol: manifest.protocol.clone(),
                command: command.clone(),
                cwd: cwd.clone(),
                args: manifest.args.clone(),
                kind: BlockingKind::StateStore,
                type_name: store.type_name.clone(),
            },
        );
    }
    for bus in &manifest.event_buses {
        host.event_buses.insert(
            bus.type_name.clone(),
            BlockingSpec {
                extension_name: manifest.name.clone(),
                extension_version: manifest.version.clone(),
                protocol: manifest.protocol.clone(),
                command: command.clone(),
                cwd: cwd.clone(),
                args: manifest.args.clone(),
                kind: BlockingKind::EventBus,
                type_name: bus.type_name.clone(),
            },
        );
    }
    host.loaded.push(LoadedExtension {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        registrations: manifest_registrations(manifest),
    });
    Ok(())
}

fn validate_manifest(
    manifest: &StdioExtensionManifest,
    registry: &ComponentRegistry,
    host: &StdioExtensionHost,
    trusted: bool,
) -> Result<()> {
    if manifest.builtin && !trusted {
        return Err(Error::Config(format!(
            "extension '{}' declares builtin=true outside Sohara's trusted builtin path",
            manifest.name
        )));
    }
    if manifest.protocol != PROTOCOL {
        return Err(Error::Config(format!(
            "extension '{}' uses unsupported protocol '{}'; expected '{}'",
            manifest.name, manifest.protocol, PROTOCOL
        )));
    }
    if manifest.steps.is_empty()
        && manifest.triggers.is_empty()
        && manifest.state_stores.is_empty()
        && manifest.event_buses.is_empty()
    {
        return Err(Error::Config(format!(
            "extension '{}' must declare at least one registration",
            manifest.name
        )));
    }
    validate_builtin_prefixes(manifest, trusted)?;
    let mut seen_steps = HashSet::new();
    for step in &manifest.steps {
        if !seen_steps.insert((step.kind, step.step_type.clone())) {
            return Err(Error::Config(format!(
                "extension '{}' declares duplicate kind='{}' type='{}'",
                manifest.name,
                step.kind.as_str(),
                step.step_type
            )));
        }
        if registry.contains(step.kind, &step.step_type) {
            return Err(Error::Config(format!(
                "extension '{}' duplicates existing registration for kind='{}' type='{}'",
                manifest.name,
                step.kind.as_str(),
                step.step_type
            )));
        }
    }
    validate_named_duplicates(manifest, "trigger", &manifest.triggers, |type_name| {
        host.triggers.contains_key(type_name)
    })?;
    validate_named_duplicates(
        manifest,
        "state_store",
        &manifest.state_stores,
        |type_name| host.state_stores.contains_key(type_name),
    )?;
    validate_named_duplicates(manifest, "event_bus", &manifest.event_buses, |type_name| {
        host.event_buses.contains_key(type_name)
    })?;
    Ok(())
}

fn validate_builtin_prefixes(manifest: &StdioExtensionManifest, trusted: bool) -> Result<()> {
    let registrations = manifest_registrations(manifest);
    let reserved = registrations.iter().find(|registration| {
        registration
            .rsplit(':')
            .next()
            .is_some_and(|name| name.starts_with("builtin-"))
    });
    let Some(reserved) = reserved else {
        return Ok(());
    };
    if !trusted || !manifest.builtin {
        return Err(Error::Config(format!(
            "registration '{}' is reserved for Sohara builtin extensions",
            reserved
        )));
    }
    Ok(())
}

fn validate_named_duplicates(
    manifest: &StdioExtensionManifest,
    kind: &str,
    registrations: &[NamedRegistration],
    exists: impl Fn(&str) -> bool,
) -> Result<()> {
    let mut seen = HashSet::new();
    for registration in registrations {
        if !seen.insert(registration.type_name.clone()) {
            return Err(Error::Config(format!(
                "extension '{}' declares duplicate {kind} type='{}'",
                manifest.name, registration.type_name
            )));
        }
        if exists(&registration.type_name) {
            return Err(Error::Config(format!(
                "extension '{}' duplicates existing {kind} type='{}'",
                manifest.name, registration.type_name
            )));
        }
    }
    Ok(())
}

fn manifest_registrations(manifest: &StdioExtensionManifest) -> Vec<String> {
    let mut result = Vec::new();
    result.extend(
        manifest
            .steps
            .iter()
            .map(|step| format!("{}:{}", step.kind.as_str(), step.step_type)),
    );
    result.extend(
        manifest
            .triggers
            .iter()
            .map(|trigger| format!("trigger:{}", trigger.type_name)),
    );
    result.extend(
        manifest
            .state_stores
            .iter()
            .map(|store| format!("state_store:{}", store.type_name)),
    );
    result.extend(
        manifest
            .event_buses
            .iter()
            .map(|bus| format!("event_bus:{}", bus.type_name)),
    );
    result
}

fn manifest_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut manifests = Vec::new();
    for path in paths {
        if path.is_dir() {
            let mut entries = std::fs::read_dir(path)
                .map_err(|error| {
                    Error::Config(format!("read extension dir '{}': {error}", path.display()))
                })?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| {
                    Error::Config(format!("read extension dir '{}': {error}", path.display()))
                })?;
            entries.sort();
            manifests.extend(
                entries
                    .into_iter()
                    .filter(|entry| entry.is_file() && supported_manifest(entry)),
            );
        } else if path.is_file() {
            if supported_manifest(path) {
                manifests.push(path.clone());
            } else {
                return Err(Error::Config(format!(
                    "unsupported extension manifest '{}'; use .json/.yaml/.yml",
                    path.display()
                )));
            }
        } else {
            return Err(Error::Config(format!(
                "extension path '{}' does not exist",
                path.display()
            )));
        }
    }
    manifests.sort();
    Ok(manifests)
}

fn supported_manifest(path: &Path) -> bool {
    matches!(
        path.extension().and_then(OsStr::to_str),
        Some("json" | "yaml" | "yml")
    )
}

fn parse_manifest(path: &Path) -> Result<StdioExtensionManifest> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        Error::Config(format!(
            "read extension manifest '{}': {error}",
            path.display()
        ))
    })?;
    match path.extension().and_then(OsStr::to_str) {
        Some("json") => serde_json::from_str(&text).map_err(|error| {
            Error::Config(format!(
                "parse extension manifest '{}': {error}",
                path.display()
            ))
        }),
        Some("yaml" | "yml") => serde_yaml::from_str(&text).map_err(|error| {
            Error::Config(format!(
                "parse extension manifest '{}': {error}",
                path.display()
            ))
        }),
        _ => Err(Error::Config(format!(
            "unsupported extension manifest '{}'; use .json/.yaml/.yml",
            path.display()
        ))),
    }
}

fn resolve_command(manifest_path: &Path, command: &str) -> String {
    let command_path = Path::new(command);
    if command_path.is_absolute() || command_path.components().count() == 1 {
        return command.to_owned();
    }
    manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(command_path)
        .to_string_lossy()
        .into_owned()
}

fn default_protocol() -> String {
    PROTOCOL.to_owned()
}
