//! External extensions over subprocess stdio + JSON-RPC.
//!
//! Split into focused submodules so each file stays within the project's
//! 300-line file gate; `super::*` re-exports keep the wiring unchanged.

mod client_async;
mod client_blocking;
mod helpers;
mod loading;
mod manifest;
mod params;
mod protocol;
mod steps;
mod store_bus;
mod types;
mod validation;

pub(crate) use std::collections::{HashMap, HashSet, VecDeque};
pub(crate) use std::ffi::OsStr;
pub(crate) use std::io::{BufRead, BufReader as StdBufReader, Write};
pub(crate) use std::path::{Path, PathBuf};
pub(crate) use std::process::{
    Child as StdChild, ChildStdin as StdChildStdin, ChildStdout as StdChildStdout,
    Command as StdCommand, Stdio as StdStdio,
};
pub(crate) use std::sync::Arc;
pub(crate) use std::sync::Mutex as StdMutex;
pub(crate) use std::time::Duration;

pub(crate) use futures::stream::{self, BoxStream};
pub(crate) use serde::{Deserialize, Serialize};
pub(crate) use serde_json::Value;
pub(crate) use sohara_config::{ComponentConfig, StoreConfig, TriggerConfig};
pub(crate) use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, Error, EventBus, Record, Result, Sink, Source,
    StateStore, StepFactory, StepKind, StepMeta, Transform, TransformOutcome, Trigger,
};
pub(crate) use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
pub(crate) use tokio::process::{Child, ChildStdin, ChildStdout, Command};
pub(crate) use tokio::sync::Mutex;

pub(crate) use helpers::{
    ensure_async_client, ensure_blocking_client, next_source_record, next_trigger_record,
    reset_async_client, reset_blocking_client,
};
pub use loading::{load_stdio_extensions, load_stdio_extensions_with_trusted};
pub(crate) use manifest::{default_protocol, manifest_paths, parse_manifest, resolve_command};
pub(crate) use params::{
    EmptyResult, EventBusPublishParams, ExtensionCapabilities, HostExtensionInfo, InitializeParams,
    InitializeResult, JsonRpcRequest, JsonRpcResponse, PullResult, SinkParams, StateLoadResult,
    StepParams, StoreKeyParams, StoreSaveParams, TransformParams, TransformResult, TriggerParams,
    TypeName,
};
pub(crate) use protocol::{
    async_error, async_kind_name, blocking_error, blocking_kind_name, flush_error, read_error,
    spawn_async_error, spawn_blocking_error, take_async_result, take_blocking_result,
    validate_jsonrpc, write_error,
};
pub(crate) use types::{
    AsyncKind, AsyncSpec, AsyncStdioClient, BlockingKind, BlockingSpec, BlockingStdioClient,
    NamedRegistration, SourceState, StdioEventBus, StdioExtensionManifest, StdioSinkStep,
    StdioSourceStep, StdioStateStore, StdioStepFactory, StdioTransformStep, StdioTriggerSource,
    TriggerMeta, TriggerState,
};
pub(crate) use validation::{register_manifest, validate_manifest};

pub(crate) const PROTOCOL: &str = "sohara.stdio/v1";
pub(crate) type SharedAsyncClient = Arc<Mutex<Option<AsyncStdioClient>>>;

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
    pub(crate) loaded: Vec<LoadedExtension>,
    pub(crate) triggers: HashMap<String, AsyncSpec>,
    pub(crate) state_stores: HashMap<String, BlockingSpec>,
    pub(crate) event_buses: HashMap<String, BlockingSpec>,
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
