//! Integration tests for stdio triggers, state stores, and event buses.

mod common;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use sohara_config::FlowConfig;
use sohara_core::{ComponentRegistry, Record};
use sohara_runtime::{
    load_stdio_extensions, run_flow_with_store, serve_with_shutdown_opts, ServeOptions,
    StatsSnapshot,
};
use sohara_triggers::InProcessBus;

use common::stdio::{assert_python3, write_extension};

const TRIGGER_REGISTRATION: &str = r#"
triggers:
  - type: remote_trigger
"#;

const TRIGGER_FLOW: &str = r#"
name: ext-trigger
version: "1"
triggers:
  - { id: remote, type: remote_trigger, config: {} }
steps:
  - { id: out, kind: sink, type: probe }
edges: [[remote, out]]
"#;

const STORE_REGISTRATION: &str = r#"
state_stores:
  - type: remote_store
"#;

const BUS_REGISTRATION: &str = r#"
event_buses:
  - type: remote_bus
"#;

fn probe_registry(records: &Arc<Mutex<Vec<Record>>>) -> ComponentRegistry {
    common::registry(vec![(
        "sink",
        "probe",
        common::probe_factory(records.clone()),
    )])
}

fn store_flow(path: &Path) -> FlowConfig {
    FlowConfig::from_yaml_str(&format!(
        r#"
name: ext-store
version: "1"
checkpoint:
  every: 1
  store:
    type: remote_store
    path: "{}"
steps:
  - {{ id: in, kind: source, type: inline, config: {{ records: [{{ n: 1 }}, {{ n: 2 }}] }} }}
  - {{ id: count, kind: transform, type: state, config: {{ expr: {{ total: "state.total + 1" }} }}, state: {{ total: 0 }} }}
  - {{ id: out, kind: sink, type: probe }}
edges: [[in, count], [count, out]]
"#,
        path.display()
    ))
    .unwrap()
}

fn bus_flow(path: &Path) -> FlowConfig {
    FlowConfig::from_yaml_str(&format!(
        r#"
name: ext-bus
version: "1"
event_bus:
  type: remote_bus
  path: "{}"
steps:
  - {{ id: in, kind: source, type: inline, config: {{ records: [{{ n: 1 }}, {{ n: 2 }}] }} }}
  - {{ id: out, kind: sink, type: queue, config: {{ topic: outbound }} }}
"#,
        path.display()
    ))
    .unwrap()
}

async fn serve(
    flow: &FlowConfig,
    registry: &ComponentRegistry,
    options: ServeOptions,
) -> StatsSnapshot {
    tokio::time::timeout(
        Duration::from_secs(5),
        serve_with_shutdown_opts(
            flow,
            registry,
            Arc::new(InProcessBus::new(16)),
            std::future::pending(),
            options,
        ),
    )
    .await
    .unwrap()
    .unwrap()
}

fn assert_store_output(path: &Path) {
    let entries: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let object = entries.as_object().unwrap();
    assert!(
        object
            .iter()
            .any(|(key, value)| key.ends_with(":state:count")
                && value.get("total") == Some(&json!(2)))
    );
}

fn assert_bus_output(path: &Path) {
    let lines = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            json!({ "topic": "outbound", "payload": { "n": 1 } }),
            json!({ "topic": "outbound", "payload": { "n": 2 } })
        ]
    );
}

#[tokio::test]
async fn stdio_trigger_extension_drives_serve_flow() {
    assert_python3();
    let dir = write_extension("remote-trigger", "remote_trigger", TRIGGER_REGISTRATION);
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut registry = probe_registry(&records);
    let host = load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = FlowConfig::from_yaml_str(TRIGGER_FLOW).unwrap();
    let options = ServeOptions {
        extension_host: Some(Arc::new(host)),
        ..ServeOptions::default()
    };
    let stats = serve(&flow, &registry, options).await;
    assert_eq!(stats.processed, 2);
    assert_eq!(records.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn stdio_state_store_persists_executor_state() {
    assert_python3();
    let dir = write_extension("remote-store", "remote_store", STORE_REGISTRATION);
    let output = dir.join("state-store.json");
    let mut registry = probe_registry(&Arc::new(Mutex::new(Vec::new())));
    let host = load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = store_flow(&output);
    let store = host
        .build_state_store(flow.checkpoint.as_ref().unwrap().store.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let stats = run_flow_with_store(&flow, &registry, Some(store), false)
        .await
        .unwrap();
    assert_eq!(stats.processed, 2);
    assert_store_output(&output);
}

#[tokio::test]
async fn stdio_event_bus_publishes_queue_sink_payloads() {
    assert_python3();
    let dir = write_extension("remote-bus", "remote_bus", BUS_REGISTRATION);
    let output = dir.join("bus-output.jsonl");
    let mut registry = common::registry(Vec::new());
    let host = load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = bus_flow(&output);
    let shared_bus = host
        .build_event_bus(flow.event_bus.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let options = ServeOptions {
        shared_bus: Some(shared_bus),
        ..ServeOptions::default()
    };
    let stats = serve(&flow, &registry, options).await;
    assert_eq!(stats.processed, 2);
    assert_bus_output(&output);
}
