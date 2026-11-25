//! Integration tests for trusted builtin stdio extensions.

mod common;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::json;
use sohara_config::FlowConfig;
use sohara_core::{ComponentRegistry, Error, Record, Result};
use sohara_runtime::{
    load_stdio_extensions, load_stdio_extensions_with_trusted, run_flow_with_store_report,
    StdioExtensionHost,
};

use common::stdio::{builtin_dir, ensure_builtin_binaries, free_tcp_port, temp_dir};

fn probe_registry() -> ComponentRegistry {
    common::registry(vec![(
        "sink",
        "probe",
        common::probe_factory(Arc::new(Mutex::new(Vec::new()))),
    )])
}

fn sqlite_store_flow(path: &Path) -> FlowConfig {
    FlowConfig::from_yaml_str(&format!(
        r#"
name: builtin-sqlite
version: "1"
checkpoint:
  every: 1
  store:
    type: builtin-sqlite-store
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

fn sqlite_bus_flow(path: &Path) -> FlowConfig {
    FlowConfig::from_yaml_str(&format!(
        r#"
name: builtin-sqlite-bus
version: "1"
event_bus:
  type: builtin-sqlite-bus
  path: "{}"
triggers:
  - id: bus
    type: builtin-sqlite-trigger
    config:
      path: "{}"
      topic: outbound
steps:
  - {{ id: noop, kind: sink, type: noop }}
"#,
        path.display(),
        path.display()
    ))
    .unwrap()
}

fn zeromq_bus_flow(endpoint: &str) -> FlowConfig {
    FlowConfig::from_yaml_str(&format!(
        r#"
name: builtin-zeromq-bus
version: "1"
event_bus:
  type: builtin-zeromq-bus
  endpoint: "{}"
  mode: connect
triggers:
  - id: bus
    type: builtin-zeromq-trigger
    config:
      endpoint: "{}"
      mode: bind
      topic: outbound
      timeout_ms: 100
steps:
  - {{ id: noop, kind: sink, type: noop }}
"#,
        endpoint, endpoint
    ))
    .unwrap()
}

async fn recv_two(stream: &mut BoxStream<'static, Result<Record>>) -> (Record, Record) {
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    (first, second)
}

fn assert_sqlite_store_registered(host: &StdioExtensionHost) {
    assert!(host
        .loaded()
        .iter()
        .flat_map(|extension| extension.registrations.iter())
        .any(|registration| registration == "state_store:builtin-sqlite-store"));
}

#[test]
fn duplicate_stdio_extension_registration_fails() {
    let dir = temp_dir("duplicate");
    let manifest = dir.join("duplicate.yaml");
    std::fs::write(
        &manifest,
        r#"
name: duplicate
version: "0.1.0"
command: python3
steps:
  - kind: transform
    type: map
"#,
    )
    .unwrap();

    let mut registry = common::registry(Vec::new());
    let error = load_stdio_extensions(&mut registry, &[manifest])
        .err()
        .unwrap_or_else(|| panic!("expected duplicate registration error"));
    match error {
        Error::Config(message) => assert!(message.contains("duplicates existing registration")),
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn untrusted_extensions_cannot_claim_builtin_prefix() {
    let dir = temp_dir("builtin-prefix");
    let manifest = dir.join("builtin-prefix.yaml");
    std::fs::write(
        &manifest,
        r#"
name: third-party
version: "0.1.0"
command: python3
steps:
  - kind: transform
    type: builtin-evil
"#,
    )
    .unwrap();

    let mut registry = common::registry(Vec::new());
    let error = load_stdio_extensions(&mut registry, &[manifest]).unwrap_err();
    match error {
        Error::Config(message) => assert!(message.contains("reserved for Sohara builtin")),
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn trusted_builtin_sqlite_store_persists_state() {
    ensure_builtin_binaries();
    let mut registry = probe_registry();
    let host = load_stdio_extensions_with_trusted(&mut registry, &[builtin_dir()], &[]).unwrap();
    assert_sqlite_store_registered(&host);

    let output = temp_dir("builtin-sqlite").join("builtin-sqlite.db");
    let flow = sqlite_store_flow(&output);
    let store = host
        .build_state_store(flow.checkpoint.as_ref().unwrap().store.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let report = run_flow_with_store_report(&flow, &registry, Some(store.clone()), false)
        .await
        .unwrap();
    let key = format!("{}:state:count", report.run_id);
    assert_eq!(store.load(&key).unwrap(), Some(json!({ "total": 2 })));
}

#[tokio::test]
async fn trusted_builtin_sqlite_bus_pairs_with_builtin_trigger() {
    ensure_builtin_binaries();
    let mut registry = common::registry(Vec::new());
    let host = load_stdio_extensions_with_trusted(&mut registry, &[builtin_dir()], &[]).unwrap();
    let output = temp_dir("builtin-sqlite-bus").join("builtin-sqlite-bus.db");
    let flow = sqlite_bus_flow(&output);
    let shared_bus = host
        .build_event_bus(flow.event_bus.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let trigger = host.build_trigger(&flow.triggers[0]).unwrap().unwrap();
    trigger.start().await.unwrap();
    let mut stream = trigger.stream().await.unwrap();

    shared_bus.publish("outbound", json!({ "n": 1 })).unwrap();
    shared_bus.publish("outbound", json!({ "n": 2 })).unwrap();
    let (first, second) = recv_two(&mut stream).await;
    trigger.stop().await.unwrap();

    assert_eq!(first.get("n"), Some(&json!(1)));
    assert_eq!(second.get("n"), Some(&json!(2)));
}

#[tokio::test]
async fn trusted_builtin_zeromq_bus_pairs_with_builtin_trigger() {
    ensure_builtin_binaries();
    let mut registry = common::registry(Vec::new());
    let host = load_stdio_extensions_with_trusted(&mut registry, &[builtin_dir()], &[]).unwrap();
    let endpoint = format!("tcp://127.0.0.1:{}", free_tcp_port());
    let flow = zeromq_bus_flow(&endpoint);
    let shared_bus = host
        .build_event_bus(flow.event_bus.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let trigger = host.build_trigger(&flow.triggers[0]).unwrap().unwrap();
    trigger.start().await.unwrap();
    let mut stream = trigger.stream().await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    shared_bus.publish("outbound", json!({ "n": 1 })).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    shared_bus.publish("outbound", json!({ "n": 2 })).unwrap();
    let (first, second) = recv_two(&mut stream).await;
    trigger.stop().await.unwrap();

    assert_eq!(first.get("n"), Some(&json!(1)));
    assert_eq!(second.get("n"), Some(&json!(2)));
}
