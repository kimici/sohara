//! Integration tests for subprocess stdio extensions (data steps).

mod common;

use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use sohara_config::FlowConfig;
use sohara_core::Record;
use sohara_runtime::{load_stdio_extensions, run_flow};

use common::stdio::{assert_python3, write_extension};

const UPPERCASE_REGISTRATION: &str = r#"
steps:
  - kind: transform
    type: uppercase
"#;

const TRANSFORM_FLOW: &str = r#"
name: ext-transform
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{ name: Alice }, { name: "" }] } }
  - { id: up, kind: transform, type: uppercase, config: { field: name } }
  - { id: out, kind: sink, type: probe }
"#;

const SOURCE_REGISTRATION: &str = r#"
steps:
  - kind: source
    type: remote_source
"#;

const SOURCE_FLOW: &str = r#"
name: ext-source
version: "1"
steps:
  - { id: in, kind: source, type: remote_source, config: {} }
  - { id: out, kind: sink, type: probe }
"#;

const SINK_REGISTRATION: &str = r#"
steps:
  - kind: sink
    type: remote_sink
"#;

fn sink_flow(path: &Path) -> FlowConfig {
    FlowConfig::from_yaml_str(&format!(
        r#"
name: ext-sink
version: "1"
steps:
  - {{ id: in, kind: source, type: inline, config: {{ records: [{{ n: 1 }}, {{ n: 2 }}] }} }}
  - {{ id: out, kind: sink, type: remote_sink, config: {{ path: "{}" }} }}
"#,
        path.display()
    ))
    .unwrap()
}

fn read_jsonl(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect()
}

#[tokio::test]
async fn stdio_transform_extension_processes_records() {
    assert_python3();
    let dir = write_extension("uppercase", "uppercase", UPPERCASE_REGISTRATION);
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut registry = common::registry(vec![(
        "sink",
        "probe",
        common::probe_factory(records.clone()),
    )]);
    let host = load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();
    assert_eq!(host.loaded().len(), 1);
    assert_eq!(host.loaded()[0].name, "uppercase");
    assert_eq!(host.loaded()[0].registrations, vec!["transform:uppercase"]);

    let flow = FlowConfig::from_yaml_str(TRANSFORM_FLOW).unwrap();
    let stats = run_flow(&flow, &registry).await.unwrap();
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.filtered, 1);
    assert_transform_output(&records);
}

fn assert_transform_output(records: &Arc<Mutex<Vec<Record>>>) {
    let records = records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].get("name").unwrap().as_str(), Some("ALICE"));
}

#[tokio::test]
async fn stdio_source_extension_emits_records() {
    assert_python3();
    let dir = write_extension("remote-source", "remote_source", SOURCE_REGISTRATION);
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut registry = common::registry(vec![(
        "sink",
        "probe",
        common::probe_factory(records.clone()),
    )]);
    load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = FlowConfig::from_yaml_str(SOURCE_FLOW).unwrap();
    let stats = run_flow(&flow, &registry).await.unwrap();
    assert_eq!(stats.processed, 2);
    assert_source_output(&records);
}

fn assert_source_output(records: &Arc<Mutex<Vec<Record>>>) {
    let records = records.lock().unwrap();
    let names = records
        .iter()
        .map(|record| record.get("name").unwrap().as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["remote-a", "remote-b"]);
}

#[tokio::test]
async fn stdio_sink_extension_receives_records() {
    assert_python3();
    let dir = write_extension("remote-sink", "remote_sink", SINK_REGISTRATION);
    let output = dir.join("sink-output.jsonl");
    let mut registry = common::registry(Vec::new());
    load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let stats = run_flow(&sink_flow(&output), &registry).await.unwrap();
    assert_eq!(stats.processed, 2);
    assert_eq!(
        read_jsonl(&output),
        vec![json!({ "n": 1 }), json!({ "n": 2 })]
    );
}
