//! Integration tests for subprocess stdio extensions.

mod common;

use futures::StreamExt;
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use sohara_config::FlowConfig;
use sohara_core::Error;
use sohara_runtime::{
    load_stdio_extensions, load_stdio_extensions_with_trusted, run_flow, run_flow_with_store,
    run_flow_with_store_report, serve_with_shutdown_opts, ServeOptions,
};
use sohara_triggers::InProcessBus;

#[tokio::test]
async fn stdio_transform_extension_processes_records() {
    assert_python3();
    let dir = write_extension(
        "uppercase",
        "uppercase",
        r#"
steps:
  - kind: transform
    type: uppercase
"#,
    );

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

    let flow = FlowConfig::from_yaml_str(
        r#"
name: ext-transform
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{ name: Alice }, { name: "" }] } }
  - { id: up, kind: transform, type: uppercase, config: { field: name } }
  - { id: out, kind: sink, type: probe }
"#,
    )
    .unwrap();

    let stats = run_flow(&flow, &registry).await.unwrap();
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.filtered, 1);

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].get("name").unwrap().as_str(), Some("ALICE"));
}

#[tokio::test]
async fn stdio_source_extension_emits_records() {
    assert_python3();
    let dir = write_extension(
        "remote-source",
        "remote_source",
        r#"
steps:
  - kind: source
    type: remote_source
"#,
    );

    let records = Arc::new(Mutex::new(Vec::new()));
    let mut registry = common::registry(vec![(
        "sink",
        "probe",
        common::probe_factory(records.clone()),
    )]);
    load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = FlowConfig::from_yaml_str(
        r#"
name: ext-source
version: "1"
steps:
  - { id: in, kind: source, type: remote_source, config: {} }
  - { id: out, kind: sink, type: probe }
"#,
    )
    .unwrap();

    let stats = run_flow(&flow, &registry).await.unwrap();
    assert_eq!(stats.processed, 2);

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
    let dir = write_extension(
        "remote-sink",
        "remote_sink",
        r#"
steps:
  - kind: sink
    type: remote_sink
"#,
    );
    let output = dir.join("sink-output.jsonl");

    let mut registry = common::registry(Vec::new());
    load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = FlowConfig::from_yaml_str(&format!(
        r#"
name: ext-sink
version: "1"
steps:
  - {{ id: in, kind: source, type: inline, config: {{ records: [{{ n: 1 }}, {{ n: 2 }}] }} }}
  - {{ id: out, kind: sink, type: remote_sink, config: {{ path: "{}" }} }}
"#,
        output.display()
    ))
    .unwrap();

    let stats = run_flow(&flow, &registry).await.unwrap();
    assert_eq!(stats.processed, 2);

    let lines = std::fs::read_to_string(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lines, vec![json!({ "n": 1 }), json!({ "n": 2 })]);
}

#[tokio::test]
async fn stdio_trigger_extension_drives_serve_flow() {
    assert_python3();
    let dir = write_extension(
        "remote-trigger",
        "remote_trigger",
        r#"
triggers:
  - type: remote_trigger
"#,
    );

    let records = Arc::new(Mutex::new(Vec::new()));
    let mut registry = common::registry(vec![(
        "sink",
        "probe",
        common::probe_factory(records.clone()),
    )]);
    let host = load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = FlowConfig::from_yaml_str(
        r#"
name: ext-trigger
version: "1"
triggers:
  - { id: remote, type: remote_trigger, config: {} }
steps:
  - { id: out, kind: sink, type: probe }
edges: [[remote, out]]
"#,
    )
    .unwrap();

    let stats = tokio::time::timeout(
        Duration::from_secs(5),
        serve_with_shutdown_opts(
            &flow,
            &registry,
            Arc::new(InProcessBus::new(16)),
            std::future::pending(),
            ServeOptions {
                extension_host: Some(Arc::new(host)),
                ..ServeOptions::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(stats.processed, 2);
    assert_eq!(records.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn stdio_state_store_persists_executor_state() {
    assert_python3();
    let dir = write_extension(
        "remote-store",
        "remote_store",
        r#"
state_stores:
  - type: remote_store
"#,
    );
    let output = dir.join("state-store.json");

    let mut registry = common::registry(vec![(
        "sink",
        "probe",
        common::probe_factory(Arc::new(Mutex::new(Vec::new()))),
    )]);
    let host = load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = FlowConfig::from_yaml_str(&format!(
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
        output.display()
    ))
    .unwrap();

    let store = host
        .build_state_store(flow.checkpoint.as_ref().unwrap().store.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let stats = run_flow_with_store(&flow, &registry, Some(store), false)
        .await
        .unwrap();
    assert_eq!(stats.processed, 2);

    let entries: Value = serde_json::from_str(&std::fs::read_to_string(output).unwrap()).unwrap();
    let object = entries.as_object().unwrap();
    assert!(
        object
            .iter()
            .any(|(key, value)| key.ends_with(":state:count")
                && value.get("total") == Some(&json!(2)))
    );
}

#[tokio::test]
async fn stdio_event_bus_publishes_queue_sink_payloads() {
    assert_python3();
    let dir = write_extension(
        "remote-bus",
        "remote_bus",
        r#"
event_buses:
  - type: remote_bus
"#,
    );
    let output = dir.join("bus-output.jsonl");

    let mut registry = common::registry(Vec::new());
    let host = load_stdio_extensions(&mut registry, std::slice::from_ref(&dir)).unwrap();

    let flow = FlowConfig::from_yaml_str(&format!(
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
        output.display()
    ))
    .unwrap();

    let shared_bus = host
        .build_event_bus(flow.event_bus.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let stats = tokio::time::timeout(
        Duration::from_secs(5),
        serve_with_shutdown_opts(
            &flow,
            &registry,
            Arc::new(InProcessBus::new(16)),
            std::future::pending(),
            ServeOptions {
                shared_bus: Some(shared_bus),
                ..ServeOptions::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(stats.processed, 2);

    let lines = std::fs::read_to_string(output)
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
    let mut registry = common::registry(vec![(
        "sink",
        "probe",
        common::probe_factory(Arc::new(Mutex::new(Vec::new()))),
    )]);
    let host = load_stdio_extensions_with_trusted(&mut registry, &[builtin_dir()], &[]).unwrap();
    assert!(host
        .loaded()
        .iter()
        .flat_map(|extension| extension.registrations.iter())
        .any(|registration| registration == "state_store:builtin-sqlite-store"));

    let output = temp_dir("builtin-sqlite").join("builtin-sqlite.db");
    let flow = FlowConfig::from_yaml_str(&format!(
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
        output.display()
    ))
    .unwrap();

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
    let flow = FlowConfig::from_yaml_str(&format!(
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
        output.display(),
        output.display()
    ))
    .unwrap();
    let shared_bus = host
        .build_event_bus(flow.event_bus.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let trigger = host.build_trigger(&flow.triggers[0]).unwrap().unwrap();
    trigger.start().await.unwrap();
    let mut stream = trigger.stream().await.unwrap();

    shared_bus.publish("outbound", json!({ "n": 1 })).unwrap();
    shared_bus.publish("outbound", json!({ "n": 2 })).unwrap();

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
    let flow = FlowConfig::from_yaml_str(&format!(
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
    .unwrap();
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
    trigger.stop().await.unwrap();

    assert_eq!(first.get("n"), Some(&json!(1)));
    assert_eq!(second.get("n"), Some(&json!(2)));
}

fn write_extension(tag: &str, name: &str, registrations_yaml: &str) -> std::path::PathBuf {
    let dir = temp_dir(tag);
    let script = dir.join("plugin.py");
    let manifest = dir.join("extension.yaml");
    std::fs::write(&script, plugin_script()).unwrap();
    std::fs::write(
        &manifest,
        format!(
            r#"
name: {name}
version: "0.1.0"
command: python3
args:
  - "{}"
{registrations_yaml}
"#,
            script.display()
        ),
    )
    .unwrap();
    dir
}

fn assert_python3() {
    let status = std::process::Command::new("python3")
        .arg("--version")
        .status()
        .unwrap_or_else(|error| panic!("python3 is required for stdio extension tests: {error}"));
    assert!(status.success(), "python3 --version failed");
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("sohara-runtime-{tag}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn builtin_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../extensions/builtin")
}

fn ensure_builtin_binaries() {
    static BUILTINS: OnceLock<()> = OnceLock::new();
    BUILTINS.get_or_init(|| {
        let status = std::process::Command::new("cargo")
            .args(["build", "-q", "-p", "sohara-builtin-extensions", "--bins"])
            .current_dir(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".."))
            .status()
            .unwrap();
        assert!(
            status.success(),
            "failed to build builtin extension binaries"
        );
    });
}

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn plugin_script() -> &'static str {
    r#"import json
import os
import sys

source_done = False
trigger_started = False
trigger_done = False

def read_store(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)

def write_store(path, data):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle)

for line in sys.stdin:
    request = json.loads(line)
    method = request["method"]
    req_id = request["id"]
    params = request.get("params", {})
    if method == "initialize":
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocol": "sohara.stdio/v1",
                "capabilities": {
                    "source": True,
                    "transform": True,
                    "sink": True,
                    "trigger": True,
                    "state_store": True,
                    "event_bus": True,
                },
            },
        }
    elif method == "source.pull":
        if source_done:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"records": [], "done": True},
            }
        else:
            source_done = True
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "records": [
                        {
                            "id": "remote-1",
                            "timestamp": "2026-01-01T00:00:00Z",
                            "payload": {"name": "remote-a"},
                            "metadata": {},
                        },
                        {
                            "id": "remote-2",
                            "timestamp": "2026-01-01T00:00:01Z",
                            "payload": {"name": "remote-b"},
                            "metadata": {},
                        },
                    ],
                    "done": True,
                },
            }
    elif method == "transform":
        record = params["record"]
        payload = record["payload"]
        field = params["config"].get("field", "name")
        value = payload.get(field, "")
        if not value:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"outcome": "filtered"},
            }
        else:
            payload[field] = str(value).upper()
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"outcome": "pass", "record": record},
            }
    elif method == "sink.send":
        path = params["config"]["path"]
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(params["record"]["payload"]) + "\n")
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "sink.flush":
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "trigger.start":
        trigger_started = True
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "trigger.pull":
        if not trigger_started or trigger_done:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"records": [], "done": True},
            }
        else:
            trigger_done = True
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "records": [
                        {
                            "id": "trigger-1",
                            "timestamp": "2026-01-01T00:00:02Z",
                            "payload": {"tick": 1},
                            "metadata": {},
                        },
                        {
                            "id": "trigger-2",
                            "timestamp": "2026-01-01T00:00:03Z",
                            "payload": {"tick": 2},
                            "metadata": {},
                        },
                    ],
                    "done": True,
                },
            }
    elif method == "trigger.stop":
        trigger_done = True
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "state.load":
        path = params["config"]["path"]
        data = read_store(path)
        key = params["key"]
        if key in data:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"found": True, "value": data[key]},
            }
        else:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"found": False, "value": None},
            }
    elif method == "state.save":
        path = params["config"]["path"]
        data = read_store(path)
        data[params["key"]] = params["value"]
        write_store(path, data)
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "state.delete":
        path = params["config"]["path"]
        data = read_store(path)
        data.pop(params["key"], None)
        write_store(path, data)
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "bus.publish":
        path = params["config"]["path"]
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {"topic": params["topic"], "payload": params["payload"]}
                )
                + "\n"
            )
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    else:
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"unknown method: {method}"},
        }
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()
"#
}
