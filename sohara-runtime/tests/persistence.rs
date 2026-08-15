//! S4 integration tests: state steps, resume dedup, and approve recovery

mod common;

use std::sync::Arc;

use serde_json::json;
use sohara_persistence::MemoryStore;
use sohara_runtime::{approve_pending, run_flow_with_store};

use common::{registry, run_with_probe};

const STATE_FLOW: &str = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 2}, {n: 3}] } }
  - { id: count, kind: transform, type: state, config: { expr: { total: "state.total + 1" } }, state: { total: 0 } }
  - { id: out, kind: sink, type: probe }
edges: [[in, count], [count, out]]
"#;

#[tokio::test]
async fn state_step_accumulates_and_persists() {
    let (_stats, records) = run_with_probe(STATE_FLOW).await;
    assert_eq!(records.len(), 3);
}

#[tokio::test]
async fn state_persists_in_store() {
    let store = Arc::new(MemoryStore::new());
    let flow = sohara_config::FlowConfig::from_yaml_str(STATE_FLOW).unwrap();
    let registry = registry(vec![(
        "sink",
        "probe",
        common::probe_factory(Arc::new(std::sync::Mutex::new(Vec::new()))),
    )]);
    run_flow_with_store(&flow, &registry, Some(store.clone()), false)
        .await
        .unwrap();
    // Find the state entry keyed by run id.
    let entries = store.entries();
    assert!(entries.iter().any(|(key, value)| {
        key.ends_with(":state:count") && value.get("total") == Some(&json!(3))
    }));
}

#[tokio::test]
async fn resume_skips_already_delivered_records() {
    let store = Arc::new(MemoryStore::new());
    let flow = sohara_config::FlowConfig::from_yaml_str(STATE_FLOW).unwrap();
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = registry(vec![(
        "sink",
        "probe",
        common::probe_factory(records.clone()),
    )]);

    let first = run_flow_with_store(&flow, &registry, Some(store.clone()), false)
        .await
        .unwrap();
    assert_eq!(first.processed, 3);

    // Same records re-emitted with the same payloads: resume dedups them.
    let second = run_flow_with_store(&flow, &registry, Some(store.clone()), true)
        .await
        .unwrap();
    assert_eq!(second.processed, 0);
    assert_eq!(second.duplicates, 3);
    assert_eq!(records.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn approve_parks_then_resumes() {
    let store = Arc::new(MemoryStore::new());
    let yaml = r#"
name: approve-test
version: "1"
checkpoint: { store: state.json }
steps:
  - { id: in, kind: source, type: inline, config: { records: [{order: A}] } }
  - { id: gate, kind: control, type: approve, config: { title: "审批" } }
  - { id: out, kind: sink, type: probe }
edges: [[in, gate], [gate, out]]
"#;
    let flow = sohara_config::FlowConfig::from_yaml_str(yaml).unwrap();
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = registry(vec![(
        "sink",
        "probe",
        common::probe_factory(records.clone()),
    )]);

    let first = run_flow_with_store(&flow, &registry, Some(store.clone()), false)
        .await
        .unwrap();
    assert_eq!(first.waiting, 1);
    assert_eq!(first.processed, 0);

    let approved = approve_pending(&flow, &registry, store.clone(), None)
        .await
        .unwrap();
    assert_eq!(approved, 1);
    assert_eq!(records.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn approve_without_store_routes_through() {
    let yaml = r#"
name: approve-test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{order: A}] } }
  - { id: gate, kind: control, type: approve, config: { title: "审批" } }
  - { id: out, kind: sink, type: probe }
edges: [[in, gate], [gate, out]]
"#;
    let (_stats, records) = run_with_probe(yaml).await;
    assert_eq!(records.len(), 1);
}
