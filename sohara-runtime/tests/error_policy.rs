//! Error-policy integration tests: retry / continue / fail

mod common;

use common::{flaky_factory, registry, run_with};

const FLOW: &str = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}] } }
  - { id: flaky, kind: transform, type: flaky, on_error: {policy}, retry: { max: 2, backoff: 1ms } }
  - { id: out, kind: sink, type: probe }
edges: [[in, flaky], [flaky, out]]
"#;

#[tokio::test]
async fn retry_recovers_when_transform_heals() {
    let yaml = FLOW.replace("{policy}", "retry");
    let records = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = registry(vec![
        ("transform", "flaky", flaky_factory(2)),
        ("sink", "probe", common::probe_factory(records.clone())),
    ]);
    let stats = run_with(&yaml, &registry).await.unwrap();
    assert_eq!(stats.errors, 0);
    assert_eq!(stats.processed, 1);
}

#[tokio::test]
async fn continue_drops_record_and_keeps_running() {
    let yaml = FLOW.replace("{policy}", "continue");
    let records = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = registry(vec![
        ("transform", "flaky", flaky_factory(1)),
        ("sink", "probe", common::probe_factory(records)),
    ]);
    let stats = run_with(&yaml, &registry).await.unwrap();
    assert_eq!(stats.errors, 1);
    assert_eq!(stats.processed, 0);
}

#[tokio::test]
async fn fail_aborts_the_run() {
    let yaml = FLOW.replace("{policy}", "fail");
    let records = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = registry(vec![
        ("transform", "flaky", flaky_factory(1)),
        ("sink", "probe", common::probe_factory(records)),
    ]);
    let error = run_with(&yaml, &registry).await.unwrap_err();
    assert!(error.to_string().contains("flaky"), "got: {error}");
}

#[tokio::test]
async fn retry_exhausted_aborts_the_run() {
    let yaml = FLOW.replace("{policy}", "retry");
    let records = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = registry(vec![
        ("transform", "flaky", flaky_factory(5)),
        ("sink", "probe", common::probe_factory(records)),
    ]);
    let error = run_with(&yaml, &registry).await.unwrap_err();
    assert!(error.to_string().contains("flaky"), "got: {error}");
}
