//! Integration tests for `sohara.flow.run(...)`.

mod common;

use std::sync::{Arc, Mutex};

use serde_json::json;
use sohara_core::FlowRunner;
use sohara_runtime::RuntimeFlowRunner;
use sohara_runtime::run_flow;

use common::registry;

#[tokio::test]
async fn flow_run_executes_finite_subflow() {
    let dir = std::env::temp_dir().join(format!(
        "sohara-flow-run-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let output = dir.join("child.jsonl");
    let records = Arc::new(Mutex::new(Vec::new()));
    let registry = registry(vec![(
        "sink",
        "probe",
        common::probe_factory(records.clone()),
    )]);
    let flow = sohara_config::FlowConfig::from_yaml_str(&format!(
        r#"
name: parent-run
version: "1"
steps:
  - {{ id: in, kind: source, type: inline, config: {{ records: [{{ n: 1 }}] }} }}
  - {{ id: js, kind: transform, type: script, config: {{ inline: "function transform(record, ctx) {{ sohara.flow.run(sohara.flow('child').source('src', 'inline', {{ records: [{{ child: 42 }}] }}).sink('out', 'file', {{ path: '{}' , format: 'jsonl' }}).build()); return record; }}" }} }}
  - {{ id: out, kind: sink, type: probe }}
edges: [[in, js], [js, out]]
"#,
        output.display()
    ))
    .unwrap();
    let stats = run_flow(&flow, &registry).await.unwrap();
    assert_eq!(stats.processed, 1);
    let output = std::fs::read_to_string(output).unwrap();
    assert!(output.contains("\"child\":42"), "got: {output}");
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn flow_run_returns_stop_handle_for_triggered_subflow() {
    let dir = std::env::temp_dir().join(format!(
        "sohara-flow-serve-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let output = dir.join("ticks.jsonl");
    let registry = registry(Vec::new());
    let flow = sohara_config::FlowConfig::from_yaml_str(&format!(
        r#"
name: parent-serve
version: "1"
steps:
  - {{ id: in, kind: source, type: inline, config: {{ records: [{{ n: 1 }}] }} }}
  - {{ id: js, kind: transform, type: script, config: {{ inline: "function transform(record, ctx) {{ var handle = sohara.flow.run(sohara.flow('child-serve').trigger('tick', 'cron', {{ expression: '*/1 * * * * *' }}).sink('out', 'file', {{ path: '{}', format: 'jsonl' }}).edge('tick', 'out').build()); sohara.sleep(2200); handle.stop(); return record; }}" }} }}
  - {{ id: out, kind: sink, type: noop }}
edges: [[in, js], [js, out]]
"#,
        output.display()
    ))
    .unwrap();
    let stats = run_flow(&flow, &registry).await.unwrap();
    assert_eq!(stats.processed, 1);
    let output = std::fs::read_to_string(output).unwrap();
    assert!(!output.trim().is_empty(), "expected at least one cron tick");
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn runtime_flow_runner_accepts_triggered_flow_json() {
    let dir = std::env::temp_dir().join(format!(
        "sohara-flow-runner-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let output = dir.join("runner.jsonl");
    let runner = RuntimeFlowRunner::new(registry(Vec::new()), None, None);
    let handle = runner
        .run(json!({
            "name": "runner-child",
            "version": "1",
            "triggers": [{
                "id": "tick",
                "type": "cron",
                "config": { "expression": "*/1 * * * * *" }
            }],
            "steps": [{
                "id": "out",
                "kind": "sink",
                "type": "file",
                "config": { "path": output, "format": "jsonl" }
            }],
            "edges": [{ "from": "tick", "to": "out" }]
        }))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1500));
    runner.stop(&handle).unwrap();
    let output = std::fs::read_to_string(output).unwrap();
    assert!(!output.trim().is_empty(), "expected cron output");
    std::fs::remove_dir_all(dir).ok();
}
