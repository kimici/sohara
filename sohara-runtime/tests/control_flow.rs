//! Control-flow integration tests: switch / foreach / loop / parallel / join / batch / delay

mod common;

use serde_json::json;

use common::{run_with, run_with_probe};

const SOURCE_THREE: &str = "
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 2}, {n: 3}] } }
";

#[tokio::test]
async fn switch_routes_by_cases() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 5}] } }
  - { id: route, kind: control, type: switch, config:
      { cases: [{ when: "n < 2", to: small }], default: big } }
  - { id: small, kind: transform, type: add_field, config: { field: bucket, value: small } }
  - { id: big, kind: transform, type: add_field, config: { field: bucket, value: big } }
  - { id: out, kind: sink, type: probe }
edges: [[in, route], [small, out], [big, out]]
"#;
    let (_stats, records) = run_with_probe(yaml).await;
    let buckets = records
        .iter()
        .map(|r| r.get("bucket").cloned())
        .collect::<Vec<_>>();
    assert_eq!(buckets, vec![Some(json!("small")), Some(json!("big"))]);
}

#[tokio::test]
async fn foreach_expands_array_items() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{items: [a, b, c]}] } }
  - { id: each, kind: control, type: foreach, config: { over: "$.items", as: item } }
  - { id: out, kind: sink, type: probe }
edges: [[in, each], [each, out]]
"#;
    let (stats, records) = run_with_probe(yaml).await;
    assert_eq!(stats.processed, 3);
    let items = records
        .iter()
        .map(|r| r.get("item").cloned())
        .collect::<Vec<_>>();
    assert_eq!(
        items,
        vec![Some(json!("a")), Some(json!("b")), Some(json!("c"))]
    );
}

#[tokio::test]
async fn loop_repeats_body_up_to_max_iterations() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}] } }
  - { id: repeat, kind: control, type: loop, config:
      { while: "true", max_iterations: 3, step: body } }
  - { id: body, kind: transform, type: add_field, config: { field: seen, value: true } }
  - { id: out, kind: sink, type: probe }
edges: [[in, repeat], [body, out]]
"#;
    let (stats, records) = run_with_probe(yaml).await;
    assert_eq!(stats.processed, 3);
    assert!(records.iter().all(|r| r.get("seen") == Some(&json!(true))));
}

#[tokio::test]
async fn parallel_branches_join_with_all_mode() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}] } }
  - { id: fan, kind: control, type: parallel, config: { branches: [left, right] } }
  - { id: left, kind: transform, type: add_field, config: { field: a, value: 1 } }
  - { id: right, kind: transform, type: add_field, config: { field: b, value: 2 } }
  - { id: gather, kind: control, type: join, config: { mode: all } }
  - { id: out, kind: sink, type: probe }
edges: [[in, fan], [left, gather], [right, gather], [gather, out]]
"#;
    let (stats, records) = run_with_probe(yaml).await;
    assert_eq!(stats.processed, 1);
    let merged = &records[0];
    assert_eq!(merged.get("a"), Some(&json!(1)));
    assert_eq!(merged.get("b"), Some(&json!(2)));
}

#[tokio::test]
async fn join_any_mode_is_a_union() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}] } }
  - { id: fan, kind: control, type: parallel, config: { branches: [left, right] } }
  - { id: left, kind: transform, type: add_field, config: { field: a, value: 1 } }
  - { id: right, kind: transform, type: add_field, config: { field: b, value: 2 } }
  - { id: gather, kind: control, type: join, config: { mode: any } }
  - { id: out, kind: sink, type: probe }
edges: [[in, fan], [left, gather], [right, gather], [gather, out]]
"#;
    let (stats, records) = run_with_probe(yaml).await;
    assert_eq!(stats.processed, 2);
    let has_a = records.iter().any(|r| r.get("a") == Some(&json!(1)));
    let has_b = records.iter().any(|r| r.get("b") == Some(&json!(2)));
    assert!(has_a && has_b);
}

#[tokio::test]
async fn batch_flushes_on_size_and_at_eof() {
    let yaml = format!(
        r#"
name: test
version: "1"
steps:
{SOURCE_THREE}
  - {{ id: buf, kind: transform, type: batch, config: {{ size: 2 }} }}
  - {{ id: out, kind: sink, type: probe }}
edges: [[in, buf], [buf, out]]
"#
    );
    let (stats, records) = run_with_probe(&yaml).await;
    assert_eq!(stats.processed, 2);
    let total_items: usize = records
        .iter()
        .map(|r| {
            r.get("items")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or(0)
        })
        .sum();
    assert_eq!(total_items, 3);
}

#[tokio::test]
async fn delay_passes_records_through() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 2}] } }
  - { id: wait, kind: control, type: delay, config: { duration: 1ms } }
  - { id: out, kind: sink, type: probe }
edges: [[in, wait], [wait, out]]
"#;
    let (stats, _records) = run_with_probe(yaml).await;
    assert_eq!(stats.processed, 2);
}

#[tokio::test]
async fn edge_when_filters_routing() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 5}] } }
  - { id: out, kind: sink, type: probe }
edges:
  - { from: in, to: out, when: "n > 2" }
"#;
    let (_stats, records) = run_with_probe(yaml).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].get("n"), Some(&json!(5)));
}

#[tokio::test]
async fn step_when_precondition_drops_records() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 5}] } }
  - { id: out, kind: sink, type: probe, when: "n > 2" }
edges: [[in, out]]
"#;
    let (_stats, records) = run_with_probe(yaml).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].get("n"), Some(&json!(5)));
}

#[tokio::test]
async fn route_to_unknown_node_fails_at_build() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}] } }
  - { id: route, kind: control, type: switch, config:
      { cases: [{ when: "true", to: ghost }], default: out } }
  - { id: out, kind: sink, type: probe }
edges: [[in, route]]
"#;
    let records = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = common::registry(vec![("sink", "probe", common::probe_factory(records))]);
    let error = run_with(yaml, &registry).await.unwrap_err().to_string();
    assert!(error.contains("ghost"), "got: {error}");
}
