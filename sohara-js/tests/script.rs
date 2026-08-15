//! Integration tests for the QuickJS script steps

use sohara_builtins::register_all;
use sohara_config::FlowConfig;
use sohara_core::ComponentRegistry;
use sohara_js::register_all as register_js;
use sohara_runtime::run_flow;

fn registry() -> ComponentRegistry {
    let mut registry = ComponentRegistry::new();
    register_all(&mut registry);
    register_js(&mut registry);
    registry
}

async fn run(yaml: &str) -> sohara_runtime::StatsSnapshot {
    let _ = tracing_subscriber::fmt().try_init();
    let flow = FlowConfig::from_yaml_str(yaml).expect("valid flow");
    match run_flow(&flow, &registry()).await {
        Ok(stats) => stats,
        Err(error) => panic!("run failed: {error}"),
    }
}

#[tokio::test]
async fn script_transform_enriches_records() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{name: " alice "}] } }
  - { id: norm, kind: transform, type: script, config:
      { inline: "function transform(record, ctx) { record.name = record.name.trim().toUpperCase(); return record; }" } }
  - { id: out, kind: sink, type: file, config: { path: OUT, format: jsonl } }
"#
    .replace("OUT", &json_path("js-transform"));
    let stats = run(&yaml).await;
    assert_eq!(stats.processed, 1);
    let output = std::fs::read_to_string(json_path("js-transform")).unwrap();
    assert!(output.contains("ALICE"), "got: {output}");
    std::fs::remove_file(json_path("js-transform")).ok();
}

#[tokio::test]
async fn script_transform_can_filter() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 5}] } }
  - { id: gate, kind: transform, type: script, config:
      { inline: "function transform(record, ctx) { if (record.n < 3) { return null; } return record; }" } }
  - { id: out, kind: sink, type: file, config: { path: OUT, format: jsonl } }
"#
    .replace("OUT", &json_path("js-filter"));
    let stats = run(&yaml).await;
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.filtered, 1);
    std::fs::remove_file(json_path("js-filter")).ok();
}

#[tokio::test]
async fn script_source_generates_records() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: gen, kind: source, type: script, config:
      { inline: "function generate(ctx) { var out = []; for (var i = 0; i < 3; i++) { out.push({ n: i }); } return out; }" } }
  - { id: out, kind: sink, type: file, config: { path: OUT, format: jsonl } }
"#
    .replace("OUT", &json_path("js-generate"));
    let stats = run(&yaml).await;
    assert_eq!(stats.processed, 3);
    std::fs::remove_file(json_path("js-generate")).ok();
}

#[tokio::test]
async fn script_sink_consumes_records() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 2}] } }
  - { id: out, kind: sink, type: script, config:
      { inline: "function consume(record, ctx) { sohara.log('info', 'consumed ' + record.n); }" } }
"#;
    let stats = run(yaml).await;
    assert_eq!(stats.processed, 2);
}

#[tokio::test]
async fn script_uses_host_bridge() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{name: alice}] } }
  - { id: tag, kind: transform, type: script, config:
      { inline: "function transform(record, ctx) { record.stamp = sohara.now(); record.id2 = sohara.uuid(); return record; }" } }
  - { id: out, kind: sink, type: file, config: { path: OUT, format: jsonl } }
"#
    .replace("OUT", &json_path("js-bridge"));
    let stats = run(&yaml).await;
    assert_eq!(stats.processed, 1);
    let output = std::fs::read_to_string(json_path("js-bridge")).unwrap();
    assert!(output.contains("stamp"), "got: {output}");
    assert!(output.contains("id2"), "got: {output}");
    std::fs::remove_file(json_path("js-bridge")).ok();
}

#[tokio::test]
async fn script_throwing_error_fails_the_step() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}] } }
  - { id: boom, kind: transform, type: script, config:
      { inline: "function transform(record, ctx) { sohara.fail('boom'); }" }, on_error: continue }
  - { id: out, kind: sink, type: file, config: { path: OUT, format: jsonl } }
"#
    .replace("OUT", &json_path("js-fail"));
    let stats = run(&yaml).await;
    assert_eq!(stats.errors, 1);
    assert_eq!(stats.processed, 0);
    std::fs::remove_file(json_path("js-fail")).ok();
}

fn json_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("sohara-js-{:?}", std::process::id()));
    std::fs::create_dir_all(&dir).ok();
    dir.join(format!("{name}.jsonl"))
        .to_string_lossy()
        .into_owned()
}
