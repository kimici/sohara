//! Integration tests for built-in steps: build from YAML config and run

use sohara_builtins::register_all;
use sohara_config::{build_flow, FlowConfig};
use sohara_core::{ComponentRegistry, Pipeline};

fn build(yaml: &str) -> sohara_core::Result<sohara_config::BuiltFlow> {
    let flow = FlowConfig::from_yaml_str(yaml).expect("flow must be valid");
    let mut registry = ComponentRegistry::new();
    register_all(&mut registry);
    build_flow(&flow, &registry)
}

#[tokio::test]
async fn inline_filter_map_to_jsonl_file() {
    let dir = unique_dir();
    let yaml = format!(
        r#"
name: test
version: "1"
steps:
  - {{ id: in, kind: source, type: inline, config:
      {{ records: [ {{ name: Alice, age: "30" }}, {{ name: Bob, age: "15" }} ] }} }}
  - {{ id: adult, kind: transform, type: filter, config: {{ where: "age >= 18" }} }}
  - {{ id: tag, kind: transform, type: map, config: {{ expr: {{ ok: "true" }} }} }}
  - {{ id: out, kind: sink, type: file, config: {{ path: "{dir}/out.jsonl", format: jsonl }} }}
"#
    );
    let built = build(&yaml).unwrap();
    let pipeline = Pipeline::new("test");
    let sink = sohara_builtins::FanoutSink::new(built.sinks);
    let stats = pipeline
        .run(&built.source, &built.transforms, &sink)
        .await
        .unwrap();

    assert_eq!(stats.processed, 1);
    assert_eq!(stats.filtered, 1);
    assert_eq!(stats.errors, 0);
    let output = std::fs::read_to_string(format!("{dir}/out.jsonl")).unwrap();
    assert!(output.contains("Alice"), "got: {output}");
    assert!(!output.contains("Bob"), "got: {output}");
    std::fs::remove_file(format!("{dir}/out.jsonl")).ok();
}

#[tokio::test]
async fn assert_on_fail_filter_drops_records() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config:
      { records: [ { name: Alice, age: "30" }, { name: Bob, age: "15" } ] } }
  - { id: check, kind: transform, type: assert, config:
      { expect: [ { field: age, op: gte, value: 18 } ], on_fail: filter } }
  - { id: out, kind: sink, type: collect }
"#;
    let built = build(yaml).unwrap();
    let pipeline = Pipeline::new("test");
    let sink = sohara_builtins::FanoutSink::new(built.sinks);
    let stats = pipeline
        .run(&built.source, &built.transforms, &sink)
        .await
        .unwrap();

    assert_eq!(stats.processed, 1);
    assert_eq!(stats.filtered, 1);
    assert_eq!(stats.errors, 0);
}

#[tokio::test]
async fn csv_source_roundtrips_through_csv_sink() {
    let dir = unique_dir();
    let input = format!("{dir}/in.csv");
    let output = format!("{dir}/out.csv");
    std::fs::write(&input, "name,age\nAlice,30\nBob,15\n").unwrap();
    let yaml = format!(
        r#"
name: test
version: "1"
steps:
  - {{ id: in, kind: source, type: file, config: {{ path: "{input}", format: csv }} }}
  - {{ id: out, kind: sink, type: file, config: {{ path: "{output}", format: csv }} }}
"#
    );
    let built = build(&yaml).unwrap();
    let pipeline = Pipeline::new("test");
    let sink = sohara_builtins::FanoutSink::new(built.sinks);
    let stats = pipeline
        .run(&built.source, &built.transforms, &sink)
        .await
        .unwrap();

    assert_eq!(stats.processed, 2);
    let written = std::fs::read_to_string(&output).unwrap();
    assert!(written.contains("Alice,30"), "got: {written}");
    std::fs::remove_file(&input).ok();
    std::fs::remove_file(&output).ok();
}

#[test]
fn bad_expression_fails_at_build() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}] } }
  - { id: adult, kind: transform, type: filter, config: { where: "n >" } }
  - { id: out, kind: sink, type: log }
"#;
    let error = build(yaml)
        .err()
        .expect("expected a build error")
        .to_string();
    assert!(error.contains("filter 'where'"), "got: {error}");
    assert!(error.contains("'adult'"), "got: {error}");
}

#[test]
fn unknown_step_type_fails_at_build() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: kafka, config: {} }
  - { id: out, kind: sink, type: log }
"#;
    let error = build(yaml)
        .err()
        .expect("expected a build error")
        .to_string();
    assert!(error.contains("unknown step"), "got: {error}");
    assert!(error.contains("kafka"), "got: {error}");
}

#[test]
fn assert_without_value_fails_at_build() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}] } }
  - { id: check, kind: transform, type: assert, config: { expect: [{ field: n, op: gte }] } }
  - { id: out, kind: sink, type: log }
"#;
    let error = build(yaml)
        .err()
        .expect("expected a build error")
        .to_string();
    assert!(error.contains("needs a 'value'"), "got: {error}");
}

#[test]
fn unknown_config_field_fails_at_build() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { recods: [{n: 1}] } }
  - { id: out, kind: sink, type: log }
"#;
    let error = build(yaml)
        .err()
        .expect("expected a build error")
        .to_string();
    assert!(error.contains("recods"), "got: {error}");
}

fn unique_dir() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("sohara-builtins-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_string_lossy().into_owned()
}
