//! Integration tests for flow config loading and validation

use serde_json::json;
use sohara_config::FlowConfig;

const BASIC: &str = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: file, config: { path: a.csv, format: csv } }
  - { id: adult, kind: transform, type: filter, config: { where: "age >= 18" } }
  - { id: out, kind: sink, type: log }
"#;

#[test]
fn loads_linear_flow() {
    assert!(FlowConfig::from_yaml_str(BASIC).is_ok());
}

#[test]
fn flat_shorthand_equals_config_block() {
    let flat = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: out, kind: sink, type: file, path: out.jsonl, format: jsonl }
"#;
    let flow = FlowConfig::from_yaml_str(flat).unwrap();
    let sink = flow.steps.last().unwrap();
    assert_eq!(sink.config().unwrap().get("format"), Some(&json!("jsonl")));
    assert_eq!(
        sink.config().unwrap().get("path"),
        Some(&json!("out.jsonl"))
    );
}

#[test]
fn unknown_top_level_field_is_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: out, kind: sink, type: file, config: { format: jsonl, path: out.jsonl }, formt: jsonl }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("unknown field 'formt'"), "got: {error}");
    assert!(error.contains("'out'"), "got: {error}");
}

#[test]
fn reserved_field_is_rejected_with_stage_hint() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: out, kind: sink, type: log, timeout: 30s }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("later stage"), "got: {error}");
    assert!(error.contains("timeout"), "got: {error}");
}

#[test]
fn multiple_sources_are_allowed() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: a, kind: source, type: inline, records: [{n: 1}] }
  - { id: b, kind: source, type: inline, records: [{n: 2}] }
  - { id: out, kind: sink, type: log }
edges: [[a, out], [b, out]]
"#;
    assert!(FlowConfig::from_yaml_str(yaml).is_ok());
}

#[test]
fn control_steps_are_allowed() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: branch, kind: control, type: switch, config: { cases: [{ when: "n > 0", to: out }], default: out } }
  - { id: out, kind: sink, type: log }
edges: [[in, branch]]
"#;
    assert!(FlowConfig::from_yaml_str(yaml).is_ok());
}

#[test]
fn step_order_is_free_in_a_dag() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: out, kind: sink, type: log }
  - { id: late, kind: transform, type: filter, config: { where: "n > 0" } }
edges: [[in, out], [in, late]]
"#;
    assert!(FlowConfig::from_yaml_str(yaml).is_ok());
}

#[test]
fn cycle_is_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: adult, kind: transform, type: filter, config: { where: "n > 0" } }
  - { id: out, kind: sink, type: log }
edges: [[in, adult], [adult, out], [out, adult]]
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("cycle"), "got: {error}");
}

#[test]
fn unknown_edge_endpoint_is_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: out, kind: sink, type: log }
edges: [[in, missing]]
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("unknown step 'missing'"), "got: {error}");
}

#[test]
fn invalid_step_when_expression_is_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: out, kind: sink, type: log, when: "n >" }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("invalid 'when'"), "got: {error}");
}

#[test]
fn invalid_edge_when_expression_is_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: out, kind: sink, type: log }
edges:
  - { from: in, to: out, when: "n >" }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("invalid 'when'"), "got: {error}");
}

#[test]
fn version_mismatch_is_rejected() {
    let yaml = r#"
name: test
version: "2"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: out, kind: sink, type: log }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("unsupported schema version"), "got: {error}");
}

#[test]
fn missing_sink_is_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("at least one sink"), "got: {error}");
}

#[test]
fn duplicate_ids_are_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: in, kind: sink, type: log }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("duplicate step id"), "got: {error}");
}
