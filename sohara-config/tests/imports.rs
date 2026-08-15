//! Integration tests for imports / templates / use (S5)

use std::fs;

use serde_json::json;
use sohara_config::FlowConfig;

#[test]
fn use_merges_template_fields_and_config() {
    let yaml = r#"
name: test
version: "1"
templates:
  normalize:
    kind: transform
    type: map
    config: { expr: { email: "lower(email)" }, project: [email] }
steps:
  - { id: in, kind: source, type: inline, records: [{email: A@X.COM}] }
  - { id: norm, use: normalize }
  - { id: out, kind: sink, type: log }
"#;
    let flow = FlowConfig::from_yaml_str(yaml).unwrap();
    let norm = &flow.steps[1];
    assert_eq!(norm.kind(), sohara_core::StepKind::Transform);
    assert_eq!(norm.step_type(), "map");
    let config = norm.config().unwrap();
    assert_eq!(config["expr"]["email"], json!("lower(email)"));
    assert_eq!(config["project"], json!(["email"]));
}

#[test]
fn use_merges_config_deeply_with_step_override() {
    let yaml = r#"
name: test
version: "1"
templates:
  tag:
    kind: transform
    type: add_field
    config: { field: tag, value: base }
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: t, use: tag, config: { value: overridden } }
  - { id: out, kind: sink, type: log }
"#;
    let flow = FlowConfig::from_yaml_str(yaml).unwrap();
    let config = flow.steps[1].config().unwrap();
    assert_eq!(config["field"], json!("tag"));
    assert_eq!(config["value"], json!("overridden"));
}

#[test]
fn unknown_template_is_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: x, use: missing }
  - { id: out, kind: sink, type: log }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("unknown template 'missing'"), "got: {error}");
}

#[test]
fn missing_kind_without_template_is_rejected() {
    let yaml = r#"
name: test
version: "1"
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: x, type: map }
  - { id: out, kind: sink, type: log }
"#;
    let error = FlowConfig::from_yaml_str(yaml).unwrap_err().to_string();
    assert!(error.contains("missing 'kind'/'type'"), "got: {error}");
}

#[test]
fn imports_bring_templates_from_files() {
    let dir = std::env::temp_dir().join(format!("sohara-imports-{:?}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("common.yaml"),
        "templates:\n  shout:\n    kind: transform\n    type: add_field\n    config: { field: loud, value: true }\n",
    )
    .unwrap();
    let main = r#"
name: test
version: "1"
imports: [common.yaml]
steps:
  - { id: in, kind: source, type: inline, records: [{n: 1}] }
  - { id: s, use: shout }
  - { id: out, kind: sink, type: log }
"#;
    fs::write(dir.join("flow.yaml"), main).unwrap();
    let flow = FlowConfig::load(&dir.join("flow.yaml")).unwrap();
    let config = flow.steps[1].config().unwrap();
    assert_eq!(config["field"], json!("loud"));
    fs::remove_dir_all(&dir).ok();
}
