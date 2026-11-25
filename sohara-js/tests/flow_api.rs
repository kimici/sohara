//! Tests for the JS flow builder / evaluator.

mod common;

use sohara_builtins::register_all as register_builtins;
use sohara_config::FlowConfig;
use sohara_core::ComponentRegistry;
use sohara_runtime::run_flow;

use common::temp_dir;

#[tokio::test]
async fn js_flow_file_builds_and_runs() {
    let dir = temp_dir("flow-run");
    let flow_path = dir.join("flow.js");
    let output_path = dir.join("out.jsonl");
    write_build_flow_script(&flow_path, &output_path);

    let value = sohara_js::evaluate_flow_file(&flow_path).unwrap();
    let flow = FlowConfig::from_json_value(value, flow_path.parent()).unwrap();
    let mut registry = ComponentRegistry::new();
    register_builtins(&mut registry);
    sohara_js::register_all(&mut registry);
    let stats = run_flow(&flow, &registry).await.unwrap();
    assert_eq!(stats.processed, 1);
    let output = std::fs::read_to_string(output_path).unwrap();
    assert!(output.contains("\"slug\":\"hello-world\""), "got: {output}");
    std::fs::remove_dir_all(dir).ok();
}

/// Write a flow script that registers an inline slugify step and sinks to
/// `output_path`, exercising the flow builder plus runtime registration.
fn write_build_flow_script(flow_path: &std::path::Path, output_path: &std::path::Path) {
    std::fs::write(
        flow_path,
        format!(
            r#"
module.exports = sohara.flow("js-flow")
  .source("in", "inline", {{ records: [{{ name: "Hello World" }}] }})
  .transform("reg", "script", {{
    inline: "sohara.registerStep('transform', 'slugify', function(record, ctx) {{ record.slug = String(record.name).toLowerCase().replace(/\\s+/g, '-'); return record; }}); function transform(record, ctx) {{ return record; }}"
  }})
  .transform("slug", "slugify", {{}})
  .sink("out", "file", {{ path: "{}", format: "jsonl" }})
  .build();
"#,
            output_path.display()
        ),
    )
    .unwrap();
}

#[test]
fn js_flow_save_writes_yaml() {
    let dir = temp_dir("flow-save");
    let flow_path = dir.join("flow.js");
    let yaml_path = dir.join("generated.yaml");
    std::fs::write(
        &flow_path,
        format!(
            r#"
module.exports = sohara.flow("saved")
  .source("in", "inline", {{ records: [{{ n: 1 }}] }})
  .sink("out", "log", {{}})
  .save("{}");
"#,
            yaml_path.display()
        ),
    )
    .unwrap();

    let value = sohara_js::evaluate_flow_file(&flow_path).unwrap();
    let flow = FlowConfig::from_json_value(value, flow_path.parent()).unwrap();
    assert_eq!(flow.name, "saved");
    let yaml = std::fs::read_to_string(yaml_path).unwrap();
    assert!(yaml.contains("name: saved"), "got: {yaml}");
    std::fs::remove_dir_all(dir).ok();
}
