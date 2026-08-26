//! Integration tests for flow-local `sohara.registerStep()`.

mod common;

use common::run_with_probe;

#[tokio::test]
async fn script_registered_transform_is_available_flow_wide() {
    let (_stats, records) = run_with_probe(
        r#"
name: script-register
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{ name: "Hello World" }] } }
  - { id: slug, kind: transform, type: slugify }
  - { id: reg, kind: transform, type: script, config: { inline: "sohara.registerStep('transform', 'slugify', function(record, ctx) { record.slug = String(record.name).toLowerCase().replace(/\\s+/g, '-'); return record; }); function transform(record, ctx) { return record; }" } }
  - { id: out, kind: sink, type: probe }
edges: [[in, slug], [slug, out]]
"#,
    )
    .await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].get("slug").and_then(serde_json::Value::as_str),
        Some("hello-world")
    );
}
