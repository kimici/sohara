//! Project scaffolding: `sohara init` (S1)

use std::path::Path;

use anyhow::Result;

/// Create a new flow project (flow.yaml + data/input.csv).
pub fn init_project(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir.join("data"))?;
    std::fs::write(dir.join("flow.yaml"), FLOW_YAML)?;
    std::fs::write(dir.join("data").join("input.csv"), INPUT_CSV)?;
    println!("Created {}/flow.yaml and data/input.csv", dir.display());
    Ok(())
}

const FLOW_YAML: &str = r#"name: basic
version: "1"
steps:
  - id: in
    kind: source
    type: file
    config: { path: data/input.csv, format: csv }
  - id: adult
    kind: transform
    type: filter
    config: { where: "age >= 18" }
  - id: enrich
    kind: transform
    type: map
    config: { expr: { processed_at: "now()" } }
  - id: out
    kind: sink
    type: file
    config: { path: output/result.jsonl, format: jsonl }
"#;

const INPUT_CSV: &str = "name,age\nAlice,30\nBob,15\nCarol,40\n";
