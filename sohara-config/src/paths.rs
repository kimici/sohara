//! Relative path resolution for string fields like `path` and `script`

use std::path::Path;

use serde_json::{Map, Value};

use crate::FlowConfig;

impl FlowConfig {
    /// Resolve `path` / `script` string fields relative to the flow file dir.
    pub(crate) fn resolve_paths(&mut self, base: &Path) {
        for step in &mut self.steps {
            resolve_in(&mut step.extra, base);
        }
    }
}

fn resolve_in(map: &mut Map<String, Value>, base: &Path) {
    let entries: Vec<(String, Value)> = map
        .iter()
        .map(|(key, value)| {
            let value = match (key.as_str(), value) {
                ("path" | "script", Value::String(text)) => {
                    let path = Path::new(text);
                    if path.is_absolute() {
                        Value::String(text.clone())
                    } else {
                        Value::String(base.join(path).to_string_lossy().into_owned())
                    }
                }
                ("config", Value::Object(inner)) => {
                    let mut inner = inner.clone();
                    resolve_in(&mut inner, base);
                    Value::Object(inner)
                }
                _ => value.clone(),
            };
            (key.clone(), value)
        })
        .collect();
    map.clear();
    map.extend(entries);
}
