//! Relative path resolution for string fields like `path` and `script`

use std::path::Path;

use serde_json::{Map, Value};

use crate::{FlowConfig, StoreConfig};

impl FlowConfig {
    /// Resolve `path` / `script` string fields relative to the flow file dir.
    pub(crate) fn resolve_paths(&mut self, base: &Path) {
        for step in &mut self.steps {
            resolve_in(&mut step.extra, base);
        }
        if let Some(checkpoint) = &mut self.checkpoint {
            match &mut checkpoint.store {
                Some(StoreConfig::Path(path)) => {
                    let candidate = Path::new(path);
                    if !candidate.is_absolute() {
                        *path = base.join(candidate).to_string_lossy().into_owned();
                    }
                }
                Some(StoreConfig::Component(component)) => {
                    resolve_in(&mut component.extra, base);
                }
                None => {}
            }
        }
        if let Some(event_bus) = &mut self.event_bus {
            resolve_in(&mut event_bus.extra, base);
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
