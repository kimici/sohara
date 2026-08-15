//! Imported template fragments and `use` merging (S5)

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

use crate::{ConfigError, FlowConfig, StepConfig};

/// A fragment file loaded via `imports` (templates only in S5).
#[derive(Debug, Default, Deserialize)]
struct ImportFile {
    #[serde(default)]
    templates: BTreeMap<String, StepConfig>,
}

impl FlowConfig {
    /// Merge templates from imported files into `self.templates`.
    pub(crate) fn resolve_imports(&mut self, base: &Path) -> Result<(), ConfigError> {
        let mut templates = self.templates.clone();
        for import in &self.imports {
            let path = base.join(import);
            let text = std::fs::read_to_string(&path).map_err(|source| ConfigError::Read {
                path: path.clone(),
                source,
            })?;
            let imported: ImportFile =
                serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
                    path: path.clone(),
                    source,
                })?;
            templates.extend(imported.templates);
        }
        self.templates = templates;
        Ok(())
    }

    /// Apply `use` references: merge each template into its step.
    pub(crate) fn apply_templates(&mut self) -> Result<(), ConfigError> {
        for step in &mut self.steps {
            let Some(name) = step.use_template.clone() else {
                continue;
            };
            let template = self.templates.get(&name).ok_or_else(|| {
                ConfigError::Invalid(format!("step '{}': unknown template '{name}'", step.id))
            })?;
            merge_template(step, template)?;
            step.use_template = None;
        }
        Ok(())
    }
}

fn merge_template(step: &mut StepConfig, template: &StepConfig) -> Result<(), ConfigError> {
    if matches!((step.kind, template.kind), (Some(a), Some(b)) if a != b) {
        return Err(ConfigError::Invalid(format!(
            "step '{}': kind conflicts with template",
            step.id
        )));
    }
    if step.kind.is_none() {
        step.kind = template.kind;
    }
    if step.step_type.is_none() {
        step.step_type = template.step_type.clone();
    }
    step.when = step.when.clone().or_else(|| template.when.clone());
    step.on_error = step.on_error.or(template.on_error);
    step.retry = step.retry.clone().or_else(|| template.retry.clone());
    step.state = step.state.clone().or_else(|| template.state.clone());
    merge_extras(step, template);
    Ok(())
}

fn merge_extras(step: &mut StepConfig, template: &StepConfig) {
    let mut merged = template.extra.clone();
    for (key, value) in &step.extra {
        match (merged.get_mut(key), value) {
            (Some(Value::Object(existing)), Value::Object(overrides)) => {
                for (field, override_value) in overrides {
                    existing.insert(field.clone(), override_value.clone());
                }
            }
            _ => {
                merged.insert(key.clone(), value.clone());
            }
        }
    }
    step.extra = merged;
}
