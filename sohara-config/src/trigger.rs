//! Trigger declarations (serve mode entry points)

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::ConfigError;

/// A trigger declaration (serve mode entry point).
#[derive(Debug, Clone, Deserialize)]
pub struct TriggerConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub trigger_type: String,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

impl TriggerConfig {
    /// The trigger's config object, supporting nested `config:` and the flat
    /// shorthand (same convention as steps).
    pub fn config(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut extra = self.extra.clone();
        if let Some(value) = extra.remove("config") {
            if let Some(key) = extra.keys().next() {
                return Err(ConfigError::UnknownField {
                    id: self.id.clone(),
                    field: key.clone(),
                });
            }
            return match value {
                Value::Object(map) => Ok(map),
                _ => Err(ConfigError::ConfigNotMap {
                    id: self.id.clone(),
                }),
            };
        }
        Ok(extra)
    }
}
