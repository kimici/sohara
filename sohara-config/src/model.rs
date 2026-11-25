//! Reusable schema value types shared across the config module tree.

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::ConfigError;

/// A provider config selected by `type`, with either nested `config:` or the
/// flat shorthand convention used elsewhere in the schema.
#[derive(Debug, Clone, Deserialize)]
pub struct ComponentConfig {
    #[serde(rename = "type")]
    pub component_type: String,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

impl ComponentConfig {
    pub fn config(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut extra = self.extra.clone();
        if let Some(value) = extra.remove("config") {
            if let Some(key) = extra.keys().next() {
                return Err(ConfigError::UnknownField {
                    id: self.component_type.clone(),
                    field: key.clone(),
                });
            }
            return match value {
                Value::Object(map) => Ok(map),
                _ => Err(ConfigError::ConfigNotMap {
                    id: self.component_type.clone(),
                }),
            };
        }
        Ok(extra)
    }
}

/// State store selection: built-in path string or external provider object.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StoreConfig {
    Path(String),
    Component(ComponentConfig),
}

/// Error strategy for a step.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    /// Abort the whole run.
    #[default]
    Fail,
    /// Drop the record and continue the run.
    Continue,
    /// Retry the record according to [`RetryConfig`].
    Retry,
}

/// Retry parameters.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Maximum retry attempts.
    pub max: u32,
    /// Fixed backoff between attempts, e.g. `1s`.
    pub backoff: String,
}
