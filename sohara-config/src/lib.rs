//! Declarative flow config: YAML schema v1, validation, and pipeline building

pub mod build;
pub mod error;
mod paths;
mod templates;
mod trigger;
mod validate;

pub use build::{build_flow, BuiltFlow};
pub use error::ConfigError;
pub use trigger::TriggerConfig;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Map, Value};
use sohara_core::StepKind;

/// The resolved config for one flow, as declared in YAML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowConfig {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub vars: BTreeMap<String, Value>,
    #[serde(default)]
    pub triggers: Vec<TriggerConfig>,
    #[serde(default)]
    pub steps: Vec<StepConfig>,
    #[serde(default)]
    pub edges: Vec<EdgeConfig>,
    #[serde(default)]
    pub checkpoint: Option<CheckpointConfig>,
    /// Imported YAML fragments contributing templates (S5).
    #[serde(default)]
    pub imports: Vec<String>,
    /// Reusable step templates referenced via `use` (S5).
    #[serde(default)]
    pub templates: BTreeMap<String, StepConfig>,
}

/// Checkpoint policy (S4).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointConfig {
    /// Persist step states every N processed records.
    #[serde(default)]
    pub every: Option<u64>,
    /// Path of the state store file.
    #[serde(default)]
    pub store: Option<String>,
}

/// One step declaration. Inside `templates`, the `id` may be omitted (the
/// template map key is the name); steps are required to carry one and it is
/// validated at load time.
#[derive(Debug, Clone, Deserialize)]
pub struct StepConfig {
    #[serde(default)]
    pub id: String,
    /// Optional when the step uses a template (`use`); required otherwise.
    #[serde(default)]
    pub kind: Option<StepKind>,
    /// Optional when the step uses a template (`use`); required otherwise.
    #[serde(rename = "type", default)]
    pub step_type: Option<String>,
    /// Reference to a template (S5): merged into this step at load time.
    #[serde(rename = "use", default)]
    pub use_template: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// Step-level precondition; the record is dropped when it evaluates false.
    #[serde(default)]
    pub when: Option<String>,
    /// Error strategy: fail (default) | continue | retry.
    #[serde(default)]
    pub on_error: Option<OnError>,
    /// Retry parameters, used when `on_error: retry`.
    #[serde(default)]
    pub retry: Option<RetryConfig>,
    /// Initial accumulated state (used by `state` steps and `loop.while`).
    #[serde(default)]
    pub state: Option<Value>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

impl StepConfig {
    /// The step kind; valid after loading (templates merged, validated).
    #[must_use]
    pub fn kind(&self) -> StepKind {
        self.kind.expect("step kind validated at load")
    }

    /// The step type; valid after loading (templates merged, validated).
    #[must_use]
    pub fn step_type(&self) -> &str {
        self.step_type
            .as_deref()
            .expect("step type validated at load")
    }
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

/// Step-level fields reserved for later stages: rejected with a friendly
/// error under schema v1 instead of being silently swallowed.
const RESERVED: &[&str] = &["timeout", "inputs"];

impl StepConfig {
    /// Split `extra` into the type config object, supporting both the nested
    /// `config:` key and the flat shorthand. Unknown/reserved keys are errors.
    pub fn config(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut extra = self.extra.clone();
        if let Some(value) = extra.remove("config") {
            // With a nested config, every remaining top-level key is either
            // reserved (later stage) or unknown.
            for key in extra.keys() {
                self.reject_key(key)?;
            }
            return match value {
                Value::Object(map) => Ok(map),
                _ => Err(ConfigError::ConfigNotMap {
                    id: self.id.clone(),
                }),
            };
        }
        // Flat shorthand: the remaining keys belong to the type config and
        // are validated by the factory at build time; only reserved keys
        // (later stages) are rejected here.
        for key in extra.keys() {
            if RESERVED.contains(&key.as_str()) {
                return Err(ConfigError::Unsupported {
                    id: self.id.clone(),
                    field: key.clone(),
                });
            }
        }
        Ok(extra)
    }

    fn reject_key(&self, key: &str) -> Result<(), ConfigError> {
        if RESERVED.contains(&key) {
            Err(ConfigError::Unsupported {
                id: self.id.clone(),
                field: key.to_owned(),
            })
        } else {
            Err(ConfigError::UnknownField {
                id: self.id.clone(),
                field: key.to_owned(),
            })
        }
    }
}

/// An edge declaration: a `[from, to]` pair or `{ from, to, when? }`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum EdgeConfig {
    Pair(String, String),
    Object {
        from: String,
        to: String,
        #[serde(default)]
        when: Option<String>,
    },
}

impl EdgeConfig {
    #[must_use]
    pub fn from(&self) -> &str {
        match self {
            Self::Pair(from, _) | Self::Object { from, .. } => from,
        }
    }

    #[must_use]
    pub fn to(&self) -> &str {
        match self {
            Self::Pair(_, to) | Self::Object { to, .. } => to,
        }
    }

    #[must_use]
    pub fn when(&self) -> Option<&str> {
        match self {
            Self::Pair(_, _) => None,
            Self::Object { when, .. } => when.as_deref(),
        }
    }
}

impl FlowConfig {
    /// Load, resolve relative paths/imports/templates, and validate.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let mut flow: Self = serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        if let Some(parent) = path.parent() {
            flow.resolve_imports(parent)?;
            flow.apply_templates()?;
            flow.resolve_paths(parent);
        }
        flow.validate()?;
        Ok(flow)
    }

    /// Parse and validate YAML text (no imports resolution; inline
    /// templates still apply).
    pub fn from_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let mut flow: Self = serde_yaml::from_str(yaml).map_err(|source| ConfigError::Parse {
            path: PathBuf::from("<inline>"),
            source,
        })?;
        flow.apply_templates()?;
        flow.validate()?;
        Ok(flow)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        validate::validate(self)
    }
}
