//! Config errors with readable messages

use std::path::PathBuf;

use thiserror::Error;

/// Errors from loading or validating a flow file.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read '{path}': {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse '{path}': {source}")]
    Parse {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    #[error("invalid flow: {0}")]
    Invalid(String),
    #[error("step '{id}': unknown field '{field}'")]
    UnknownField { id: String, field: String },
    #[error("step '{id}': field '{field}' belongs to a later stage (not supported in schema v1)")]
    Unsupported { id: String, field: String },
    #[error("step '{id}': 'config' must be a mapping")]
    ConfigNotMap { id: String },
}
