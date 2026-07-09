//! Error types for Sohara

use thiserror::Error;

/// Result type alias for Sohara operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for Sohara
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Pipeline error: {0}")]
    Pipeline(String),

    #[error("Source error: {0}")]
    Source(String),

    #[error("Sink error: {0}")]
    Sink(String),

    #[error("Transform error: {0}")]
    Transform(String),

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Timeout error")]
    Timeout,

    #[error("Channel closed")]
    ChannelClosed,

    #[error("Unknown error: {0}")]
    Unknown(String),
}
