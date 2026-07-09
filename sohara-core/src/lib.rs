//! Sohara Core - A lightweight event-driven data processing framework
//!
//! This crate provides the core abstractions for building data pipelines:
//! - `Source`: Data producers that emit records
//! - `Sink`: Data consumers that receive records
//! - `Transform`: Data transformers that modify records
//! - `Pipeline`: Combines source, transforms, and sink into a processing chain
//! - `Record`: Generic record type for data interchange

// Clippy: strict mode
#![deny(clippy::all)]
#![warn(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

pub mod error;
pub mod pipeline;
pub mod record;
pub mod runtime;
pub mod sink;
pub mod source;
pub mod transform;

pub use error::{Error, Result};
pub use pipeline::Pipeline;
pub use record::{Record, RecordBuilder};
pub use runtime::Runtime;
pub use sink::Sink;
pub use source::Source;
pub use transform::Transform;
