//! Sohara Core - A lightweight event-driven data processing framework
//!
//! This crate provides the core abstractions for building data pipelines:
//! - `Source`: Data producers that emit records
//! - `Sink`: Data consumers that receive records
//! - `Transform`: Data transformers that modify records (returning `TransformOutcome`)
//! - `Pipeline`: Combines source, transforms, and sink into a processing chain
//! - `Record`: Generic JSON record type for data interchange
//! - `expr`: Minimal expression language used by declarative steps
//! - `registry`: `(kind, type)` component registry for building steps from config

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

pub mod bus;
pub mod control;
pub mod error;
pub mod expr;
pub mod pipeline;
pub mod record;
pub mod registry;
pub mod runtime;
pub mod sink;
pub mod source;
pub mod store;
pub mod transform;
pub mod util;

pub use bus::EventBus;
pub use control::{ControlNode, JoinMode, SwitchCase};
pub use error::{Error, Result};
pub use expr::{eval, is_truthy, parse, CmpOp, EvalContext, Expr, ExprError};
pub use pipeline::{Pipeline, PipelineStats};
pub use record::{Record, RecordBuilder};
pub use registry::{BuildContext, BuiltStep, ComponentRegistry, StepFactory, StepKind};
pub use runtime::Runtime;
pub use sink::{LogSink, Sink, VecSink};
pub use source::{Source, Trigger, VecSource};
pub use store::StateStore;
pub use transform::{
    AddFieldTransform, AssertOnFail, AssertTransform, Assertion, Check, FilterTransform,
    MapTransform, Transform, TransformOutcome,
};
pub use util::parse_duration;

/// Commonly used items for building pipelines.
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::expr::{eval, is_truthy, parse, CmpOp, EvalContext, Expr, ExprError};
    pub use crate::pipeline::{Pipeline, PipelineStats};
    pub use crate::record::{Record, RecordBuilder};
    pub use crate::registry::{BuildContext, BuiltStep, ComponentRegistry, StepFactory, StepKind};
    pub use crate::sink::{LogSink, Sink, VecSink};
    pub use crate::source::{Source, VecSource};
    pub use crate::transform::{
        AddFieldTransform, AssertOnFail, AssertTransform, Assertion, Check, FilterTransform,
        MapTransform, Transform, TransformOutcome,
    };
}
