//! Component registry: maps `(kind, type)` to a step factory

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::control::ControlNode;
use crate::error::Result;
use crate::sink::Sink;
use crate::source::Source;
use crate::transform::Transform;

/// The role a step plays in a flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StepKind {
    Source,
    Transform,
    Sink,
    Control,
}

impl StepKind {
    /// Lowercase name for error messages and YAML.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Transform => "transform",
            Self::Sink => "sink",
            Self::Control => "control",
        }
    }
}

/// A step instance produced by a factory.
pub enum BuiltStep {
    Source(Box<dyn Source>),
    Transform(Box<dyn Transform>),
    Sink(Box<dyn Sink>),
    Control(ControlNode),
}

/// Step identity available to factories (exposed to scripts via `ctx.step`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StepMeta {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub step_type: String,
}

/// Context available to factories at build time.
#[derive(Clone, Default)]
pub struct BuildContext {
    /// Flow-level variables (`vars`), usable from expressions via `var(name)`.
    pub vars: serde_json::Map<String, Value>,
    /// Shared event bus (present in `serve` mode).
    pub bus: Option<std::sync::Arc<dyn crate::bus::EventBus>>,
    /// Flow name, set by the runtime/serve entry points.
    pub flow: String,
    /// Step identity, set per step by the builder.
    pub step: Option<StepMeta>,
}

/// Builds a step of a specific `(kind, type)` from its config object.
pub trait StepFactory: Send + Sync {
    fn build(&self, config: &Value, ctx: &BuildContext) -> Result<BuiltStep>;
}

/// Registry of step factories keyed by `(kind, type)`.
#[derive(Default)]
pub struct ComponentRegistry {
    factories: HashMap<(StepKind, String), Arc<dyn StepFactory>>,
}

impl ComponentRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a factory for a `(kind, type)` pair.
    pub fn register(&mut self, kind: StepKind, ty: &str, factory: Arc<dyn StepFactory>) {
        self.factories.insert((kind, ty.to_owned()), factory);
    }

    /// Whether a `(kind, type)` factory is already registered.
    #[must_use]
    pub fn contains(&self, kind: StepKind, ty: &str) -> bool {
        self.factories.contains_key(&(kind, ty.to_owned()))
    }

    /// Build a step, returning a readable error for unknown `(kind, type)`.
    pub fn build(
        &self,
        kind: StepKind,
        ty: &str,
        config: &Value,
        ctx: &BuildContext,
    ) -> Result<BuiltStep> {
        let key = (kind, ty.to_owned());
        let factory = self.factories.get(&key).ok_or_else(|| {
            crate::Error::Config(format!(
                "unknown step: kind='{}' type='{}'; registered {} types: {}",
                kind.as_str(),
                ty,
                kind.as_str(),
                self.names(kind).join(", ")
            ))
        })?;
        factory.build(config, ctx)
    }

    fn names(&self, kind: StepKind) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .factories
            .keys()
            .filter(|(k, _)| *k == kind)
            .map(|(_, ty)| ty.as_str())
            .collect();
        names.sort_unstable();
        names
    }
}
