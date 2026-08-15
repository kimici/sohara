//! Connectors for S5: HTTP client and SQLite database steps

pub mod db;
pub mod http;

pub use db::{DbSink, DbSource};
pub use http::{HttpSink, HttpSource};

use std::sync::Arc;

use serde_json::Value;
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, Error, Result, StepFactory, StepKind,
};

/// Register the S5 connector steps into a registry.
pub fn register_all(registry: &mut ComponentRegistry) {
    registry.register(StepKind::Source, "http", factory(HttpSource::build));
    registry.register(StepKind::Sink, "http", factory(HttpSink::build));
    registry.register(StepKind::Source, "db", factory(DbSource::build));
    registry.register(StepKind::Sink, "db", factory(DbSink::build));
}

struct FactoryFn<F>(F);

impl<F> StepFactory for FactoryFn<F>
where
    F: Fn(&Value, &BuildContext) -> Result<BuiltStep> + Send + Sync,
{
    fn build(&self, config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        (self.0)(config, ctx)
    }
}

fn factory<F>(build: F) -> Arc<dyn StepFactory>
where
    F: Fn(&Value, &BuildContext) -> Result<BuiltStep> + Send + Sync + 'static,
{
    Arc::new(FactoryFn(build))
}

/// Parse a config object into a typed struct with strict unknown-field
/// rejection, wrapped in a readable config error.
pub(crate) fn parse_config<C: serde::de::DeserializeOwned>(
    config: &Value,
    what: &str,
) -> Result<C> {
    serde_json::from_value(config.clone())
        .map_err(|error| Error::Config(format!("{what}: {error}")))
}
