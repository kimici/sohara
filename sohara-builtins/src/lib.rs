//! Built-in steps registered into a `ComponentRegistry`

mod control;
mod file;
mod inline;
mod sinks;
mod transforms;

pub use control::ControlSteps;
pub use file::{FileSink, FileSource};
pub use inline::InlineSource;
pub use sinks::{CollectSink, FanoutSink, LogSink, NoopSink, QueueSink};
pub use transforms::{AddFieldStep, AssertStep, FilterStep, MapStep};

use std::sync::Arc;

use serde_json::Value;
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, Error, Result, StepFactory, StepKind,
};

/// Register all built-in steps into the registry.
pub fn register_all(registry: &mut ComponentRegistry) {
    register_data_steps(registry);
    register_control_steps(registry);
}

fn register_data_steps(registry: &mut ComponentRegistry) {
    registry.register(StepKind::Source, "file", factory(FileSource::build));
    registry.register(StepKind::Source, "inline", factory(InlineSource::build));
    registry.register(StepKind::Transform, "filter", factory(FilterStep::build));
    registry.register(StepKind::Transform, "map", factory(MapStep::build));
    registry.register(
        StepKind::Transform,
        "add_field",
        factory(AddFieldStep::build),
    );
    registry.register(StepKind::Transform, "assert", factory(AssertStep::build));
    registry.register(StepKind::Sink, "file", factory(FileSink::build));
    registry.register(StepKind::Sink, "log", factory(LogSink::build));
    registry.register(StepKind::Sink, "noop", factory(NoopSink::build));
    registry.register(StepKind::Sink, "collect", factory(CollectSink::build));
    registry.register(StepKind::Sink, "queue", factory(QueueSink::build));
}

type ControlFactory = fn(&Value, &BuildContext) -> Result<BuiltStep>;

fn register_control_steps(registry: &mut ComponentRegistry) {
    let controls: [(&str, ControlFactory); 6] = [
        ("switch", ControlSteps::build_switch),
        ("foreach", ControlSteps::build_foreach),
        ("loop", ControlSteps::build_loop),
        ("parallel", ControlSteps::build_parallel),
        ("join", ControlSteps::build_join),
        ("delay", ControlSteps::build_delay),
    ];
    for (ty, build) in controls {
        registry.register(StepKind::Control, ty, factory(build));
    }
    registry.register(
        StepKind::Transform,
        "batch",
        factory(ControlSteps::build_batch),
    );
    registry.register(
        StepKind::Transform,
        "state",
        factory(ControlSteps::build_state),
    );
    registry.register(
        StepKind::Control,
        "approve",
        factory(ControlSteps::build_approve),
    );
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
