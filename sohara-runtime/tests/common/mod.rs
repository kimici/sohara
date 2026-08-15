//! Shared helpers for runtime integration tests

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use serde_json::Value;
use sohara_builtins::register_all;
use sohara_config::FlowConfig;
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, Error, Record, Result, Sink, StepFactory,
    Transform, TransformOutcome,
};
use sohara_runtime::{run_flow, StatsSnapshot};

/// A sink that appends every record into a shared vec (for assertions).
pub struct ProbeSink {
    pub records: Arc<Mutex<Vec<Record>>>,
}

#[async_trait::async_trait]
impl Sink for ProbeSink {
    async fn send(&self, record: Record) -> Result<()> {
        self.records.lock().unwrap().push(record);
        Ok(())
    }

    fn name(&self) -> &str {
        "probe"
    }
}

/// A transform that fails the first `failures` times, then passes records.
pub struct Flaky {
    name: String,
    remaining: Mutex<usize>,
    message: String,
}

#[async_trait::async_trait]
impl Transform for Flaky {
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        let mut remaining = self.remaining.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            return Ok(TransformOutcome::Fail(Error::Transform(
                self.message.clone(),
            )));
        }
        Ok(TransformOutcome::Pass(record))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Factory producing a probe sink writing into `records`.
pub fn probe_factory(records: Arc<Mutex<Vec<Record>>>) -> Arc<dyn StepFactory> {
    struct Factory(Arc<Mutex<Vec<Record>>>);
    impl StepFactory for Factory {
        fn build(&self, _config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
            Ok(BuiltStep::Sink(Box::new(ProbeSink {
                records: self.0.clone(),
            })))
        }
    }
    Arc::new(Factory(records))
}

/// Factory producing a flaky transform failing `failures` times.
pub fn flaky_factory(failures: usize) -> Arc<dyn StepFactory> {
    struct Factory(usize);
    impl StepFactory for Factory {
        fn build(&self, _config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
            Ok(BuiltStep::Transform(Box::new(Flaky {
                name: "flaky".to_owned(),
                remaining: Mutex::new(self.0),
                message: "flaky failed".to_owned(),
            })))
        }
    }
    Arc::new(Factory(failures))
}

/// Build a registry with all built-ins plus the given extras.
pub fn registry(extras: Vec<(&str, &str, Arc<dyn StepFactory>)>) -> ComponentRegistry {
    let mut registry = ComponentRegistry::new();
    register_all(&mut registry);
    for (kind, ty, factory) in extras {
        let kind = match kind {
            "source" => sohara_core::StepKind::Source,
            "transform" => sohara_core::StepKind::Transform,
            "sink" => sohara_core::StepKind::Sink,
            _ => sohara_core::StepKind::Control,
        };
        registry.register(kind, ty, factory);
    }
    registry
}

/// Parse and run a flow.
pub async fn run_with(
    yaml: &str,
    registry: &ComponentRegistry,
) -> std::result::Result<StatsSnapshot, Error> {
    let flow = FlowConfig::from_yaml_str(yaml).expect("flow must be valid");
    run_flow(&flow, registry).await
}

/// Run with a fresh probe sink, returning (stats, records).
pub async fn run_with_probe(yaml: &str) -> (StatsSnapshot, Vec<Record>) {
    let records = Arc::new(Mutex::new(Vec::new()));
    let registry = registry(vec![("sink", "probe", probe_factory(records.clone()))]);
    let stats = run_with(yaml, &registry).await.expect("run must succeed");
    let records = records.lock().unwrap().clone();
    (stats, records)
}
