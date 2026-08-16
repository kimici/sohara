//! The `script` source / transform / sink step types (S5/S6)

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::Value;
use sohara_core::{BuiltStep, Record, Result, Sink, Source, Transform, TransformOutcome};

use crate::bridge::{call_consume, call_source, call_transform, invocation, merge_emitted};
use crate::env::StepEnv;
use crate::{load_source, outcome_from_js, parse_config, step_env, ScriptConfig};

/// A transform that runs a QuickJS script per record.
pub struct ScriptStep {
    name: String,
    source: String,
    entry: String,
    env: StepEnv,
}

impl ScriptStep {
    /// Build the step from config.
    pub fn build(config: &Value, ctx: &sohara_core::BuildContext) -> Result<BuiltStep> {
        let cfg: ScriptConfig = parse_config(config, "script step")?;
        let env = step_env(&cfg, ctx);
        let step = Self {
            name: env.name.clone(),
            source: load_source(&cfg, "script step")?,
            entry: cfg.entry.unwrap_or_else(|| "transform".to_owned()),
            env,
        };
        Ok(BuiltStep::Transform(Box::new(step)))
    }
}

#[async_trait]
impl Transform for ScriptStep {
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        let env = invocation(&self.env);
        let context = crate::bridge::setup_context(&env)?;
        crate::bridge::run_script(&context, &self.source)?;
        let result = call_transform(&context, &env, &self.entry, &record)?;
        merge_emitted(&env, outcome_from_js(result)?)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A source that generates records from a QuickJS script.
pub struct ScriptSource {
    name: String,
    source: String,
    entry: String,
    env: StepEnv,
}

impl ScriptSource {
    /// Build the step from config.
    pub fn build(config: &Value, ctx: &sohara_core::BuildContext) -> Result<BuiltStep> {
        let cfg: ScriptConfig = parse_config(config, "script source")?;
        let env = step_env(&cfg, ctx);
        let step = Self {
            name: env.name.clone(),
            source: load_source(&cfg, "script source")?,
            entry: cfg.entry.unwrap_or_else(|| "generate".to_owned()),
            env,
        };
        Ok(BuiltStep::Source(Box::new(step)))
    }
}

#[async_trait]
impl Source for ScriptSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let env = invocation(&self.env);
        let context = crate::bridge::setup_context(&env)?;
        crate::bridge::run_script(&context, &self.source)?;
        let result = call_source(&context, &env, &self.entry)?;
        let outcome = merge_emitted(&env, outcome_from_js(result)?)?;
        let records = records_from_outcome(outcome)?;
        Ok(Box::pin(futures::stream::iter(records.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A sink that consumes records via a QuickJS script.
pub struct ScriptSink {
    name: String,
    source: String,
    entry: String,
    env: StepEnv,
}

impl ScriptSink {
    /// Build the step from config.
    pub fn build(config: &Value, ctx: &sohara_core::BuildContext) -> Result<BuiltStep> {
        let cfg: ScriptConfig = parse_config(config, "script sink")?;
        let env = step_env(&cfg, ctx);
        let step = Self {
            name: env.name.clone(),
            source: load_source(&cfg, "script sink")?,
            entry: cfg.entry.unwrap_or_else(|| "consume".to_owned()),
            env,
        };
        Ok(BuiltStep::Sink(Box::new(step)))
    }
}

#[async_trait]
impl Sink for ScriptSink {
    async fn send(&self, record: Record) -> Result<()> {
        let env = invocation(&self.env);
        let context = crate::bridge::setup_context(&env)?;
        crate::bridge::run_script(&context, &self.source)?;
        call_consume(&context, &env, &self.entry, &record)?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn records_from_outcome(outcome: TransformOutcome) -> Result<Vec<Record>> {
    match outcome {
        TransformOutcome::Pass(record) => Ok(vec![record]),
        TransformOutcome::Expand(records) => Ok(records),
        TransformOutcome::Filtered => Ok(Vec::new()),
        TransformOutcome::Fail(error) => Err(error),
    }
}
