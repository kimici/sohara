//! Control-step factories: switch / foreach / loop / parallel / join / delay / batch / state / approve

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;
use sohara_core::{
    parse_duration, BuildContext, BuiltStep, ControlNode, Error, JoinMode, Result, SwitchCase,
};

use crate::parse_config;

const DEFAULT_MAX_ITERATIONS: usize = 10_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SwitchCaseConfig {
    #[serde(rename = "when")]
    when: String,
    to: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SwitchConfig {
    cases: Vec<SwitchCaseConfig>,
    #[serde(default)]
    default: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForeachConfig {
    over: String,
    #[serde(rename = "as", default)]
    as_field: Option<String>,
    #[serde(default)]
    max_iterations: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoopConfig {
    #[serde(rename = "while")]
    while_: String,
    max_iterations: usize,
    #[serde(default)]
    step: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParallelConfig {
    branches: Vec<String>,
    #[serde(default)]
    concurrency: Option<usize>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
enum JoinModeConfig {
    #[default]
    All,
    Any,
    N,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinConfig {
    #[serde(default)]
    mode: Option<JoinModeConfig>,
    #[serde(default)]
    n: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DelayConfig {
    duration: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchConfig {
    #[serde(default)]
    size: Option<usize>,
    #[serde(default)]
    within: Option<String>,
    #[serde(default)]
    group_by: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateConfig {
    expr: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApproveConfig {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    owners: Option<Vec<String>>,
}

/// Factories for the S2 control steps.
pub struct ControlSteps;

impl ControlSteps {
    /// `switch`: route each record to the first matching case, else `default`.
    pub fn build_switch(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: SwitchConfig = parse_config(config, "switch")?;
        if cfg.cases.is_empty() {
            return Err(Error::Config("switch needs at least one case".to_owned()));
        }
        let mut cases = Vec::new();
        for case in cfg.cases {
            let when = sohara_core::parse(&case.when)
                .map_err(|error| Error::Config(format!("switch case '{}': {error}", case.when)))?;
            cases.push(SwitchCase { when, to: case.to });
        }
        let default = cfg
            .default
            .ok_or_else(|| Error::Config("switch needs a 'default' target".to_owned()))?;
        Ok(BuiltStep::Control(ControlNode::Switch { cases, default }))
    }

    /// `foreach`: iterate an array, binding each item to `as` (default `item`).
    pub fn build_foreach(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: ForeachConfig = parse_config(config, "foreach")?;
        let over = sohara_core::parse(&cfg.over)
            .map_err(|error| Error::Config(format!("foreach 'over': {error}")))?;
        Ok(BuiltStep::Control(ControlNode::Foreach {
            over,
            as_field: cfg.as_field,
            max_iterations: cfg.max_iterations.unwrap_or(DEFAULT_MAX_ITERATIONS),
        }))
    }

    /// `loop`: repeat the body while the condition holds (bounded).
    pub fn build_loop(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: LoopConfig = parse_config(config, "loop")?;
        let while_expr = sohara_core::parse(&cfg.while_)
            .map_err(|error| Error::Config(format!("loop 'while': {error}")))?;
        Ok(BuiltStep::Control(ControlNode::Loop {
            while_expr,
            max_iterations: cfg.max_iterations,
            body: cfg.step,
        }))
    }

    /// `parallel`: fan each record out to every branch concurrently.
    pub fn build_parallel(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: ParallelConfig = parse_config(config, "parallel")?;
        if cfg.branches.is_empty() {
            return Err(Error::Config(
                "parallel needs at least one branch".to_owned(),
            ));
        }
        if cfg.concurrency.is_some() {
            tracing::warn!("parallel 'concurrency' is not enforced yet (S2)");
        }
        Ok(BuiltStep::Control(ControlNode::Parallel {
            branches: cfg.branches,
        }))
    }

    /// `join`: gather records per correlation and release by mode.
    pub fn build_join(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: JoinConfig = parse_config(config, "join")?;
        let mode = match cfg.mode.unwrap_or_default() {
            JoinModeConfig::All => JoinMode::All,
            JoinModeConfig::Any => JoinMode::Any,
            JoinModeConfig::N => JoinMode::N,
        };
        let n = cfg.n.unwrap_or(0);
        if mode == JoinMode::N && n == 0 {
            return Err(Error::Config(
                "join mode 'n' needs a positive 'n'".to_owned(),
            ));
        }
        Ok(BuiltStep::Control(ControlNode::Join { mode, n }))
    }

    /// `delay`: pause each record for the given duration.
    pub fn build_delay(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: DelayConfig = parse_config(config, "delay")?;
        let duration = parse_duration(&cfg.duration)
            .map_err(|error| Error::Config(format!("delay duration: {error}")))?;
        Ok(BuiltStep::Control(ControlNode::Delay { duration }))
    }

    /// `batch`: buffer records and emit them combined as `{ items: [...] }`.
    pub fn build_batch(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: BatchConfig = parse_config(config, "batch")?;
        if cfg.group_by.is_some() {
            return Err(Error::Config(
                "batch 'group_by' is not supported yet (later stage)".to_owned(),
            ));
        }
        if cfg.size.is_none() && cfg.within.is_none() {
            return Err(Error::Config(
                "batch needs a 'size' and/or 'within'".to_owned(),
            ));
        }
        let within = cfg
            .within
            .as_deref()
            .map(parse_duration)
            .transpose()
            .map_err(|error| Error::Config(format!("batch 'within': {error}")))?;
        Ok(BuiltStep::Control(ControlNode::Batch {
            size: cfg.size,
            within,
        }))
    }

    /// `state`: update the node's persistent state from expressions.
    pub fn build_state(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: StateConfig = parse_config(config, "state step")?;
        if cfg.expr.is_empty() {
            return Err(Error::Config(
                "state step needs at least one 'expr' entry".to_owned(),
            ));
        }
        let mut exprs = Vec::new();
        for (field, source) in cfg.expr {
            let expr = sohara_core::parse(&source)
                .map_err(|error| Error::Config(format!("state expr '{field}': {error}")))?;
            exprs.push((field, expr));
        }
        Ok(BuiltStep::Control(ControlNode::State { exprs }))
    }

    /// `approve`: park records for human approval (S4).
    pub fn build_approve(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: ApproveConfig = parse_config(config, "approve")?;
        if let Some(owners) = &cfg.owners {
            tracing::warn!("approve 'owners' recorded but not enforced yet: {owners:?}");
        }
        Ok(BuiltStep::Control(ControlNode::Approve {
            title: cfg.title.unwrap_or_else(|| "approval".to_owned()),
        }))
    }
}
