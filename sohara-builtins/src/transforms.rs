//! Declarative transform steps: filter / map / add_field / assert

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value};
use sohara_core::{
    eval, is_truthy, parse, AddFieldTransform, AssertOnFail, AssertTransform, Assertion,
    BuildContext, BuiltStep, Check, Error, EvalContext, Expr, Record, Result, Transform,
    TransformOutcome,
};

use crate::parse_config;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilterConfig {
    #[serde(rename = "where", default)]
    where_: Option<String>,
    #[serde(default)]
    invert: bool,
}

/// `transform.filter` step: keep records matching a `where` expression.
pub struct FilterStep {
    name: String,
    expr: Option<Expr>,
    invert: bool,
    vars: Map<String, Value>,
}

impl FilterStep {
    /// Build the step from config.
    pub fn build(config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: FilterConfig = parse_config(config, "filter config")?;
        let expr = cfg
            .where_
            .as_deref()
            .map(parse)
            .transpose()
            .map_err(|error| Error::Config(format!("filter 'where': {error}")))?;
        let step = Self {
            name: "filter".to_owned(),
            expr,
            invert: cfg.invert,
            vars: ctx.vars.clone(),
        };
        Ok(BuiltStep::Transform(Box::new(step)))
    }
}

#[async_trait]
impl Transform for FilterStep {
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        let passed = match &self.expr {
            None => true,
            Some(expr) => {
                let ctx = EvalContext { vars: &self.vars };
                is_truthy(&eval(expr, &record.payload, &ctx)?)
            }
        };
        if passed != self.invert {
            Ok(TransformOutcome::Pass(record))
        } else {
            Ok(TransformOutcome::Filtered)
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MapConfig {
    #[serde(default)]
    expr: Option<BTreeMap<String, String>>,
    #[serde(default)]
    project: Option<Vec<String>>,
}

/// `transform.map` step: compute fields from expressions, optionally project.
pub struct MapStep {
    name: String,
    exprs: Vec<(String, Expr)>,
    project: Option<Vec<String>>,
    vars: Map<String, Value>,
}

impl MapStep {
    /// Build the step from config, parsing every expression eagerly.
    pub fn build(config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: MapConfig = parse_config(config, "map config")?;
        let mut exprs = Vec::new();
        for (field, source) in cfg.expr.unwrap_or_default() {
            let expr = parse(&source)
                .map_err(|error| Error::Config(format!("map expr '{field}': {error}")))?;
            exprs.push((field, expr));
        }
        let step = Self {
            name: "map".to_owned(),
            exprs,
            project: cfg.project,
            vars: ctx.vars.clone(),
        };
        Ok(BuiltStep::Transform(Box::new(step)))
    }
}

#[async_trait]
impl Transform for MapStep {
    async fn transform(&self, mut record: Record) -> Result<TransformOutcome> {
        let ctx = EvalContext { vars: &self.vars };
        for (field, expr) in &self.exprs {
            let value = eval(expr, &record.payload, &ctx)?;
            record.set(field, value);
        }
        if let Some(fields) = &self.project {
            let mut object = Map::new();
            for field in fields {
                if let Some(value) = record.get(field) {
                    object.insert(field.clone(), value.clone());
                }
            }
            record.payload = Value::Object(object);
        }
        Ok(TransformOutcome::Pass(record))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AddFieldConfig {
    field: String,
    value: Value,
}

/// `transform.add_field` step: set a constant field on every record.
pub struct AddFieldStep;

impl AddFieldStep {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: AddFieldConfig = parse_config(config, "add_field config")?;
        let step = AddFieldTransform::new("add_field", cfg.field, cfg.value);
        Ok(BuiltStep::Transform(Box::new(step)))
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AssertOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Contains,
    IsNull,
    NotNull,
    Exists,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertItem {
    field: String,
    op: AssertOp,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum FailMode {
    Error,
    Filter,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertConfig {
    expect: Vec<AssertItem>,
    #[serde(default)]
    on_fail: Option<FailMode>,
}

/// `transform.assert` step: validate records against `expect` assertions.
pub struct AssertStep;

impl AssertStep {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: AssertConfig = parse_config(config, "assert config")?;
        let assertions = cfg
            .expect
            .into_iter()
            .map(to_assertion)
            .collect::<Result<Vec<_>>>()?;
        let mut step = AssertTransform::new("assert", assertions);
        if cfg.on_fail == Some(FailMode::Filter) {
            step = step.on_fail(AssertOnFail::Filter);
        }
        Ok(BuiltStep::Transform(Box::new(step)))
    }
}

fn to_assertion(item: AssertItem) -> Result<Assertion> {
    let AssertItem {
        field,
        op,
        value,
        message,
    } = item;
    let check = to_check(&field, op, value)?;
    let mut assertion = Assertion::new(field, check);
    if let Some(message) = message {
        assertion = assertion.with_message(message);
    }
    Ok(assertion)
}

fn to_check(field: &str, op: AssertOp, raw: Option<Value>) -> Result<Check> {
    let needs_value = !matches!(op, AssertOp::IsNull | AssertOp::NotNull | AssertOp::Exists);
    let value = match (needs_value, raw) {
        (true, None) => {
            return Err(Error::Config(format!(
                "assert field '{field}': op '{op:?}' needs a 'value'"
            )));
        }
        (_, value) => value.unwrap_or(Value::Null),
    };
    match op {
        AssertOp::Eq => Ok(Check::Eq(value)),
        AssertOp::Neq => Ok(Check::Neq(value)),
        AssertOp::Gt => Ok(Check::Gt(number(&value, field)?)),
        AssertOp::Gte => Ok(Check::Gte(number(&value, field)?)),
        AssertOp::Lt => Ok(Check::Lt(number(&value, field)?)),
        AssertOp::Lte => Ok(Check::Lte(number(&value, field)?)),
        AssertOp::In => match value {
            Value::Array(items) => Ok(Check::In(items)),
            other => Err(invalid_value(field, op, other)),
        },
        AssertOp::Contains => Ok(Check::Contains(value)),
        AssertOp::IsNull => Ok(Check::Null),
        AssertOp::NotNull => Ok(Check::NotNull),
        AssertOp::Exists => Ok(Check::Exists),
    }
}

fn number(value: &Value, field: &str) -> Result<f64> {
    value.as_f64().ok_or_else(|| {
        Error::Config(format!(
            "assert field '{field}': expected a number, got {value}"
        ))
    })
}

fn invalid_value(field: &str, op: AssertOp, value: Value) -> Error {
    Error::Config(format!(
        "assert field '{field}': op '{op:?}' needs an array value, got {value}"
    ))
}
