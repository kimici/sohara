//! Expression evaluation against JSON records

use serde_json::{Map, Value};
use std::sync::OnceLock;

use super::ast::Expr;
use super::compare::{compare, to_number};
use super::ExprError;

/// Runtime context for expression evaluation.
pub struct EvalContext<'a> {
    /// Flow-level variables, accessible via `var(name)`.
    pub vars: &'a Map<String, Value>,
}

impl Default for EvalContext<'_> {
    fn default() -> Self {
        Self { vars: empty_vars() }
    }
}

fn empty_vars() -> &'static Map<String, Value> {
    static VARS: OnceLock<Map<String, Value>> = OnceLock::new();
    VARS.get_or_init(Map::new)
}

/// Evaluate an expression against a record payload.
pub fn eval(expr: &Expr, record: &Value, ctx: &EvalContext<'_>) -> Result<Value, ExprError> {
    match expr {
        Expr::Lit(value) => Ok(value.clone()),
        Expr::Path(segments) => resolve_path(record, segments),
        Expr::List(items) => eval_list(items, record, ctx),
        Expr::Not(inner) => Ok(Value::Bool(!is_truthy(&eval(inner, record, ctx)?))),
        Expr::And(lhs, rhs) => and(lhs, rhs, record, ctx),
        Expr::Or(lhs, rhs) => or(lhs, rhs, record, ctx),
        Expr::Cmp(op, lhs, rhs) => {
            let left = eval(lhs, record, ctx)?;
            let right = eval(rhs, record, ctx)?;
            compare(*op, &left, &right)
        }
        Expr::In(item, list) => membership(item, list, record, ctx),
        Expr::Add(lhs, rhs) => arith(lhs, rhs, record, ctx, Arith::Add),
        Expr::Sub(lhs, rhs) => arith(lhs, rhs, record, ctx, Arith::Sub),
        Expr::Mul(lhs, rhs) => arith(lhs, rhs, record, ctx, Arith::Mul),
        Expr::Div(lhs, rhs) => arith(lhs, rhs, record, ctx, Arith::Div),
        Expr::Rem(lhs, rhs) => arith(lhs, rhs, record, ctx, Arith::Rem),
        Expr::Call(name, args) => call(name, args, record, ctx),
    }
}

fn eval_list(items: &[Expr], record: &Value, ctx: &EvalContext<'_>) -> Result<Value, ExprError> {
    items
        .iter()
        .map(|item| eval(item, record, ctx))
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn and(lhs: &Expr, rhs: &Expr, record: &Value, ctx: &EvalContext<'_>) -> Result<Value, ExprError> {
    if !is_truthy(&eval(lhs, record, ctx)?) {
        return Ok(Value::Bool(false));
    }
    Ok(Value::Bool(is_truthy(&eval(rhs, record, ctx)?)))
}

fn or(lhs: &Expr, rhs: &Expr, record: &Value, ctx: &EvalContext<'_>) -> Result<Value, ExprError> {
    if is_truthy(&eval(lhs, record, ctx)?) {
        return Ok(Value::Bool(true));
    }
    Ok(Value::Bool(is_truthy(&eval(rhs, record, ctx)?)))
}

fn membership(
    item: &Expr,
    list: &Expr,
    record: &Value,
    ctx: &EvalContext<'_>,
) -> Result<Value, ExprError> {
    let needle = eval(item, record, ctx)?;
    match eval(list, record, ctx)? {
        Value::Array(values) => Ok(Value::Bool(values.contains(&needle))),
        other => Err(ExprError::Eval(format!(
            "right side of 'in' must be a list, got {other}"
        ))),
    }
}

/// JSON-style truthiness: `false`/`null` are falsy.
#[must_use]
pub const fn is_truthy(value: &Value) -> bool {
    !matches!(value, Value::Bool(false) | Value::Null)
}

fn resolve_path(record: &Value, segments: &[String]) -> Result<Value, ExprError> {
    let mut current = record;
    for segment in segments {
        current = current
            .as_object()
            .and_then(|object| object.get(segment))
            .ok_or_else(|| ExprError::Eval(format!("field '{segment}' not found")))?;
    }
    Ok(current.clone())
}

#[derive(Clone, Copy)]
enum Arith {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

fn arith(
    lhs: &Expr,
    rhs: &Expr,
    record: &Value,
    ctx: &EvalContext<'_>,
    op: Arith,
) -> Result<Value, ExprError> {
    let left = eval(lhs, record, ctx)?;
    let right = eval(rhs, record, ctx)?;
    if matches!(op, Arith::Add) && (left.is_string() || right.is_string()) {
        return Ok(Value::String(format!(
            "{}{}",
            as_text(&left)?,
            as_text(&right)?
        )));
    }
    let (a, b) = (to_number(&left)?, to_number(&right)?);
    let result = match op {
        Arith::Add => a + b,
        Arith::Sub => a - b,
        Arith::Mul => a * b,
        Arith::Div => a / b,
        Arith::Rem => a % b,
    };
    if !result.is_finite() {
        return Err(ExprError::Eval(
            "arithmetic result is not finite".to_owned(),
        ));
    }
    let value = if result.fract() == 0.0 && result.abs() < 9.0e15 {
        #[allow(clippy::cast_possible_truncation)]
        Value::from(result as i64)
    } else {
        Value::from(result)
    };
    Ok(value)
}

fn as_text(value: &Value) -> Result<String, ExprError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Null => Ok(String::new()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).map_err(|e| ExprError::Eval(e.to_string()))
        }
    }
}

fn call(
    name: &str,
    args: &[Expr],
    record: &Value,
    ctx: &EvalContext<'_>,
) -> Result<Value, ExprError> {
    let values = args
        .iter()
        .map(|arg| eval(arg, record, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    call_with(name, values, ctx)
}

fn call_with(
    name: &str,
    mut values: Vec<Value>,
    ctx: &EvalContext<'_>,
) -> Result<Value, ExprError> {
    let one = |values: &mut Vec<Value>| values.pop().ok_or_else(no_arg);
    match name {
        "int" => Ok(Value::from(to_i64(to_number(&one(&mut values)?)?))),
        "float" => Ok(Value::from(to_number(&one(&mut values)?)?)),
        "str" => Ok(Value::String(as_text(&one(&mut values)?)?)),
        "len" => Ok(Value::from(len(&one(&mut values)?)?)),
        "now" => call_now(&values),
        "uuid" => call_uuid(&values),
        "env" => call_env(one(&mut values)?),
        "var" => call_var(one(&mut values)?, ctx),
        other => Err(ExprError::Eval(format!("unknown function '{other}'"))),
    }
}

fn call_now(values: &[Value]) -> Result<Value, ExprError> {
    if !values.is_empty() {
        return Err(ExprError::Eval("now() takes no arguments".to_owned()));
    }
    Ok(Value::String(chrono::Utc::now().to_rfc3339()))
}

fn call_uuid(values: &[Value]) -> Result<Value, ExprError> {
    if !values.is_empty() {
        return Err(ExprError::Eval("uuid() takes no arguments".to_owned()));
    }
    Ok(Value::String(uuid::Uuid::new_v4().to_string()))
}

fn call_env(value: Value) -> Result<Value, ExprError> {
    match value {
        Value::String(name) => std::env::var(&name)
            .map(Value::String)
            .map_err(|_| ExprError::Eval(format!("environment variable '{name}' not set"))),
        other => Err(ExprError::Eval(format!(
            "env() expects a string, got {other}"
        ))),
    }
}

fn call_var(value: Value, ctx: &EvalContext<'_>) -> Result<Value, ExprError> {
    match value {
        Value::String(name) => ctx
            .vars
            .get(&name)
            .cloned()
            .ok_or_else(|| ExprError::Eval(format!("flow variable '{name}' not set"))),
        other => Err(ExprError::Eval(format!(
            "var() expects a string, got {other}"
        ))),
    }
}

fn no_arg() -> ExprError {
    ExprError::Eval("function expects an argument".to_owned())
}

fn len(value: &Value) -> Result<usize, ExprError> {
    match value {
        Value::String(text) => Ok(text.chars().count()),
        Value::Array(items) => Ok(items.len()),
        Value::Object(map) => Ok(map.len()),
        Value::Null => Ok(0),
        other => Err(ExprError::Eval(format!("len() not defined for {other}"))),
    }
}

#[allow(clippy::cast_possible_truncation)]
const fn to_i64(number: f64) -> i64 {
    number.trunc() as i64
}
