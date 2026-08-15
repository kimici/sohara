//! Comparison and numeric coercion for expressions

use serde_json::Value;

use super::ast::CmpOp;
use super::ExprError;

pub fn compare(op: CmpOp, lhs: &Value, rhs: &Value) -> std::result::Result<Value, ExprError> {
    let ordering = match numeric_pair(lhs, rhs)? {
        Some((a, b)) => a.partial_cmp(&b),
        None if lhs.is_string() && rhs.is_string() => match (lhs.as_str(), rhs.as_str()) {
            (Some(a), Some(b)) => Some(a.cmp(b)),
            _ => None,
        },
        None => None,
    };
    let result = match (op, ordering) {
        (CmpOp::Eq, Some(ordering)) => ordering.is_eq(),
        (CmpOp::Eq, None) => lhs == rhs,
        (CmpOp::Neq, Some(ordering)) => !ordering.is_eq(),
        (CmpOp::Neq, None) => lhs != rhs,
        (CmpOp::Gt, Some(ordering)) => ordering.is_gt(),
        (CmpOp::Gte, Some(ordering)) => ordering.is_ge(),
        (CmpOp::Lt, Some(ordering)) => ordering.is_lt(),
        (CmpOp::Lte, Some(ordering)) => ordering.is_le(),
        (_, None) => {
            return Err(ExprError::Eval(format!("cannot compare {lhs} and {rhs}")));
        }
    };
    Ok(Value::Bool(result))
}

/// Coerce to numbers when either side is numeric (accepting numeric strings).
pub fn numeric_pair(
    lhs: &Value,
    rhs: &Value,
) -> std::result::Result<Option<(f64, f64)>, ExprError> {
    if !is_numericish(lhs) && !is_numericish(rhs) {
        return Ok(None);
    }
    let a = to_number(lhs)?;
    let b = to_number(rhs)?;
    Ok(Some((a, b)))
}

fn is_numericish(value: &Value) -> bool {
    value.as_f64().is_some()
        || value
            .as_str()
            .is_some_and(|s| s.trim().parse::<f64>().is_ok())
}

pub fn to_number(value: &Value) -> std::result::Result<f64, ExprError> {
    if let Some(number) = value.as_f64() {
        return Ok(number);
    }
    if let Some(text) = value.as_str() {
        return text
            .trim()
            .parse::<f64>()
            .map_err(|_| ExprError::Eval(format!("'{text}' is not a number")));
    }
    Err(ExprError::Eval(format!("{value} is not a number")))
}
