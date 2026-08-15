//! Minimal expression language (S1 subset)
//!
//! Used by `where` / `expr` fields in YAML steps. Grammar (loosest to
//! tightest): `or` → `and` → `not` → comparisons (`== != > >= < <=`, `in`)
//! → `+ -` → `* / %` → paths / literals / lists / function calls.
//!
//! Numeric comparisons coerce both sides to numbers when either side is
//! numeric (accepting numeric strings, so CSV string columns work).
//! Complex logic should live in a `QuickJS` `script` step (S5).

pub mod ast;
mod compare;
pub mod eval;
pub mod parser;
mod token;

pub use ast::{CmpOp, Expr};
pub use eval::{eval, is_truthy, EvalContext};
pub use parser::parse;

use crate::error::Error;
use thiserror::Error as ThisError;

/// Errors from parsing or evaluating an expression.
#[derive(Debug, ThisError)]
pub enum ExprError {
    #[error("expression parse error: {0}")]
    Parse(String),
    #[error("expression evaluation error: {0}")]
    Eval(String),
}

impl From<ExprError> for Error {
    fn from(value: ExprError) -> Self {
        Self::Expression(value.to_string())
    }
}
