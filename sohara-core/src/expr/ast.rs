//! Expression AST types (S1 minimal subset)

use serde_json::Value;

/// A parsed expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// JSON literal (number / string / bool / null)
    Lit(Value),
    /// Dotted field path relative to the record, e.g. `a.b.c` or `$.a.b`
    Path(Vec<String>),
    /// List literal, e.g. `[1, "a"]`
    List(Vec<Self>),
    /// Logical negation
    Not(Box<Self>),
    /// Logical and
    And(Box<Self>, Box<Self>),
    /// Logical or
    Or(Box<Self>, Box<Self>),
    /// Comparison
    Cmp(CmpOp, Box<Self>, Box<Self>),
    /// Membership test: `item in list`
    In(Box<Self>, Box<Self>),
    /// Arithmetic
    Add(Box<Self>, Box<Self>),
    Sub(Box<Self>, Box<Self>),
    Mul(Box<Self>, Box<Self>),
    Div(Box<Self>, Box<Self>),
    Rem(Box<Self>, Box<Self>),
    /// Function call, e.g. `int(age)` or `now()`
    Call(String, Vec<Self>),
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `==`
    Eq,
    /// `!=`
    Neq,
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,
}
