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
    List(Vec<Expr>),
    /// Logical negation
    Not(Box<Expr>),
    /// Logical and
    And(Box<Expr>, Box<Expr>),
    /// Logical or
    Or(Box<Expr>, Box<Expr>),
    /// Comparison
    Cmp(CmpOp, Box<Expr>, Box<Expr>),
    /// Membership test: `item in list`
    In(Box<Expr>, Box<Expr>),
    /// Arithmetic
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Rem(Box<Expr>, Box<Expr>),
    /// Function call, e.g. `int(age)` or `now()`
    Call(String, Vec<Expr>),
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
