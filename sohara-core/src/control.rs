//! Control-flow step definitions, interpreted by the graph executor

use std::time::Duration;

use crate::expr::Expr;

/// One case of a `switch` step: route to `to` when `when` holds.
#[derive(Debug, Clone)]
pub struct SwitchCase {
    pub when: Expr,
    pub to: String,
}

/// How a `join` step releases records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JoinMode {
    /// Wait until every incoming branch delivered for a correlation.
    #[default]
    All,
    /// Release the first record per correlation (union).
    Any,
    /// Wait until N records arrived for a correlation.
    N,
}

/// A control step, interpreted by the runtime's graph executor.
///
/// `switch`/`parallel`/`loop` route explicitly; `foreach`/`delay`/`batch`/
/// `join` continue along their graph successors.
#[derive(Debug, Clone)]
pub enum ControlNode {
    Switch {
        cases: Vec<SwitchCase>,
        default: String,
    },
    Foreach {
        over: Expr,
        as_field: Option<String>,
        max_iterations: usize,
    },
    Loop {
        while_expr: Expr,
        max_iterations: usize,
        body: Option<String>,
    },
    Parallel {
        branches: Vec<String>,
    },
    Join {
        mode: JoinMode,
        n: usize,
    },
    Delay {
        duration: Duration,
    },
    Batch {
        size: Option<usize>,
        within: Option<Duration>,
    },
    /// Update and persist the node's accumulated state from expressions.
    State {
        exprs: Vec<(String, Expr)>,
    },
    /// Park records for human approval (S4).
    Approve {
        title: String,
    },
}
