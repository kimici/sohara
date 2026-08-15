//! Statistics, run reports, and executor options

use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use serde::Serialize;
use sohara_core::StateStore;

use crate::pause::PauseGate;

/// Copy of the executor counters for reporting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct StatsSnapshot {
    pub processed: usize,
    pub filtered: usize,
    pub errors: usize,
    /// Records parked by `approve` steps.
    pub waiting: usize,
    /// Records skipped because they were already delivered (resume).
    pub duplicates: usize,
}

/// Per-step counters and cumulative execution time (S6).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct StepStat {
    /// Records this step processed successfully.
    pub processed: u64,
    /// Records this step filtered out.
    pub filtered: u64,
    /// Failures attributed to this step.
    pub errors: u64,
    /// Cumulative execution time in nanoseconds.
    pub nanos: u64,
}

/// A complete run summary for history and metrics (S6).
#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    pub run_id: String,
    pub flow: String,
    pub started_at: String,
    pub stats: StatsSnapshot,
    pub steps: BTreeMap<String, StepStat>,
}

#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) processed: AtomicUsize,
    pub(crate) filtered: AtomicUsize,
    pub(crate) errors: AtomicUsize,
    pub(crate) waiting: AtomicUsize,
    pub(crate) duplicates: AtomicUsize,
}

/// Executor options for persistence, recovery, and pause control.
#[derive(Default)]
pub struct ExecutorConfig {
    pub store: Option<Arc<dyn StateStore>>,
    pub resume: bool,
    pub checkpoint_every: Option<u64>,
    /// Optional cooperative pause gate for serve-mode admin control (S6).
    pub pause: Option<Arc<PauseGate>>,
}
