//! Run-report printing helpers (S6/S7)

use sohara_runtime::{RunReport, StatsSnapshot};

/// Print the one-line run summary.
pub fn print_stats(action: &str, name: &str, stats: &StatsSnapshot) {
    println!(
        "Flow '{name}' {action}: processed={}, filtered={}, errors={}, waiting={}, duplicates={}",
        stats.processed, stats.filtered, stats.errors, stats.waiting, stats.duplicates
    );
}

/// Print the per-step statistics table (`--verbose`).
pub fn print_steps(report: &RunReport) {
    println!(
        "{:<16} {:>10} {:>10} {:>8} {:>12}",
        "step", "processed", "filtered", "errors", "ms"
    );
    for (id, stat) in &report.steps {
        let millis = millis(stat.nanos);
        println!(
            "{id:<16} {:>10} {:>10} {:>8} {:>12}",
            stat.processed, stat.filtered, stat.errors, millis
        );
    }
}

fn millis(nanos: u64) -> String {
    format!("{:.2}", nanos as f64 / 1_000_000.0)
}
