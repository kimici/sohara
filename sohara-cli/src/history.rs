//! Run-history recording and display (S6)

use std::path::Path;

use anyhow::Result;
use serde::Serialize;
use sohara_runtime::{RunReport, StatsSnapshot, StepStat};

/// One run-history entry (S6).
#[derive(Serialize)]
pub struct HistoryEntry {
    run_id: String,
    flow: String,
    started_at: String,
    finished_at: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    stats: StatsSnapshot,
    steps: Vec<(String, StepStat)>,
}

#[must_use]
pub fn default_history_path() -> &'static Path {
    Path::new(".sohara/history.jsonl")
}

/// A placeholder report for runs that failed before producing one.
#[must_use]
pub fn failed_report(flow: &str, started_at: String) -> RunReport {
    RunReport {
        run_id: format!("failed-{}", uuid::Uuid::new_v4()),
        flow: flow.to_owned(),
        started_at,
        stats: StatsSnapshot::default(),
        steps: Default::default(),
    }
}

/// Append one entry to the history file (creating parent dirs as needed).
pub fn append_history(path: &Path, report: &RunReport, error: Option<String>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let entry = HistoryEntry {
        run_id: report.run_id.clone(),
        flow: report.flow.clone(),
        started_at: report.started_at.clone(),
        finished_at: chrono::Utc::now().to_rfc3339(),
        status: if error.is_some() { "error" } else { "ok" },
        error,
        stats: report.stats,
        steps: report
            .steps
            .iter()
            .map(|(id, stat)| (id.clone(), *stat))
            .collect(),
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    use std::io::Write;
    writeln!(file, "{}", serde_json::to_string(&entry)?)?;
    Ok(())
}

/// Print the most recent run-history entries.
pub fn show_history(path: Option<&Path>, limit: usize) -> Result<()> {
    let path = path.unwrap_or(default_history_path());
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => {
            println!("No history yet at {}", path.display());
            return Ok(());
        }
    };
    println!(
        "{:<8} {:<14} {:<30} {:<10} {:<6} {:<8}",
        "status", "flow", "run_id", "finished", "proc", "errors"
    );
    let lines: Vec<&str> = text.lines().collect();
    for line in lines.into_iter().rev().take(limit).rev() {
        let value: serde_json::Value = serde_json::from_str(line)?;
        let finished = value["finished_at"].as_str().unwrap_or("?");
        println!(
            "{:<8} {:<14} {:<30} {:<10} {:<6} {:<8}",
            value["status"].as_str().unwrap_or("?"),
            value["flow"].as_str().unwrap_or("?"),
            value["run_id"].as_str().unwrap_or("?"),
            truncate(finished, 10),
            value["stats"]["processed"],
            value["stats"]["errors"]
        );
    }
    Ok(())
}

fn truncate(text: &str, max: usize) -> &str {
    let end = text.len().min(max);
    &text[..end]
}
