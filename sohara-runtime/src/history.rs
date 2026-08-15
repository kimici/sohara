//! Run-history file helpers for serve mode (D1)

use std::path::Path;

use serde::Serialize;

use sohara_core::{Error, Result};

use crate::stats::{RunReport, StatsSnapshot, StepStat};

#[derive(Serialize)]
struct Entry<'a> {
    run_id: &'a str,
    flow: &'a str,
    started_at: &'a str,
    finished_at: String,
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
    stats: &'a StatsSnapshot,
    steps: Vec<(String, StepStat)>,
}

/// Append one run entry to a JSONL history file (creating parent dirs).
pub fn append(path: &Path, report: &RunReport, status: &str, error: Option<&str>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let entry = Entry {
        run_id: &report.run_id,
        flow: &report.flow,
        started_at: &report.started_at,
        finished_at: chrono::Utc::now().to_rfc3339(),
        status,
        error,
        stats: &report.stats,
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

/// Read the most recent `limit` entries (newest first); missing file is empty.
pub fn read_recent(path: &Path, limit: usize) -> Result<Vec<serde_json::Value>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(Error::Io(error)),
    };
    let mut entries: Vec<serde_json::Value> = text
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    entries.reverse();
    entries.truncate(limit);
    Ok(entries)
}
