//! Cron trigger: emit a record on each schedule hit

use std::str::FromStr;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::{Map, Value};
use sohara_core::{Error, Record, Result, Source, Trigger};
use tokio::sync::watch;

/// A source that emits one record per cron schedule hit.
pub struct CronSource {
    name: String,
    schedule: cron::Schedule,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl CronSource {
    /// Build from a cron expression (seconds resolution, e.g. `*/5 * * * * *`).
    pub fn new(expression: &str) -> Result<Self> {
        let schedule = cron::Schedule::from_str(expression).map_err(|error| {
            Error::Config(format!("invalid cron expression '{expression}': {error}"))
        })?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Ok(Self {
            name: format!("cron:{expression}"),
            schedule,
            shutdown_tx,
            shutdown_rx,
        })
    }
}

#[async_trait]
impl Source for CronSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let schedule = self.schedule.clone();
        let shutdown = self.shutdown_rx.clone();
        Ok(Box::pin(futures::stream::unfold((), move |()| {
            let schedule = schedule.clone();
            let mut shutdown = shutdown.clone();
            async move { poll_once(&schedule, &mut shutdown).await }
        })))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Trigger for CronSource {
    async fn stop(&self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }
}

async fn poll_once(
    schedule: &cron::Schedule,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<(Result<Record>, ())> {
    loop {
        let next = next_run(schedule);
        let until = sleep_until(next);
        tokio::select! {
            _ = tokio::time::sleep(until) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return None;
                }
                continue;
            }
        }
        return Some((Ok(record_for(next)), ()));
    }
}

fn next_run(schedule: &cron::Schedule) -> chrono::DateTime<chrono::Utc> {
    schedule
        .upcoming(chrono::Utc)
        .next()
        .expect("a cron schedule always has a next run")
}

fn sleep_until(next: chrono::DateTime<chrono::Utc>) -> std::time::Duration {
    let delta = next - chrono::Utc::now();
    delta.to_std().unwrap_or(std::time::Duration::ZERO)
}

fn record_for(next: chrono::DateTime<chrono::Utc>) -> Record {
    let mut payload = Map::new();
    payload.insert("scheduled_at".to_owned(), Value::String(next.to_rfc3339()));
    Record::new(Value::Object(payload))
}
