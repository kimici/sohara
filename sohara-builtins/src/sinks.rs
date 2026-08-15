//! Declarative sink steps (log / noop / collect) plus a fan-out sink

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use sohara_core::{BuildContext, BuiltStep, Error, EventBus, Record, Result, Sink};

use crate::parse_config;

#[derive(Debug, Clone, Copy)]
enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => Err(Error::Config(format!(
                "log level '{other}' not supported (debug|info|warn|error)"
            ))),
        }
    }

    fn log(self, record: &Record) {
        let id = &record.id;
        match self {
            Self::Debug => tracing::debug!(record = ?record.payload, "record {id}"),
            Self::Info => tracing::info!(record = ?record.payload, "record {id}"),
            Self::Warn => tracing::warn!(record = ?record.payload, "record {id}"),
            Self::Error => tracing::error!(record = ?record.payload, "record {id}"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSinkConfig {
    #[serde(default)]
    level: Option<String>,
}

/// `sink.log` step: emit records via tracing at the configured level.
pub struct LogSink {
    name: String,
    level: Level,
}

impl LogSink {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: LogSinkConfig = parse_config(config, "log sink")?;
        let level = match cfg.level.as_deref() {
            None => Level::Info,
            Some(value) => Level::parse(value)?,
        };
        Ok(BuiltStep::Sink(Box::new(Self {
            name: "log".to_owned(),
            level,
        })))
    }
}

#[async_trait]
impl Sink for LogSink {
    async fn send(&self, record: Record) -> Result<()> {
        self.level.log(&record);
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoopConfig {}

/// `sink.noop` step: discard all records.
pub struct NoopSink {
    name: String,
}

impl NoopSink {
    /// Build the step from config (accepts no fields).
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let _: NoopConfig = parse_config(config, "noop sink")?;
        Ok(BuiltStep::Sink(Box::new(Self {
            name: "noop".to_owned(),
        })))
    }
}

#[async_trait]
impl Sink for NoopSink {
    async fn send(&self, _record: Record) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectConfig {}
/// `sink.collect` step: collect records in memory (for tests and debugging).
pub struct CollectSink {
    name: String,
    records: tokio::sync::Mutex<Vec<Record>>,
}

impl CollectSink {
    /// Build the step from config (accepts no fields).
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let _: CollectConfig = parse_config(config, "collect sink")?;
        Ok(BuiltStep::Sink(Box::new(Self::new("collect"))))
    }

    /// Create a collect sink directly (used by tests).
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            records: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    /// Take the collected records.
    #[must_use]
    pub fn into_records(self) -> Vec<Record> {
        self.records.into_inner()
    }
}

#[async_trait]
impl Sink for CollectSink {
    async fn send(&self, record: Record) -> Result<()> {
        self.records.lock().await.push(record);
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Fan a record out to every inner sink (used when a flow declares several
/// sinks; the first error wins, all sinks still receive the record).
pub struct FanoutSink {
    name: String,
    sinks: Vec<Box<dyn Sink>>,
}

impl FanoutSink {
    #[must_use]
    pub fn new(sinks: Vec<Box<dyn Sink>>) -> Self {
        Self {
            name: "fanout".to_owned(),
            sinks,
        }
    }
}

#[async_trait]
impl Sink for FanoutSink {
    async fn send(&self, record: Record) -> Result<()> {
        let mut first_error = None;
        for sink in &self.sinks {
            if let Err(error) = sink.send(record.clone()).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn flush(&self) -> Result<()> {
        for sink in &self.sinks {
            sink.flush().await?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueSinkConfig {
    topic: String,
}

/// `sink.queue` step: publish each record payload to the shared event bus.
pub struct QueueSink {
    name: String,
    topic: String,
    bus: Option<std::sync::Arc<dyn EventBus>>,
}

impl QueueSink {
    /// Build the step from config (requires an event bus in the context).
    pub fn build(config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: QueueSinkConfig = parse_config(config, "queue sink")?;
        if ctx.bus.is_none() {
            return Err(Error::Config(
                "queue sink needs an event bus (serve mode)".to_owned(),
            ));
        }
        Ok(BuiltStep::Sink(Box::new(Self {
            name: format!("queue:{}", cfg.topic),
            topic: cfg.topic,
            bus: ctx.bus.clone(),
        })))
    }
}

#[async_trait]
impl Sink for QueueSink {
    async fn send(&self, record: Record) -> Result<()> {
        let bus = self
            .bus
            .as_ref()
            .ok_or_else(|| Error::Config("queue sink has no event bus".to_owned()))?;
        bus.publish(&self.topic, record.to_json())
    }

    fn name(&self) -> &str {
        &self.name
    }
}
