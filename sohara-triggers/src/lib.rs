//! Event-driven triggers for serve mode: http / cron / queue

pub mod bus;
pub mod cron;
pub mod http;
pub mod queue;
pub mod relay;

pub use bus::InProcessBus;
pub use cron::CronSource;
pub use http::HttpSource;
pub use queue::QueueSource;
pub use relay::RelayBus;

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sohara_config::TriggerConfig;
use sohara_core::{Error, Result, Trigger};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpConfig {
    method: String,
    path: String,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    port: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CronConfig {
    expression: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueConfig {
    topic: String,
}

/// Build a trigger instance from its config declaration.
///
/// # Errors
/// Fails on unknown trigger types or invalid per-type config.
pub fn build_trigger(
    config: &TriggerConfig,
    bus: Option<Arc<InProcessBus>>,
) -> Result<Arc<dyn Trigger>> {
    let raw = config
        .config()
        .map_err(|error| Error::Config(error.to_string()))?;
    match config.trigger_type.as_str() {
        "http" => {
            let cfg: HttpConfig = parse(&raw, "http trigger")?;
            let trigger = HttpSource::new(
                &cfg.method,
                &cfg.path,
                &cfg.host.unwrap_or_else(|| "127.0.0.1".to_owned()),
                cfg.port.unwrap_or(9527),
            );
            Ok(Arc::new(trigger))
        }
        "cron" => {
            let cfg: CronConfig = parse(&raw, "cron trigger")?;
            Ok(Arc::new(CronSource::new(&cfg.expression)?))
        }
        "queue" => {
            let cfg: QueueConfig = parse(&raw, "queue trigger")?;
            let bus = bus.ok_or_else(|| {
                Error::Config("queue trigger needs an event bus (serve mode)".to_owned())
            })?;
            Ok(Arc::new(QueueSource::new(&cfg.topic, &bus)))
        }
        other => Err(Error::Config(format!(
            "unknown trigger type '{other}' (supported: http | cron | queue)"
        ))),
    }
}

fn parse<C: serde::de::DeserializeOwned>(config: &Map<String, Value>, what: &str) -> Result<C> {
    serde_json::from_value(Value::Object(config.clone()))
        .map_err(|error| Error::Config(format!("{what}: {error}")))
}
