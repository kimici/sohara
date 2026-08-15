//! Inline source: records declared directly in the YAML

use serde::Deserialize;
use serde_json::Value;
use sohara_core::{BuildContext, BuiltStep, Record, Result, VecSource};

use crate::parse_config;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InlineConfig {
    records: Vec<Value>,
}

/// Factory for the `source.inline` step.
pub struct InlineSource;

impl InlineSource {
    /// Build a `VecSource` from a `records` config array.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: InlineConfig = parse_config(config, "inline source")?;
        let records = cfg.records.into_iter().map(Record::from_json).collect();
        Ok(BuiltStep::Source(Box::new(VecSource::new(
            "inline", records,
        ))))
    }
}
