//! Transform trait - data transformers that modify records

use async_trait::async_trait;

use crate::record::Record;
use crate::Result;

/// A transform that modifies records
#[async_trait]
pub trait Transform: Send + Sync {
    /// Transform a single record
    async fn transform(&self, record: Record) -> Result<Record>;

    /// Name of this transform for logging/debugging
    fn name(&self) -> &str;
}

/// A transform that filters records based on a predicate
pub struct FilterTransform<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    name: String,
    predicate: F,
}

impl<F> FilterTransform<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    pub fn new(name: impl Into<String>, predicate: F) -> Self {
        Self {
            name: name.into(),
            predicate,
        }
    }
}

#[async_trait]
impl<F> Transform for FilterTransform<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    async fn transform(&self, record: Record) -> Result<Record> {
        if (self.predicate)(&record) {
            Ok(record)
        } else {
            // Return an error to signal the record should be filtered out
            Err(crate::Error::Transform("Record filtered out".to_string()))
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A transform that maps records through a function
pub struct MapTransform<F>
where
    F: Fn(Record) -> Record + Send + Sync,
{
    name: String,
    mapper: F,
}

impl<F> MapTransform<F>
where
    F: Fn(Record) -> Record + Send + Sync,
{
    pub fn new(name: impl Into<String>, mapper: F) -> Self {
        Self {
            name: name.into(),
            mapper,
        }
    }
}

#[async_trait]
impl<F> Transform for MapTransform<F>
where
    F: Fn(Record) -> Record + Send + Sync,
{
    async fn transform(&self, record: Record) -> Result<Record> {
        Ok((self.mapper)(record))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A transform that adds a field to each record
pub struct AddFieldTransform {
    name: String,
    field: String,
    value: serde_json::Value,
}

impl AddFieldTransform {
    pub fn new(
        name: impl Into<String>,
        field: impl Into<String>,
        value: serde_json::Value,
    ) -> Self {
        Self {
            name: name.into(),
            field: field.into(),
            value,
        }
    }
}

#[async_trait]
impl Transform for AddFieldTransform {
    async fn transform(&self, mut record: Record) -> Result<Record> {
        record.set(&self.field, self.value.clone());
        Ok(record)
    }

    fn name(&self) -> &str {
        &self.name
    }
}
