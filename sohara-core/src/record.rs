//! Generic record type for data interchange

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use base64::Engine;

/// A generic record that can hold any structured data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Unique identifier for this record
    pub id: String,
    /// Timestamp when the record was created
    pub timestamp: DateTime<Utc>,
    /// The actual data payload
    pub data: RecordData,
    /// Optional metadata
    pub metadata: HashMap<String, String>,
}

/// The data payload of a record
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RecordData {
    /// JSON object
    Object(serde_json::Map<String, serde_json::Value>),
    /// JSON array
    Array(Vec<serde_json::Value>),
    /// Simple string value
    Text(String),
    /// Binary data (base64 encoded)
    Binary(Vec<u8>),
}

impl Record {
    /// Create a new record with the given data
    pub fn new(data: impl Into<RecordData>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            data: data.into(),
            metadata: HashMap::new(),
        }
    }

    /// Create a record from a JSON value
    #[must_use]
    pub fn from_json(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Object(map) => Self::new(RecordData::Object(map)),
            serde_json::Value::Array(arr) => Self::new(RecordData::Array(arr)),
            serde_json::Value::String(s) => Self::new(RecordData::Text(s)),
            other => Self::new(RecordData::Text(other.to_string())),
        }
    }

    /// Get a field value by name (for object records)
    #[must_use]
    pub fn get(&self, field: &str) -> Option<&serde_json::Value> {
        match &self.data {
            RecordData::Object(map) => map.get(field),
            _ => None,
        }
    }

    /// Set a field value (for object records)
    pub fn set(&mut self, field: impl Into<String>, value: serde_json::Value) {
        if let RecordData::Object(map) = &mut self.data {
            map.insert(field.into(), value);
        } else {
            // Convert to object if not already
            let mut map = serde_json::Map::new();
            map.insert(field.into(), value);
            self.data = RecordData::Object(map);
        }
    }

    /// Add metadata to the record
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Convert to JSON value
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        match &self.data {
            RecordData::Object(map) => serde_json::Value::Object(map.clone()),
            RecordData::Array(arr) => serde_json::Value::Array(arr.clone()),
            RecordData::Text(s) => serde_json::Value::String(s.clone()),
            RecordData::Binary(bytes) => {
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
            }
        }
    }
}

/// Builder for creating records
pub struct RecordBuilder {
    id: Option<String>,
    data: Option<RecordData>,
    metadata: HashMap<String, String>,
}

impl RecordBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: None,
            data: None,
            metadata: HashMap::new(),
        }
    }

    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    #[must_use]
    pub fn data(mut self, data: impl Into<RecordData>) -> Self {
        self.data = Some(data.into());
        self
    }

    #[must_use]
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn build(self) -> Record {
        Record {
            id: self.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            timestamp: Utc::now(),
            data: self
                .data
                .unwrap_or_else(|| RecordData::Object(serde_json::Map::new())),
            metadata: self.metadata,
        }
    }
}

impl Default for RecordBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Conversions
impl From<serde_json::Value> for RecordData {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Object(map) => Self::Object(map),
            serde_json::Value::Array(arr) => Self::Array(arr),
            serde_json::Value::String(s) => Self::Text(s),
            other => Self::Text(other.to_string()),
        }
    }
}

impl From<String> for RecordData {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<&str> for RecordData {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for RecordData {
    fn from(map: serde_json::Map<String, serde_json::Value>) -> Self {
        Self::Object(map)
    }
}

impl From<Vec<serde_json::Value>> for RecordData {
    fn from(arr: Vec<serde_json::Value>) -> Self {
        Self::Array(arr)
    }
}
