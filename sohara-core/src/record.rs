//! Generic JSON record type for data interchange

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// A record that flows through pipelines.
///
/// S0 data model: the payload is a plain JSON value (single path); a typed
/// `Schema` is a later (S5) optional enhancement that does not replace it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Unique identifier for this record
    pub id: String,
    /// Timestamp when the record was created
    pub timestamp: DateTime<Utc>,
    /// The actual JSON payload
    pub payload: Value,
    /// Optional metadata
    pub metadata: HashMap<String, String>,
}

impl Record {
    /// Create a new record with the given JSON payload
    pub fn new(data: impl Into<Value>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            payload: data.into(),
            metadata: HashMap::new(),
        }
    }

    /// Create a record from a JSON value (alias of [`Record::new`])
    #[must_use]
    pub fn from_json(value: Value) -> Self {
        Self::new(value)
    }

    /// Get a field by dot path (e.g. `"user.name"`), returns `None` if any
    /// segment is missing or not an object.
    #[must_use]
    pub fn get(&self, path: &str) -> Option<&Value> {
        get_path(&self.payload, path)
    }

    /// Set a field by dot path, creating intermediate objects as needed.
    /// A non-object payload is replaced by a new object before insertion.
    pub fn set(&mut self, path: impl Into<String>, value: Value) {
        set_path(&mut self.payload, &path.into(), value);
    }

    /// Whether the dot path exists (and is not `null`? no - only existence).
    #[must_use]
    pub fn has(&self, path: &str) -> bool {
        self.get(path).is_some()
    }

    /// Add metadata to the record
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Convert to a JSON value (clone of the payload)
    #[must_use]
    pub fn to_json(&self) -> Value {
        self.payload.clone()
    }
}

/// Navigate a dot path on a JSON value.
fn get_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = root;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

/// Insert a value at a dot path, creating intermediate objects as needed.
fn set_path(root: &mut Value, path: &str, value: Value) {
    let mut parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        *root = value;
        return;
    }
    if !root.is_object() {
        *root = Value::Object(Map::new());
    }
    let last = parts.pop().unwrap_or_default();
    let mut current = root;
    for part in parts {
        let Some(obj) = current.as_object_mut() else {
            return;
        };
        current = obj
            .entry(part.to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    if let Some(obj) = current.as_object_mut() {
        obj.insert(last.to_owned(), value);
    }
}

/// Builder for creating records
pub struct RecordBuilder {
    id: Option<String>,
    data: Option<Value>,
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
    pub fn data(mut self, data: impl Into<Value>) -> Self {
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
            payload: self.data.unwrap_or_else(|| Value::Object(Map::new())),
            metadata: self.metadata,
        }
    }
}

impl Default for RecordBuilder {
    fn default() -> Self {
        Self::new()
    }
}
