//! Single-machine `StateStore` implementations: memory and JSON file

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use serde_json::Value;
use sohara_core::{Result, StateStore};

/// In-memory store (useful for tests and non-durable runs).
#[derive(Default)]
pub struct MemoryStore {
    entries: Mutex<HashMap<String, Value>>,
}

impl MemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of all entries (test helper).
    #[must_use]
    pub fn entries(&self) -> HashMap<String, Value> {
        self.entries.lock().expect("store lock poisoned").clone()
    }
}

impl StateStore for MemoryStore {
    fn load(&self, key: &str) -> Result<Option<Value>> {
        Ok(self
            .entries
            .lock()
            .expect("store lock poisoned")
            .get(key)
            .cloned())
    }

    fn save(&self, key: &str, value: Value) -> Result<()> {
        self.entries
            .lock()
            .expect("store lock poisoned")
            .insert(key.to_owned(), value);
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.entries
            .lock()
            .expect("store lock poisoned")
            .remove(key);
        Ok(())
    }
}

/// Durable JSON-file store: every save rewrites the file atomically.
pub struct JsonFileStore {
    path: PathBuf,
    entries: Mutex<HashMap<String, Value>>,
}

impl JsonFileStore {
    /// Open (or create) the store file.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let entries = match std::fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
            Err(_) => HashMap::new(),
        };
        Ok(Self {
            path,
            entries: Mutex::new(entries),
        })
    }

    fn persist(&self) -> Result<()> {
        let data = serde_json::to_string_pretty(&*self.entries.lock().expect("lock poisoned"))
            .map_err(sohara_core::Error::Serialization)?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(sohara_core::Error::Io)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, data).map_err(sohara_core::Error::Io)?;
        std::fs::rename(&tmp, &self.path).map_err(sohara_core::Error::Io)?;
        Ok(())
    }
}

impl StateStore for JsonFileStore {
    fn load(&self, key: &str) -> Result<Option<Value>> {
        Ok(self
            .entries
            .lock()
            .expect("store lock poisoned")
            .get(key)
            .cloned())
    }

    fn save(&self, key: &str, value: Value) -> Result<()> {
        self.entries
            .lock()
            .expect("store lock poisoned")
            .insert(key.to_owned(), value);
        self.persist()
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.entries
            .lock()
            .expect("store lock poisoned")
            .remove(key);
        self.persist()
    }
}
