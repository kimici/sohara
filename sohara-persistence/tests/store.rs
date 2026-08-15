//! Tests for the single-machine StateStore implementations

use std::path::PathBuf;

use serde_json::json;
use sohara_core::StateStore;
use sohara_persistence::{JsonFileStore, MemoryStore};

#[test]
fn memory_store_roundtrips() {
    let store = MemoryStore::new();
    assert_eq!(store.load("a").unwrap(), None);
    store.save("a", json!({"n": 1})).unwrap();
    assert_eq!(store.load("a").unwrap(), Some(json!({"n": 1})));
    store.delete("a").unwrap();
    assert_eq!(store.load("a").unwrap(), None);
}

#[test]
fn json_file_store_survives_reopen() {
    let path = temp_path("json-store");
    {
        let store = JsonFileStore::new(&path).unwrap();
        store.save("state", json!({"count": 3})).unwrap();
    }
    let store = JsonFileStore::new(&path).unwrap();
    assert_eq!(store.load("state").unwrap(), Some(json!({"count": 3})));
    store.delete("state").unwrap();
    assert_eq!(store.load("state").unwrap(), None);
    std::fs::remove_file(&path).ok();
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("sohara-{name}-{:?}.json", std::process::id()))
}
