//! Integration tests for `Record` (JSON payload, dot-path get/set)

use serde_json::json;
use sohara_core::{Record, RecordBuilder};

#[test]
fn record_from_json_roundtrip() {
    let record = Record::from_json(json!({"name": "Alice", "age": 30}));
    assert_eq!(record.get("name"), Some(&json!("Alice")));
    assert_eq!(record.get("age"), Some(&json!(30)));
    assert_eq!(record.to_json(), json!({"name": "Alice", "age": 30}));
    assert!(!record.id.is_empty());
}

#[test]
fn get_supports_dot_paths() {
    let record = Record::from_json(json!({
        "user": { "name": "Alice", "profile": { "city": "Beijing" } }
    }));
    assert_eq!(record.get("user.name"), Some(&json!("Alice")));
    assert_eq!(record.get("user.profile.city"), Some(&json!("Beijing")));
    assert_eq!(record.get("user.missing"), None);
    assert_eq!(record.get("user.name.first"), None); // name is not an object
    assert_eq!(record.get("missing.deep.path"), None);
}

#[test]
fn set_creates_intermediate_objects() {
    let mut record = Record::from_json(json!({"a": 1}));
    record.set("user.profile.city", json!("Shanghai"));
    assert_eq!(
        record.to_json(),
        json!({"a": 1, "user": { "profile": { "city": "Shanghai" } }})
    );

    // Overwrite an existing nested value
    record.set("user.profile.city", json!("Shenzhen"));
    assert_eq!(record.get("user.profile.city"), Some(&json!("Shenzhen")));
}

#[test]
fn set_on_non_object_payload_wraps_into_object() {
    let mut record = Record::new(json!(42));
    record.set("answer", json!(42));
    assert_eq!(record.to_json(), json!({"answer": 42}));
}

#[test]
fn has_checks_path_existence() {
    let record = Record::from_json(json!({"a": {"b": null}}));
    assert!(record.has("a"));
    assert!(record.has("a.b")); // exists even though null
    assert!(!record.has("a.c"));
}

#[test]
fn metadata_and_builder() {
    let record = RecordBuilder::new()
        .id("fixed-id")
        .data(json!({"n": 1}))
        .metadata("source", "test")
        .build();
    assert_eq!(record.id, "fixed-id");
    assert_eq!(
        record.metadata.get("source").map(String::as_str),
        Some("test")
    );
    assert_eq!(record.get("n"), Some(&json!(1)));

    let with_meta = Record::from_json(json!({})).with_metadata("k", "v");
    assert_eq!(with_meta.metadata.get("k").map(String::as_str), Some("v"));
}
