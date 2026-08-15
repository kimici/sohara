//! Integration tests for the built-in transforms

use serde_json::json;
use sohara_core::{
    AddFieldTransform, AssertOnFail, AssertTransform, Assertion, Check, Error, FilterTransform,
    MapTransform, Record, Transform, TransformOutcome,
};

fn record(age: i64) -> Record {
    Record::from_json(json!({"name": "Alice", "age": age}))
}

#[tokio::test]
async fn filter_passes_or_filters() {
    let adult = FilterTransform::new("adult", |r| {
        r.get("age").and_then(serde_json::Value::as_i64) >= Some(18)
    });
    assert!(matches!(
        adult.transform(record(30)).await.unwrap(),
        TransformOutcome::Pass(_)
    ));
    assert!(matches!(
        adult.transform(record(15)).await.unwrap(),
        TransformOutcome::Filtered
    ));
}

#[tokio::test]
async fn map_transforms_record() {
    let upper = MapTransform::new("upper", |mut r| {
        r.set("name", json!("BOB"));
        r
    });
    match upper.transform(record(20)).await.unwrap() {
        TransformOutcome::Pass(r) => assert_eq!(r.get("name"), Some(&json!("BOB"))),
        other => panic!("expected Pass, got {other:?}"),
    }
}

#[tokio::test]
async fn add_field_sets_value() {
    let add = AddFieldTransform::new("add", "region", json!("cn"));
    match add.transform(record(20)).await.unwrap() {
        TransformOutcome::Pass(r) => assert_eq!(r.get("region"), Some(&json!("cn"))),
        other => panic!("expected Pass, got {other:?}"),
    }
}

#[tokio::test]
async fn assert_passes_when_all_checks_hold() {
    let assertions = vec![
        Assertion::new("name", Check::NotNull),
        Assertion::new("age", Check::Gte(18.0)),
    ];
    let assert = AssertTransform::new("validate", assertions);
    assert!(matches!(
        assert.transform(record(25)).await.unwrap(),
        TransformOutcome::Pass(_)
    ));
}

#[tokio::test]
async fn assert_fail_becomes_fail_outcome() {
    let assertions = vec![Assertion::new("age", Check::Lt(18.0))];
    let assert = AssertTransform::new("validate", assertions);
    match assert.transform(record(25)).await.unwrap() {
        TransformOutcome::Fail(Error::Assertion(message)) => {
            assert!(message.contains("age"), "unexpected message: {message}");
        }
        other => panic!("expected Fail, got {other:?}"),
    }
}

#[tokio::test]
async fn assert_filter_drops_record() {
    let assertions = vec![Assertion::new("age", Check::Lt(18.0))];
    let assert = AssertTransform::new("validate", assertions).on_fail(AssertOnFail::Filter);
    assert!(matches!(
        assert.transform(record(25)).await.unwrap(),
        TransformOutcome::Filtered
    ));
}

#[tokio::test]
async fn assert_custom_message_is_used() {
    let assertions = vec![Assertion::new("age", Check::Eq(json!(1))).with_message("age must be 1")];
    let assert = AssertTransform::new("validate", assertions);
    match assert.transform(record(2)).await.unwrap() {
        TransformOutcome::Fail(Error::Assertion(message)) => assert_eq!(message, "age must be 1"),
        other => panic!("expected Fail, got {other:?}"),
    }
}
