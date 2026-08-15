//! Integration tests for pipeline statistics and TransformOutcome semantics

use serde_json::json;
use sohara_core::{
    AddFieldTransform, AssertOnFail, AssertTransform, Assertion, Check, FilterTransform, Pipeline,
    Record, Transform, TransformOutcome, VecSink, VecSource,
};

fn source() -> VecSource {
    VecSource::new(
        "input",
        vec![
            Record::from_json(json!({"name": "Alice", "age": 30})),
            Record::from_json(json!({"name": "Bob", "age": 15})),
            Record::from_json(json!({"name": "Carol", "age": 40})),
        ],
    )
}

fn only(transform: Box<dyn Transform>) -> Vec<Box<dyn Transform>> {
    vec![transform]
}

#[tokio::test]
async fn run_counts_processed_and_filtered() {
    let pipeline = Pipeline::new("test");
    let adult = FilterTransform::new("adult", |r| {
        r.get("age").and_then(serde_json::Value::as_i64) >= Some(18)
    });
    let sink = VecSink::new("out");

    let stats = pipeline
        .run(&source(), &only(Box::new(adult)), &sink)
        .await
        .unwrap();

    assert_eq!(stats.processed, 2);
    assert_eq!(stats.filtered, 1);
    assert_eq!(stats.errors, 0);
    assert_eq!(sink.into_records().len(), 2);
}

#[tokio::test]
async fn failing_transform_counts_errors_and_stops_record() {
    let pipeline = Pipeline::new("test");
    let validate = AssertTransform::new(
        "validate",
        vec![Assertion::new("age", Check::Gt(100.0))], // nobody passes
    );
    let sink = VecSink::new("out");

    let stats = pipeline
        .run(&source(), &only(Box::new(validate)), &sink)
        .await
        .unwrap();

    assert_eq!(stats.errors, 3);
    assert_eq!(stats.processed, 0);
    assert_eq!(sink.into_records().len(), 0);
}

#[tokio::test]
async fn assert_on_fail_filter_counts_filtered() {
    let pipeline = Pipeline::new("test");
    let validate = AssertTransform::new("validate", vec![Assertion::new("age", Check::Gte(18.0))])
        .on_fail(AssertOnFail::Filter);
    let sink = VecSink::new("out");

    let stats = pipeline
        .run(&source(), &only(Box::new(validate)), &sink)
        .await
        .unwrap();

    assert_eq!(stats.processed, 2);
    assert_eq!(stats.filtered, 1);
    assert_eq!(stats.errors, 0);
}

#[tokio::test]
async fn expand_records_flow_through_downstream_transforms() {
    let pipeline = Pipeline::new("test");
    let expand: Box<dyn Transform> = Box::new(ExpandTransform);
    let tag = AddFieldTransform::new("tag", "tagged", json!(true));
    let transforms: Vec<Box<dyn Transform>> = vec![expand, Box::new(tag)];
    let sink = VecSink::new("out");

    let stats = pipeline.run(&source(), &transforms, &sink).await.unwrap();

    assert_eq!(stats.processed, 6);
    assert_eq!(stats.filtered, 0);
    assert_eq!(stats.errors, 0);
    for record in sink.into_records() {
        assert_eq!(record.get("tagged"), Some(&json!(true)));
    }
}

#[tokio::test]
async fn run_collect_collects_surviving_records() {
    let pipeline = Pipeline::new("test");
    let adult = FilterTransform::new("adult", |r| {
        r.get("age").and_then(serde_json::Value::as_i64) >= Some(18)
    });

    let records = pipeline
        .run_collect(&source(), &only(Box::new(adult)))
        .await
        .unwrap();

    assert_eq!(records.len(), 2);
    assert!(records
        .iter()
        .all(|r| r.get("age").and_then(serde_json::Value::as_i64) >= Some(18)));
}

/// A transform that expands every record into two copies.
struct ExpandTransform;

#[async_trait::async_trait]
impl Transform for ExpandTransform {
    async fn transform(&self, record: Record) -> sohara_core::Result<TransformOutcome> {
        Ok(TransformOutcome::Expand(vec![record.clone(), record]))
    }

    fn name(&self) -> &str {
        "expand"
    }
}
