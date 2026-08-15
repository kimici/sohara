//! Minimal end-to-end pipeline example (S0 acceptance).
//!
//! Run with: `cargo run --example basic`

use sohara_core::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let pipeline = Pipeline::new("example");
    let stats = pipeline
        .run(&source(), &transforms(), &LogSink::new("output"))
        .await?;
    println!(
        "Processed: {}, Filtered: {}, Errors: {}",
        stats.processed, stats.filtered, stats.errors
    );
    Ok(())
}

fn source() -> VecSource {
    VecSource::new(
        "input",
        vec![
            Record::from_json(serde_json::json!({"name": "Alice", "age": 30})),
            Record::from_json(serde_json::json!({"name": "Bob", "age": 15})),
            Record::from_json(serde_json::json!({"name": "Carol", "age": 40})),
        ],
    )
}

fn transforms() -> Vec<Box<dyn Transform>> {
    vec![
        Box::new(FilterTransform::new("adults", |record| {
            record
                .get("age")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|age| age >= 18)
        })),
        Box::new(AssertTransform::new(
            "validate",
            vec![Assertion::new("name", Check::NotNull)],
        )),
        Box::new(MapTransform::new("add-timestamp", |mut record| {
            record.set(
                "processed_at",
                serde_json::json!(chrono::Utc::now().to_rfc3339()),
            );
            record
        })),
    ]
}
