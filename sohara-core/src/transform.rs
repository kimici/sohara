//! Transform trait - data transformers that modify records

use async_trait::async_trait;
use serde_json::Value;

use crate::record::Record;
use crate::{Error, Result};

/// Outcome of applying a transform to a record.
#[derive(Debug)]
pub enum TransformOutcome {
    /// The record continues downstream.
    Pass(Record),
    /// The record is filtered out (counted as `filtered`).
    Filtered,
    /// One-to-many: several records continue downstream (split / flat-map).
    Expand(Vec<Record>),
    /// Logical step failure (counted as `errors`, handled by `on_error`).
    Fail(Error),
}

/// A transform that modifies records.
#[async_trait]
pub trait Transform: Send + Sync {
    /// Transform a single record.
    async fn transform(&self, record: Record) -> Result<TransformOutcome>;

    /// Name of this transform for logging/debugging.
    fn name(&self) -> &str;
}

/// A transform that filters records based on a predicate.
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
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        if (self.predicate)(&record) {
            Ok(TransformOutcome::Pass(record))
        } else {
            Ok(TransformOutcome::Filtered)
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A transform that maps records through a function.
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
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        Ok(TransformOutcome::Pass((self.mapper)(record)))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A transform that adds a field to each record.
pub struct AddFieldTransform {
    name: String,
    field: String,
    value: Value,
}

impl AddFieldTransform {
    pub fn new(name: impl Into<String>, field: impl Into<String>, value: Value) -> Self {
        Self {
            name: name.into(),
            field: field.into(),
            value,
        }
    }
}

#[async_trait]
impl Transform for AddFieldTransform {
    async fn transform(&self, mut record: Record) -> Result<TransformOutcome> {
        record.set(&self.field, self.value.clone());
        Ok(TransformOutcome::Pass(record))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A single field check used by [`AssertTransform`].
#[derive(Debug, Clone)]
pub enum Check {
    /// Value equals the given JSON value.
    Eq(Value),
    /// Value does not equal the given JSON value.
    Neq(Value),
    /// Numeric value is greater than the threshold.
    Gt(f64),
    /// Numeric value is greater than or equal to the threshold.
    Gte(f64),
    /// Numeric value is less than the threshold.
    Lt(f64),
    /// Numeric value is less than or equal to the threshold.
    Lte(f64),
    /// The field exists.
    Exists,
    /// The field exists and is not JSON `null`.
    NotNull,
    /// The field exists and is JSON `null`.
    Null,
    /// Value is one of the given JSON values.
    In(Vec<Value>),
    /// String contains the substring, or array contains the value.
    Contains(Value),
}

/// A field assertion: `field` checked against `check`.
#[derive(Debug, Clone)]
pub struct Assertion {
    pub field: String,
    pub check: Check,
    pub message: Option<String>,
}

impl Assertion {
    pub fn new(field: impl Into<String>, check: Check) -> Self {
        Self {
            field: field.into(),
            check,
            message: None,
        }
    }

    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}

/// What happens when an assertion fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AssertOnFail {
    /// The step fails (counted as `errors`).
    #[default]
    Fail,
    /// The record is filtered out (counted as `filtered`).
    Filter,
}

/// A transform that validates records against a list of assertions.
pub struct AssertTransform {
    name: String,
    assertions: Vec<Assertion>,
    on_fail: AssertOnFail,
}

impl AssertTransform {
    pub fn new(name: impl Into<String>, assertions: Vec<Assertion>) -> Self {
        Self {
            name: name.into(),
            assertions,
            on_fail: AssertOnFail::default(),
        }
    }

    #[must_use]
    pub const fn on_fail(mut self, on_fail: AssertOnFail) -> Self {
        self.on_fail = on_fail;
        self
    }

    fn failed_outcome(&self, assertion: &Assertion) -> TransformOutcome {
        let message = assertion.message.clone().unwrap_or_else(|| {
            format!(
                "assertion failed: field '{}' check '{:?}'",
                assertion.field, assertion.check
            )
        });
        match self.on_fail {
            AssertOnFail::Fail => TransformOutcome::Fail(Error::Assertion(message)),
            AssertOnFail::Filter => TransformOutcome::Filtered,
        }
    }
}

impl AssertTransform {
    fn check_value(check: &Check, value: Option<&Value>) -> bool {
        match check {
            Check::Exists => value.is_some(),
            Check::NotNull => value.is_some_and(|v| !v.is_null()),
            Check::Eq(expected) => value.is_some_and(|v| v == expected),
            Check::Neq(expected) => value.is_some_and(|v| v != expected),
            Check::Gt(threshold) => value.and_then(to_f64).is_some_and(|n| n > *threshold),
            Check::Gte(threshold) => value.and_then(to_f64).is_some_and(|n| n >= *threshold),
            Check::Lt(threshold) => value.and_then(to_f64).is_some_and(|n| n < *threshold),
            Check::Lte(threshold) => value.and_then(to_f64).is_some_and(|n| n <= *threshold),
            Check::Null => value.is_some_and(Value::is_null),
            Check::In(expected) => value.is_some_and(|v| expected.contains(v)),
            Check::Contains(needle) => match (value, needle.as_str()) {
                (Some(Value::String(text)), Some(needle)) => text.contains(needle),
                (Some(Value::Array(items)), _) => items.contains(needle),
                _ => false,
            },
        }
    }
}

/// Numeric coercion accepting numeric strings (CSV columns are strings).
fn to_f64(value: &Value) -> Option<f64> {
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str()?.trim().parse().ok()
}

#[async_trait]
impl Transform for AssertTransform {
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        for assertion in &self.assertions {
            if !Self::check_value(&assertion.check, record.get(&assertion.field)) {
                return Ok(self.failed_outcome(assertion));
            }
        }
        Ok(TransformOutcome::Pass(record))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Transform for Box<dyn Transform> {
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        (**self).transform(record).await
    }

    fn name(&self) -> &str {
        (**self).name()
    }
}
