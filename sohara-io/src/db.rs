//! SQLite database source and sink (S5)

use async_trait::async_trait;
use futures::stream::BoxStream;
use rusqlite::types::ValueRef;
use serde::Deserialize;
use serde_json::{Map, Value};
use sohara_core::{BuildContext, BuiltStep, Error, Record, Result, Sink, Source};
use tokio::sync::Mutex;

use crate::parse_config;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DbSourceConfig {
    path: String,
    query: String,
}

/// `source.db` step: run a SQL query and emit one record per row.
pub struct DbSource {
    name: String,
    path: String,
    query: String,
}

impl DbSource {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: DbSourceConfig = parse_config(config, "db source")?;
        let step = Self {
            name: format!("db:{}", cfg.path),
            path: cfg.path,
            query: cfg.query,
        };
        Ok(BuiltStep::Source(Box::new(step)))
    }
}

#[async_trait]
impl Source for DbSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let records = tokio::task::spawn_blocking({
            let path = self.path.clone();
            let query = self.query.clone();
            move || query_records(&path, &query)
        })
        .await
        .map_err(|error| Error::Runtime(format!("db task failed: {error}")))??;
        Ok(Box::pin(futures::stream::iter(records.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn query_records(path: &str, query: &str) -> Result<Vec<Record>> {
    let connection = rusqlite::Connection::open(path)
        .map_err(|error| Error::Config(format!("db open failed: {error}")))?;
    let mut statement = connection
        .prepare(query)
        .map_err(|error| Error::Config(format!("db query failed: {error}")))?;
    let rows = statement
        .query_map([], row_to_record)
        .map_err(|error| Error::Config(format!("db query failed: {error}")))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| Error::Config(format!("db row error: {error}")))
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<Record> {
    let mut object = Map::new();
    for index in 0..row.as_ref().column_count() {
        let name = row.as_ref().column_name(index)?.to_owned();
        let value = match row.get_ref(index)? {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(number) => Value::from(number),
            ValueRef::Real(number) => Value::from(number),
            ValueRef::Text(text) => Value::String(String::from_utf8_lossy(text).into_owned()),
            ValueRef::Blob(blob) => Value::String(format!("blob:{}bytes", blob.len())),
        };
        object.insert(name, value);
    }
    Ok(Record::new(Value::Object(object)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DbSinkConfig {
    path: String,
    table: String,
}

/// `sink.db` step: buffer records and insert them into a SQLite table on flush.
pub struct DbSink {
    name: String,
    path: String,
    table: String,
    buffer: Mutex<Vec<Record>>,
}

impl DbSink {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: DbSinkConfig = parse_config(config, "db sink")?;
        let step = Self {
            name: format!("db:{}", cfg.path),
            path: cfg.path,
            table: cfg.table,
            buffer: Mutex::new(Vec::new()),
        };
        Ok(BuiltStep::Sink(Box::new(step)))
    }
}

#[async_trait]
impl Sink for DbSink {
    async fn send(&self, record: Record) -> Result<()> {
        self.buffer.lock().await.push(record);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let records = std::mem::take(&mut *self.buffer.lock().await);
        if records.is_empty() {
            return Ok(());
        }
        let path = self.path.clone();
        let table = self.table.clone();
        tokio::task::spawn_blocking(move || insert_records(&path, &table, &records))
            .await
            .map_err(|error| Error::Runtime(format!("db task failed: {error}")))?
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn insert_records(path: &str, table: &str, records: &[Record]) -> Result<()> {
    let connection = rusqlite::Connection::open(path)
        .map_err(|error| Error::Config(format!("db open failed: {error}")))?;
    let columns = first_object_keys(&records[0].payload)?;
    create_table(&connection, table, &columns)?;
    let insert = insert_sql(table, &columns);
    for record in records {
        insert_row(&connection, &insert, &columns, &record.payload)?;
    }
    Ok(())
}

fn first_object_keys(payload: &Value) -> Result<Vec<String>> {
    match payload {
        Value::Object(object) => Ok(object.keys().cloned().collect()),
        _ => Err(Error::Config("db sink requires object records".to_owned())),
    }
}

fn create_table(connection: &rusqlite::Connection, table: &str, columns: &[String]) -> Result<()> {
    let definitions = columns
        .iter()
        .map(|column| format!("{column} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute(
            &format!("CREATE TABLE IF NOT EXISTS {table} ({definitions})"),
            [],
        )
        .map(|_| ())
        .map_err(|error| Error::Config(format!("db create table failed: {error}")))
}

fn insert_sql(table: &str, columns: &[String]) -> String {
    let column_sql = columns.join(", ");
    let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    format!("INSERT INTO {table} ({column_sql}) VALUES ({placeholders})")
}

fn insert_row(
    connection: &rusqlite::Connection,
    insert: &str,
    columns: &[String],
    payload: &Value,
) -> Result<()> {
    let Value::Object(object) = payload else {
        return Ok(());
    };
    let params = rusqlite::params_from_iter(
        columns
            .iter()
            .map(|column| object.get(column).map(json_text).unwrap_or_default()),
    );
    connection
        .execute(insert, params)
        .map(|_| ())
        .map_err(|error| Error::Config(format!("db insert failed: {error}")))
}

fn json_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}
