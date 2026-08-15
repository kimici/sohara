//! File source and sink: csv / json / jsonl

use std::path::PathBuf;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::Deserialize;
use serde_json::{Map, Value};
use sohara_core::{BuildContext, BuiltStep, Error, Record, Result, Sink, Source};

use crate::parse_config;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Format {
    Csv,
    Json,
    Jsonl,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSourceConfig {
    path: String,
    format: Format,
    #[serde(default)]
    columns: Option<Vec<String>>,
    #[serde(default)]
    delimiter: Option<String>,
    #[serde(default)]
    header: Option<bool>,
}

/// `source.file` step: read records from a csv / json / jsonl file.
pub struct FileSource {
    name: String,
    path: PathBuf,
    format: Format,
    columns: Option<Vec<String>>,
    delimiter: u8,
    header: bool,
}

impl FileSource {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: FileSourceConfig = parse_config(config, "file source")?;
        let delimiter = match cfg.delimiter {
            None => b',',
            Some(text) => parse_delimiter(&text)?,
        };
        let step = Self {
            name: format!("file:{}", cfg.path),
            path: cfg.path.into(),
            format: cfg.format,
            columns: cfg.columns,
            delimiter,
            header: cfg.header.unwrap_or(true),
        };
        Ok(BuiltStep::Source(Box::new(step)))
    }

    async fn read_records(&self) -> Result<Vec<Record>> {
        let text = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(Error::Io)?;
        match self.format {
            Format::Csv => self.parse_csv(&text),
            Format::Jsonl => self.parse_jsonl(&text),
            Format::Json => self.parse_json(&text),
        }
    }

    fn parse_csv(&self, text: &str) -> Result<Vec<Record>> {
        let mut reader = self.csv_reader(text);
        let headers = self.resolve_headers(&mut reader)?;
        if !self.header {
            reader.set_headers(csv::StringRecord::from(headers.clone()));
        }
        let mut records = Vec::new();
        for row in reader.records() {
            let row = row.map_err(|error| Error::Config(format!("csv row error: {error}")))?;
            records.push(record_from_row(&row, &headers));
        }
        Ok(records)
    }

    fn csv_reader<'a>(&self, text: &'a str) -> csv::Reader<&'a [u8]> {
        let mut builder = csv::ReaderBuilder::new();
        builder
            .delimiter(self.delimiter)
            .has_headers(self.header)
            .flexible(true);
        builder.from_reader(text.as_bytes())
    }

    fn resolve_headers(&self, reader: &mut csv::Reader<&[u8]>) -> Result<Vec<String>> {
        if self.header {
            return reader
                .headers()
                .map(|headers| headers.iter().map(ToOwned::to_owned).collect())
                .map_err(|error| Error::Config(format!("csv header error: {error}")));
        }
        self.columns
            .clone()
            .ok_or_else(|| Error::Config("csv source without a header needs 'columns'".to_owned()))
    }

    fn parse_jsonl(&self, text: &str) -> Result<Vec<Record>> {
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let value: Value = serde_json::from_str(line)
                    .map_err(|error| Error::Config(format!("invalid jsonl line: {error}")))?;
                Ok(Record::new(value))
            })
            .collect()
    }

    fn parse_json(&self, text: &str) -> Result<Vec<Record>> {
        let value: Value = serde_json::from_str(text)
            .map_err(|error| Error::Config(format!("invalid json: {error}")))?;
        match value {
            Value::Array(items) => Ok(items.into_iter().map(Record::new).collect()),
            other => Ok(vec![Record::new(other)]),
        }
    }
}

#[async_trait]
impl Source for FileSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let records = self.read_records().await?;
        Ok(Box::pin(futures::stream::iter(records.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSinkConfig {
    path: String,
    format: Format,
    #[serde(default)]
    append: bool,
}

/// `sink.file` step: buffer records and write them on flush.
pub struct FileSink {
    name: String,
    path: PathBuf,
    format: Format,
    append: bool,
    buffer: tokio::sync::Mutex<Vec<Record>>,
}

impl FileSink {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: FileSinkConfig = parse_config(config, "file sink")?;
        if cfg.append && cfg.format == Format::Json {
            return Err(Error::Config(
                "append is not supported for json format".to_owned(),
            ));
        }
        let step = Self {
            name: format!("file:{}", cfg.path),
            path: cfg.path.into(),
            format: cfg.format,
            append: cfg.append,
            buffer: tokio::sync::Mutex::new(Vec::new()),
        };
        Ok(BuiltStep::Sink(Box::new(step)))
    }

    fn render(&self, records: &[Record]) -> Result<String> {
        match self.format {
            Format::Jsonl => {
                let lines = records
                    .iter()
                    .map(|record| record.to_json().to_string())
                    .collect::<Vec<_>>();
                Ok(format!("{}\n", lines.join("\n")))
            }
            Format::Json => {
                let array = Value::Array(records.iter().map(Record::to_json).collect());
                serde_json::to_string(&array).map_err(Error::Serialization)
            }
            Format::Csv => self.render_csv(records),
        }
    }

    fn render_csv(&self, records: &[Record]) -> Result<String> {
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        let mut wrote_header = false;
        for record in records {
            let Value::Object(object) = &record.payload else {
                return Err(Error::Config("csv sink requires object records".to_owned()));
            };
            if !wrote_header {
                writer
                    .write_record(object.keys().cloned())
                    .map_err(|error| Error::Config(format!("csv header error: {error}")))?;
                wrote_header = true;
            }
            let row = object.values().map(csv_text).collect::<Vec<_>>();
            writer
                .write_record(row)
                .map_err(|error| Error::Config(format!("csv row error: {error}")))?;
        }
        let bytes = writer
            .into_inner()
            .map_err(|error| Error::Config(format!("csv flush error: {error}")))?;
        String::from_utf8(bytes).map_err(|error| Error::Config(error.to_string()))
    }

    async fn write(&self, content: &str) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(Error::Io)?;
        }
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).write(true).truncate(!self.append);
        if self.append {
            options.append(true);
        }
        let mut file = options.open(&self.path).await.map_err(Error::Io)?;
        use tokio::io::AsyncWriteExt;
        file.write_all(content.as_bytes())
            .await
            .map_err(Error::Io)?;
        Ok(())
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn send(&self, record: Record) -> Result<()> {
        self.buffer.lock().await.push(record);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let records = std::mem::take(&mut *self.buffer.lock().await);
        if records.is_empty() {
            return Ok(());
        }
        self.write(&self.render(&records)?).await
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn record_from_row(row: &csv::StringRecord, headers: &[String]) -> Record {
    let mut object = Map::new();
    for (index, header) in headers.iter().enumerate() {
        object.insert(
            header.clone(),
            Value::String(row.get(index).unwrap_or_default().to_owned()),
        );
    }
    Record::new(Value::Object(object))
}

fn parse_delimiter(text: &str) -> Result<u8> {
    let bytes = text.as_bytes();
    match bytes.len() {
        1 => Ok(bytes[0]),
        _ => Err(Error::Config(format!(
            "delimiter must be a single character, got '{text}'"
        ))),
    }
}

fn csv_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}
