//! HTTP client source and sink (S5): minimal HTTP/1.1 over TCP

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::Deserialize;
use serde_json::Value;
use sohara_core::{BuildContext, BuiltStep, Error, Record, Result, Sink, Source};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::parse_config;

const MAX_BODY: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
struct UrlParts {
    host: String,
    port: u16,
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpSourceConfig {
    url: String,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    headers: Option<BTreeMap<String, String>>,
    #[serde(default)]
    poll_interval: Option<String>,
}

/// `source.http` step: fetch JSON from an HTTP endpoint (optionally polling).
pub struct HttpSource {
    name: String,
    url: UrlParts,
    method: String,
    headers: BTreeMap<String, String>,
    poll: Option<Duration>,
}

impl HttpSource {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: HttpSourceConfig = parse_config(config, "http source")?;
        let poll = cfg
            .poll_interval
            .as_deref()
            .map(sohara_core::parse_duration)
            .transpose()
            .map_err(|error| Error::Config(format!("http poll_interval: {error}")))?;
        let step = Self {
            name: format!("http:{}", cfg.url),
            url: parse_url(&cfg.url)?,
            method: cfg.method.unwrap_or_else(|| "GET".to_owned()),
            headers: cfg.headers.unwrap_or_default(),
            poll,
        };
        Ok(BuiltStep::Source(Box::new(step)))
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        match self.poll {
            Some(interval) => {
                let url = self.url.clone();
                let method = self.method.clone();
                let headers = self.headers.clone();
                Ok(Box::pin(futures::stream::unfold((), move |()| {
                    let url = url.clone();
                    let method = method.clone();
                    let headers = headers.clone();
                    async move {
                        tokio::time::sleep(interval).await;
                        Some((fetch(&url, &method, &headers).await.map(Record::new), ()))
                    }
                })))
            }
            None => {
                let value = fetch(&self.url, &self.method, &self.headers).await?;
                Ok(Box::pin(futures::stream::iter(vec![Ok(Record::new(
                    value,
                ))])))
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpSinkConfig {
    url: String,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    headers: Option<BTreeMap<String, String>>,
}

/// `sink.http` step: POST each record payload as JSON to an endpoint.
pub struct HttpSink {
    name: String,
    url: UrlParts,
    method: String,
    headers: BTreeMap<String, String>,
}

impl HttpSink {
    /// Build the step from config.
    pub fn build(config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
        let cfg: HttpSinkConfig = parse_config(config, "http sink")?;
        let step = Self {
            name: format!("http:{}", cfg.url),
            url: parse_url(&cfg.url)?,
            method: cfg.method.unwrap_or_else(|| "POST".to_owned()),
            headers: cfg.headers.unwrap_or_default(),
        };
        Ok(BuiltStep::Sink(Box::new(step)))
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn send(&self, record: Record) -> Result<()> {
        post_json(&self.url, &self.method, &self.headers, &record.to_json()).await?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

async fn fetch(url: &UrlParts, method: &str, headers: &BTreeMap<String, String>) -> Result<Value> {
    let response = request(url, method, headers, None).await?;
    let body = response.trim();
    if body.starts_with('{') || body.starts_with('[') {
        serde_json::from_str(body)
            .map_err(|error| Error::Config(format!("invalid json response: {error}")))
    } else {
        Ok(Value::String(body.to_owned()))
    }
}

async fn post_json(
    url: &UrlParts,
    method: &str,
    headers: &BTreeMap<String, String>,
    payload: &Value,
) -> Result<()> {
    let body = payload.to_string();
    let mut all = headers.clone();
    all.insert("Content-Type".to_owned(), "application/json".to_owned());
    request(url, method, &all, Some(&body)).await?;
    Ok(())
}

async fn request(
    url: &UrlParts,
    method: &str,
    headers: &BTreeMap<String, String>,
    body: Option<&str>,
) -> Result<String> {
    let mut stream = tokio::net::TcpStream::connect((url.host.as_str(), url.port))
        .await
        .map_err(Error::Io)?;
    let mut request = format!(
        "{method} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
        url.path, url.host
    );
    for (key, value) in headers {
        request.push_str(&format!("{key}: {value}\r\n"));
    }
    let body = body.unwrap_or_default();
    request.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(Error::Io)?;
    let mut buffer = Vec::new();
    stream
        .take(MAX_BODY as u64)
        .read_to_end(&mut buffer)
        .await
        .map_err(Error::Io)?;
    let text = String::from_utf8_lossy(&buffer);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| Error::Config("malformed http response".to_owned()))?;
    let status = head
        .lines()
        .next()
        .ok_or_else(|| Error::Config("http response without status line".to_owned()))?;
    if !status.contains(" 2") {
        return Err(Error::Config(format!("http request failed: {status}")));
    }
    Ok(body.to_owned())
}

fn parse_url(url: &str) -> Result<UrlParts> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| Error::Config("only http:// urls are supported (S5)".to_owned()))?;
    let (host_port, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = match host_port.split_once(':') {
        Some((host, port)) => (
            host.to_owned(),
            port.parse::<u16>()
                .map_err(|_| Error::Config(format!("invalid port in url '{url}'")))?,
        ),
        None => (host_port.to_owned(), 80),
    };
    Ok(UrlParts {
        host,
        port,
        path: format!("/{path}"),
    })
}
