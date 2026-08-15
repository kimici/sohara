//! HTTP trigger: axum server pushing request bodies into a bounded channel

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::Value;
use sohara_core::{Error, Record, Result, Source, Trigger};
use tokio::sync::{mpsc, watch, Mutex};
use tokio_stream::wrappers::ReceiverStream;

const DEFAULT_LIMIT: usize = 1024 * 1024;

/// State shared with the axum handler.
#[derive(Clone)]
struct HandlerState {
    method: Method,
    sender: mpsc::Sender<Value>,
    limit: usize,
}

/// A source yielding one record per matching HTTP request.
pub struct HttpSource {
    name: String,
    method: Method,
    path: String,
    host: String,
    port: u16,
    sender: Mutex<Option<mpsc::Sender<Value>>>,
    receiver: Mutex<Option<mpsc::Receiver<Value>>>,
    shutdown: watch::Sender<bool>,
}

impl HttpSource {
    #[must_use]
    pub fn new(method: &str, path: &str, host: &str, port: u16) -> Self {
        let (sender, receiver) = mpsc::channel(128);
        let (shutdown, _) = watch::channel(false);
        Self {
            name: format!("http:{method} {path}"),
            method: Method::from_bytes(method.as_bytes()).unwrap_or(Method::GET),
            path: path.to_owned(),
            host: host.to_owned(),
            port,
            sender: Mutex::new(Some(sender)),
            receiver: Mutex::new(Some(receiver)),
            shutdown,
        }
    }
}

#[async_trait]
impl Trigger for HttpSource {
    async fn start(&self) -> Result<()> {
        let sender =
            self.sender.lock().await.clone().ok_or_else(|| {
                Error::Source(format!("http source '{}' already started", self.name))
            })?;
        let state = HandlerState {
            method: self.method.clone(),
            sender,
            limit: DEFAULT_LIMIT,
        };
        let router = Router::new()
            .route(&self.path, any(handle_request))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind((self.host.as_str(), self.port))
            .await
            .map_err(Error::Io)?;
        let address = listener.local_addr().map_err(Error::Io)?;
        let mut shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            let server = axum::serve(listener, router).with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            });
            if let Err(error) = std::future::IntoFuture::into_future(server).await {
                tracing::error!("http trigger server error: {error}");
            }
        });
        tracing::info!("http trigger '{}' listening on http://{address}", self.name);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let _ = self.shutdown.send(true);
        self.sender.lock().await.take();
        Ok(())
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let receiver = self.receiver.lock().await.take().ok_or_else(|| {
            Error::Source(format!("http source '{}' already consumed", self.name))
        })?;
        Ok(Box::pin(
            ReceiverStream::new(receiver).map(|payload| Ok(Record::new(payload))),
        ))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

async fn handle_request(State(state): State<HandlerState>, request: Request<Body>) -> Response {
    if request.method() != state.method {
        return (StatusCode::METHOD_NOT_ALLOWED, "method not allowed").into_response();
    }
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, state.limit).await {
        Ok(bytes) => bytes,
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response(),
    };
    let content_type = parts
        .headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let value = if content_type.contains("json") {
        match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => return (StatusCode::BAD_REQUEST, "invalid json").into_response(),
        }
    } else {
        Value::String(String::from_utf8_lossy(&bytes).into_owned())
    };
    if state.sender.send(value).await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "trigger shutting down").into_response();
    }
    StatusCode::ACCEPTED.into_response()
}
