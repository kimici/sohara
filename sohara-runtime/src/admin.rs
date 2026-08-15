//! Single-machine admin API (S6): health, pause/resume, and metrics

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::task::JoinHandle;

use crate::executor::Executor;

use sohara_core::Result;

type ExecutorState = State<Arc<Executor>>;

/// Build the admin router backed by an executor.
pub fn router(executor: Arc<Executor>) -> Router {
    Router::new()
        .route("/admin/health", get(health))
        .route("/admin/metrics", get(metrics))
        .route("/admin/pause", post(pause))
        .route("/admin/resume", post(resume))
        .with_state(executor)
}

/// Bind and serve the admin API; abort the returned handle to stop it.
pub async fn spawn(addr: SocketAddr, executor: Arc<Executor>) -> Result<JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(sohara_core::Error::Io)?;
    Ok(tokio::spawn(async move {
        let _ = axum::serve(listener, router(executor)).await;
    }))
}

async fn health(State(executor): ExecutorState) -> Json<Value> {
    Json(json!({ "status": "running", "paused": executor.is_paused() }))
}

async fn metrics(State(executor): ExecutorState) -> Json<Value> {
    Json(serde_json::to_value(executor.report().await).unwrap_or(Value::Null))
}

async fn pause(State(executor): ExecutorState) -> Json<Value> {
    executor.pause();
    Json(json!({ "paused": true }))
}

async fn resume(State(executor): ExecutorState) -> Json<Value> {
    executor.resume();
    Json(json!({ "paused": false }))
}
