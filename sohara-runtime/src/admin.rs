//! Single-machine admin API + embedded dashboard (S6/D1):
//! health, pause/resume, metrics, status, history, approvals, errors, ui

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::task::JoinHandle;

use crate::executor::Executor;
use crate::history;
use crate::persist::list_approve_queues;

use sohara_core::Result;

/// One trigger of the served flow (for the status endpoint).
#[derive(Debug, Clone, Serialize)]
pub struct TriggerInfo {
    pub id: String,
    pub kind: String,
}

/// Shared admin state: the executor plus serve-mode extras.
pub struct AdminState {
    pub executor: Arc<Executor>,
    pub triggers: Vec<TriggerInfo>,
    pub token: Option<String>,
    pub history: Option<PathBuf>,
}

type Admin = State<Arc<AdminState>>;

/// Build the admin router backed by shared state.
pub fn router(state: Arc<AdminState>) -> Router {
    Router::new()
        .route("/admin/health", get(health))
        .route("/admin/metrics", get(metrics))
        .route("/admin/pause", post(pause))
        .route("/admin/resume", post(resume))
        .route("/admin/status", get(status))
        .route("/admin/history", get(history))
        .route("/admin/approvals", get(approvals))
        .route("/admin/errors", get(errors))
        .route("/admin/ui", get(ui))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state)
}

/// Bind and serve the admin API; abort the returned handle to stop it.
pub async fn spawn(addr: SocketAddr, state: Arc<AdminState>) -> Result<JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(sohara_core::Error::Io)?;
    Ok(tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    }))
}

async fn auth(State(state): Admin, request: Request, next: Next) -> Response {
    let authorized = state
        .token
        .as_deref()
        .is_none_or(|token| bearer_matches(request.headers(), token));
    if authorized {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

fn bearer_matches(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| value == token)
}

async fn health(State(state): Admin) -> Json<Value> {
    Json(json!({ "status": "running", "paused": state.executor.is_paused() }))
}

async fn metrics(State(state): Admin) -> Json<Value> {
    Json(serde_json::to_value(state.executor.report().await).unwrap_or(Value::Null))
}

async fn pause(State(state): Admin) -> Json<Value> {
    state.executor.pause();
    Json(json!({ "paused": true }))
}

async fn resume(State(state): Admin) -> Json<Value> {
    state.executor.resume();
    Json(json!({ "paused": false }))
}

/// Flow meta, triggers, per-step stats, paused flag, and run identity (D1).
async fn status(State(state): Admin) -> Json<Value> {
    let report = state.executor.report().await;
    let mut value = serde_json::to_value(report).unwrap_or(Value::Null);
    if let Value::Object(map) = &mut value {
        map.insert("paused".to_owned(), Value::Bool(state.executor.is_paused()));
        map.insert(
            "triggers".to_owned(),
            serde_json::to_value(&state.triggers).unwrap_or(Value::Null),
        );
    }
    Json(value)
}

async fn history(State(state): Admin) -> Json<Value> {
    let entries = match &state.history {
        Some(path) => history::read_recent(path, 50).unwrap_or_default(),
        None => Vec::new(),
    };
    Json(Value::Array(entries))
}

async fn approvals(State(state): Admin) -> Json<Value> {
    let queues = list_approve_queues(&state.executor).unwrap_or_default();
    Json(serde_json::to_value(queues).unwrap_or(Value::Null))
}

async fn errors(State(state): Admin) -> Json<Value> {
    Json(serde_json::to_value(state.executor.error_events().await).unwrap_or(Value::Null))
}

async fn ui() -> Html<&'static str> {
    Html(include_str!("../assets/dashboard.html"))
}
