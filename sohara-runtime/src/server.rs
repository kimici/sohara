//! Zero-config server: empty startup with a mandatory-token admin API (C1).
//!
//! Unlike `serve`, which binds admin to an existing flow executor, this server
//! starts with **no flows loaded** and every route fails closed. It is the
//! bootstrap for the flow-management API (C2) and restart recovery (C3).

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::task::JoinHandle;

use crate::admin::bearer_matches;
use sohara_core::Result;

/// Shared state for the empty server. The token is mandatory (never `Option`),
/// so there is no code path that serves the API without authentication.
pub struct ServerState {
    pub token: String,
}

type Server = State<Arc<ServerState>>;

/// Build the empty-server router. Every route sits behind fail-closed auth.
pub(crate) fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/admin/health", get(health))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state)
}

/// Bind and serve; abort the returned handle to stop.
pub async fn spawn_server(addr: SocketAddr, state: Arc<ServerState>) -> Result<JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(sohara_core::Error::Io)?;
    Ok(tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    }))
}

/// Fail-closed bearer auth: a missing or wrong token is always rejected.
async fn auth(State(state): Server, request: Request, next: Next) -> Response {
    if bearer_matches(request.headers(), &state.token) {
        next.run(request).await
    } else {
        (axum::http::StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "running", "flows": 0 }))
}
