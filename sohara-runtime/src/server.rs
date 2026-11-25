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
use sohara_core::{Error, Result};

/// Shared state for the empty server. The token is mandatory (never `Option`)
/// and is validated non-blank at construction, so there is no code path that
/// serves the API without authentication.
#[derive(Debug, Clone)]
pub struct ServerState {
    token: String,
}

impl ServerState {
    /// Create server state, rejecting an empty or all-whitespace token.
    ///
    /// # Errors
    /// Returns `Error::Config` when `token` is blank, keeping the public API
    /// fail-closed at the type boundary rather than at request time.
    pub fn new(token: impl Into<String>) -> Result<Self> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(Error::Config(
                "admin token must not be empty or whitespace".to_owned(),
            ));
        }
        Ok(Self { token })
    }
}

type Server = State<Arc<ServerState>>;

/// Build the empty-server router. Every route sits behind fail-closed auth.
pub(crate) fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/admin/health", get(health))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state)
}

/// Bind and serve until the returned shutdown future resolves.
///
/// Returns the actually bound address (useful when `addr` uses port `0`) and a
/// handle for the serving task. Server errors are logged rather than swallowed.
pub async fn spawn_server(
    addr: SocketAddr,
    state: Arc<ServerState>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(Error::Io)?;
    let bound = listener.local_addr().map_err(Error::Io)?;
    let handle = tokio::spawn(async move {
        let served = axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown)
            .await;
        if let Err(error) = served {
            tracing::error!("sohara server stopped with an error: {error}");
        }
    });
    Ok((bound, handle))
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
