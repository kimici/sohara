//! Sohara control plane: registry + manager API + agent API (D3)

mod agent_api;
mod gateway;
mod manager;
mod reconcile;
mod registry;
mod relay_api;
mod store;
pub mod types;

pub use registry::Registry;
pub use types::{
    Desired, FlowDecl, InstanceDecl, InstanceView, NodeView, RouteDecl, RouteMode, Strategy,
};

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;

/// The control plane: registry plus shared auth config.
pub struct Plane {
    pub registry: Arc<Registry>,
    token: Option<String>,
}

impl Plane {
    /// Build a plane over a registry; `token` guards `/agent/*` and `/api/*`.
    #[must_use]
    pub fn new(registry: Arc<Registry>, token: Option<String>) -> Arc<Self> {
        Arc::new(Self { registry, token })
    }

    /// Open a registry from an optional state file and wrap it in a plane.
    #[must_use]
    pub fn open(state: Option<PathBuf>, token: Option<String>) -> Arc<Self> {
        Self::new(Registry::load(state), token)
    }

    /// The combined router. `/agent/*` and `/api/*` require the token when
    /// one is configured; the gateway is the unauthenticated external entry.
    pub fn router(self: &Arc<Self>) -> Router {
        let plane = self.clone();
        let authed = |router: Router| {
            router.layer(axum::middleware::from_fn_with_state(plane.clone(), auth))
        };
        Router::new()
            .merge(authed(agent_api::agent_router(plane.clone())))
            .merge(authed(manager::manager_router(plane.clone())))
            .merge(authed(relay_api::relay_router(plane.clone())))
            .merge(gateway::gateway_router(plane.clone()))
    }
}

async fn auth(State(plane): State<Arc<Plane>>, request: Request, next: Next) -> Response {
    let authorized = plane
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
