//! Gateway: unified entry routing external requests to instance triggers (D4)

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;

use crate::registry::{Inner, Registry};
use crate::types::RouteMode;
use crate::Plane;

/// The gateway router; served without the plane token (external entry).
pub fn gateway_router(plane: Arc<Plane>) -> Router {
    Router::new()
        .route("/gw/*path", any(gateway))
        .with_state(plane)
}

async fn gateway(
    State(plane): State<Arc<Plane>>,
    Path(path): Path<String>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(route) = plane.registry.find_route(&path).await else {
        return (StatusCode::NOT_FOUND, "no route matches this path").into_response();
    };
    match route.mode {
        RouteMode::Bus => bus_dispatch(&plane, &route, &body).await,
        RouteMode::Proxy => {
            let request = ForwardRequest {
                path: &path,
                method: &method,
                headers: &headers,
                body: &body,
            };
            proxy_request(&plane, &route, &request).await
        }
    }
}

/// The parts of an incoming request the gateway forwards (D4).
struct ForwardRequest<'a> {
    path: &'a str,
    method: &'a Method,
    headers: &'a HeaderMap,
    body: &'a Bytes,
}

/// Publish the request body into the route topic's relay mailbox (D5a).
async fn bus_dispatch(plane: &Plane, route: &crate::types::RouteDecl, body: &Bytes) -> Response {
    let Some(topic) = &route.topic else {
        return (StatusCode::BAD_REQUEST, "bus route needs a topic").into_response();
    };
    let payload: serde_json::Value = serde_json::from_slice(body)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(body).into_owned()));
    plane.registry.relay_publish(topic, payload).await;
    (StatusCode::ACCEPTED, "accepted").into_response()
}

/// Forward to candidate instance triggers, retrying once on failure (D4).
async fn proxy_request(
    plane: &Arc<Plane>,
    route: &crate::types::RouteDecl,
    request: &ForwardRequest<'_>,
) -> Response {
    let hash_key = route
        .sticky_key
        .as_ref()
        .and_then(|key| request.headers.get(key))
        .and_then(|value| value.to_str().ok());
    let targets = plane.registry.select_targets(route, hash_key).await;
    if targets.is_empty() {
        return (StatusCode::SERVICE_UNAVAILABLE, "no healthy instance").into_response();
    }
    let client = sohara_agent::http_client();
    for target in targets.iter().take(2) {
        match forward_once(&client, target, request).await {
            Ok((status, bytes)) => return (status, bytes).into_response(),
            Err(error) => {
                tracing::warn!("gateway forward to {target} failed: {error}");
            }
        }
    }
    (StatusCode::BAD_GATEWAY, "all candidates failed").into_response()
}

async fn forward_once(
    client: &reqwest::Client,
    target: &str,
    request: &ForwardRequest<'_>,
) -> anyhow::Result<(StatusCode, Vec<u8>)> {
    let mut builder = client
        .request(
            request.method.clone(),
            format!("http://{target}/{}", request.path),
        )
        .timeout(Duration::from_secs(5));
    for (name, value) in request
        .headers
        .iter()
        .filter(|(name, _)| *name != header::HOST && *name != header::CONTENT_LENGTH)
    {
        builder = builder.header(name.clone(), value.clone());
    }
    let response = builder.body(request.body.clone()).send().await?;
    let status = response.status();
    let bytes = response.bytes().await?.to_vec();
    Ok((status, bytes))
}

impl Registry {
    /// Longest-prefix route match for a gateway request path.
    pub async fn find_route(&self, path: &str) -> Option<crate::types::RouteDecl> {
        let path = path.trim_start_matches('/');
        let inner = self.inner.lock().await;
        inner
            .routes
            .values()
            .filter(|route| {
                let prefix = route.path.trim_start_matches('/');
                path == prefix || path.starts_with(&format!("{prefix}/"))
            })
            .max_by_key(|route| route.path.len())
            .cloned()
    }

    /// Ordered trigger addresses for a route request (D4).
    pub async fn select_targets(
        &self,
        route: &crate::types::RouteDecl,
        hash_key: Option<&str>,
    ) -> Vec<String> {
        let mut inner = self.inner.lock().await;
        let targets = running_targets(&inner, route);
        if targets.is_empty() {
            return Vec::new();
        }
        order_targets(&mut inner, route, hash_key, targets)
    }
}

/// Eligible (id, trigger) pairs for a route: running instances with a
/// known trigger address in the route's flow group.
fn running_targets(inner: &Inner, route: &crate::types::RouteDecl) -> Vec<(String, String)> {
    let mut targets = Vec::new();
    for decl in inner
        .instances
        .values()
        .filter(|decl| decl.flow_id.as_deref() == Some(route.flow_id.as_str()))
    {
        let Some(report) = crate::reconcile::actual_of(inner, &decl.node, &decl.id) else {
            continue;
        };
        if report.state != sohara_agent::InstanceState::Running {
            continue;
        }
        if let Some(trigger) = &report.trigger {
            targets.push((decl.id.clone(), trigger.clone()));
        }
    }
    targets
}

/// Order targets by strategy: hash sorts deterministically by the sticky
/// key; round-robin rotates by a per-route counter.
fn order_targets(
    inner: &mut Inner,
    route: &crate::types::RouteDecl,
    hash_key: Option<&str>,
    mut targets: Vec<(String, String)>,
) -> Vec<String> {
    match (route.strategy, hash_key) {
        (crate::types::Strategy::Hash, Some(key)) => {
            targets.sort_by_key(|(id, _)| hash_key_order(key, id));
        }
        _ => {
            let counter = inner.round_robin.entry(route.id.clone()).or_insert(0);
            let shift = (*counter as usize) % targets.len();
            *counter += 1;
            targets.rotate_left(shift);
        }
    }
    targets.into_iter().map(|(_, trigger)| trigger).collect()
}

fn hash_key_order(key: &str, id: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    id.hash(&mut hasher);
    hasher.finish()
}
