//! Gateway: unified entry routing external requests to instance triggers (D4)

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;

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
        RouteMode::Bus => (StatusCode::NOT_IMPLEMENTED, "bus mode arrives with D5").into_response(),
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
