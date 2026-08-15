//! Manager API: declare/undeclare instances, desired state, views (D3)

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::types::{Desired, FlowDecl, InstanceDecl, RouteDecl};
use crate::Plane;

#[derive(Debug, Deserialize)]
struct DesiredBody {
    desired: Desired,
}

/// The `/api/*` manager router.
pub fn manager_router(plane: Arc<Plane>) -> Router {
    Router::new()
        .route("/api/nodes", get(nodes))
        .route("/api/instances", get(instances).post(declare_instance))
        .route("/api/instances/:id/desired", put(set_desired))
        .route("/api/instances/:id", delete(remove_instance))
        .route("/api/flows", get(flows))
        .route("/api/flows", post(put_flow))
        .route("/api/routes", get(routes).post(declare_route))
        .route("/api/routes/:id", delete(remove_route))
        .route("/api/events", get(events))
        .route("/api/instances/:id/status", get(instance_status))
        .route("/ui", get(ui))
        .with_state(plane)
}

async fn events(State(plane): State<Arc<Plane>>) -> Json<Value> {
    Json(json!(plane.registry.list_events().await))
}

/// Proxy one instance's `/admin/status` (D6 instance detail).
async fn instance_status(State(plane): State<Arc<Plane>>, Path(id): Path<String>) -> Response {
    let Some(decl) = plane.registry.instance_decl(&id).await else {
        return (StatusCode::NOT_FOUND, "unknown instance").into_response();
    };
    let Some(admin) = &decl.spec.admin else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "instance has no admin address",
        )
            .into_response();
    };
    let mut request = sohara_agent::http_client().get(format!("http://{admin}/admin/status"));
    if let Some(token) = &decl.spec.admin_token {
        request = request.bearer_auth(token);
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let value: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));
            (status, Json(value)).into_response()
        }
        Err(error) => {
            tracing::warn!("status proxy to {admin} failed: {error}");
            (StatusCode::BAD_GATEWAY, "instance unreachable").into_response()
        }
    }
}

async fn ui() -> Html<&'static str> {
    Html(include_str!("../assets/ui.html"))
}

async fn nodes(State(plane): State<Arc<Plane>>) -> Json<Value> {
    Json(json!(plane.registry.list_nodes().await))
}

async fn instances(State(plane): State<Arc<Plane>>) -> Json<Value> {
    Json(json!(plane.registry.list_instances().await))
}

async fn declare_instance(
    State(plane): State<Arc<Plane>>,
    Json(decl): Json<InstanceDecl>,
) -> StatusCode {
    match plane.registry.declare_instance(decl).await {
        Ok(()) => StatusCode::CREATED,
        Err(error) => {
            tracing::error!("declare instance failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn set_desired(
    State(plane): State<Arc<Plane>>,
    Path(id): Path<String>,
    Json(body): Json<DesiredBody>,
) -> StatusCode {
    match plane.registry.set_desired(&id, body.desired).await {
        Ok(()) => StatusCode::OK,
        Err(error) => {
            tracing::error!("set desired failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn remove_instance(State(plane): State<Arc<Plane>>, Path(id): Path<String>) -> StatusCode {
    match plane.registry.remove_instance(&id).await {
        Ok(()) => StatusCode::OK,
        Err(error) => {
            tracing::error!("remove instance failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn flows(State(plane): State<Arc<Plane>>) -> Json<Value> {
    Json(json!(plane.registry.list_flows().await))
}

async fn routes(State(plane): State<Arc<Plane>>) -> Json<Value> {
    Json(json!(plane.registry.list_routes().await))
}

async fn declare_route(
    State(plane): State<Arc<Plane>>,
    Json(route): Json<RouteDecl>,
) -> StatusCode {
    match plane.registry.declare_route(route).await {
        Ok(()) => StatusCode::CREATED,
        Err(error) => {
            tracing::error!("declare route failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn remove_route(State(plane): State<Arc<Plane>>, Path(id): Path<String>) -> StatusCode {
    match plane.registry.remove_route(&id).await {
        Ok(()) => StatusCode::OK,
        Err(error) => {
            tracing::error!("remove route failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn put_flow(State(plane): State<Arc<Plane>>, Json(flow): Json<FlowDecl>) -> StatusCode {
    match plane.registry.put_flow(flow).await {
        Ok(()) => StatusCode::CREATED,
        Err(error) => {
            tracing::error!("put flow failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
