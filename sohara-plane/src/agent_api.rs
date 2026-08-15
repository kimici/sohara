//! Agent-facing API: heartbeat intake, command delivery, acks (D3)

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use sohara_agent::{CommandAck, Heartbeat};

use crate::Plane;

/// The `/agent/*` router.
pub fn agent_router(plane: Arc<Plane>) -> Router {
    Router::new()
        .route("/agent/heartbeat", post(heartbeat))
        .route("/agent/ack", post(ack))
        .with_state(plane)
}

async fn heartbeat(State(plane): State<Arc<Plane>>, Json(body): Json<Heartbeat>) -> Json<Value> {
    let (commands, desired) = plane.registry.heartbeat(&body).await;
    Json(json!({ "commands": commands, "desired": desired }))
}

async fn ack(State(plane): State<Arc<Plane>>, Json(body): Json<CommandAck>) -> StatusCode {
    plane.registry.ack(&body).await;
    StatusCode::OK
}
