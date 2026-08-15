//! D6 manager UI tests: event history, instance status proxy, ui auth

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use sohara_agent::{Heartbeat, InstanceReport, InstanceState};
use sohara_plane::{Plane, Registry};

#[tokio::test]
async fn events_are_recorded_for_lifecycle_and_state_changes() {
    let registry = Registry::load(None);
    declare_basic(&registry).await;
    registry
        .set_desired("i1", sohara_plane::Desired::Stopped)
        .await
        .unwrap();
    let heartbeat = running_heartbeat();
    registry.heartbeat(&heartbeat).await;
    registry.heartbeat(&heartbeat).await;
    let events = registry.list_events().await;
    let kinds: Vec<&str> = events.iter().map(|e| e["kind"].as_str().unwrap()).collect();
    assert_eq!(kinds, vec!["state", "desired", "declare"]);
    let state_events = events.iter().filter(|e| e["kind"] == "state").count();
    assert_eq!(state_events, 1, "unchanged state must not emit events");
}

async fn declare_basic(registry: &Registry) {
    registry
        .declare_instance(sohara_plane::InstanceDecl {
            id: "i1".to_owned(),
            node: "n1".to_owned(),
            flow_id: None,
            desired: sohara_plane::Desired::Running,
            spec: sohara_agent::InstanceSpec {
                id: "i1".to_owned(),
                ..sohara_agent::InstanceSpec::default()
            },
        })
        .await
        .unwrap();
}

fn running_heartbeat() -> Heartbeat {
    Heartbeat {
        node_id: "n1".to_owned(),
        time: "now".to_owned(),
        instances: vec![InstanceReport {
            id: "i1".to_owned(),
            state: InstanceState::Running,
            paused: false,
            healthy: true,
            restarts: 0,
            admin: None,
            trigger: None,
        }],
    }
}

#[tokio::test]
async fn instance_status_proxy_passes_the_admin_token() {
    let registry = Registry::load(None);
    let seen_token = Arc::new(Mutex::new(None));
    let admin_addr = serve_admin_stub(seen_token.clone()).await;
    declare_proxied_instance(&registry, admin_addr).await;
    let plane = Plane::new(registry, None);
    let addr = serve_plane(plane).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let status: Value = client
        .get(format!("http://{addr}/api/instances/i1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["flow"], json!("stub-flow"));
    assert_eq!(
        seen_token.lock().unwrap().as_deref(),
        Some("Bearer secret"),
        "proxy must forward the instance admin token"
    );
}

async fn declare_proxied_instance(registry: &Registry, admin_addr: SocketAddr) {
    registry
        .declare_instance(sohara_plane::InstanceDecl {
            id: "i1".to_owned(),
            node: "n1".to_owned(),
            flow_id: None,
            desired: sohara_plane::Desired::Running,
            spec: sohara_agent::InstanceSpec {
                id: "i1".to_owned(),
                admin: Some(admin_addr.to_string()),
                admin_token: Some("secret".to_owned()),
                ..sohara_agent::InstanceSpec::default()
            },
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn ui_and_relay_endpoints_are_token_guarded() {
    let plane = Plane::new(Registry::load(None), Some("tok".to_owned()));
    let addr = serve_plane(plane).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let status = client
        .get(format!("http://{addr}/ui"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_ui_served(&client, addr).await;
    let status = client
        .post(format!("http://{addr}/relay/publish"))
        .json(&json!({ "topic": "t", "payload": 1 }))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "relay endpoints must be guarded"
    );
}

async fn assert_ui_served(client: &reqwest::Client, addr: SocketAddr) {
    let response = client
        .get(format!("http://{addr}/ui"))
        .bearer_auth("tok")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .text()
        .await
        .unwrap()
        .contains("<title>Sohara Manager"));
}

async fn serve_admin_stub(seen_token: Arc<Mutex<Option<String>>>) -> SocketAddr {
    let router = Router::new()
        .route("/admin/status", get(stub_status))
        .with_state(seen_token);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    addr
}

async fn stub_status(
    State(seen_token): State<Arc<Mutex<Option<String>>>>,
    headers: axum::http::HeaderMap,
) -> Json<Value> {
    *seen_token.lock().unwrap() = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    Json(json!({ "flow": "stub-flow", "stats": { "processed": 7 } }))
}

async fn serve_plane(plane: Arc<Plane>) -> SocketAddr {
    let router = plane.router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    addr
}

/// Keep the atomic import used even if assertions change.
#[allow(dead_code)]
fn _touch(flag: &AtomicBool) -> bool {
    flag.load(Ordering::Relaxed)
}
