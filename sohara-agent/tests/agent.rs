//! D2 agent tests: process lifecycle, health-restart, plane interaction

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use sohara_agent::{
    Agent, AgentConfig, HttpTransport, InstanceCommand, InstanceManager, InstanceSpec,
    InstanceState, NodeConfig, PlaneConfig, Policy,
};

/// A plane + instance-admin stub recording what it sees.
#[derive(Default)]
struct Stub {
    heartbeats: Mutex<Vec<Value>>,
    acks: Mutex<Vec<Value>>,
    paused: AtomicBool,
    healthy: AtomicBool,
    pending: Mutex<Vec<Value>>,
}

fn healthy_stub() -> Stub {
    Stub {
        healthy: AtomicBool::new(true),
        ..Stub::default()
    }
}

#[tokio::test]
async fn manager_spawns_and_stops_a_process() {
    let spec = InstanceSpec {
        id: "p1".to_owned(),
        flow: "unused".to_owned(),
        bin: "sh".to_owned(),
        args: vec!["-c".to_owned(), "exec sleep 60".to_owned()],
        health_enabled: false,
        policy: Policy {
            max_restarts: 0,
            ..Policy::default()
        },
        ..InstanceSpec::default()
    };
    let manager = InstanceManager::spawn(spec, reqwest::Client::new());
    assert!(wait_state(&manager, InstanceState::Running).await);
    manager.send(InstanceCommand::Stop);
    assert!(
        wait_state(&manager, InstanceState::Stopped).await,
        "stop must terminate the child"
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn unhealthy_instance_gets_restarted() {
    let stub = Arc::new(healthy_stub());
    stub.healthy.store(false, Ordering::Relaxed);
    let addr = serve_stub(stub.clone()).await;
    let spec = InstanceSpec {
        id: "i1".to_owned(),
        flow: "unused".to_owned(),
        bin: "sh".to_owned(),
        args: vec!["-c".to_owned(), "exec sleep 60".to_owned()],
        admin: Some(addr.to_string()),
        policy: Policy {
            restart: true,
            max_restarts: 3,
            backoff_ms: 10,
            health_failures: 2,
        },
        ..InstanceSpec::default()
    };
    let manager = InstanceManager::spawn(spec, reqwest::Client::new());
    assert!(wait_state(&manager, InstanceState::Running).await);
    assert!(
        wait_restarts(&manager, 1).await,
        "health failures must trigger a restart"
    );
    stub.healthy.store(true, Ordering::Relaxed);
    assert!(wait_state(&manager, InstanceState::Running).await);
    let snapshot = manager.snapshot().await;
    assert!(snapshot.healthy, "recovered instance must be healthy");
    assert!(snapshot.restarts >= 1);
    manager.shutdown().await;
}

#[tokio::test]
async fn agent_heartbeats_and_executes_plane_commands() {
    let stub = Arc::new(healthy_stub());
    let addr = serve_stub(stub.clone()).await;
    let command = json!({"seq": 1, "op": "pause", "instance": "orders-1"});
    stub.pending.lock().unwrap().push(command);
    let config = agent_config(addr);
    let transport = Arc::new(HttpTransport::new(config.plane.url.clone(), None));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let agent = spawn_agent(config, transport, shutdown_rx);
    let acked = || {
        stub.acks
            .lock()
            .unwrap()
            .iter()
            .any(|ack| ack["seq"] == json!(1) && ack["ok"] == json!(true))
    };
    assert!(wait_until(acked).await, "pause command must be acked");
    assert!(
        stub.paused.load(Ordering::Relaxed),
        "instance admin must be paused"
    );
    let reported = || {
        let heartbeats = stub.heartbeats.lock().unwrap();
        heartbeat_reports(&heartbeats, "orders-1", "paused")
    };
    assert!(
        wait_until(reported).await,
        "heartbeat must report the paused state"
    );
    shutdown_tx.send(()).unwrap();
    agent.await.unwrap().unwrap();
}

fn spawn_agent(
    config: AgentConfig,
    transport: Arc<HttpTransport>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        Agent::new(&config, transport)
            .run(async move {
                let _ = shutdown_rx.await;
            })
            .await
    })
}

fn heartbeat_reports(heartbeats: &[Value], instance: &str, state: &str) -> bool {
    heartbeats.iter().any(|hb| {
        hb["instances"].as_array().is_some_and(|instances| {
            instances
                .iter()
                .any(|i| i["id"] == instance && i["state"] == state)
        })
    })
}

fn agent_config(addr: SocketAddr) -> AgentConfig {
    AgentConfig {
        node: NodeConfig {
            id: "n1".to_owned(),
        },
        plane: PlaneConfig {
            url: format!("http://{addr}"),
            token: None,
        },
        instances: vec![InstanceSpec {
            id: "orders-1".to_owned(),
            flow: "unused".to_owned(),
            bin: "sh".to_owned(),
            args: vec!["-c".to_owned(), "exec sleep 60".to_owned()],
            admin: Some(addr.to_string()),
            policy: Policy {
                health_failures: 1000,
                ..Policy::default()
            },
            ..InstanceSpec::default()
        }],
        heartbeat_ms: 100,
    }
}

async fn serve_stub(stub: Arc<Stub>) -> SocketAddr {
    let router = Router::new()
        .route("/agent/heartbeat", post(heartbeat))
        .route("/agent/ack", post(ack))
        .route("/admin/health", get(health))
        .route("/admin/pause", post(pause))
        .route("/admin/resume", post(resume))
        .with_state(stub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    addr
}

async fn heartbeat(State(stub): State<Arc<Stub>>, Json(body): Json<Value>) -> Json<Value> {
    stub.heartbeats.lock().unwrap().push(body);
    let commands = std::mem::take(&mut *stub.pending.lock().unwrap());
    Json(Value::Array(commands))
}

async fn ack(State(stub): State<Arc<Stub>>, Json(body): Json<Value>) -> axum::http::StatusCode {
    stub.acks.lock().unwrap().push(body);
    axum::http::StatusCode::OK
}

async fn health(State(stub): State<Arc<Stub>>) -> (axum::http::StatusCode, Json<Value>) {
    if stub.healthy.load(Ordering::Relaxed) {
        (
            axum::http::StatusCode::OK,
            Json(json!({
                "status": "running",
                "paused": stub.paused.load(Ordering::Relaxed)
            })),
        )
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(json!({})))
    }
}

async fn pause(State(stub): State<Arc<Stub>>) -> axum::http::StatusCode {
    stub.paused.store(true, Ordering::Relaxed);
    axum::http::StatusCode::OK
}

async fn resume(State(stub): State<Arc<Stub>>) -> axum::http::StatusCode {
    stub.paused.store(false, Ordering::Relaxed);
    axum::http::StatusCode::OK
}

async fn wait_state(manager: &InstanceManager, expected: InstanceState) -> bool {
    for _ in 0..200 {
        if manager.snapshot().await.state == expected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn wait_restarts(manager: &InstanceManager, min: u32) -> bool {
    for _ in 0..200 {
        if manager.snapshot().await.restarts >= min {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn wait_until(condition: impl Fn() -> bool) -> bool {
    for _ in 0..200 {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}
