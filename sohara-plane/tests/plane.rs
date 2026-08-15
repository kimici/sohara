//! D3 integration: declare on the plane → agent spawns/stops a real process

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use sohara_agent::{Agent, AgentConfig, HttpTransport, NodeConfig, PlaneConfig};
use sohara_plane::{Plane, Registry};

/// A running plane + agent pair over a temp state file.
struct Env {
    addr: SocketAddr,
    state: PathBuf,
    agent: tokio::task::JoinHandle<anyhow::Result<()>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    client: reqwest::Client,
}

impl Env {
    async fn start(state: PathBuf) -> Self {
        std::fs::remove_file(&state).ok();
        let registry = Registry::load(Some(state.clone()));
        let plane = Plane::new(registry, None);
        let addr = serve_plane(plane).await;
        let (agent, shutdown) = spawn_agent(addr);
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        Self {
            addr,
            state,
            agent,
            shutdown,
            client,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    async fn declare(&self, desired: &str) -> reqwest::Response {
        self.client
            .post(self.url("/api/instances"))
            .json(&json!({
                "id": "i1",
                "node": "n1",
                "desired": desired,
                "spec": {
                    "id": "i1",
                    "flow": "unused",
                    "bin": "sh",
                    "args": ["-c", "exec sleep 60"],
                    "health_enabled": false
                }
            }))
            .send()
            .await
            .unwrap()
    }

    async fn set_desired(&self, desired: &str) -> reqwest::Response {
        self.client
            .put(self.url("/api/instances/i1/desired"))
            .json(&json!({ "desired": desired }))
            .send()
            .await
            .unwrap()
    }

    async fn get_json(&self, path: &str) -> Value {
        self.client
            .get(self.url(path))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn wait_actual(&self, expected: &str) -> String {
        for _ in 0..200 {
            let value = self.get_json("/api/instances").await;
            if let Some(actual) = value
                .as_array()
                .and_then(|items| items.iter().find(|item| item["id"] == json!("i1")))
                .and_then(|item| item["actual"].as_str())
            {
                if actual == expected {
                    return actual.to_owned();
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        "not-found".to_owned()
    }

    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).unwrap();
        }
        self.agent.await.unwrap().unwrap();
        std::fs::remove_file(&self.state).ok();
    }
}

fn spawn_agent(
    addr: SocketAddr,
) -> (
    tokio::task::JoinHandle<anyhow::Result<()>>,
    Option<tokio::sync::oneshot::Sender<()>>,
) {
    let config = AgentConfig {
        node: NodeConfig {
            id: "n1".to_owned(),
        },
        plane: PlaneConfig {
            url: format!("http://{addr}"),
            token: None,
        },
        instances: vec![],
        heartbeat_ms: 100,
    };
    let transport = Arc::new(HttpTransport::new(config.plane.url.clone(), None));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let agent = tokio::spawn(async move {
        Agent::new(&config, transport)
            .run(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    (agent, Some(shutdown_tx))
}

#[tokio::test]
async fn declared_instance_runs_via_reconciliation() {
    let env = Env::start(temp_state("sohara-plane-run")).await;
    assert_eq!(env.declare("running").await.status(), 201);
    assert_eq!(env.wait_actual("running").await, "running");
    let nodes = env.get_json("/api/nodes").await;
    assert!(
        nodes[0]["id"] == json!("n1"),
        "plane must discover the node"
    );
    env.stop().await;
}

#[tokio::test]
async fn desired_stop_halts_instance_and_persists() {
    let env = Env::start(temp_state("sohara-plane-stop")).await;
    env.declare("running").await;
    env.wait_actual("running").await;
    assert_eq!(env.set_desired("stopped").await.status(), 200);
    assert_eq!(env.wait_actual("stopped").await, "stopped");
    let persisted = std::fs::read_to_string(&env.state).expect("state persisted");
    assert!(
        persisted.contains(r#""desired": "stopped""#),
        "got: {persisted}"
    );
    env.stop().await;
}

#[tokio::test]
async fn plane_token_is_enforced() {
    let registry = Registry::load(None);
    let plane = Plane::new(registry, Some("plane-token".to_owned()));
    let addr = serve_plane(plane).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let status = client
        .get(format!("http://{addr}/api/nodes"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 401);
    let status = client
        .get(format!("http://{addr}/api/nodes"))
        .bearer_auth("plane-token")
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 200);
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

fn temp_state(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{label}-{}.json", std::process::id()))
}
