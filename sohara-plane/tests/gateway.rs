//! D4 gateway tests: routing strategies, health eviction, auth scope

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use sohara_agent::{Heartbeat, InstanceReport, InstanceState};
use sohara_plane::{Plane, Registry, RouteDecl, RouteMode, Strategy};

struct Env {
    addr: SocketAddr,
    registry: Arc<Registry>,
    hits: Vec<Arc<AtomicUsize>>,
    instances: Vec<(String, String)>,
    client: reqwest::Client,
}

impl Env {
    /// Two fake instance triggers + one plane with two declared running
    /// instances and the given route.
    async fn start(strategy: Strategy) -> Self {
        let registry = Registry::load(None);
        let (hits, triggers) = spawn_instances(2).await;
        let instances = declare_instances(&registry, &triggers).await;
        registry
            .declare_route(RouteDecl {
                id: "r1".to_owned(),
                path: "/webhook/orders".to_owned(),
                flow_id: "orders".to_owned(),
                mode: RouteMode::Proxy,
                strategy,
                sticky_key: Some("X-Order-Id".to_owned()),
                topic: None,
            })
            .await
            .unwrap();
        let plane = Plane::new(registry.clone(), Some("tok".to_owned()));
        let addr = serve_plane(plane).await;
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        Self {
            addr,
            registry,
            hits,
            instances,
            client,
        }
    }

    async fn forward(&self, header: Option<&str>) -> reqwest::Response {
        let mut request = self
            .client
            .post(format!("http://{}/gw/webhook/orders/new", self.addr))
            .json(&json!({"n": 1}));
        if let Some(value) = header {
            request = request.header("X-Order-Id", value);
        }
        request.send().await.unwrap()
    }

    /// Update one instance's reported state via a synthetic heartbeat.
    async fn set_state(&self, id: &str, state: InstanceState) {
        let states: Vec<(String, InstanceState, String)> = self
            .instances
            .iter()
            .map(|(other, trigger)| {
                let state = if other == id {
                    state
                } else {
                    InstanceState::Running
                };
                (other.clone(), state, trigger.clone())
            })
            .collect();
        report_states(&self.registry, &states).await;
    }
}

#[tokio::test]
async fn round_robin_distributes_across_instances() {
    let env = Env::start(Strategy::RoundRobin).await;
    for _ in 0..4 {
        assert_eq!(env.forward(None).await.status(), 200);
    }
    assert_eq!(env.hits[0].load(Ordering::Relaxed), 2);
    assert_eq!(env.hits[1].load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn hash_strategy_sticks_one_key_to_one_instance() {
    let env = Env::start(Strategy::Hash).await;
    for _ in 0..4 {
        assert_eq!(env.forward(Some("same-key")).await.status(), 200);
    }
    let first = env.hits[0].load(Ordering::Relaxed);
    let second = env.hits[1].load(Ordering::Relaxed);
    assert!(
        (first, second) == (4, 0) || (first, second) == (0, 4),
        "one key must stick to one instance, got {first}/{second}"
    );
}

#[tokio::test]
async fn stopped_instances_are_evicted() {
    let env = Env::start(Strategy::RoundRobin).await;
    assert_eq!(env.forward(None).await.status(), 200);
    let stopped_id = if env.hits[0].load(Ordering::Relaxed) == 1 {
        "orders-1"
    } else {
        "orders-2"
    };
    let survivor = if stopped_id == "orders-1" { 1 } else { 0 };
    env.set_state(stopped_id, InstanceState::Stopped).await;
    for _ in 0..4 {
        assert_eq!(env.forward(None).await.status(), 200);
    }
    assert_eq!(
        env.hits[survivor].load(Ordering::Relaxed),
        4,
        "post-eviction traffic must go only to the healthy instance"
    );
}

#[tokio::test]
async fn no_candidates_returns_503_and_gateway_skips_auth() {
    let registry = Registry::load(None);
    registry
        .declare_route(RouteDecl {
            id: "r1".to_owned(),
            path: "/webhook/orders".to_owned(),
            flow_id: "orders".to_owned(),
            mode: RouteMode::Proxy,
            strategy: Strategy::RoundRobin,
            sticky_key: None,
            topic: None,
        })
        .await
        .unwrap();
    let plane = Plane::new(registry, Some("tok".to_owned()));
    let addr = serve_plane(plane).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let status = client
        .get(format!("http://{addr}/gw/webhook/orders/new"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 503, "no candidates must yield 503 without auth");
}

#[tokio::test]
async fn bus_mode_publishes_into_the_relay_mailbox() {
    let registry = Registry::load(None);
    registry.declare_route(bus_route()).await.unwrap();
    let plane = Plane::new(registry, None);
    let addr = serve_plane(plane).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let status = client
        .post(format!("http://{addr}/gw/tasks/orders/new"))
        .json(&json!({"order": "A"}))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 202);
    let pulled = pull_relay(&client, addr).await;
    assert_eq!(pulled["messages"][0]["payload"], json!({"order": "A"}));
}

fn bus_route() -> RouteDecl {
    RouteDecl {
        id: "r1".to_owned(),
        path: "/tasks/orders".to_owned(),
        flow_id: "orders".to_owned(),
        mode: RouteMode::Bus,
        strategy: Strategy::RoundRobin,
        sticky_key: None,
        topic: Some("orders.events".to_owned()),
    }
}

async fn pull_relay(client: &reqwest::Client, addr: SocketAddr) -> Value {
    client
        .post(format!("http://{addr}/relay/pull"))
        .json(&json!({
            "subscriber": "sub-1",
            "subscriptions": [{ "topic": "orders.events", "after": 0 }]
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn declare_instances(registry: &Registry, triggers: &[String]) -> Vec<(String, String)> {
    let mut instances = Vec::new();
    for (index, trigger) in triggers.iter().enumerate() {
        let id = format!("orders-{}", index + 1);
        registry
            .declare_instance(sohara_plane::InstanceDecl {
                id: id.clone(),
                node: "n1".to_owned(),
                flow_id: Some("orders".to_owned()),
                desired: sohara_plane::Desired::Running,
                spec: sohara_agent::InstanceSpec {
                    id: id.clone(),
                    flow: "unused".to_owned(),
                    trigger: Some(trigger.clone()),
                    ..sohara_agent::InstanceSpec::default()
                },
            })
            .await
            .unwrap();
        instances.push((id, trigger.clone()));
    }
    let states: Vec<(String, InstanceState, String)> = instances
        .iter()
        .map(|(id, trigger)| (id.clone(), InstanceState::Running, trigger.clone()))
        .collect();
    report_states(registry, &states).await;
    instances
}

async fn report_states(registry: &Registry, states: &[(String, InstanceState, String)]) {
    let reports: Vec<InstanceReport> = states
        .iter()
        .map(|(id, state, trigger)| InstanceReport {
            id: id.clone(),
            state: *state,
            paused: false,
            healthy: true,
            restarts: 0,
            admin: None,
            trigger: Some(trigger.clone()),
        })
        .collect();
    let _ = registry
        .heartbeat(&Heartbeat {
            node_id: "n1".to_owned(),
            time: "now".to_owned(),
            instances: reports,
        })
        .await;
}

async fn spawn_instances(count: usize) -> (Vec<Arc<AtomicUsize>>, Vec<String>) {
    let mut hits = Vec::new();
    let mut triggers = Vec::new();
    for _ in 0..count {
        let counter = Arc::new(AtomicUsize::new(0));
        let addr = serve_instance(counter.clone()).await;
        hits.push(counter);
        triggers.push(addr.to_string());
    }
    (hits, triggers)
}

async fn serve_instance(hits: Arc<AtomicUsize>) -> SocketAddr {
    let router = Router::new()
        .route("/*path", get(handler).post(handler))
        .with_state(hits);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    addr
}

async fn handler(State(hits): State<Arc<AtomicUsize>>) -> Json<Value> {
    hits.fetch_add(1, Ordering::Relaxed);
    Json(json!({ "ok": true }))
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
