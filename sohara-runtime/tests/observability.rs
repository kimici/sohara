//! S6 observability tests: pause gate, run reports, and the admin API

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use sohara_builtins::register_all;
use sohara_config::FlowConfig;
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, EventBus, Record, Result, Sink, StepFactory,
};
use sohara_runtime::{serve_with_shutdown_opts, PauseGate, ServeOptions, StatsSnapshot};
use sohara_triggers::InProcessBus;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn pause_gate_blocks_until_resumed() {
    let gate = Arc::new(PauseGate::default());
    gate.set_paused(true);
    let waiter = tokio::spawn({
        let gate = gate.clone();
        async move {
            gate.wait_unpaused().await;
            true
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "wait must block while paused");
    gate.set_paused(false);
    assert!(waiter.await.unwrap(), "wait must complete after resume");
}

#[tokio::test]
async fn run_report_carries_run_id_and_step_stats() {
    let yaml = r#"
name: report-test
version: "1"
steps:
  - { id: in, kind: source, type: inline, config: { records: [{n: 1}, {n: 2}, {n: 3}] } }
  - { id: adult, kind: transform, type: filter, config: { where: "n >= 2" } }
  - { id: out, kind: sink, type: log }
"#;
    let flow = FlowConfig::from_yaml_str(yaml).unwrap();
    let mut registry = ComponentRegistry::new();
    register_all(&mut registry);
    let report = sohara_runtime::run_flow_with_store_report(&flow, &registry, None, false)
        .await
        .unwrap();
    assert!(!report.run_id.is_empty());
    assert!(!report.started_at.is_empty());
    assert_eq!(report.stats.processed, 2);
    assert_eq!(report.stats.filtered, 1);
    let adult = report.steps.get("adult").expect("filter step stat");
    assert_eq!(adult.filtered, 1);
    let out = report.steps.get("out").expect("sink step stat");
    assert_eq!(out.processed, 2);
}

#[tokio::test]
async fn admin_api_pauses_intake_and_serves_metrics() {
    let flow = Arc::new(FlowConfig::from_yaml_str(SERVE_YAML).expect("valid flow"));
    let records = Arc::new(Mutex::new(Vec::new()));
    let registry = Arc::new(registry_with_probe(records.clone()));
    let bus = Arc::new(InProcessBus::new(16));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let addr = free_addr();
    let serve = spawn_admin_serve(
        flow.clone(),
        registry.clone(),
        bus.clone(),
        shutdown_rx,
        addr,
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_body(addr, "GET", "/admin/health", r#""paused":false"#).await;
    assert_body(addr, "POST", "/admin/pause", r#""paused":true"#).await;
    bus.publish("hello", json!({"n": 1})).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        records.lock().unwrap().is_empty(),
        "paused intake must block"
    );
    assert_body(addr, "POST", "/admin/resume", r#""paused":false"#).await;
    wait_until(|| !records.lock().unwrap().is_empty()).await;
    assert_body(addr, "GET", "/admin/metrics", r#""processed":1"#).await;
    shutdown_tx.send(()).unwrap();
    let stats = finish_serve(serve).await;
    assert_eq!(stats.processed, 1);
}

async fn assert_body(addr: SocketAddr, method: &str, path: &str, needle: &str) {
    let body = request(addr, path, method).await;
    assert!(body.contains(needle), "got: {body}");
}

async fn finish_serve(serve: tokio::task::JoinHandle<Result<StatsSnapshot>>) -> StatsSnapshot {
    tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve must shut down")
        .unwrap()
        .unwrap()
}

fn spawn_admin_serve(
    flow: Arc<FlowConfig>,
    registry: Arc<ComponentRegistry>,
    bus: Arc<InProcessBus>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    addr: SocketAddr,
) -> tokio::task::JoinHandle<Result<StatsSnapshot>> {
    tokio::spawn(async move {
        let options = ServeOptions {
            store: None,
            admin: Some(addr),
        };
        serve_with_shutdown_opts(
            &flow,
            &registry,
            bus,
            async move {
                let _ = shutdown_rx.await;
            },
            options,
        )
        .await
    })
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn request(addr: SocketAddr, path: &str, method: &str) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut buffer = Vec::new();
    socket.read_to_end(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer).into_owned()
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..50 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("condition not met in time");
}

const SERVE_YAML: &str = r#"
name: admin-test
version: "1"
triggers:
  - { id: bus, type: queue, topic: hello }
steps:
  - { id: out, kind: sink, type: probe }
edges: [[bus, out]]
"#;

struct Probe {
    records: Arc<Mutex<Vec<Record>>>,
}

#[async_trait::async_trait]
impl Sink for Probe {
    async fn send(&self, record: Record) -> Result<()> {
        self.records.lock().unwrap().push(record);
        Ok(())
    }

    fn name(&self) -> &str {
        "probe"
    }
}

fn registry_with_probe(records: Arc<Mutex<Vec<Record>>>) -> ComponentRegistry {
    let mut registry = ComponentRegistry::new();
    register_all(&mut registry);
    registry.register(sohara_core::StepKind::Sink, "probe", probe_factory(records));
    registry
}

fn probe_factory(records: Arc<Mutex<Vec<Record>>>) -> Arc<dyn StepFactory> {
    struct Factory(Arc<Mutex<Vec<Record>>>);
    impl StepFactory for Factory {
        fn build(&self, _config: &Value, _ctx: &BuildContext) -> Result<BuiltStep> {
            Ok(BuiltStep::Sink(Box::new(Probe {
                records: self.0.clone(),
            })))
        }
    }
    Arc::new(Factory(records))
}
