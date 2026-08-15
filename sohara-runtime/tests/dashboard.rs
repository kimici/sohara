//! D1 dashboard tests: status / errors / history / approvals / token auth

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use sohara_builtins::register_all;
use sohara_config::FlowConfig;
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, EventBus, Record, Result, Sink, StepFactory,
};
use sohara_persistence::JsonFileStore;
use sohara_runtime::{serve_with_shutdown_opts, ServeOptions, StatsSnapshot};
use sohara_triggers::InProcessBus;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct ServeHarness {
    bus: Arc<InProcessBus>,
    serve: tokio::task::JoinHandle<Result<StatsSnapshot>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ServeHarness {
    async fn stop(mut self) -> StatsSnapshot {
        self.shutdown.take().unwrap().send(()).unwrap();
        finish_serve(self.serve).await
    }
}

#[tokio::test]
async fn dashboard_exposes_status_errors_and_history() {
    let history = temp_path("sohara-dash", "jsonl");
    let addr = free_addr();
    let options = ServeOptions {
        admin: Some(addr),
        history: Some(history.clone()),
        ..ServeOptions::default()
    };
    let harness = start_serve(ERROR_FLOW, options);
    tokio::time::sleep(Duration::from_millis(200)).await;
    harness.bus.publish("hello", json!({"n": 1})).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    check_status_and_errors(addr).await;
    let stats = harness.stop().await;
    assert_eq!(stats.errors, 1);
    assert_history_entry(&history, "dash-test");
    std::fs::remove_file(&history).ok();
}

#[tokio::test]
async fn admin_token_is_enforced() {
    let addr = free_addr();
    let options = ServeOptions {
        admin: Some(addr),
        admin_token: Some("sekret".to_owned()),
        ..ServeOptions::default()
    };
    let harness = start_serve(ERROR_FLOW, options);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (status, _) = request(addr, "/admin/health", "GET", None).await;
    assert_eq!(status, 401);
    let (status, body) = request(addr, "/admin/health", "GET", Some("sekret")).await;
    assert_eq!(status, 200);
    assert!(body.contains(r#""status":"running""#), "got: {body}");
    let _ = harness.stop().await;
}

#[tokio::test]
async fn approvals_endpoint_lists_parked_records() {
    let store_path = temp_path("sohara-dash-store", "json");
    let addr = free_addr();
    let options = ServeOptions {
        admin: Some(addr),
        store: Some(Arc::new(JsonFileStore::new(store_path.clone()).unwrap())),
        ..ServeOptions::default()
    };
    let harness = start_serve(APPROVE_FLOW, options);
    tokio::time::sleep(Duration::from_millis(200)).await;
    harness.bus.publish("hello", json!({"order": "A"})).unwrap();
    harness.bus.publish("hello", json!({"order": "B"})).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let approvals: Value = get_json(addr, "/admin/approvals").await;
    let queues = approvals.as_array().unwrap();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["step"], json!("gate"));
    assert_eq!(queues[0]["count"], json!(2));
    let stats = harness.stop().await;
    assert_eq!(stats.waiting, 2);
    std::fs::remove_file(&store_path).ok();
}

fn start_serve(yaml: &str, options: ServeOptions) -> ServeHarness {
    let flow = Arc::new(FlowConfig::from_yaml_str(yaml).expect("valid flow"));
    let registry = Arc::new(registry_with_probe(Arc::new(Mutex::new(Vec::new()))));
    let bus = Arc::new(InProcessBus::new(16));
    let serve_bus = bus.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        serve_with_shutdown_opts(
            &flow,
            &registry,
            serve_bus,
            async move {
                let _ = shutdown_rx.await;
            },
            options,
        )
        .await
    });
    ServeHarness {
        bus,
        serve,
        shutdown: Some(shutdown_tx),
    }
}

async fn finish_serve(serve: tokio::task::JoinHandle<Result<StatsSnapshot>>) -> StatsSnapshot {
    tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve must shut down")
        .unwrap()
        .unwrap()
}

async fn check_status_and_errors(addr: SocketAddr) {
    let status: Value = get_json(addr, "/admin/status").await;
    assert_eq!(status["flow"], json!("dash-test"));
    assert!(!status["run_id"].as_str().unwrap().is_empty());
    assert_eq!(status["paused"], json!(false));
    assert_eq!(status["triggers"], json!([{"id": "bus", "kind": "queue"}]));
    assert_eq!(status["stats"]["errors"], json!(1));
    let errors: Value = get_json(addr, "/admin/errors").await;
    assert_eq!(errors.as_array().unwrap().len(), 1);
    assert_eq!(errors[0]["step"], json!("check"));
    assert_eq!(errors[0]["kind"], json!("transform"));
    let approvals: Value = get_json(addr, "/admin/approvals").await;
    assert_eq!(approvals, json!([]));
}

fn assert_history_entry(path: &Path, flow: &str) {
    let text = std::fs::read_to_string(path).expect("history file written");
    assert_eq!(text.lines().count(), 1);
    assert!(text.contains(&format!(r#""flow":"{flow}""#)), "got: {text}");
    assert!(text.contains(r#""status":"ok""#), "got: {text}");
}

fn temp_path(prefix: &str, ext: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{prefix}-{}.{ext}", std::process::id()));
    std::fs::remove_file(&path).ok();
    path
}

async fn get_json(addr: SocketAddr, path: &str) -> Value {
    let (status, body) = request(addr, path, "GET", None).await;
    assert_eq!(status, 200, "got: {body}");
    serde_json::from_str(&body).expect("valid json")
}

async fn request(addr: SocketAddr, path: &str, method: &str, token: Option<&str>) -> (u16, String) {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = token
        .map(|value| format!("Authorization: Bearer {value}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\nContent-Length: 0\r\n\r\n"
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut buffer = Vec::new();
    socket.read_to_end(&mut buffer).await.unwrap();
    let text = String::from_utf8_lossy(&buffer).into_owned();
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    (status, decode_chunked(body))
}

/// Decode an HTTP/1.1 chunked response body (axum uses chunked encoding).
fn decode_chunked(body: &str) -> String {
    if !body.contains("\r\n") {
        return body.to_owned();
    }
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut rest = bytes;
    loop {
        let Some(end) = rest.windows(2).position(|window| window == b"\r\n") else {
            out.extend_from_slice(rest);
            break;
        };
        let size_text = String::from_utf8_lossy(&rest[..end]);
        let Ok(size) = usize::from_str_radix(size_text.trim(), 16) else {
            out.extend_from_slice(rest);
            break;
        };
        if size == 0 {
            break;
        }
        let data_start = end + 2;
        out.extend_from_slice(&rest[data_start..data_start + size]);
        rest = &rest[data_start + size + 2..];
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

const ERROR_FLOW: &str = r#"
name: dash-test
version: "1"
triggers:
  - { id: bus, type: queue, topic: hello }
steps:
  - id: check
    kind: transform
    type: assert
    on_error: continue
    config: { expect: [{ field: n, op: gt, value: 10 }] }
  - { id: out, kind: sink, type: probe }
edges: [[bus, check], [check, out]]
"#;

const APPROVE_FLOW: &str = r#"
name: dash-approve
version: "1"
triggers:
  - { id: bus, type: queue, topic: hello }
steps:
  - { id: gate, kind: control, type: approve, config: { title: "审批", owners: [alice] } }
  - { id: out, kind: sink, type: probe }
edges: [[bus, gate], [gate, out]]
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
