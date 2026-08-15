//! Serve-mode integration test: queue trigger → pipeline → probe sink

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use sohara_builtins::register_all;
use sohara_config::FlowConfig;
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, EventBus, Record, Result, Sink, StepFactory,
};
use sohara_runtime::{serve_with_shutdown, StatsSnapshot};
use sohara_triggers::InProcessBus;

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

struct Probe {
    records: Arc<Mutex<Vec<Record>>>,
}

#[tokio::test]
async fn queue_trigger_flows_to_sink_and_graceful_shutdown() {
    let flow = Arc::new(FlowConfig::from_yaml_str(SERVE_YAML).expect("valid flow"));
    let records = Arc::new(Mutex::new(Vec::new()));
    let registry = Arc::new(registry_with_probe(records.clone()));
    let bus = Arc::new(InProcessBus::new(16));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = spawn_serve(flow, registry, bus.clone(), shutdown_rx);
    publish_and_wait(&bus).await;
    shutdown_tx.send(()).unwrap();
    let stats = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve must shut down")
        .unwrap()
        .unwrap();
    assert_eq!(stats.processed, 2);
    let records = records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|r| r.get("seen") == Some(&json!(true))));
}

fn spawn_serve(
    flow: Arc<FlowConfig>,
    registry: Arc<ComponentRegistry>,
    bus: Arc<InProcessBus>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<Result<StatsSnapshot>> {
    tokio::spawn(async move {
        serve_with_shutdown(&flow, &registry, bus, None, async move {
            let _ = shutdown_rx.await;
        })
        .await
    })
}

async fn publish_and_wait(bus: &InProcessBus) {
    tokio::time::sleep(Duration::from_millis(100)).await;
    bus.publish("hello", json!({"n": 1})).unwrap();
    bus.publish("hello", json!({"n": 2})).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

const SERVE_YAML: &str = r#"
name: serve-test
version: "1"
triggers:
  - { id: bus, type: queue, topic: hello }
steps:
  - { id: tag, kind: transform, type: add_field, config: { field: seen, value: true } }
  - { id: out, kind: sink, type: probe }
edges: [[bus, tag], [tag, out]]
"#;

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
