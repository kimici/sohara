//! Shared helpers for the sohara-js host API tests
#![allow(dead_code)] // each test binary uses a different subset

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{json, Map, Value};
use sohara_core::{
    BuildContext, BuiltStep, ComponentRegistry, EventBus, Record, StepKind, StepMeta, Transform,
    TransformOutcome,
};

/// A registry with the script steps registered.
pub fn registry() -> ComponentRegistry {
    let mut registry = ComponentRegistry::new();
    sohara_js::register_all(&mut registry);
    registry
}

/// A build context with the default `test` flow / `s1` step identity.
pub fn ctx(vars: Map<String, Value>, bus: Option<Arc<dyn EventBus>>) -> BuildContext {
    BuildContext {
        vars,
        bus,
        flow: "test".to_owned(),
        step: Some(StepMeta {
            id: "s1".to_owned(),
            name: "tag".to_owned(),
            kind: "transform".to_owned(),
            step_type: "script".to_owned(),
        }),
    }
}

/// Build a script transform step from config.
pub fn transform(config: Value, ctx: BuildContext) -> Box<dyn Transform> {
    let registry = registry();
    match registry
        .build(StepKind::Transform, "script", &config, &ctx)
        .unwrap()
    {
        BuiltStep::Transform(step) => step,
        _ => panic!("expected transform step"),
    }
}

/// Wrap inline code as a script config object.
pub fn inline(code: &str) -> Value {
    json!({ "inline": code })
}

/// Unwrap a `Pass` outcome into its payload.
pub fn pass(outcome: TransformOutcome) -> Value {
    match outcome {
        TransformOutcome::Pass(record) => record.payload,
        other => panic!("expected pass, got {other:?}"),
    }
}

/// Unwrap an `Expand` outcome into its records.
pub fn expand(outcome: TransformOutcome) -> Vec<Record> {
    match outcome {
        TransformOutcome::Expand(records) => records,
        other => panic!("expected expand, got {other:?}"),
    }
}

/// A scratch directory unique to this test binary.
pub fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sohara-js-{tag}-{:?}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A minimal HTTP/1.1 stub on its own thread (the script's blocking call must
/// never run on the same executor as the server).
pub fn spawn_http_stub() -> (std::thread::JoinHandle<()>, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let body = r#"{"greeting":"hi","n":3}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Test: yes\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            });
        }
    });
    (handle, port)
}

/// An in-memory event bus recording published events.
#[derive(Default)]
pub struct TestBus {
    pub events: std::sync::Mutex<Vec<(String, Value)>>,
}

impl EventBus for TestBus {
    fn publish(&self, topic: &str, payload: Value) -> sohara_core::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push((topic.to_owned(), payload));
        Ok(())
    }
}
