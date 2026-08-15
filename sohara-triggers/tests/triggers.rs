//! Integration tests for triggers: bus / queue / cron / http

use std::time::Duration;

use futures::StreamExt;
use serde_json::json;
use sohara_core::{EventBus, Source, Trigger};
use sohara_triggers::{CronSource, HttpSource, InProcessBus, QueueSource};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn bus_delivers_to_subscribers() {
    let bus = InProcessBus::new(4);
    let mut first = bus.subscribe("hello");
    let mut second = bus.subscribe("hello");
    bus.publish("hello", json!({"n": 1})).unwrap();
    bus.publish("other", json!({"n": 2})).unwrap();
    assert_eq!(first.recv().await, Some(json!({"n": 1})));
    assert_eq!(second.recv().await, Some(json!({"n": 1})));
    bus.publish("hello", json!({"n": 3})).unwrap();
    assert_eq!(first.recv().await, Some(json!({"n": 3})));
    assert_eq!(second.recv().await, Some(json!({"n": 3})));
}

#[tokio::test]
async fn queue_source_yields_published_records_and_stops() {
    let bus = InProcessBus::new(4);
    let source = QueueSource::new("hello", &bus);
    let mut stream = source.stream().await.unwrap();
    bus.publish("hello", json!({"a": 1})).unwrap();
    let record = stream.next().await.unwrap().unwrap();
    assert_eq!(record.to_json(), json!({"a": 1}));
    source.stop().await.unwrap();
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn cron_source_emits_on_schedule_and_stops() {
    let source = CronSource::new("*/1 * * * * *").unwrap();
    let mut stream = source.stream().await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("first tick within 3s")
        .unwrap()
        .unwrap();
    assert!(first.get("scheduled_at").is_some());
    source.stop().await.unwrap();
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn http_source_accepts_requests_and_stops() {
    let port = free_port().await;
    let source = HttpSource::new("POST", "/webhook", "127.0.0.1", port);
    source.start().await.unwrap();
    let mut stream = source.stream().await.unwrap();

    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let body = r#"{"hello":"world"}"#;
    let request = format!(
        "POST /webhook HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 202"), "got: {response}");

    let record = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("record within 3s")
        .unwrap()
        .unwrap();
    assert_eq!(record.to_json(), json!({"hello": "world"}));

    source.stop().await.unwrap();
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn http_source_rejects_wrong_method() {
    let port = free_port().await;
    let source = HttpSource::new("POST", "/webhook", "127.0.0.1", port);
    source.start().await.unwrap();
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    socket
        .write_all(b"GET /webhook HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 405"), "got: {response}");
    source.stop().await.unwrap();
}

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    listener.local_addr().unwrap().port()
}
