//! D5a relay bridge tests: publish forwarding and remote-message injection

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use sohara_core::EventBus;
use sohara_triggers::relay::{PullRequest, PullResponse, RelayMessage};
use sohara_triggers::{InProcessBus, RelayBus};

#[derive(Default)]
struct Mailbox {
    published: Mutex<Vec<Value>>,
    queues: Mutex<HashMap<String, Vec<(u64, Value)>>>,
}

#[tokio::test]
async fn relay_publishes_locally_and_forwards_to_the_plane() {
    let mailbox = Arc::new(Mailbox::default());
    let addr = serve_mailbox(mailbox.clone()).await;
    let local = Arc::new(InProcessBus::new(16));
    let mut receiver = local.subscribe("orders");
    let bridge = RelayBus::spawn(
        local,
        format!("http://{addr}"),
        None,
        vec!["orders".to_owned()],
    );
    bridge.publish("orders", json!({"n": 1})).unwrap();
    let local_message = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("local subscriber must receive")
        .expect("message");
    assert_eq!(local_message, json!({"n": 1}));
    wait_until(|| mailbox.published.lock().unwrap().len() == 1).await;
    let published = mailbox.published.lock().unwrap();
    assert_eq!(published[0]["topic"], json!("orders"));
    assert_eq!(published[0]["payload"], json!({"n": 1}));
    bridge.stop();
}

#[tokio::test]
async fn relay_injects_remote_messages_into_the_local_bus() {
    let mailbox = Arc::new(Mailbox::default());
    mailbox
        .queues
        .lock()
        .unwrap()
        .insert("orders".to_owned(), vec![(1, json!({"from": "remote"}))]);
    let addr = serve_mailbox(mailbox).await;
    let local = Arc::new(InProcessBus::new(16));
    let mut receiver = local.subscribe("orders");
    let bridge = RelayBus::spawn(
        local,
        format!("http://{addr}"),
        None,
        vec!["orders".to_owned()],
    );
    let injected = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("pull loop must inject remote messages")
        .expect("message");
    assert_eq!(injected, json!({"from": "remote"}));
    bridge.stop();
}

async fn serve_mailbox(mailbox: Arc<Mailbox>) -> SocketAddr {
    let router = Router::new()
        .route("/relay/publish", post(publish))
        .route("/relay/pull", post(pull))
        .with_state(mailbox);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    addr
}

async fn publish(
    State(mailbox): State<Arc<Mailbox>>,
    Json(body): Json<Value>,
) -> axum::http::StatusCode {
    mailbox.published.lock().unwrap().push(body);
    axum::http::StatusCode::ACCEPTED
}

async fn pull(
    State(mailbox): State<Arc<Mailbox>>,
    Json(body): Json<PullRequest>,
) -> Json<PullResponse> {
    let queues = mailbox.queues.lock().unwrap();
    let mut response = PullResponse::default();
    for sub in &body.subscriptions {
        let Some(messages) = queues.get(&sub.topic) else {
            continue;
        };
        for (seq, payload) in messages {
            if *seq > sub.after {
                response.messages.push(RelayMessage {
                    topic: sub.topic.clone(),
                    seq: *seq,
                    payload: payload.clone(),
                });
            }
        }
        if let Some((seq, _)) = messages.last() {
            response
                .next
                .push(sohara_triggers::relay::PullSubscription {
                    topic: sub.topic.clone(),
                    after: *seq,
                });
        }
    }
    Json(response)
}

async fn wait_until(condition: impl Fn() -> bool) {
    for _ in 0..100 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("condition not met in time");
}
