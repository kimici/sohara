//! Plane relay bridge (D5a): forwards local publishes to the plane and
//! injects remote messages from the plane into the local in-process bus.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;
use tokio::sync::Mutex;

use sohara_core::{EventBus, Result};

use crate::bus::InProcessBus;

/// One subscription cursor update from the plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullSubscription {
    pub topic: String,
    pub after: u64,
}

/// One relayed message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayMessage {
    pub topic: String,
    pub seq: u64,
    pub payload: Value,
}

/// Body of `POST /relay/pull`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequest {
    pub subscriber: String,
    pub subscriptions: Vec<PullSubscription>,
}

/// Response of `POST /relay/pull`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PullResponse {
    #[serde(default)]
    pub messages: Vec<RelayMessage>,
    #[serde(default)]
    pub next: Vec<PullSubscription>,
}

/// A bus bridge: publish fans out locally and to the plane relay; a pull
/// loop moves remote messages into the local bus (D5a).
pub struct RelayBus {
    local: Arc<InProcessBus>,
    client: reqwest::Client,
    plane: String,
    token: Option<String>,
    subscriber: String,
    cursors: Mutex<HashMap<String, u64>>,
    stop: watch::Sender<bool>,
}

impl RelayBus {
    /// Build the bridge and spawn the pull loop for `topics`.
    ///
    /// `subscriber` identifies this consumer to the plane; pass a stable id
    /// (e.g. the instance admin address) so the plane's cursor floor survives
    /// process restarts. Falls back to a random id when `None`.
    #[must_use]
    pub fn spawn(
        local: Arc<InProcessBus>,
        plane: String,
        token: Option<String>,
        topics: Vec<String>,
        subscriber: Option<String>,
    ) -> Arc<Self> {
        let (stop, stop_rx) = watch::channel(false);
        let bridge = Arc::new(Self {
            local,
            client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("relay http client"),
            plane,
            token,
            subscriber: subscriber.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            cursors: Mutex::new(HashMap::new()),
            stop,
        });
        tokio::spawn(pull_loop(bridge.clone(), topics, stop_rx));
        bridge
    }

    /// Stop the pull loop (graceful shutdown).
    pub fn stop(&self) {
        let _ = self.stop.send(true);
    }
}

impl EventBus for RelayBus {
    fn publish(&self, topic: &str, payload: Value) -> Result<()> {
        self.local.publish(topic, payload.clone())?;
        let body = serde_json::json!({ "topic": topic, "payload": payload });
        let client = self.client.clone();
        let plane = self.plane.clone();
        let token = self.token.clone();
        tokio::spawn(async move {
            let mut request = client.post(format!("{plane}/relay/publish")).json(&body);
            if let Some(token) = &token {
                request = request.bearer_auth(token);
            }
            if let Err(error) = request.send().await {
                tracing::warn!("relay publish failed: {error}");
            }
        });
        Ok(())
    }
}

async fn pull_loop(bridge: Arc<RelayBus>, topics: Vec<String>, mut stop: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(error) = pull_once(&bridge, &topics).await {
                    tracing::debug!("relay pull failed: {error}");
                }
            }
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
        }
    }
}

async fn pull_once(bridge: &RelayBus, topics: &[String]) -> anyhow::Result<()> {
    let (response, subscriptions) = request_pull(bridge, topics).await?;
    tracing::debug!(
        "relay pull: subscriber={} sent_after={subscriptions:?} got={} messages",
        bridge.subscriber,
        response.messages.len()
    );
    apply_response(bridge, response).await;
    Ok(())
}

async fn request_pull(
    bridge: &RelayBus,
    topics: &[String],
) -> anyhow::Result<(PullResponse, Vec<PullSubscription>)> {
    let subscriptions: Vec<PullSubscription> = {
        let cursors = bridge.cursors.lock().await;
        topics
            .iter()
            .map(|topic| PullSubscription {
                topic: topic.clone(),
                after: cursors.get(topic).copied().unwrap_or(0),
            })
            .collect()
    };
    let request = PullRequest {
        subscriber: bridge.subscriber.clone(),
        subscriptions: subscriptions.clone(),
    };
    let mut http = bridge
        .client
        .post(format!("{}/relay/pull", bridge.plane))
        .json(&request);
    if let Some(token) = &bridge.token {
        http = http.bearer_auth(token);
    }
    let response: PullResponse = http.send().await?.json().await?;
    Ok((response, subscriptions))
}

async fn apply_response(bridge: &RelayBus, response: PullResponse) {
    for message in &response.messages {
        let _ = bridge
            .local
            .publish(&message.topic, message.payload.clone());
    }
    let mut cursors = bridge.cursors.lock().await;
    for next in response.next {
        cursors.insert(next.topic, next.after);
    }
}
