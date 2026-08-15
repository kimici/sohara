//! Plane relay endpoints (D5a): topic mailbox for instance buses

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use sohara_triggers::relay::{PullRequest, PullResponse};

use std::collections::VecDeque;

use crate::{Plane, Registry};

#[derive(Debug, Deserialize)]
struct PublishBody {
    topic: String,
    payload: Value,
}

/// Per-topic bounded message buffer for the plane relay (D5a).
#[derive(Default)]
pub(crate) struct RelayQueue {
    seq: u64,
    buffer: VecDeque<(u64, Value)>,
}

/// Maximum retained messages per relay topic (overflow drops the oldest).
const RELAY_CAP: usize = 1000;
const RELAY_BATCH: u64 = 100;

impl Registry {
    /// Append one message to a relay topic's bounded buffer (D5a).
    pub async fn relay_publish(&self, topic: &str, payload: Value) {
        let mut inner = self.inner.lock().await;
        let queue = inner.relay.entry(topic.to_owned()).or_default();
        queue.seq += 1;
        queue.buffer.push_back((queue.seq, payload));
        if queue.buffer.len() > RELAY_CAP {
            queue.buffer.pop_front();
        }
    }

    /// Deliver queued relay messages newer than each subscription cursor,
    /// advancing the cursors (D5a; at-most-once per cursor).
    pub async fn relay_pull(
        &self,
        _subscriber: &str,
        subscriptions: &[(String, u64)],
    ) -> (
        Vec<sohara_triggers::relay::RelayMessage>,
        Vec<sohara_triggers::relay::PullSubscription>,
    ) {
        let inner = self.inner.lock().await;
        let mut messages = Vec::new();
        let mut next = Vec::new();
        for (topic, after) in subscriptions {
            let Some(queue) = inner.relay.get(topic) else {
                continue;
            };
            let mut delivered = 0u64;
            let mut cursor = *after;
            for (seq, payload) in &queue.buffer {
                if *seq <= *after || delivered >= RELAY_BATCH {
                    continue;
                }
                messages.push(sohara_triggers::relay::RelayMessage {
                    topic: topic.clone(),
                    seq: *seq,
                    payload: payload.clone(),
                });
                cursor = *seq;
                delivered += 1;
            }
            next.push(sohara_triggers::relay::PullSubscription {
                topic: topic.clone(),
                after: cursor.max(*after),
            });
        }
        (messages, next)
    }
}

/// The `/relay/*` router (plane-token guarded).
pub fn relay_router(plane: Arc<Plane>) -> Router {
    Router::new()
        .route("/relay/publish", post(publish))
        .route("/relay/pull", post(pull))
        .with_state(plane)
}

async fn publish(State(plane): State<Arc<Plane>>, Json(body): Json<PublishBody>) -> StatusCode {
    plane
        .registry
        .relay_publish(&body.topic, body.payload)
        .await;
    StatusCode::ACCEPTED
}

async fn pull(
    State(plane): State<Arc<Plane>>,
    Json(body): Json<PullRequest>,
) -> Json<PullResponse> {
    let subscriptions: Vec<(String, u64)> = body
        .subscriptions
        .iter()
        .map(|sub| (sub.topic.clone(), sub.after))
        .collect();
    let (messages, next) = plane
        .registry
        .relay_pull(&body.subscriber, &subscriptions)
        .await;
    Json(PullResponse { messages, next })
}
