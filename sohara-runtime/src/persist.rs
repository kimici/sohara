//! S4 persistence: step states, checkpointing, and idempotent delivery

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::{Map, Value};

use sohara_core::{Record, Result, StateStore};

use crate::executor::Executor;
use crate::graph::{FlowGraph, Node};
use crate::stats::ExecutorConfig;

impl Executor {
    pub(crate) fn store(&self) -> Option<&Arc<dyn StateStore>> {
        self.store.as_ref()
    }

    /// The value expressions evaluate against: `{ record, state }` when the
    /// node has accumulated state, otherwise the plain payload.
    pub(crate) async fn eval_value(&self, node: &Arc<Node>, record: &Record) -> Value {
        let states = self.states.lock().await;
        match states.get(&node.id) {
            Some(state) => {
                serde_json::json!({ "record": record.payload.clone(), "state": state.clone() })
            }
            None => record.payload.clone(),
        }
    }

    pub(crate) async fn state_value(&self, node_id: &str) -> Value {
        self.states
            .lock()
            .await
            .get(node_id)
            .cloned()
            .unwrap_or(Value::Object(Map::new()))
    }

    /// Update a node's accumulated state and persist it immediately.
    pub(crate) async fn update_state(&self, node_id: &str, value: Value) {
        self.states
            .lock()
            .await
            .insert(node_id.to_owned(), value.clone());
        if let Some(store) = self.store() {
            let key = format!("{}:state:{node_id}", self.run_id());
            if let Err(error) = store.save(&key, value) {
                tracing::error!("failed to persist state of '{node_id}': {error}");
            }
        }
    }

    /// Persist every accumulated state.
    pub(crate) async fn checkpoint(&self) {
        let Some(store) = self.store() else {
            return;
        };
        let states = self.states.lock().await.clone();
        for (node_id, state) in states {
            let key = format!("{}:state:{node_id}", self.run_id());
            if let Err(error) = store.save(&key, state) {
                tracing::error!("checkpoint failed for '{node_id}': {error}");
            }
        }
        self.since_checkpoint.store(0, Ordering::Relaxed);
    }

    pub(crate) async fn maybe_checkpoint(&self) {
        let Some(every) = self.checkpoint_every else {
            return;
        };
        let since = self.since_checkpoint.fetch_add(1, Ordering::Relaxed) + 1;
        if since >= every as usize {
            self.checkpoint().await;
        }
    }

    /// Whether the record was already delivered in a previous attempt.
    pub(crate) fn is_delivered(&self, record: &Record) -> bool {
        let Some(store) = self.store() else {
            return false;
        };
        matches!(
            store.load(&delivered_key(self.run_id(), record)),
            Ok(Some(_))
        )
    }

    /// Mark a record as delivered (idempotency marker).
    pub(crate) fn mark_delivered(&self, record: &Record) {
        if let Some(store) = self.store() {
            let key = delivered_key(self.run_id(), record);
            if let Err(error) = store.save(&key, Value::Bool(true)) {
                tracing::error!("failed to mark delivered record {}: {error}", record.id);
            }
        }
    }
}

/// Park a record in the approve queue of a node.
pub(crate) fn park_approve(executor: &Executor, node: &Node, record: &Record) -> Result<()> {
    let Some(store) = executor.store() else {
        return Ok(());
    };
    let key = format!("{}:approve:{}", executor.flow_name.as_str(), node.id);
    let mut parked = store
        .load(&key)?
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    parked.push(record.to_json());
    store.save(&key, Value::Array(parked))
}

/// Take every parked record from a node's approve queue (clearing it).
pub(crate) fn take_approve_queue(executor: &Executor, node_id: &str) -> Result<Vec<Record>> {
    let Some(store) = executor.store() else {
        return Ok(Vec::new());
    };
    let key = format!("{}:approve:{node_id}", executor.flow_name.as_str());
    let parked = store
        .load(&key)?
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    store.delete(&key)?;
    Ok(parked.into_iter().map(Record::new).collect())
}

/// A stable idempotency key: the explicit metadata key or a payload hash.
fn delivered_key(run_id: &str, record: &Record) -> String {
    let key = record
        .metadata
        .get("idempotency_key")
        .cloned()
        .unwrap_or_else(|| format!("hash-{:x}", payload_hash(&record.payload)));
    format!("{run_id}:delivered:{key}")
}

fn payload_hash(value: &Value) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(value)
        .expect("payload serializes")
        .hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn resolve_run_id(
    flow: &str,
    store: Option<&Arc<dyn StateStore>>,
    resume: bool,
) -> String {
    let key = format!("{flow}:run_id");
    if resume {
        if let Some(store) = store {
            if let Ok(Some(Value::String(id))) = store.load(&key) {
                return id;
            }
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    if let Some(store) = store {
        let _ = store.save(&key, Value::String(id.clone()));
    }
    id
}

pub(crate) fn initial_states(
    graph: &FlowGraph,
    run_id: &str,
    config: &ExecutorConfig,
) -> HashMap<String, Value> {
    let mut states = HashMap::new();
    for (id, node) in &graph.nodes {
        let mut initial = node.state.clone().unwrap_or(Value::Object(Map::new()));
        if config.resume {
            if let Some(store) = &config.store {
                let key = format!("{run_id}:state:{id}");
                if let Ok(Some(saved)) = store.load(&key) {
                    initial = saved;
                }
            }
        }
        if node.state.is_some() {
            states.insert(id.clone(), initial);
        }
    }
    states
}
