//! S4 persistence: step states, checkpointing, and idempotent delivery

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::{atomic::AtomicUsize, Mutex};

use serde_json::{Map, Value};

use sohara_core::{Record, Result, StateAccessor, StateStore};

use crate::executor::Executor;
use crate::graph::{FlowGraph, Node, NodeStep};
use crate::stats::ExecutorConfig;

pub(crate) struct RuntimeState {
    pub(crate) store: Option<Arc<dyn StateStore>>,
    pub(crate) run_id: String,
    pub(crate) since_checkpoint: Arc<AtomicUsize>,
    pub(crate) states: Arc<Mutex<HashMap<String, Value>>>,
}

impl RuntimeState {
    pub(crate) fn new(flow: &str, config: &ExecutorConfig) -> Arc<Self> {
        Arc::new(Self {
            store: config.store.clone(),
            run_id: resolve_run_id(flow, config.store.as_ref(), config.resume),
            since_checkpoint: Arc::new(AtomicUsize::new(0)),
            states: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(crate) fn seed(&self, states: HashMap<String, Value>) {
        *self.states.lock().expect("runtime state poisoned") = states;
    }
}

impl StateAccessor for RuntimeState {
    fn get(&self, step_id: &str) -> Value {
        self.states
            .lock()
            .expect("runtime state poisoned")
            .get(step_id)
            .cloned()
            .unwrap_or(Value::Object(Map::new()))
    }

    fn set(&self, step_id: &str, value: Value) {
        // SAFETY: script callbacks must not call this while the executor holds
        // the same state mutex across an await point.
        self.states
            .lock()
            .expect("runtime state poisoned")
            .insert(step_id.to_owned(), value);
    }

    fn checkpoint(&self) -> Result<()> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let states = self.states.lock().expect("runtime state poisoned").clone();
        for (node_id, state) in states {
            let key = format!("{}:state:{node_id}", self.run_id);
            store.save(&key, state)?;
        }
        self.since_checkpoint.store(0, Ordering::Relaxed);
        Ok(())
    }
}

impl Executor {
    pub(crate) fn store(&self) -> Option<&Arc<dyn StateStore>> {
        self.runtime.store.as_ref()
    }

    /// The value expressions evaluate against: `{ record, state }` when the
    /// node has accumulated state, otherwise the plain payload.
    pub(crate) async fn eval_value(&self, node: &Arc<Node>, record: &Record) -> Value {
        let states = self.runtime.states.lock().expect("runtime state poisoned");
        match states.get(&node.id) {
            Some(state) => {
                serde_json::json!({ "record": record.payload.clone(), "state": state.clone() })
            }
            None => record.payload.clone(),
        }
    }

    pub(crate) async fn state_value(&self, node_id: &str) -> Value {
        self.runtime
            .states
            .lock()
            .expect("runtime state poisoned")
            .get(node_id)
            .cloned()
            .unwrap_or(Value::Object(Map::new()))
    }

    /// Update a node's accumulated state in memory; persistence is driven by
    /// checkpointing.
    pub(crate) async fn update_state(&self, node_id: &str, value: Value) {
        self.runtime
            .states
            .lock()
            .expect("runtime state poisoned")
            .insert(node_id.to_owned(), value.clone());
    }

    /// Persist every accumulated state.
    pub(crate) async fn checkpoint(&self) {
        if let Err(error) = self.runtime.checkpoint() {
            tracing::error!("checkpoint failed: {error}");
        }
    }

    pub(crate) async fn maybe_checkpoint(&self) {
        let Some(every) = self.checkpoint_every else {
            return;
        };
        let since = self
            .runtime
            .since_checkpoint
            .fetch_add(1, Ordering::Relaxed)
            + 1;
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

/// A summary of one approve node's parked queue (D1 dashboard).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ApproveQueueInfo {
    pub step: String,
    pub count: usize,
    /// The most recent parked payload (or null when empty).
    pub sample: Value,
}

/// List every approve node's parked queue without consuming it.
pub fn list_approve_queues(executor: &Executor) -> Result<Vec<ApproveQueueInfo>> {
    let Some(store) = executor.store() else {
        return Ok(Vec::new());
    };
    let mut result = Vec::new();
    for (id, node) in &executor.graph().nodes {
        if !matches!(
            node.step,
            NodeStep::Control(sohara_core::ControlNode::Approve { .. })
        ) {
            continue;
        }
        let key = format!("{}:approve:{id}", executor.flow_name.as_str());
        let parked = store
            .load(&key)?
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        result.push(ApproveQueueInfo {
            step: id.clone(),
            count: parked.len(),
            sample: parked.last().cloned().unwrap_or(Value::Null),
        });
    }
    Ok(result)
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
        let restored = if config.resume {
            config.store.as_ref().and_then(|store| {
                let key = format!("{run_id}:state:{id}");
                store.load(&key).ok().flatten()
            })
        } else {
            None
        };
        if let Some(saved) = restored {
            states.insert(id.clone(), saved);
        } else if let Some(initial) = node.state.clone() {
            states.insert(id.clone(), initial);
        }
    }
    states
}
