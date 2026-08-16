//! Registry: desired state, actual state from heartbeats, command queues (D3)

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use sohara_agent::{Command, CommandAck, DesiredInstance, Heartbeat, InstanceReport};

use crate::events::record_event_locked;
use crate::reconcile::{actual_of, declared_for, enqueue_reconcile, state_transitions};
use crate::store;
use crate::types::{Desired, FlowDecl, InstanceDecl, InstanceView, NodeView, RouteDecl};

/// The persisted subset: flows + instance declarations (desired state).
#[derive(Debug, Default, Serialize, Deserialize)]
struct Persisted {
    #[serde(default)]
    flows: Vec<FlowDecl>,
    #[serde(default)]
    instances: Vec<InstanceDecl>,
    #[serde(default)]
    routes: Vec<RouteDecl>,
}

#[derive(Default)]
pub(crate) struct Inner {
    pub(crate) flows: HashMap<String, FlowDecl>,
    pub(crate) instances: HashMap<String, InstanceDecl>,
    pub(crate) actual: HashMap<String, Vec<InstanceReport>>,
    pub(crate) last_seen: HashMap<String, String>,
    pub(crate) pending: HashMap<String, Vec<Command>>,
    pub(crate) seq: HashMap<String, u64>,
    pub(crate) routes: HashMap<String, RouteDecl>,
    pub(crate) round_robin: HashMap<String, u64>,
    pub(crate) relay: HashMap<String, crate::relay_api::RelayQueue>,
    pub(crate) relay_cursors: HashMap<String, HashMap<String, u64>>,
    pub(crate) events: VecDeque<serde_json::Value>,
}

/// The plane's source of truth plus live agent state.
pub struct Registry {
    pub(crate) inner: Mutex<Inner>,
    path: Option<PathBuf>,
}

impl Registry {
    /// Load persisted state (or start empty) and bind to `path` for saves.
    #[must_use]
    pub fn load(path: Option<PathBuf>) -> Arc<Self> {
        let persisted: Persisted = path.as_deref().map(store::load).unwrap_or_default();
        let flows = persisted
            .flows
            .into_iter()
            .map(|flow| (flow.id.clone(), flow))
            .collect();
        let instances = persisted
            .instances
            .into_iter()
            .map(|decl| (decl.id.clone(), decl))
            .collect();
        let routes = persisted
            .routes
            .into_iter()
            .map(|route| (route.id.clone(), route))
            .collect();
        Arc::new(Self {
            inner: Mutex::new(Inner {
                flows,
                instances,
                routes,
                ..Inner::default()
            }),
            path,
        })
    }

    /// Declare (or replace) an instance; its spec id is made authoritative.
    pub async fn declare_instance(&self, mut decl: InstanceDecl) -> Result<()> {
        decl.spec.id = decl.id.clone();
        let id = decl.id.clone();
        self.inner.lock().await.instances.insert(id.clone(), decl);
        self.record_event("declare", &format!("instance '{id}' declared"))
            .await;
        self.persist().await
    }

    /// Remove an instance declaration.
    pub async fn remove_instance(&self, id: &str) -> Result<()> {
        self.inner.lock().await.instances.remove(id);
        self.record_event("undeclare", &format!("instance '{id}' removed"))
            .await;
        self.persist().await
    }

    /// Change the desired state of one instance.
    pub async fn set_desired(&self, id: &str, desired: Desired) -> Result<()> {
        if let Some(decl) = self.inner.lock().await.instances.get_mut(id) {
            decl.desired = desired;
        }
        self.record_event(
            "desired",
            &format!("instance '{id}' desired -> {}", desired.as_str()),
        )
        .await;
        self.persist().await
    }

    /// Uploaded flow fragments.
    pub async fn list_flows(&self) -> Vec<FlowDecl> {
        self.inner.lock().await.flows.values().cloned().collect()
    }

    /// Store an uploaded flow fragment.
    pub async fn put_flow(&self, flow: FlowDecl) -> Result<()> {
        self.inner.lock().await.flows.insert(flow.id.clone(), flow);
        self.persist().await
    }

    /// Declared instances merged with the last reported actual state.
    pub async fn list_instances(&self) -> Vec<InstanceView> {
        let inner = self.inner.lock().await;
        inner
            .instances
            .values()
            .map(|decl| InstanceView {
                id: decl.id.clone(),
                node: decl.node.clone(),
                flow_id: decl.flow_id.clone(),
                desired: decl.desired.as_str().to_owned(),
                actual: actual_of(&inner, &decl.node, &decl.id)
                    .map(|a| a.state.as_str().to_owned()),
                healthy: actual_of(&inner, &decl.node, &decl.id).map(|a| a.healthy),
                paused: actual_of(&inner, &decl.node, &decl.id).map(|a| a.paused),
                restarts: actual_of(&inner, &decl.node, &decl.id).map(|a| a.restarts),
                admin: actual_of(&inner, &decl.node, &decl.id).and_then(|a| a.admin.clone()),
                trigger: actual_of(&inner, &decl.node, &decl.id).and_then(|a| a.trigger.clone()),
            })
            .collect()
    }

    /// Nodes seen in heartbeats.
    pub async fn list_nodes(&self) -> Vec<NodeView> {
        let inner = self.inner.lock().await;
        inner
            .last_seen
            .iter()
            .map(|(id, seen)| NodeView {
                id: id.clone(),
                last_seen: Some(seen.clone()),
            })
            .collect()
    }

    /// Record one heartbeat, enqueue reconcile commands, and return the
    /// queued commands plus the node's desired instance set.
    pub async fn heartbeat(&self, heartbeat: &Heartbeat) -> (Vec<Command>, Vec<DesiredInstance>) {
        let mut inner = self.inner.lock().await;
        let node = heartbeat.node_id.clone();
        let transitions = state_transitions(&inner, &node, &heartbeat.instances);
        inner
            .actual
            .insert(node.clone(), heartbeat.instances.clone());
        inner.last_seen.insert(node.clone(), heartbeat.time.clone());
        for transition in transitions {
            record_event_locked(&mut inner, "state", &transition);
        }
        let declared = declared_for(&inner, &node);
        enqueue_reconcile(&mut inner, &node, &declared, &heartbeat.instances);
        let commands = inner.pending.get(&node).cloned().unwrap_or_default();
        let desired = declared
            .into_iter()
            .map(|decl| DesiredInstance {
                spec: decl.spec,
                desired: decl.desired.as_str().to_owned(),
            })
            .collect();
        (commands, desired)
    }

    /// Remove an acknowledged command (at-least-once delivery, agent dedups).
    pub async fn ack(&self, ack: &CommandAck) {
        let mut inner = self.inner.lock().await;
        for pending in inner.pending.values_mut() {
            pending.retain(|command| command.seq != ack.seq);
        }
    }

    /// Declare (or replace) a gateway route.
    pub async fn declare_route(&self, route: RouteDecl) -> Result<()> {
        self.inner
            .lock()
            .await
            .routes
            .insert(route.id.clone(), route);
        self.persist().await
    }

    /// Remove a gateway route.
    pub async fn remove_route(&self, id: &str) -> Result<()> {
        self.inner.lock().await.routes.remove(id);
        self.persist().await
    }

    /// List gateway routes.
    pub async fn list_routes(&self) -> Vec<RouteDecl> {
        self.inner.lock().await.routes.values().cloned().collect()
    }

    async fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let inner = self.inner.lock().await;
        let persisted = Persisted {
            flows: inner.flows.values().cloned().collect(),
            instances: inner.instances.values().cloned().collect(),
            routes: inner.routes.values().cloned().collect(),
        };
        store::save(path, &persisted)
    }
}
