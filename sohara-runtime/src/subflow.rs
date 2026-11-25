use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;

use sohara_core::{ComponentRegistry, EventBus, FlowRunner, Result, StateStore};

struct RunningFlow {
    stop_tx: mpsc::Sender<()>,
    join: JoinHandle<()>,
}

/// Runtime-backed implementation of `sohara.flow.run(...)`.
pub struct RuntimeFlowRunner {
    registry: ComponentRegistry,
    shared_bus: Option<Arc<dyn EventBus>>,
    extension_host: Option<Arc<crate::StdioExtensionHost>>,
    running: Mutex<HashMap<String, RunningFlow>>,
}

impl RuntimeFlowRunner {
    pub fn new(
        registry: ComponentRegistry,
        shared_bus: Option<Arc<dyn EventBus>>,
        extension_host: Option<Arc<crate::StdioExtensionHost>>,
    ) -> Self {
        Self {
            registry,
            shared_bus,
            extension_host,
            running: Mutex::new(HashMap::new()),
        }
    }
}

impl FlowRunner for RuntimeFlowRunner {
    fn run(&self, flow: serde_json::Value) -> Result<String> {
        let flow = sohara_config::FlowConfig::from_json_value(flow, None)
            .map_err(|error| sohara_core::Error::Config(error.to_string()))?;
        let handle_id = uuid::Uuid::new_v4().to_string();
        let store = store_for(&flow, self.extension_host.as_deref())?;
        if flow.triggers.is_empty() {
            run_finite(flow, self.registry.clone(), store)?;
            return Ok(handle_id);
        }

        let running = spawn_serving(
            flow,
            self.registry.clone(),
            self.shared_bus.clone(),
            self.extension_host.clone(),
            store,
        );
        self.running
            .lock()
            .expect("subflow map poisoned")
            .insert(handle_id.clone(), running);
        Ok(handle_id)
    }

    fn stop(&self, handle_id: &str) -> Result<()> {
        let Some(running) = self
            .running
            .lock()
            .expect("subflow map poisoned")
            .remove(handle_id)
        else {
            return Ok(());
        };
        let _ = running.stop_tx.send(());
        running
            .join
            .join()
            .map_err(|_| sohara_core::Error::Runtime("subflow thread panicked".to_owned()))?;
        Ok(())
    }
}

fn run_finite(
    flow: sohara_config::FlowConfig,
    registry: ComponentRegistry,
    store: Option<Arc<dyn StateStore>>,
) -> Result<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().expect("subflow runtime");
        runtime
            .block_on(crate::run_flow_with_store(&flow, &registry, store, false))
            .expect("subflow run");
    })
    .join()
    .map_err(|_| sohara_core::Error::Runtime("subflow thread panicked".to_owned()))
}

fn spawn_serving(
    flow: sohara_config::FlowConfig,
    registry: ComponentRegistry,
    shared_bus: Option<Arc<dyn EventBus>>,
    extension_host: Option<Arc<crate::StdioExtensionHost>>,
    store: Option<Arc<dyn StateStore>>,
) -> RunningFlow {
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let join = std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().expect("subflow runtime");
        let bus = Arc::new(sohara_triggers::InProcessBus::new(128));
        let options = crate::ServeOptions {
            store,
            shared_bus,
            extension_host,
            ..crate::ServeOptions::default()
        };
        let _ = runtime.block_on(crate::serve_with_shutdown_opts(
            &flow,
            &registry,
            bus,
            async move {
                let _ = tokio::task::spawn_blocking(move || stop_rx.recv()).await;
            },
            options,
        ));
    });
    RunningFlow { stop_tx, join }
}

fn store_for(
    flow: &sohara_config::FlowConfig,
    host: Option<&crate::StdioExtensionHost>,
) -> Result<Option<Arc<dyn StateStore>>> {
    let Some(store) = flow
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.store.as_ref())
    else {
        return Ok(None);
    };
    match store {
        sohara_config::StoreConfig::Path(path) => Ok(Some(Arc::new(
            sohara_persistence::JsonFileStore::new(PathBuf::from(path))
                .map_err(|error| sohara_core::Error::Config(error.to_string()))?,
        ))),
        sohara_config::StoreConfig::Component(component) => host
            .and_then(|host| {
                host.build_state_store(&sohara_config::StoreConfig::Component(component.clone()))
                    .ok()
                    .flatten()
            })
            .map_or_else(
                || {
                    Err(sohara_core::Error::Config(format!(
                        "unknown external state store type '{}'",
                        component.component_type
                    )))
                },
                |store| Ok(Some(store)),
            ),
    }
}
