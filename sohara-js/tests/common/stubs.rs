//! In-memory `StateAccessor` / `FlowRunner` stubs used by the shared test context.

use std::sync::Mutex as StdMutex;

use serde_json::{Map, Value};
use sohara_core::{FlowRunner, StateAccessor};

#[derive(Default)]
pub struct TestStateAccessor(StdMutex<Map<String, Value>>);

impl StateAccessor for TestStateAccessor {
    fn get(&self, step_id: &str) -> Value {
        self.0
            .lock()
            .unwrap()
            .get(step_id)
            .cloned()
            .unwrap_or(Value::Object(Map::new()))
    }

    fn set(&self, step_id: &str, value: Value) {
        self.0.lock().unwrap().insert(step_id.to_owned(), value);
    }

    fn checkpoint(&self) -> sohara_core::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct TestFlowRunner;

impl FlowRunner for TestFlowRunner {
    fn run(&self, _flow: Value) -> sohara_core::Result<String> {
        Ok("test-handle".to_owned())
    }

    fn stop(&self, _handle_id: &str) -> sohara_core::Result<()> {
        Ok(())
    }
}
