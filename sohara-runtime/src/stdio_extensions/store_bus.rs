//! State store and event bus trait implementations over the blocking client.

use super::*;

impl StateStore for StdioStateStore {
    fn load(&self, key: &str) -> Result<Option<Value>> {
        let mut guard = self.client.lock().expect("state store lock poisoned");
        let result = {
            let client = ensure_blocking_client(&mut guard, &self.spec)?;
            client.state_load(&self.spec, &self.config, key)
        };
        match result {
            Ok(result) => Ok(if result.found { result.value } else { None }),
            Err(error) => {
                reset_blocking_client(&mut guard);
                Err(error)
            }
        }
    }

    fn save(&self, key: &str, value: Value) -> Result<()> {
        let mut guard = self.client.lock().expect("state store lock poisoned");
        let result = {
            let client = ensure_blocking_client(&mut guard, &self.spec)?;
            client.state_save(&self.spec, &self.config, key, &value)
        };
        if let Err(error) = result {
            reset_blocking_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let mut guard = self.client.lock().expect("state store lock poisoned");
        let result = {
            let client = ensure_blocking_client(&mut guard, &self.spec)?;
            client.state_delete(&self.spec, &self.config, key)
        };
        if let Err(error) = result {
            reset_blocking_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }
}

impl EventBus for StdioEventBus {
    fn publish(&self, topic: &str, payload: Value) -> Result<()> {
        let mut guard = self.client.lock().expect("event bus lock poisoned");
        let result = {
            let client = ensure_blocking_client(&mut guard, &self.spec)?;
            client.bus_publish(&self.spec, &self.config, topic, &payload)
        };
        if let Err(error) = result {
            reset_blocking_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }
}
