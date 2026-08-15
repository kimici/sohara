//! Queue trigger: subscribe to an event-bus topic

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::Value;
use sohara_core::{Error, Record, Result, Source, Trigger};
use tokio::sync::{mpsc, watch, Mutex};

use crate::InProcessBus;

/// A source that yields one record per payload published on its topic.
pub struct QueueSource {
    name: String,
    receiver: Mutex<Option<mpsc::Receiver<Value>>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl QueueSource {
    #[must_use]
    pub fn new(topic: &str, bus: &InProcessBus) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            name: format!("queue:{topic}"),
            receiver: Mutex::new(Some(bus.subscribe(topic))),
            shutdown_tx,
            shutdown_rx,
        }
    }
}

#[async_trait]
impl Source for QueueSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let receiver = self.receiver.lock().await.take().ok_or_else(|| {
            Error::Source(format!("queue source '{}' already consumed", self.name))
        })?;
        let shutdown = self.shutdown_rx.clone();
        Ok(Box::pin(futures::stream::unfold(
            receiver,
            move |mut receiver| {
                let mut shutdown = shutdown.clone();
                async move {
                    loop {
                        tokio::select! {
                            item = receiver.recv() => {
                                return item.map(|payload| (Ok(Record::new(payload)), receiver));
                            }
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    return None;
                                }
                            }
                        }
                    }
                }
            },
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Trigger for QueueSource {
    async fn stop(&self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }
}
