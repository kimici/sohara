//! Record walking: per-record graph traversal (split from executor.rs, D1)

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use sohara_core::{eval, is_truthy, Record, Result};

use crate::control::apply_control;
use crate::executor::{BoxFuture, Executor};
use crate::graph::{Node, NodeStep};

impl Executor {
    pub(crate) fn walk(
        self: &Arc<Self>,
        record: Record,
        node_id: String,
    ) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        Box::pin(async move {
            let Some(node) = this.graph.node(&node_id).cloned() else {
                tracing::warn!("route to unknown node '{node_id}', dropping record");
                return Ok(());
            };
            if let Some(when) = &node.when {
                match eval(when, &record.payload, &this.ectx()) {
                    Ok(value) if is_truthy(&value) => {}
                    Ok(_) => return Ok(()),
                    Err(error) => {
                        tracing::warn!("[{}] 'when' failed: {error}", node.id);
                        this.counters.errors.fetch_add(1, Ordering::Relaxed);
                        this.tick_step(&node.id, |stat| stat.errors += 1).await;
                        this.record_error(&node.id, "when", error.to_string()).await;
                        return Ok(());
                    }
                }
            }
            match &node.step {
                NodeStep::Sink(sink) => {
                    if this.is_delivered(&record) {
                        this.counters.duplicates.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    let started = Instant::now();
                    let result = sink.send(record.clone()).await;
                    let nanos = started.elapsed().as_nanos() as u64;
                    match result {
                        Ok(()) => {
                            this.mark_delivered(&record);
                            this.counters.processed.fetch_add(1, Ordering::Relaxed);
                            this.tick_step(&node.id, |stat| {
                                stat.processed += 1;
                                stat.nanos += nanos;
                            })
                            .await;
                            this.maybe_checkpoint().await;
                        }
                        Err(error) => {
                            tracing::error!("[{}] sink failed: {error}", node.id);
                            this.counters.errors.fetch_add(1, Ordering::Relaxed);
                            this.tick_step(&node.id, |stat| {
                                stat.errors += 1;
                                stat.nanos += nanos;
                            })
                            .await;
                            this.record_error(&node.id, "sink", error.to_string()).await;
                        }
                    }
                }
                NodeStep::Transform(transform) => {
                    this.apply_transform(&node, transform.as_ref(), record)
                        .await?;
                }
                NodeStep::Control(control) => {
                    apply_control(&this, &node, control, record).await?;
                }
                NodeStep::Source(_) => {
                    this.route(record, &node).await?;
                }
            }
            Ok(())
        })
    }

    pub(crate) fn route(
        self: &Arc<Self>,
        record: Record,
        node: &Arc<Node>,
    ) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        let routes = node.routes.clone();
        let node_id = node.id.clone();
        Box::pin(async move {
            for route in &routes {
                if let Some(when) = &route.when {
                    match eval(when, &record.payload, &this.ectx()) {
                        Ok(value) if is_truthy(&value) => {}
                        Ok(_) => continue,
                        Err(error) => {
                            tracing::warn!("[{node_id}] edge 'when' failed: {error}");
                            this.counters.errors.fetch_add(1, Ordering::Relaxed);
                            this.record_error(&node_id, "edge", error.to_string()).await;
                            continue;
                        }
                    }
                }
                this.walk(record.clone(), route.to.clone()).await?;
            }
            Ok(())
        })
    }
}
