//! Control-node semantics: switch / foreach / loop / parallel / join / delay / batch

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use sohara_core::{eval, is_truthy, ControlNode, EvalContext, JoinMode, Record, Result};

use crate::executor::Executor;
use crate::graph::Node;
use crate::persist::park_approve;

/// Execute a control node for one record.
pub(crate) async fn apply_control(
    executor: &Arc<Executor>,
    node: &Arc<Node>,
    control: &ControlNode,
    record: Record,
) -> Result<()> {
    match control {
        ControlNode::Switch { cases, default } => {
            for case in cases {
                if eval_bool(executor, &case.when, &record.payload, &node.id) {
                    return executor.walk(record, case.to.clone()).await;
                }
            }
            executor.walk(record, default.clone()).await
        }
        ControlNode::Foreach {
            over,
            as_field,
            max_iterations,
        } => {
            let items = eval_items(executor, over, &record, &node.id);
            let items = match items {
                Some(items) => items,
                None => return Ok(()),
            };
            if items.len() > *max_iterations {
                tracing::warn!(
                    "[{}] foreach has {} items, capped at max_iterations ({max_iterations})",
                    node.id,
                    items.len()
                );
            }
            let field = as_field.clone().unwrap_or_else(|| "item".to_owned());
            for item in items.into_iter().take(*max_iterations) {
                let mut child = record.clone();
                child.set(&field, item);
                executor.route(child, node).await?;
            }
            Ok(())
        }
        ControlNode::Loop {
            while_expr,
            max_iterations,
            body,
        } => {
            for _ in 0..*max_iterations {
                let value = executor.eval_value(node, &record).await;
                if !eval_bool(executor, while_expr, &value, &node.id) {
                    break;
                }
                match body {
                    Some(target) => executor.walk(record.clone(), target.clone()).await?,
                    None => executor.route(record.clone(), node).await?,
                }
            }
            Ok(())
        }
        ControlNode::Parallel { branches } => {
            let mut record = record;
            if !record.metadata.contains_key("correlation") {
                record
                    .metadata
                    .insert("correlation".to_owned(), uuid::Uuid::new_v4().to_string());
            }
            let mut futures = Vec::new();
            for branch in branches {
                let executor = executor.clone();
                let record = record.clone();
                let branch = branch.clone();
                futures.push(Box::pin(async move { executor.walk(record, branch).await })
                    as crate::executor::BoxFuture<'static, Result<()>>);
            }
            futures::future::try_join_all(futures).await?;
            Ok(())
        }
        ControlNode::Join { .. } => apply_join(executor, node, record).await,
        ControlNode::Delay { duration } => {
            tokio::time::sleep(*duration).await;
            executor.route(record, node).await
        }
        ControlNode::Batch { size, within } => {
            apply_batch(executor, node, *size, *within, record).await
        }
        ControlNode::State { exprs } => apply_state(executor, node, exprs, record).await,
        ControlNode::Approve { title } => {
            if executor.store().is_some() {
                if let Err(error) = park_approve(executor, node, &record) {
                    tracing::error!("[{}] failed to park approval: {error}", node.id);
                    executor
                        .counters()
                        .errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(());
                }
                executor
                    .counters()
                    .waiting
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::info!("[{}] waiting for approval: {title}", node.id);
                Ok(())
            } else {
                tracing::warn!(
                    "[{}] approve without a state store, routing through",
                    node.id
                );
                executor.route(record, node).await
            }
        }
    }
}

async fn apply_state(
    executor: &Arc<Executor>,
    node: &Arc<Node>,
    exprs: &[(String, sohara_core::Expr)],
    record: Record,
) -> Result<()> {
    let synthetic = executor.eval_value(node, &record).await;
    let mut state = executor.state_value(&node.id).await;
    let ctx = EvalContext {
        vars: executor.vars(),
    };
    for (field, expr) in exprs {
        match eval(expr, &synthetic, &ctx) {
            Ok(value) => {
                if let Value::Object(object) = &mut state {
                    object.insert(field.clone(), value);
                }
            }
            Err(error) => {
                tracing::warn!("[{}] state expr '{field}' failed: {error}", node.id);
                executor
                    .counters()
                    .errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }
        }
    }
    executor.update_state(&node.id, state).await;
    executor.route(record, node).await
}

fn eval_bool(executor: &Executor, expr: &sohara_core::Expr, value: &Value, node: &str) -> bool {
    let ctx = EvalContext {
        vars: executor.vars(),
    };
    match eval(expr, value, &ctx) {
        Ok(value) => is_truthy(&value),
        Err(error) => {
            tracing::warn!("[{node}] expression failed: {error}");
            executor
                .counters()
                .errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            false
        }
    }
}

fn eval_items(
    executor: &Executor,
    expr: &sohara_core::Expr,
    record: &Record,
    node: &str,
) -> Option<Vec<Value>> {
    let ctx = EvalContext {
        vars: executor.vars(),
    };
    match eval(expr, &record.payload, &ctx) {
        Ok(Value::Array(items)) => Some(items),
        Ok(other) => {
            tracing::warn!("[{node}] foreach 'over' must be an array, got {other}");
            executor
                .counters()
                .errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
        Err(error) => {
            tracing::warn!("[{node}] foreach 'over' failed: {error}");
            executor
                .counters()
                .errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }
}

async fn apply_join(executor: &Arc<Executor>, node: &Arc<Node>, record: Record) -> Result<()> {
    let correlation = record
        .metadata
        .get("correlation")
        .cloned()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let group = {
        let Some(buffer) = executor.joins().get(&node.id) else {
            tracing::warn!("[{}] missing join buffer", node.id);
            return Ok(());
        };
        let mut buffer = buffer.lock().await;
        let (mode, expected, threshold) = (buffer.mode, buffer.expected.max(1), buffer.n.max(1));
        let group = buffer.groups.entry(correlation.clone()).or_default();
        group.push(record);
        let ready = match mode {
            JoinMode::Any => true,
            JoinMode::All => group.len() >= expected,
            JoinMode::N => group.len() >= threshold,
        };
        ready.then(|| std::mem::take(group))
    };
    if let Some(records) = group {
        executor.route(merge_records(records), node).await?;
    }
    Ok(())
}

fn merge_records(records: Vec<Record>) -> Record {
    let mut iter = records.into_iter();
    let Some(mut merged) = iter.next() else {
        return Record::new(Value::Object(Map::new()));
    };
    for other in iter {
        if let (Value::Object(target), Value::Object(source)) =
            (&mut merged.payload, &other.payload)
        {
            for (key, value) in source {
                target.insert(key.clone(), value.clone());
            }
        }
    }
    merged
}

async fn apply_batch(
    executor: &Arc<Executor>,
    node: &Arc<Node>,
    size: Option<usize>,
    within: Option<Duration>,
    record: Record,
) -> Result<()> {
    let flush = {
        let Some(buffer) = executor.batches().get(&node.id) else {
            tracing::warn!("[{}] missing batch buffer", node.id);
            return Ok(());
        };
        let mut buffer = buffer.lock().await;
        buffer.records.push(record);
        let size_hit = size.is_some_and(|limit| buffer.records.len() >= limit);
        let window_hit = within
            .is_some_and(|window| buffer.since.is_some_and(|start| start.elapsed() >= window));
        if size_hit || window_hit {
            buffer.since = None;
            Some(std::mem::take(&mut buffer.records))
        } else {
            if buffer.since.is_none() {
                buffer.since = Some(Instant::now());
            }
            None
        }
    };
    if let Some(records) = flush {
        emit_batch(executor, node, records).await?;
    }
    Ok(())
}

/// Emit a buffered batch as a single `{ items: [...] }` record.
pub(crate) async fn emit_batch(
    executor: &Arc<Executor>,
    node: &Arc<Node>,
    records: Vec<Record>,
) -> Result<()> {
    let items = Value::Array(records.iter().map(Record::to_json).collect());
    let mut payload = Map::new();
    payload.insert("items".to_owned(), items);
    executor
        .route(Record::new(Value::Object(payload)), node)
        .await
}
