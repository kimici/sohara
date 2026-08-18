use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sohara_builtin_extensions::{initialize_result, run_loop, Capabilities, JsonRpcRequest};

fn main() -> Result<()> {
    let mut state = TriggerState::default();
    run_loop(move |request| handle(&mut state, request))
}

#[derive(Default)]
struct TriggerState {
    last_id: i64,
    stopped: bool,
}

fn handle(state: &mut TriggerState, request: JsonRpcRequest) -> Result<Value> {
    match request.method.as_str() {
        "initialize" => Ok(initialize_result(Capabilities {
            trigger: true,
            state_store: true,
            event_bus: true,
            ..Capabilities::default()
        })),
        "state.load" => {
            let config = config(&request.params)?;
            let path = config_path(config)?;
            let key = request
                .params
                .get("key")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing key"))?;
            let connection = Connection::open(path)?;
            ensure_store_table(&connection, config)?;
            let mut stmt = connection.prepare("select v from state_store where k = ?1")?;
            let mut rows = stmt.query(params![key])?;
            if let Some(row) = rows.next()? {
                let text: String = row.get(0)?;
                Ok(json!({ "found": true, "value": serde_json::from_str::<Value>(&text)? }))
            } else {
                Ok(json!({ "found": false, "value": Value::Null }))
            }
        }
        "state.save" => {
            let config = config(&request.params)?;
            let path = config_path(config)?;
            let key = request
                .params
                .get("key")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing key"))?;
            let value = request
                .params
                .get("value")
                .ok_or_else(|| anyhow!("missing value"))?;
            let connection = Connection::open(path)?;
            ensure_store_table(&connection, config)?;
            connection.execute(
                "insert into state_store (k, v) values (?1, ?2) \
                 on conflict(k) do update set v = excluded.v",
                params![key, serde_json::to_string(value)?],
            )?;
            Ok(json!({}))
        }
        "state.delete" => {
            let config = config(&request.params)?;
            let path = config_path(config)?;
            let key = request
                .params
                .get("key")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing key"))?;
            let connection = Connection::open(path)?;
            ensure_store_table(&connection, config)?;
            connection.execute("delete from state_store where k = ?1", params![key])?;
            Ok(json!({}))
        }
        "bus.publish" => {
            let config = config(&request.params)?;
            let path = config_path(config)?;
            let topic = request
                .params
                .get("topic")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing topic"))?;
            let payload = request
                .params
                .get("payload")
                .ok_or_else(|| anyhow!("missing payload"))?;
            let connection = Connection::open(path)?;
            ensure_bus_table(&connection)?;
            connection.execute(
                "insert into event_bus (topic, payload) values (?1, ?2)",
                params![topic, serde_json::to_string(payload)?],
            )?;
            Ok(json!({}))
        }
        "trigger.start" => {
            state.stopped = false;
            Ok(json!({}))
        }
        "trigger.pull" => {
            if state.stopped {
                return Ok(json!({ "records": [], "done": true }));
            }
            let config = config(&request.params)?;
            let path = config_path(config)?;
            let topic = config
                .get("topic")
                .and_then(Value::as_str)
                .unwrap_or("default");
            let batch_size = config
                .get("batch_size")
                .and_then(Value::as_u64)
                .unwrap_or(64);
            let connection = Connection::open(path)?;
            ensure_bus_table(&connection)?;
            let mut stmt = connection.prepare(
                "select id, payload, published_at from event_bus where id > ?1 and topic = ?2 order by id asc limit ?3",
            )?;
            let rows = stmt.query_map(params![state.last_id, topic, batch_size], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            let mut records = Vec::new();
            for row in rows {
                let (id, payload, published_at) = row?;
                state.last_id = id;
                let timestamp = if published_at.contains('T') {
                    published_at
                } else {
                    format!("{}Z", published_at.replace(' ', "T"))
                };
                records.push(json!({
                    "id": format!("sqlite-{id}"),
                    "timestamp": timestamp,
                    "payload": serde_json::from_str::<Value>(&payload)?,
                    "metadata": {}
                }));
            }
            Ok(json!({ "records": records, "done": false }))
        }
        "trigger.stop" => {
            state.stopped = true;
            Ok(json!({}))
        }
        other => Err(anyhow!("unknown method: {other}")),
    }
}

fn config(params: &Value) -> Result<&serde_json::Map<String, Value>> {
    params
        .get("config")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("missing config"))
}

fn config_path(config: &serde_json::Map<String, Value>) -> Result<&str> {
    config
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("config.path is required"))
}

fn ensure_store_table(
    connection: &Connection,
    _config: &serde_json::Map<String, Value>,
) -> Result<()> {
    connection.execute(
        "create table if not exists state_store (k text primary key, v text not null)",
        [],
    )?;
    Ok(())
}

fn ensure_bus_table(connection: &Connection) -> Result<()> {
    connection.execute(
        "create table if not exists event_bus (
            id integer primary key autoincrement,
            topic text not null,
            payload text not null,
            published_at text default current_timestamp
        )",
        [],
    )?;
    Ok(())
}
