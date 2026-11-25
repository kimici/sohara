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
        "state.load" => handle_load(&request.params),
        "state.save" => handle_save(&request.params),
        "state.delete" => handle_delete(&request.params),
        "bus.publish" => handle_publish(&request.params),
        "trigger.start" => {
            state.stopped = false;
            Ok(json!({}))
        }
        "trigger.pull" => handle_pull(state, &request.params),
        "trigger.stop" => {
            state.stopped = true;
            Ok(json!({}))
        }
        other => Err(anyhow!("unknown method: {other}")),
    }
}

fn handle_load(params: &Value) -> Result<Value> {
    let config = config(params)?;
    let path = config_path(config)?;
    let key = params
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

fn handle_save(params: &Value) -> Result<Value> {
    let config = config(params)?;
    let path = config_path(config)?;
    let key = params
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let value = params
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

fn handle_delete(params: &Value) -> Result<Value> {
    let config = config(params)?;
    let path = config_path(config)?;
    let key = params
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let connection = Connection::open(path)?;
    ensure_store_table(&connection, config)?;
    connection.execute("delete from state_store where k = ?1", params![key])?;
    Ok(json!({}))
}

fn handle_publish(params: &Value) -> Result<Value> {
    let config = config(params)?;
    let path = config_path(config)?;
    let topic = params
        .get("topic")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing topic"))?;
    let payload = params
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

fn handle_pull(state: &mut TriggerState, params: &Value) -> Result<Value> {
    if state.stopped {
        return Ok(json!({ "records": [], "done": true }));
    }
    let config = config(params)?;
    let path = config_path(config)?;
    let topic = config
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or("default");
    let batch_size = config
        .get("batch_size")
        .and_then(Value::as_u64)
        .unwrap_or(64);
    let records = collect_records(state, path, topic, batch_size)?;
    Ok(json!({ "records": records, "done": false }))
}

fn collect_records(
    state: &mut TriggerState,
    path: &str,
    topic: &str,
    batch_size: u64,
) -> Result<Vec<Value>> {
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
        records.push(make_record(state, id, payload, published_at)?);
    }
    Ok(records)
}

fn make_record(
    state: &mut TriggerState,
    id: i64,
    payload: String,
    published_at: String,
) -> Result<Value> {
    state.last_id = id;
    let timestamp = if published_at.contains('T') {
        published_at
    } else {
        format!("{}Z", published_at.replace(' ', "T"))
    };
    Ok(json!({
        "id": format!("sqlite-{id}"),
        "timestamp": timestamp,
        "payload": serde_json::from_str::<Value>(&payload)?,
        "metadata": {}
    }))
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
