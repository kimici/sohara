//! SQLite host callback (`sohara.db.query`).

use quick_js::JsValue;
use serde_json::Value;

use crate::convert::js_to_value;
use crate::env::StepEnv;

pub fn db_query(env: &StepEnv, sql: String, params: JsValue) -> Result<JsValue, String> {
    env.require(crate::env::PERM_DB)?;
    let path = env
        .db
        .as_ref()
        .ok_or_else(|| "sohara.db.query: no 'db' path in the script step config".to_owned())?;
    let connection = rusqlite::Connection::open(path)
        .map_err(|error| format!("sohara.db.query: open '{path}': {error}"))?;
    let params = js_to_value(&params);
    let params = sql_params(&params);
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("sohara.db.query: prepare: {error}"))?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(params.iter()), row_to_object)
        .map_err(|error| format!("sohara.db.query: {error}"))?;
    let mut result = Vec::new();
    for row in rows {
        let row = row.map_err(|error| format!("sohara.db.query: row: {error}"))?;
        result.push(row);
    }
    Ok(crate::convert::value_to_js(&Value::Array(result)))
}

fn sql_params(params: &Value) -> Vec<rusqlite::types::Value> {
    params
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|value| match value {
            Value::Null => rusqlite::types::Value::Null,
            Value::Bool(flag) => rusqlite::types::Value::Integer(i64::from(flag)),
            Value::Number(number) => {
                if let Some(integer) = number.as_i64() {
                    rusqlite::types::Value::Integer(integer)
                } else {
                    rusqlite::types::Value::Real(number.as_f64().unwrap_or(0.0))
                }
            }
            Value::String(text) => rusqlite::types::Value::Text(text),
            other => rusqlite::types::Value::Text(other.to_string()),
        })
        .collect()
}

fn row_to_object(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    let mut object = serde_json::Map::new();
    for index in 0..row.as_ref().column_count() {
        let name = row.as_ref().column_name(index)?.to_owned();
        let value = match row.get_ref(index)? {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(number) => Value::from(number),
            rusqlite::types::ValueRef::Real(number) => Value::from(number),
            rusqlite::types::ValueRef::Text(text) => {
                Value::String(String::from_utf8_lossy(text).into_owned())
            }
            rusqlite::types::ValueRef::Blob(blob) => {
                Value::String(format!("blob:{}bytes", blob.len()))
            }
        };
        object.insert(name, value);
    }
    Ok(Value::Object(object))
}
