//! Conversion between `serde_json::Value` and QuickJS `JsValue`

use quick_js::JsValue;
use serde_json::Value;

/// Convert a JSON value into a QuickJS value.
pub(crate) fn value_to_js(value: &Value) -> JsValue {
    match value {
        Value::Null => JsValue::Null,
        Value::Bool(flag) => JsValue::Bool(*flag),
        Value::Number(number) => number
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .map(JsValue::Int)
            .or_else(|| number.as_f64().map(JsValue::Float))
            .unwrap_or(JsValue::Undefined),
        Value::String(text) => JsValue::String(text.clone()),
        Value::Array(items) => JsValue::Array(items.iter().map(value_to_js).collect()),
        Value::Object(map) => JsValue::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), value_to_js(value)))
                .collect(),
        ),
    }
}

/// Convert a QuickJS value into a JSON value.
pub(crate) fn js_to_value(value: &JsValue) -> Value {
    match value {
        JsValue::Undefined | JsValue::Null => Value::Null,
        JsValue::Bool(flag) => Value::Bool(*flag),
        JsValue::Int(number) => Value::from(*number),
        JsValue::Float(number) => Value::from(*number),
        JsValue::String(text) => Value::String(text.clone()),
        JsValue::Array(items) => Value::Array(items.iter().map(js_to_value).collect()),
        JsValue::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), js_to_value(value)))
                .collect(),
        ),
        _ => Value::Null,
    }
}
