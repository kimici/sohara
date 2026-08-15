//! Integration tests for the expression language

use serde_json::{json, Map, Value};
use sohara_core::{eval, parse, EvalContext};

#[test]
fn comparison_coerces_numeric_strings() {
    let expr = parse("age >= 18").unwrap();
    let ctx = EvalContext::default();
    assert_eq!(
        eval(&expr, &json!({"age": "30"}), &ctx).unwrap(),
        json!(true)
    );
    assert_eq!(eval(&expr, &json!({"age": 30}), &ctx).unwrap(), json!(true));
    assert_eq!(
        eval(&expr, &json!({"age": "15"}), &ctx).unwrap(),
        json!(false)
    );
}

#[test]
fn logic_and_dollar_paths() {
    let expr = parse("$.user.age > 10 and name == 'Alice'").unwrap();
    let ctx = EvalContext::default();
    let record = json!({"user": {"age": 30}, "name": "Alice"});
    assert_eq!(eval(&expr, &record, &ctx).unwrap(), json!(true));
    let other = json!({"user": {"age": 30}, "name": "Bob"});
    assert_eq!(eval(&expr, &other, &ctx).unwrap(), json!(false));
}

#[test]
fn arithmetic_and_functions() {
    let ctx = EvalContext::default();
    let record = json!({"age": "30", "name": "abc"});
    let expr = parse("int(age) + 1").unwrap();
    assert_eq!(eval(&expr, &record, &ctx).unwrap(), json!(31));
    let expr = parse("len(name)").unwrap();
    assert_eq!(eval(&expr, &record, &ctx).unwrap(), json!(3));
}

#[test]
fn membership_in_list() {
    let expr = parse("status in ['a', 'b']").unwrap();
    let ctx = EvalContext::default();
    assert_eq!(
        eval(&expr, &json!({"status": "a"}), &ctx).unwrap(),
        json!(true)
    );
    assert_eq!(
        eval(&expr, &json!({"status": "c"}), &ctx).unwrap(),
        json!(false)
    );
}

#[test]
fn not_and_string_concat() {
    let ctx = EvalContext::default();
    let expr = parse("not (disabled)").unwrap();
    assert_eq!(
        eval(&expr, &json!({"disabled": false}), &ctx).unwrap(),
        json!(true)
    );
    let expr = parse("first + ' ' + last").unwrap();
    assert_eq!(
        eval(&expr, &json!({"first": "a", "last": "b"}), &ctx).unwrap(),
        json!("a b")
    );
}

#[test]
fn flow_vars_are_visible() {
    let vars: Map<String, Value> = [("threshold".to_owned(), json!(5))].into_iter().collect();
    let ctx = EvalContext { vars: &vars };
    let expr = parse("var(threshold)").unwrap();
    assert_eq!(eval(&expr, &json!({}), &ctx).unwrap(), json!(5));
}

#[test]
fn missing_field_is_an_error() {
    let expr = parse("a.b").unwrap();
    let ctx = EvalContext::default();
    assert!(eval(&expr, &json!({}), &ctx).is_err());
}

#[test]
fn parse_errors_are_reported() {
    assert!(parse("age >").is_err());
    assert!(parse("(a + 1").is_err());
    assert!(parse("foo(").is_err());
    assert!(parse("a && b").is_err());
}
