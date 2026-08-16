//! Host API tests for the script bridge core: `sohara.var/env`, `ctx.*`,
//! record methods, `ctx.emit`, and `require` modules.

mod common;

use serde_json::{json, Map};
use sohara_core::Record;

use common::{ctx, expand, inline, pass, temp_dir, transform};

#[tokio::test]
async fn host_var_and_env_with_fallbacks() {
    let mut vars = Map::new();
    vars.insert("name".to_owned(), json!("world"));
    vars.insert("num".to_owned(), json!(42));
    std::env::set_var("SOHARA_JS_TEST_ENV", "hello");
    let step = transform(
        inline(
            r#"
            function transform(record, ctx) {
                record.name = sohara.var("name");
                record.fb = sohara.var("missing", "fallback");
                record.num = sohara.var("num");
                record.numType = typeof sohara.var("num");
                record.envv = sohara.env("SOHARA_JS_TEST_ENV");
                record.envfb = sohara.env("SOHARA_JS_TEST_MISSING", "env-fb");
                return record;
            }
            "#,
        ),
        ctx(vars, None),
    );
    let payload = pass(step.transform(Record::new(json!({}))).await.unwrap());
    assert_eq!(payload["name"], "world");
    assert_eq!(payload["fb"], "fallback");
    assert_eq!(payload["num"], 42);
    assert_eq!(payload["numType"], "number");
    assert_eq!(payload["envv"], "hello");
    assert_eq!(payload["envfb"], "env-fb");
}

#[tokio::test]
async fn host_ctx_identity() {
    let step = transform(
        inline(
            r#"
            function transform(record, ctx) {
                record.stepId = ctx.step.id;
                record.stepName = ctx.step.name;
                record.stepKind = ctx.step.kind;
                record.stepType = ctx.step.type;
                record.flowName = ctx.flow.name;
                record.corrLen = ctx.correlation_id.length;
                return record;
            }
            "#,
        ),
        ctx(Map::new(), None),
    );
    let first = pass(
        step.transform(Record::new(json!({ "n": 1 })))
            .await
            .unwrap(),
    );
    assert_eq!(first["stepId"], "s1");
    assert_eq!(first["stepName"], "tag");
    assert_eq!(first["stepKind"], "transform");
    assert_eq!(first["stepType"], "script");
    assert_eq!(first["flowName"], "test");
    assert!(first["corrLen"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn host_ctx_state_persists_across_records() {
    let step = transform(
        inline(
            r#"
            function transform(record, ctx) {
                ctx.state.count = (ctx.state.count || 0) + 1;
                record.count = ctx.state.count;
                return record;
            }
            "#,
        ),
        ctx(Map::new(), None),
    );
    let first = pass(
        step.transform(Record::new(json!({ "n": 1 })))
            .await
            .unwrap(),
    );
    assert_eq!(first["count"], 1);
    let second = pass(
        step.transform(Record::new(json!({ "n": 2 })))
            .await
            .unwrap(),
    );
    assert_eq!(second["count"], 2, "ctx.state must persist across records");
}

#[tokio::test]
async fn host_record_methods_and_metadata() {
    let step = transform(
        inline(
            r#"
            function transform(record, ctx) {
                record.set("a.b.c", 7);
                record.got = record.get("a.b.c");
                record.hasDeep = record.has("a.b.c");
                record.hasMissing = record.has("nope");
                record.unset("a.b.c");
                record.gone = record.get("a.b.c");
                record.rid = record.id;
                record.ts = record.timestamp;
                record.mk = record.metadata && record.metadata.k;
                return record;
            }
            "#,
        ),
        ctx(Map::new(), None),
    );
    let record = Record::new(json!({ "name": "x" })).with_metadata("k", "v");
    let payload = pass(step.transform(record).await.unwrap());
    assert_eq!(payload["got"], 7);
    assert_eq!(payload["hasDeep"], true);
    assert_eq!(payload["hasMissing"], false);
    assert!(payload["gone"].is_null(), "unset must delete the path");
    assert!(!payload["rid"].as_str().unwrap().is_empty());
    assert!(!payload["ts"].as_str().unwrap().is_empty());
    assert_eq!(payload["mk"], "v");
}

#[tokio::test]
async fn host_emit_expands_downstream() {
    let step = transform(
        inline(
            r#"
            function transform(record, ctx) {
                ctx.emit({ extra: record.n * 10 });
                return record;
            }
            "#,
        ),
        ctx(Map::new(), None),
    );
    let records = expand(
        step.transform(Record::new(json!({ "n": 4 })))
            .await
            .unwrap(),
    );
    assert_eq!(records.len(), 2, "pass + emitted = expand");
    assert_eq!(records[0].payload["n"], 4);
    assert_eq!(records[1].payload["extra"], 40);
}

#[tokio::test]
async fn host_require_modules() {
    let dir = temp_dir("req");
    std::fs::write(
        dir.join("helper.js"),
        "module.exports = { add: function(a, b) { return a + b; } };",
    )
    .unwrap();
    std::fs::write(
        dir.join("main.js"),
        "var helper = require(\"./helper\"); var soharaLib = require(\"sohara\"); function transform(record, ctx) { record.sum = helper.add(record.n, 1); record.hasSohara = typeof soharaLib.log === \"function\"; return record; }",
    )
    .unwrap();
    let config = json!({
        "script": dir.join("main.js").to_string_lossy().into_owned(),
        "entry": "transform",
    });
    let step = transform(config, ctx(Map::new(), None));
    let payload = pass(
        step.transform(Record::new(json!({ "n": 5 })))
            .await
            .unwrap(),
    );
    assert_eq!(payload["sum"], 6);
    assert_eq!(payload["hasSohara"], true);
    std::fs::remove_dir_all(&dir).ok();
}
