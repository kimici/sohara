//! Host API tests for the script bridge I/O: `sohara.file/http/db/notify`,
//! permission enforcement, and sources.

mod common;

use std::sync::Arc;

use serde_json::{json, Map};
use sohara_core::{BuiltStep, Record, StepKind};

use common::{ctx, inline, pass, registry, spawn_http_stub, temp_dir, transform, TestBus};

#[tokio::test]
async fn host_file_read_is_always_allowed() {
    let dir = temp_dir("file-read");
    let path = dir.join("data.txt");
    let path_str = path.to_string_lossy().into_owned();
    std::fs::write(&path, "hello file").unwrap();
    let step = transform(
        inline(&format!(
            r#"function transform(record, ctx) {{ record.content = sohara.file.read("{path_str}"); return record; }}"#
        )),
        ctx(Map::new(), None),
    );
    let payload = pass(step.transform(Record::new(json!({}))).await.unwrap());
    assert_eq!(payload["content"], "hello file");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn host_file_write_requires_permission() {
    let dir = temp_dir("file-deny");
    let path_str = dir.join("data.txt").to_string_lossy().into_owned();
    let step = transform(
        inline(&format!(
            r#"function transform(record, ctx) {{ sohara.file.write("{path_str}", "nope"); return record; }}"#
        )),
        ctx(Map::new(), None),
    );
    let error = step.transform(Record::new(json!({}))).await.unwrap_err();
    assert!(
        error.to_string().contains("permission 'file.write'"),
        "got: {error}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn host_file_write_with_permission() {
    let dir = temp_dir("file-write");
    let path_str = dir.join("data.txt").to_string_lossy().into_owned();
    let step = transform(
        json!({
            "inline": format!(r#"function transform(record, ctx) {{ sohara.file.write("{path_str}", "written"); return record; }}"#),
            "allow": ["file.write"],
        }),
        ctx(Map::new(), None),
    );
    step.transform(Record::new(json!({}))).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.join("data.txt")).unwrap(),
        "written"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn host_http_request_against_stub() {
    let (_server, port) = spawn_http_stub();
    let step = transform(
        json!({
            "inline": format!(
                r#"
                function transform(record, ctx) {{
                    var res = sohara.http.request({{
                        url: "http://127.0.0.1:{port}/hello",
                        method: "GET",
                        timeout_ms: 5000
                    }});
                    record.status = res.status;
                    record.ok = res.ok;
                    record.greeting = res.json.greeting;
                    record.header = res.headers["x-test"];
                    return record;
                }}
                "#
            ),
            "allow": ["http"],
        }),
        ctx(Map::new(), None),
    );
    let payload = pass(step.transform(Record::new(json!({}))).await.unwrap());
    assert_eq!(payload["status"], 200);
    assert_eq!(payload["ok"], true);
    assert_eq!(payload["greeting"], "hi");
    assert_eq!(payload["header"], "yes");
}

#[tokio::test]
async fn host_http_requires_permission() {
    let step = transform(
        inline(
            r#"
            function transform(record, ctx) {
                sohara.http.request({ url: "http://127.0.0.1:1/" });
                return record;
            }
            "#,
        ),
        ctx(Map::new(), None),
    );
    let error = step.transform(Record::new(json!({}))).await.unwrap_err();
    assert!(
        error.to_string().contains("permission 'http'"),
        "got: {error}"
    );
}

#[tokio::test]
async fn host_db_query_and_permissions() {
    let dir = temp_dir("db");
    let db_str = dir.join("test.sqlite").to_string_lossy().into_owned();
    let config = json!({
        "inline": format!(
            r#"
            function transform(record, ctx) {{
                sohara.db.query("CREATE TABLE IF NOT EXISTS t (id INTEGER, name TEXT)");
                sohara.db.query("INSERT INTO t VALUES (?, ?)", [1, "alice"]);
                record.rows = sohara.db.query("SELECT id, name FROM t WHERE id = ?", [1]);
                return record;
            }}
            "#
        ),
        "allow": ["db"],
        "db": db_str,
    });
    let step = transform(config, ctx(Map::new(), None));
    let payload = pass(step.transform(Record::new(json!({}))).await.unwrap());
    let rows = payload["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "alice");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn host_db_requires_permission() {
    let step = transform(
        inline("function transform(record, ctx) { sohara.db.query('SELECT 1'); return record; }"),
        ctx(Map::new(), None),
    );
    let error = step.transform(Record::new(json!({}))).await.unwrap_err();
    assert!(
        error.to_string().contains("permission 'db'"),
        "got: {error}"
    );
}

#[tokio::test]
async fn host_notify_requires_bus_and_permission() {
    let script = inline(
        "function transform(record, ctx) { sohara.notify('topic', { n: 1 }); return record; }",
    );
    let step = transform(script.clone(), ctx(Map::new(), None));
    let error = step.transform(Record::new(json!({}))).await.unwrap_err();
    assert!(
        error.to_string().contains("permission 'notify'"),
        "got: {error}"
    );

    let step = transform(
        json!({ "inline": script["inline"].clone(), "allow": ["notify"] }),
        ctx(Map::new(), None),
    );
    let error = step.transform(Record::new(json!({}))).await.unwrap_err();
    assert!(error.to_string().contains("no event bus"), "got: {error}");
}

#[tokio::test]
async fn host_notify_publishes_with_bus_and_permission() {
    let bus = Arc::new(TestBus::default());
    let step = transform(
        json!({
            "inline": "function transform(record, ctx) { sohara.notify('topic', { n: 1 }); return record; }",
            "allow": ["notify"],
        }),
        ctx(Map::new(), Some(bus.clone())),
    );
    step.transform(Record::new(json!({}))).await.unwrap();
    let events = bus.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "topic");
    assert_eq!(events[0].1["n"], 1);
}

#[tokio::test]
async fn host_source_with_ctx_and_emit() {
    let registry = registry();
    let config = json!({
        "inline": r#"
        function generate(ctx) {
            ctx.emit({ n: 9 });
            var out = [];
            for (var i = 0; i < 2; i++) { out.push({ n: i }); }
            return out;
        }
        "#,
    });
    let step = match registry
        .build(StepKind::Source, "script", &config, &ctx(Map::new(), None))
        .unwrap()
    {
        BuiltStep::Source(step) => step,
        _ => panic!("expected source step"),
    };
    let mut records = Vec::new();
    let mut stream = step.stream().await.unwrap();
    while let Some(record) = futures::StreamExt::next(&mut stream).await {
        records.push(record.unwrap());
    }
    let payloads: Vec<_> = records.into_iter().map(|record| record.payload).collect();
    assert_eq!(payloads.len(), 3, "2 returned + 1 emitted");
    assert_eq!(payloads[0]["n"], 0);
    assert_eq!(payloads[1]["n"], 1);
    assert_eq!(payloads[2]["n"], 9);
}
