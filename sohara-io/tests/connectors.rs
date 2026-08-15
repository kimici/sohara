//! Integration tests for S5 connectors: HTTP client and SQLite database steps

use futures::StreamExt;
use sohara_core::{BuildContext, BuiltStep, Record, Sink, Source};
use sohara_io::{DbSink, DbSource, HttpSink, HttpSource};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Start a loopback HTTP server handling `connections` requests; each request
/// text is handed to `respond` to produce the response body.
async fn serve(
    connections: usize,
    respond: impl Fn(String) -> String + Send + Sync + 'static,
) -> (u16, tokio::sync::mpsc::Receiver<String>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = tokio::sync::mpsc::channel(connections);
    tokio::spawn(async move {
        for _ in 0..connections {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let request = read_request(&mut socket).await;
            let body = respond(request.clone());
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = tx.send(request).await;
        }
    });
    (port, rx)
}

/// Read one full HTTP request (headers plus Content-Length body) from a socket.
async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = socket.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let Some(head_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let head_end = head_end + 4;
        let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
        let length = head
            .lines()
            .find_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if buffer.len() >= head_end + length {
            break;
        }
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

fn as_source(built: BuiltStep) -> Box<dyn Source> {
    match built {
        BuiltStep::Source(source) => source,
        _ => panic!("expected a source"),
    }
}

fn as_sink(built: BuiltStep) -> Box<dyn Sink> {
    match built {
        BuiltStep::Sink(sink) => sink,
        _ => panic!("expected a sink"),
    }
}

#[tokio::test]
async fn http_source_fetches_json_payload() {
    let (port, mut requests) = serve(1, |_| r#"{"ok":true,"items":[1,2,3]}"#.to_owned()).await;
    let config = serde_json::json!({ "url": format!("http://127.0.0.1:{port}/data.json") });
    let source = as_source(HttpSource::build(&config, &BuildContext::default()).unwrap());
    let records: Vec<_> = source.stream().await.unwrap().collect().await;
    let mut records = records
        .into_iter()
        .map(|record| record.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records.remove(0).payload,
        serde_json::json!({"ok": true, "items": [1, 2, 3]})
    );
    let request = requests.recv().await.unwrap();
    assert!(
        request.starts_with("GET /data.json HTTP/1.1"),
        "got: {request}"
    );
}

#[tokio::test]
async fn http_source_keeps_plain_body_as_string() {
    let (port, _) = serve(1, |_| "hello world".to_owned()).await;
    let config = serde_json::json!({ "url": format!("http://127.0.0.1:{port}/plain") });
    let source = as_source(HttpSource::build(&config, &BuildContext::default()).unwrap());
    let records: Vec<_> = source.stream().await.unwrap().collect().await;
    let record = records.into_iter().next().unwrap().unwrap();
    assert_eq!(record.payload, serde_json::json!("hello world"));
}

#[tokio::test]
async fn http_sink_posts_json_body() {
    let (port, mut requests) = serve(1, |_| r#"{"status":"ok"}"#.to_owned()).await;
    let config = serde_json::json!({ "url": format!("http://127.0.0.1:{port}/submit") });
    let sink = as_sink(HttpSink::build(&config, &BuildContext::default()).unwrap());
    sink.send(Record::new(serde_json::json!({"n": 42})))
        .await
        .unwrap();
    let request = requests.recv().await.unwrap();
    assert!(
        request.starts_with("POST /submit HTTP/1.1"),
        "got: {request}"
    );
    assert!(
        request.contains("Content-Type: application/json"),
        "got: {request}"
    );
    assert!(request.ends_with("{\"n\":42}"), "got: {request}");
}

#[tokio::test]
async fn http_build_rejects_non_http_urls() {
    let config = serde_json::json!({ "url": "https://example.com/data" });
    match HttpSource::build(&config, &BuildContext::default()) {
        Err(error) => assert!(error.to_string().contains("http://"), "got: {error}"),
        Ok(_) => panic!("https urls must be rejected"),
    }
}

/// Read `(text, text)` row pairs from a SQLite file.
fn read_pairs(path: &std::path::Path, sql: &str) -> Vec<(String, String)> {
    let connection = rusqlite::Connection::open(path).unwrap();
    let mut statement = connection.prepare(sql).unwrap();
    statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

#[tokio::test]
async fn db_sink_flush_inserts_rows() {
    let path = std::env::temp_dir().join(format!("sohara-db-sink-{}.db", std::process::id()));
    std::fs::remove_file(&path).ok();
    let config = serde_json::json!({ "path": path.to_str().unwrap(), "table": "people" });
    let sink = as_sink(DbSink::build(&config, &BuildContext::default()).unwrap());
    sink.send(Record::new(serde_json::json!({"name": "Alice", "age": 30})))
        .await
        .unwrap();
    sink.send(Record::new(serde_json::json!({"name": "Bob", "age": 25})))
        .await
        .unwrap();
    sink.flush().await.unwrap();
    let rows = read_pairs(&path, "SELECT name, age FROM people ORDER BY name");
    assert_eq!(
        rows,
        vec![
            ("Alice".to_owned(), "30".to_owned()),
            ("Bob".to_owned(), "25".to_owned())
        ]
    );
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn db_source_reads_typed_rows() {
    let path = std::env::temp_dir().join(format!("sohara-db-source-{}.db", std::process::id()));
    std::fs::remove_file(&path).ok();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "CREATE TABLE metrics (name TEXT, count INTEGER, ratio REAL, note TEXT)",
            [],
        )
        .unwrap();
    connection
        .execute("INSERT INTO metrics VALUES ('a', 3, 1.5, NULL)", [])
        .unwrap();
    drop(connection);
    let config = serde_json::json!({
        "path": path.to_str().unwrap(),
        "query": "SELECT name, count, ratio, note FROM metrics"
    });
    let source = as_source(DbSource::build(&config, &BuildContext::default()).unwrap());
    let records: Vec<_> = source.stream().await.unwrap().collect().await;
    let record = records.into_iter().next().unwrap().unwrap();
    assert_eq!(
        record.payload,
        serde_json::json!({"name": "a", "count": 3, "ratio": 1.5, "note": null})
    );
    std::fs::remove_file(&path).ok();
}
