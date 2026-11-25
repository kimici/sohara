//! C1 tests: empty server health endpoint + fail-closed bearer auth.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sohara_runtime::{spawn_server, ServerState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn health_rejects_missing_token() {
    let (addr, handle) = start_server().await;
    let (status, _) = request(addr, None).await;
    assert_eq!(status, 401, "missing token must be rejected");
    handle.abort();
}

#[tokio::test]
async fn health_rejects_wrong_token() {
    let (addr, handle) = start_server().await;
    let (status, _) = request(addr, Some("wrong")).await;
    assert_eq!(status, 401, "wrong token must be rejected");
    handle.abort();
}

#[tokio::test]
async fn health_accepts_correct_token() {
    let (addr, handle) = start_server().await;
    let (status, body) = request(addr, Some("test")).await;
    assert_eq!(status, 200, "correct token must be accepted");
    assert!(body.contains(r#""status":"running""#), "got: {body}");
    handle.abort();
}

async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let addr = free_addr();
    let state = Arc::new(ServerState {
        token: "test".to_owned(),
    });
    let handle = spawn_server(addr, state).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

async fn request(addr: SocketAddr, token: Option<&str>) -> (u16, String) {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = token
        .map(|value| format!("Authorization: Bearer {value}\r\n"))
        .unwrap_or_default();
    let request =
        format!("GET /admin/health HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\n\r\n");
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut buffer = Vec::new();
    socket.read_to_end(&mut buffer).await.unwrap();
    let text = String::from_utf8_lossy(&buffer).into_owned();
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    (status, decode_chunked(body))
}

/// Decode an HTTP/1.1 chunked response body (axum uses chunked encoding).
fn decode_chunked(body: &str) -> String {
    if !body.contains("\r\n") {
        return body.to_owned();
    }
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut rest = bytes;
    loop {
        let Some(end) = rest.windows(2).position(|window| window == b"\r\n") else {
            out.extend_from_slice(rest);
            break;
        };
        let size_text = String::from_utf8_lossy(&rest[..end]);
        let Ok(size) = usize::from_str_radix(size_text.trim(), 16) else {
            out.extend_from_slice(rest);
            break;
        };
        if size == 0 {
            break;
        }
        let data_start = end + 2;
        out.extend_from_slice(&rest[data_start..data_start + size]);
        rest = &rest[data_start + size + 2..];
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}
