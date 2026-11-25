//! C1 tests: empty server health endpoint + fail-closed bearer auth.

use std::net::SocketAddr;
use std::sync::Arc;

use sohara_runtime::{spawn_server, ServerState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

/// One raw HTTP exchange result: status code and decoded body.
struct Response {
    status: u16,
    body: String,
}

#[tokio::test]
async fn health_rejects_missing_token() {
    let server = start_server().await;
    let response = request(server.addr, "/admin/health", None).await;
    assert_eq!(response.status, 401, "missing token must be rejected");
}

#[tokio::test]
async fn health_rejects_wrong_token() {
    let server = start_server().await;
    let response = request(server.addr, "/admin/health", Some("wrong")).await;
    assert_eq!(response.status, 401, "wrong token must be rejected");
}

#[tokio::test]
async fn health_accepts_correct_token() {
    let server = start_server().await;
    let response = request(server.addr, "/admin/health", Some("Bearer test")).await;
    assert_eq!(response.status, 200, "correct token must be accepted");
    let body: serde_json::Value = serde_json::from_str(&response.body).expect("valid JSON body");
    assert_eq!(body["status"], "running");
    assert_eq!(body["flows"], 0);
}

#[tokio::test]
async fn auth_scheme_is_case_insensitive() {
    let server = start_server().await;
    let response = request(server.addr, "/admin/health", Some("bearer test")).await;
    assert_eq!(response.status, 200, "RFC 9110 scheme is case-insensitive");
}

#[tokio::test]
async fn non_bearer_scheme_is_rejected() {
    let server = start_server().await;
    let response = request(server.addr, "/admin/health", Some("Basic test")).await;
    assert_eq!(response.status, 401, "non-Bearer scheme must be rejected");
}

#[tokio::test]
async fn unknown_route_is_unauthorized_before_not_found() {
    let server = start_server().await;
    let response = request(server.addr, "/unknown", None).await;
    assert_eq!(response.status, 401, "unknown routes stay fail-closed");
}

#[test]
fn blank_tokens_are_rejected_at_construction() {
    assert!(
        ServerState::new("").is_err(),
        "empty token must be rejected"
    );
    assert!(
        ServerState::new("   ").is_err(),
        "whitespace-only token must be rejected"
    );
    assert!(
        ServerState::new("ok").is_ok(),
        "non-blank token is accepted"
    );
}

struct Server {
    addr: SocketAddr,
    #[allow(dead_code)]
    handle: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn start_server() -> Server {
    let state = Arc::new(ServerState::new("test").expect("valid token"));
    // Bind port 0 and use the actually bound address, avoiding the
    // bind-then-free TOCTOU race and any fixed sleep.
    let (addr, handle) = spawn_server(
        "127.0.0.1:0".parse().unwrap(),
        state,
        std::future::pending(),
    )
    .await
    .expect("server binds");
    Server { addr, handle }
}

/// Send one request. `auth` is the value for the `Authorization` header.
async fn request(addr: SocketAddr, path: &str, auth: Option<&str>) -> Response {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let header = auth
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    let raw =
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n{header}Connection: close\r\n\r\n");
    socket.write_all(raw.as_bytes()).await.unwrap();
    let mut buffer = Vec::new();
    socket.read_to_end(&mut buffer).await.unwrap();
    let text = String::from_utf8_lossy(&buffer).into_owned();
    let status = parse_status(&text);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    Response {
        status,
        body: decode_chunked(body),
    }
}

fn parse_status(text: &str) -> u16 {
    text.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0)
}

/// Decode an HTTP/1.1 chunked body defensively (axum uses chunked encoding).
/// Bounds are checked so a truncated or non-chunked body cannot panic.
fn decode_chunked(body: &str) -> String {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut rest = bytes;
    while let Some(end) = rest.windows(2).position(|window| window == b"\r\n") {
        let size_text = String::from_utf8_lossy(&rest[..end]);
        let Ok(size) = usize::from_str_radix(size_text.trim(), 16) else {
            return body.to_owned();
        };
        if size == 0 {
            break;
        }
        let data_start = end + 2;
        let data_end = data_start + size;
        if data_end > rest.len() {
            return body.to_owned();
        }
        out.extend_from_slice(&rest[data_start..data_end]);
        let after = data_end + 2;
        if after > rest.len() {
            break;
        }
        rest = &rest[after..];
    }
    if out.is_empty() {
        return body.to_owned();
    }
    String::from_utf8_lossy(&out).into_owned()
}
