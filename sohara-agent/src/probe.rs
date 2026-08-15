//! Instance admin HTTP helpers (D2): health probe and control posts

use anyhow::{bail, Result};
use reqwest::Client;
use serde_json::Value;

/// Result of one admin health probe.
#[derive(Debug, Clone, Copy)]
pub struct Health {
    pub healthy: bool,
    pub paused: bool,
}

/// Probe `/admin/health`; a non-2xx response is `healthy: false`.
pub async fn probe_health(client: &Client, admin: &str, token: Option<&str>) -> Result<Health> {
    let mut request = client.get(format!("http://{admin}/admin/health"));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        return Ok(Health {
            healthy: false,
            paused: false,
        });
    }
    let value: Value = response.json().await?;
    Ok(Health {
        healthy: true,
        paused: value
            .get("paused")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// POST to an admin control endpoint (`/admin/pause`, `/admin/resume`, …).
pub async fn admin_post(
    client: &Client,
    admin: &str,
    token: Option<&str>,
    path: &str,
) -> Result<()> {
    let mut request = client.post(format!("http://{admin}{path}"));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if response.status().is_success() {
        Ok(())
    } else {
        bail!("admin {path} -> {}", response.status())
    }
}
