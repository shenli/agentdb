//! Admin HTTP-client subcommands. Each one is a thin wrapper around
//! an existing `/api/admin/*` endpoint, formatted for terminal use.
//!
//! The surface matches the operator workflow:
//!
//!   agentdb drain                  → POST /api/admin/drain
//!   agentdb show-tablets           → GET  /api/admin/tablets
//!   agentdb show-scheduler         → GET  /api/admin/scheduler
//!   agentdb show-nodes             → GET  /api/admin/nodes
//!
//! The body is fetched as JSON and pretty-printed. Non-2xx responses
//! exit with status 1 after printing the body — the REST API already
//! returns structured errors with `doc_url`, so the operator gets the
//! runbook URL without us duplicating it here.

use anyhow::{Context, Result};
use reqwest::StatusCode;
use serde_json::Value;

use crate::cli::{AdminOpts, ShowShardOpts};

/// `POST /api/admin/drain`.
pub async fn drain(opts: AdminOpts) -> Result<()> {
    let url = format!("{}/api/admin/drain", opts.server_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `GET /api/admin/tablets`.
pub async fn show_tablets(opts: AdminOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tablets",
        opts.server_url.trim_end_matches('/')
    );
    fetch_and_print(&url).await
}

/// `GET /api/admin/scheduler`.
pub async fn show_scheduler(opts: AdminOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/scheduler",
        opts.server_url.trim_end_matches('/')
    );
    fetch_and_print(&url).await
}

/// `GET /api/admin/nodes`.
pub async fn show_nodes(opts: AdminOpts) -> Result<()> {
    let url = format!("{}/api/admin/nodes", opts.server_url.trim_end_matches('/'));
    fetch_and_print(&url).await
}

/// `GET /api/admin/shards?session_id=...`.
pub async fn show_shard(opts: ShowShardOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/shards?session_id={}",
        opts.server_url.trim_end_matches('/'),
        urlencode(&opts.session_id),
    );
    fetch_and_print(&url).await
}

/// Minimal percent-encoding for a query value. Covers the characters
/// session ids are likely to include; the admin CLI isn't a
/// general-purpose URL builder so we don't need full RFC 3986.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(c),
            _ => {
                for b in c.to_string().as_bytes() {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    out
}

async fn fetch_and_print(url: &str) -> Result<()> {
    let resp = reqwest::get(url)
        .await
        .with_context(|| format!("GET {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

async fn print_json_and_exit_on_failure(resp: reqwest::Response) -> Result<()> {
    let status = resp.status();
    let body = resp.text().await.context("read response body")?;
    let pretty = match serde_json::from_str::<Value>(&body) {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap_or(body.clone()),
        Err(_) => body.clone(),
    };
    println!("{pretty}");
    if !status.is_success() {
        // Don't print an extra line — the server's JSON already
        // carries `code`, `message`, `doc_url`. Just exit non-zero so
        // shell pipelines notice.
        std::process::exit(exit_code_for(status));
    }
    Ok(())
}

/// Map HTTP status to an exit code distinguishable in shell scripts.
/// `1` for 4xx (client issue — misconfig, bad URL), `2` for 5xx
/// (server issue — runbook territory).
fn exit_code_for(status: StatusCode) -> i32 {
    if status.is_client_error() {
        1
    } else if status.is_server_error() {
        2
    } else {
        1
    }
}
