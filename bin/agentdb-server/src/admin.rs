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

use crate::cli::{
    AdminOpts, CreateTabletOpts, SetShardOpts, ShedTabletOpts, ShowShardOpts, UserAddOpts,
    UserDeleteOpts,
};

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

/// `POST /api/admin/shards/:id`.
pub async fn set_shard(opts: SetShardOpts) -> Result<()> {
    if opts.tablet.is_none() && !opts.clear {
        anyhow::bail!("must supply --tablet <id> or --clear");
    }
    let url = format!(
        "{}/api/admin/shards/{}",
        opts.server_url.trim_end_matches('/'),
        opts.shard_id
    );
    let body = serde_json::json!({
        // `--clear` and missing `--tablet` both produce null here.
        "tablet_id": opts.tablet,
    });
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `GET /api/admin/shards/list`.
pub async fn list_shard_overrides(opts: AdminOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/shards/list",
        opts.server_url.trim_end_matches('/')
    );
    fetch_and_print(&url).await
}

/// `POST /api/admin/users` — create a user. Reads the password
/// from `--password` if supplied, otherwise from stdin with a
/// prompt (no echo suppression — we don't pull a terminfo/tty
/// dep just for this; callers running in automation should
/// supply `--password`).
pub async fn user_add(opts: UserAddOpts) -> Result<()> {
    let password = match opts.password {
        Some(p) => p,
        None => prompt_password_from_stdin()?,
    };
    let url = format!(
        "{}/api/admin/users",
        opts.server_url.trim_end_matches('/')
    );
    let mut body = serde_json::json!({
        "user_id": opts.user_id,
        "tenant_external": opts.tenant,
        "password": password,
    });
    if let Some(iter) = opts.iterations {
        body["iterations"] = serde_json::Value::from(iter);
    }
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `GET /api/admin/users` — list users.
pub async fn user_list(opts: AdminOpts) -> Result<()> {
    let url = format!("{}/api/admin/users", opts.server_url.trim_end_matches('/'));
    fetch_and_print(&url).await
}

/// `DELETE /api/admin/users/:user_id` — delete a user
/// (idempotent; always 204).
pub async fn user_delete(opts: UserDeleteOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/users/{}",
        opts.server_url.trim_end_matches('/'),
        urlencode(&opts.user_id),
    );
    let client = reqwest::Client::new();
    let resp = client
        .delete(&url)
        .send()
        .await
        .with_context(|| format!("DELETE {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        eprintln!("delete failed ({status}): {body}");
        std::process::exit(exit_code_for(status));
    }
    Ok(())
}

/// `GET /api/admin/tablets/hot` — show flagged hot tablets.
pub async fn show_hot_tablets(opts: AdminOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tablets/hot",
        opts.server_url.trim_end_matches('/')
    );
    fetch_and_print(&url).await
}

/// `GET /api/admin/tablets/load` — all tablets with rate info.
pub async fn show_tablet_load(opts: AdminOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tablets/load",
        opts.server_url.trim_end_matches('/')
    );
    fetch_and_print(&url).await
}

/// `POST /api/admin/tablets/:id/shed` — bulk-move shards off a
/// hot tablet.
pub async fn shed_tablet(opts: ShedTabletOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tablets/{}/shed",
        opts.server_url.trim_end_matches('/'),
        opts.tablet_id,
    );
    let mut body = serde_json::json!({});
    if let Some(to) = opts.to {
        body["to_tablet_id"] = serde_json::Value::from(to);
    }
    if let Some(f) = opts.fraction {
        body["fraction"] = serde_json::Value::from(f);
    }
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `POST /api/admin/tablets/create` — M25 dynamic-tablet
/// creation.
pub async fn create_tablet(opts: CreateTabletOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tablets/create",
        opts.server_url.trim_end_matches('/'),
    );
    let body = match opts.id {
        Some(id) => serde_json::json!({ "tablet_id": id }),
        None => serde_json::json!({}),
    };
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// Read a line from stdin with a minimal prompt. Not a
/// password-masking prompt — that requires a terminfo dep. The
/// plan doc explicitly acknowledges this tradeoff: scripts
/// should pass `--password`, interactive users can live with
/// echoed input in a prototype.
fn prompt_password_from_stdin() -> Result<String> {
    use std::io::{BufRead, Write};
    eprint!("password: ");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .context("read password from stdin")?;
    let trimmed = line.trim_end_matches(|c: char| c == '\n' || c == '\r');
    if trimmed.is_empty() {
        anyhow::bail!("empty password");
    }
    Ok(trimmed.to_string())
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
