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
    AdminOpts, AdminTokenAddOpts, AdminTokenRevokeOpts, AuditLogOpts, BackupOpts,
    ClearTenantQuotaOpts, CreateTabletOpts, MigrateSessionOpts, ReloadOpts, SetShardOpts,
    SetTenantQuotaOpts, ShedTabletOpts, ShowLatencyOpts, ShowShardOpts, ShowSlowQueriesOpts,
    ShowTenantQuotaOpts, UserAddOpts, UserDeleteOpts,
};

/// `POST /api/admin/drain`.
pub async fn drain(opts: AdminOpts) -> Result<()> {
    let url = format!("{}/api/admin/drain", opts.server_url.trim_end_matches('/'));
    let client = admin_client();
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
    let client = admin_client();
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
    let client = admin_client();
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
    let client = admin_client();
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
    if opts.migrate_data {
        body["migrate_data"] = serde_json::Value::from(true);
    }
    let client = admin_client();
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
    let client = admin_client();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `POST /api/admin/tokens` — mint a new admin bearer token.
/// The server returns the plaintext token in the response
/// body ONCE; we print the whole JSON so the operator can
/// pipe it through `jq -r .token` into an env var.
pub async fn admin_token_add(opts: AdminTokenAddOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tokens",
        opts.server_url.trim_end_matches('/')
    );
    let mut body = serde_json::json!({});
    if let Some(d) = opts.description {
        body["description"] = serde_json::Value::from(d);
    }
    // Reject unknown scope strings locally so operators get a
    // fast, clear error instead of bouncing off the server.
    let scope_lc = opts.scope.to_ascii_lowercase();
    if scope_lc != "admin" && scope_lc != "readonly" {
        anyhow::bail!(
            "invalid scope '{}': expected 'admin' or 'readonly'",
            opts.scope
        );
    }
    body["scope"] = serde_json::Value::from(scope_lc);
    let client = admin_client_with_opt(opts.token.as_deref());
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `GET /api/admin/tokens` — list active tokens.
pub async fn admin_token_list(opts: AdminOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tokens",
        opts.server_url.trim_end_matches('/')
    );
    // Uses the env-var-driven admin_client; list doesn't
    // take a --token flag of its own.
    fetch_and_print(&url).await
}

/// `DELETE /api/admin/tokens/:id` — soft-revoke.
pub async fn admin_token_revoke(opts: AdminTokenRevokeOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tokens/{}",
        opts.server_url.trim_end_matches('/'),
        urlencode(&opts.token_id),
    );
    let client = admin_client_with_opt(opts.token.as_deref());
    let resp = client
        .delete(&url)
        .send()
        .await
        .with_context(|| format!("DELETE {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        eprintln!("revoke failed ({status}): {body}");
        std::process::exit(exit_code_for(status));
    }
    Ok(())
}

/// `POST /api/admin/sessions/:sid/migrate` — warm session
/// data handoff between two local tablets on this node.
pub async fn migrate_session(opts: MigrateSessionOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/sessions/{}/migrate",
        opts.server_url.trim_end_matches('/'),
        urlencode(&opts.session_id),
    );
    let body = serde_json::json!({
        "tenant_id": opts.tenant,
        "to_tablet_id": opts.to,
    });
    let client = admin_client();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `GET /api/admin/boxes/:id/latency` — show p50/p95/p99.
pub async fn show_latency(opts: ShowLatencyOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/boxes/{}/latency",
        opts.server_url.trim_end_matches('/'),
        opts.box_id,
    );
    fetch_and_print(&url).await
}

/// `PUT /api/admin/tenant-quotas/:tenant_id` — set a global
/// per-tenant rate limit.
pub async fn set_tenant_quota(opts: SetTenantQuotaOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tenant-quotas/{}",
        opts.server_url.trim_end_matches('/'),
        urlencode(&opts.tenant_id),
    );
    let body = serde_json::json!({"max_qps": opts.max_qps, "burst": opts.burst});
    let client = admin_client();
    let resp = client
        .put(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("PUT {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `GET /api/admin/tenant-quotas[/:tenant_id]` — show one or all.
pub async fn show_tenant_quota(opts: ShowTenantQuotaOpts) -> Result<()> {
    let url = match &opts.tenant_id {
        Some(tid) => format!(
            "{}/api/admin/tenant-quotas/{}",
            opts.server_url.trim_end_matches('/'),
            urlencode(tid),
        ),
        None => format!(
            "{}/api/admin/tenant-quotas",
            opts.server_url.trim_end_matches('/'),
        ),
    };
    fetch_and_print(&url).await
}

/// `DELETE /api/admin/tenant-quotas/:tenant_id` — clear.
pub async fn clear_tenant_quota(opts: ClearTenantQuotaOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/tenant-quotas/{}",
        opts.server_url.trim_end_matches('/'),
        urlencode(&opts.tenant_id),
    );
    let client = admin_client();
    let resp = client
        .delete(&url)
        .send()
        .await
        .with_context(|| format!("DELETE {url}"))?;
    // 204 No Content expected; print_json_and_exit_on_failure
    // handles success and error bodies.
    print_json_and_exit_on_failure(resp).await
}

/// `POST /api/admin/reload` — hot-reload config.
pub async fn reload(opts: ReloadOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/reload",
        opts.server_url.trim_end_matches('/'),
    );
    let client = admin_client();
    let resp = client
        .post(&url)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `POST /api/admin/backup` — point-in-time backup.
pub async fn backup(opts: BackupOpts) -> Result<()> {
    let url = format!(
        "{}/api/admin/backup",
        opts.server_url.trim_end_matches('/'),
    );
    let body = serde_json::json!({"out_dir": opts.out_dir});
    let client = admin_client();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    print_json_and_exit_on_failure(resp).await
}

/// `GET /api/admin/slow-queries` — list recent slow requests.
pub async fn show_slow_queries(opts: ShowSlowQueriesOpts) -> Result<()> {
    let mut qs: Vec<(String, String)> = vec![("limit".into(), opts.limit.to_string())];
    if let Some(k) = &opts.kind {
        qs.push(("kind".into(), k.clone()));
    }
    if let Some(t) = &opts.tenant {
        qs.push(("tenant".into(), t.clone()));
    }
    if let Some(a) = opts.after_id {
        qs.push(("after_id".into(), a.to_string()));
    }
    let qstr = qs
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect::<Vec<_>>()
        .join("&");
    let url = format!(
        "{}/api/admin/slow-queries?{qstr}",
        opts.server_url.trim_end_matches('/'),
    );
    fetch_and_print(&url).await
}

/// `GET /api/admin/audit` — list recent admin-plane requests.
pub async fn audit_log(opts: AuditLogOpts) -> Result<()> {
    let mut qs: Vec<(String, String)> = vec![("limit".into(), opts.limit.to_string())];
    if let Some(v) = opts.since_ms {
        qs.push(("since_ms".into(), v.to_string()));
    }
    if let Some(v) = opts.until_ms {
        qs.push(("until_ms".into(), v.to_string()));
    }
    if let Some(v) = &opts.actor_id {
        qs.push(("actor_id".into(), v.clone()));
    }
    if let Some(v) = &opts.path_prefix {
        qs.push(("path_prefix".into(), v.clone()));
    }
    let qstr = qs
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect::<Vec<_>>()
        .join("&");
    let url = format!(
        "{}/api/admin/audit?{qstr}",
        opts.server_url.trim_end_matches('/'),
    );
    fetch_and_print(&url).await
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

/// Build an admin HTTP client that attaches
/// `Authorization: Bearer <token>` automatically when
/// `AGENTDB_ADMIN_TOKEN` is set in the environment. Every
/// admin subcommand routes through this so operators don't
/// have to thread `--token` through each call — a single
/// export handles the whole session.
fn admin_client() -> reqwest::Client {
    let mut builder = reqwest::Client::builder();
    if let Ok(token) = std::env::var("AGENTDB_ADMIN_TOKEN") {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")) {
            headers.insert(reqwest::header::AUTHORIZATION, v);
            builder = builder.default_headers(headers);
        }
    }
    builder
        .build()
        .expect("reqwest Client::builder().build() is infallible for our config")
}

/// Variant of [`admin_client`] that uses an explicit
/// override token when present, falling back to
/// `AGENTDB_ADMIN_TOKEN`. Used by the token subcommands
/// which accept `--token` directly.
fn admin_client_with_opt(explicit: Option<&str>) -> reqwest::Client {
    let token = explicit.map(str::to_string).or_else(|| std::env::var("AGENTDB_ADMIN_TOKEN").ok());
    let mut builder = reqwest::Client::builder();
    if let Some(t) = token {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("Bearer {t}")) {
            headers.insert(reqwest::header::AUTHORIZATION, v);
            builder = builder.default_headers(headers);
        }
    }
    builder
        .build()
        .expect("reqwest Client::builder().build() is infallible for our config")
}

async fn fetch_and_print(url: &str) -> Result<()> {
    let resp = admin_client()
        .get(url)
        .send()
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
