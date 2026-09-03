//! Import coding-agent session logs (Claude Code, Codex) into a **running**
//! AgentDB server over HTTP.
//!
//! One JSONL file is one session; the repository path (the log's `cwd`) is the
//! workspace; the session UUID is the session alias; each line becomes an event
//! whose kind is the line's `type`. Events are sent in bulk to the `/events/batch`
//! endpoint. A local state file remembers each file's resolved ids and import
//! offset, so re-runs — and `--watch` — send only new lines.
//!
//! Shared parsing lives in [`agentdb_import`]. For a one-shot bulk load into the
//! local store with the server stopped, `agentdb import` is faster (in-process,
//! no HTTP).

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

use agentdb_import::{
    classify_line, collect_jsonl, derive_identity, read_prefix_lines, resolve_dir, LineClass, State,
};
pub(crate) use agentdb_import::Source;

use crate::{Api, ImportArgs};

const STATE_FILE: &str = "agentdbctl-import-state.json";
const MAX_BATCH: usize = 512;
/// Keep a request comfortably under the server's HTTP body limit.
const MAX_BATCH_BYTES: usize = 1_000_000;

#[derive(Default)]
struct Summary {
    files_seen: usize,
    sessions_touched: usize,
    events_appended: usize,
    errors: usize,
}

impl Summary {
    fn to_value(&self, dir: &Path) -> Value {
        json!({
            "directory": dir.display().to_string(),
            "files_seen": self.files_seen,
            "sessions_touched": self.sessions_touched,
            "events_appended": self.events_appended,
            "errors": self.errors,
        })
    }
}

pub(crate) async fn run(api: &Api, source: Source, args: ImportArgs) -> Result<Value> {
    let dir = resolve_dir(source, args.dir.clone())?;
    let mut state = State::load(STATE_FILE)?;
    let summary = import_once(api, source, &dir, &mut state).await?;

    if !args.watch {
        return Ok(summary.to_value(&dir));
    }

    eprintln!(
        "imported {} event(s); watching {} every {}ms (Ctrl-C to stop)",
        summary.events_appended,
        dir.display(),
        args.poll_ms
    );
    loop {
        tokio::time::sleep(Duration::from_millis(args.poll_ms)).await;
        match import_once(api, source, &dir, &mut state).await {
            Ok(s) if s.events_appended > 0 => {
                eprintln!(
                    "imported {} new event(s) across {} session(s)",
                    s.events_appended, s.sessions_touched
                );
            }
            Ok(_) => {}
            Err(error) => eprintln!("import pass failed (will retry): {error:#}"),
        }
    }
}

async fn import_once(
    api: &Api,
    source: Source,
    dir: &Path,
    state: &mut State,
) -> Result<Summary> {
    let mut summary = Summary::default();
    let mut files = Vec::new();
    collect_jsonl(dir, &mut files)?;
    files.sort();
    for file in files {
        summary.files_seen += 1;
        match import_file(api, source, &file, state).await {
            Ok(appended) if appended > 0 => {
                summary.sessions_touched += 1;
                summary.events_appended += appended;
                // Persist the offset after each file so a crash re-imports at
                // most one in-flight file (the batch path carries no per-event
                // idempotency key).
                state.save()?;
            }
            Ok(_) => {}
            Err(error) => {
                summary.errors += 1;
                eprintln!("skip {}: {error:#}", file.display());
            }
        }
    }
    state.save()?;
    Ok(summary)
}

async fn import_file(api: &Api, source: Source, file: &Path, state: &mut State) -> Result<usize> {
    let key = file.to_string_lossy().into_owned();
    let start = state.next_line(&key);

    // Resolve (and cache) the tenant/agent/session ids. Identity comes from a
    // bounded prefix so we never load a whole (possibly multi-GB) file.
    let (tenant_id, agent_id, session_id) = if let Some(entry) = state.entry(&key) {
        (entry.tenant_id, entry.agent_id, entry.session_id)
    } else {
        let prefix = read_prefix_lines(file, 64)?;
        let prefix_refs: Vec<&str> = prefix.iter().map(String::as_str).collect();
        let (session_alias, workspace) = derive_identity(source, file, &prefix_refs)?;
        let tenant_id = ensure_workspace(api, &workspace).await?;
        let agent_id = ensure_agent(api, &tenant_id, source.agent_alias()).await?;
        let session_id = ensure_session(api, &tenant_id, &agent_id, &session_alias).await?;
        let entry = state.set_entry(&key, &tenant_id, &agent_id, &session_id);
        (entry.tenant_id, entry.agent_id, entry.session_id)
    };

    let path = format!("/api/tenants/{tenant_id}/sessions/{session_id}/events/batch");

    // Stream the file line by line so memory stays bounded to one line plus one
    // batch, and batch events to one HTTP request instead of one per event.
    let reader = BufReader::new(
        std::fs::File::open(file).with_context(|| format!("open {}", file.display()))?,
    );
    let mut batch: Vec<Value> = Vec::new();
    let mut batch_bytes = 0usize;
    let mut appended = 0usize;
    let mut skipped_oversized = 0usize;
    let mut last_idx = start;
    for (idx, line) in reader.lines().enumerate() {
        if idx < start {
            continue; // already imported on a previous run
        }
        let line = line.with_context(|| format!("read line {idx} of {}", file.display()))?;
        last_idx = idx;
        match classify_line(&line) {
            LineClass::Skip => {}
            // The HTTP importer skips oversized events; use `agentdb import
            // --large-events artifact` (offline, in-process) to externalize them.
            LineClass::Oversized(_) => skipped_oversized += 1,
            LineClass::Event(parsed) => {
                batch.push(json!({ "kind": parsed.kind, "payload": parsed.payload }));
                batch_bytes += parsed.payload_bytes;
                if batch.len() >= MAX_BATCH || batch_bytes >= MAX_BATCH_BYTES {
                    appended += flush(api, &path, &agent_id, &mut batch, file, last_idx).await?;
                    batch_bytes = 0;
                    state.set_next_line(&key, idx + 1);
                }
            }
        }
    }
    if !batch.is_empty() {
        appended += flush(api, &path, &agent_id, &mut batch, file, last_idx).await?;
    }
    // Advance past any trailing blank/skipped lines so they aren't re-read.
    state.set_next_line(&key, last_idx + 1);
    if skipped_oversized > 0 {
        eprintln!(
            "note: skipped {skipped_oversized} oversized event(s) in {}",
            file.display()
        );
    }
    Ok(appended)
}

async fn flush(
    api: &Api,
    path: &str,
    agent_id: &str,
    batch: &mut Vec<Value>,
    file: &Path,
    last_idx: usize,
) -> Result<usize> {
    let n = batch.len();
    let body = json!({ "agent_id": agent_id, "events": std::mem::take(batch) });
    api.post(path, body)
        .await
        .with_context(|| format!("append batch ending at line {last_idx} of {}", file.display()))?;
    Ok(n)
}

async fn ensure_workspace(api: &Api, external_key: &str) -> Result<String> {
    let list = api.get("/api/tenants").await?;
    if let Some(found) = list["tenants"]
        .as_array()
        .and_then(|a| a.iter().find(|t| t["external_key"] == json!(external_key)))
    {
        return id_field(found, "tenant_id");
    }
    let created = api
        .post("/api/tenants", json!({ "external_key": external_key, "policy": {} }))
        .await?;
    id_field(&created, "tenant_id")
}

async fn ensure_agent(api: &Api, tenant_id: &str, alias: &str) -> Result<String> {
    let list = api.get(&format!("/api/tenants/{tenant_id}/agents")).await?;
    if let Some(found) = list["agents"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["alias"] == json!(alias)))
    {
        return id_field(found, "agent_id");
    }
    let created = api
        .post(
            &format!("/api/tenants/{tenant_id}/agents"),
            json!({ "alias": alias, "config": {} }),
        )
        .await?;
    id_field(&created, "agent_id")
}

async fn ensure_session(api: &Api, tenant_id: &str, agent_id: &str, alias: &str) -> Result<String> {
    let list = api.get(&format!("/api/tenants/{tenant_id}/sessions")).await?;
    if let Some(found) = list["sessions"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["alias"] == json!(alias)))
    {
        return id_field(found, "session_id");
    }
    let created = api
        .post(
            &format!("/api/tenants/{tenant_id}/agents/{agent_id}/sessions"),
            json!({ "alias": alias }),
        )
        .await?;
    id_field(&created, "session_id")
}

fn id_field(value: &Value, field: &str) -> Result<String> {
    value[field]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("response missing {field}: {value}"))
}
