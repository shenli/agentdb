//! Offline, in-process bulk import of coding-agent session logs directly into
//! the local store.
//!
//! The `agentdb` binary owns the RocksDB store, so this opens it in-process and
//! writes through the runtime — no HTTP, no round-trips, no request-body limits.
//! It is the right tool for a one-shot bulk load of local files. Because
//! RocksDB is single-writer, the server must be stopped (the store open fails
//! fast with a clear message otherwise); use `agentdbctl import` against a
//! running server for the online / `--watch` case.

use std::path::PathBuf;
use std::sync::Arc;

use std::io::{BufRead, BufReader};

use anyhow::{anyhow, Context, Result};
use serde_json::json;

use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::{Id, IdKind, NewAgent, NewSession, NewTenant};
use agentdb_import::{
    classify_line, collect_jsonl, derive_identity, read_prefix_lines, resolve_dir, LineClass,
    Source, State,
};

/// Events per `append_batch` write batch (no HTTP body limit applies here).
const APPEND_CHUNK: usize = 512;
/// Byte ceiling on one write batch, so a run of large-ish events still commits
/// in bounded chunks.
const APPEND_CHUNK_BYTES: usize = 8 * 1024 * 1024;

const STATE_FILE: &str = "agentdb-import-state.json";

pub async fn run(runtime: Arc<Runtime>, source: Source, dir: Option<PathBuf>) -> Result<()> {
    let dir = resolve_dir(source, dir)?;
    let mut state = State::load(STATE_FILE)?;

    let mut files = Vec::new();
    collect_jsonl(&dir, &mut files)?;
    files.sort();

    let mut sessions = 0usize;
    let mut events = 0usize;
    let mut errors = 0usize;
    for file in &files {
        match import_file(&runtime, source, file, &mut state).await {
            Ok(n) if n > 0 => {
                sessions += 1;
                events += n;
                // Persist the offset after each file so a crash re-imports at
                // most one in-flight file (the batch path carries no per-event
                // idempotency key).
                state.save()?;
            }
            Ok(_) => {}
            Err(error) => {
                errors += 1;
                eprintln!("skip {}: {error:#}", file.display());
            }
        }
    }
    state.save()?;

    println!(
        "imported {events} event(s) across {sessions} session(s) from {} ({} file(s)){}",
        dir.display(),
        files.len(),
        if errors > 0 {
            format!("; {errors} file(s) had errors")
        } else {
            String::new()
        }
    );
    Ok(())
}

async fn import_file(
    runtime: &Arc<Runtime>,
    source: Source,
    file: &std::path::Path,
    state: &mut State,
) -> Result<usize> {
    let key = file.to_string_lossy().into_owned();
    let start = state.next_line(&key);

    // Resolve (and cache) the tenant/agent/session ids. Identity comes from a
    // bounded prefix (session_meta / first cwd) so we never load the whole file
    // — sessions can be tens of GB of embedded tool output.
    let (tenant_id, agent_id, session_id) = if let Some(entry) = state.entry(&key) {
        (
            Id::parse_kind(&entry.tenant_id, IdKind::Tenant).context("stored tenant id")?,
            Id::parse_kind(&entry.agent_id, IdKind::Agent).context("stored agent id")?,
            Id::parse_kind(&entry.session_id, IdKind::Session).context("stored session id")?,
        )
    } else {
        let prefix = read_prefix_lines(file, 64)?;
        let prefix_refs: Vec<&str> = prefix.iter().map(String::as_str).collect();
        let (session_alias, workspace) = derive_identity(source, file, &prefix_refs)?;
        let ids = ensure_ids(runtime, source, &workspace, &session_alias).await?;
        state.set_entry(&key, ids.0.as_str(), ids.1.as_str(), ids.2.as_str());
        ids
    };

    let make_req = |kind: String, payload: serde_json::Value| AppendRequest {
        tenant_id: tenant_id.clone(),
        agent_id: agent_id.clone(),
        session_id: session_id.clone(),
        kind,
        payload,
        artifact_refs: Vec::new(),
        source_refs: Vec::new(),
        credential_refs: Vec::new(),
        idempotency_key: None,
    };

    // Stream the file line by line so memory stays bounded to one line plus one
    // batch, regardless of file size.
    let reader = BufReader::new(std::fs::File::open(file).with_context(|| {
        format!("open {}", file.display())
    })?);
    let mut batch: Vec<AppendRequest> = Vec::new();
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
            LineClass::Oversized => skipped_oversized += 1,
            LineClass::Event {
                kind,
                payload,
                payload_bytes,
            } => {
                batch.push(make_req(kind, payload));
                batch_bytes += payload_bytes;
                if batch.len() >= APPEND_CHUNK || batch_bytes >= APPEND_CHUNK_BYTES {
                    let n = batch.len();
                    runtime
                        .append_batch(std::mem::take(&mut batch))
                        .await
                        .map_err(|error| anyhow!(error.message))
                        .with_context(|| format!("append batch in {}", file.display()))?;
                    appended += n;
                    batch_bytes = 0;
                    state.set_next_line(&key, idx + 1);
                }
            }
        }
    }
    if !batch.is_empty() {
        let n = batch.len();
        runtime
            .append_batch(batch)
            .await
            .map_err(|error| anyhow!(error.message))
            .with_context(|| format!("append final batch in {}", file.display()))?;
        appended += n;
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

/// Resolve-or-create the `(tenant, agent, session)` for a file, mirroring the
/// server's `open_session` idempotency (aliases are the stable keys).
async fn ensure_ids(
    runtime: &Arc<Runtime>,
    source: Source,
    workspace: &str,
    session_alias: &str,
) -> Result<(Id, Id, Id)> {
    let map = |error: agentdb_core::Error| anyhow!(error.message);

    let tenant = match runtime
        .resolve_tenant_by_external_key(workspace)
        .await
        .map_err(map)?
    {
        Some(tenant) => tenant,
        None => runtime
            .create_tenant(NewTenant {
                external_key: workspace.to_string(),
                policy: json!({}),
            })
            .await
            .map_err(map)?,
    };

    let agent_alias = source.agent_alias();
    let agent = match runtime
        .resolve_agent_by_alias(&tenant.tenant_id, agent_alias)
        .await
        .map_err(map)?
    {
        Some(agent) => agent,
        None => runtime
            .create_agent(NewAgent {
                tenant_id: tenant.tenant_id.clone(),
                alias: Some(agent_alias.to_string()),
                config: json!({}),
            })
            .await
            .map_err(map)?,
    };

    let session = match runtime
        .resolve_session_by_alias(&tenant.tenant_id, session_alias)
        .await
        .map_err(map)?
    {
        Some(session) => session,
        None => runtime
            .create_session(NewSession {
                tenant_id: tenant.tenant_id.clone(),
                agent_id: agent.agent_id.clone(),
                alias: Some(session_alias.to_string()),
            })
            .await
            .map_err(map)?,
    };

    Ok((tenant.tenant_id, agent.agent_id, session.session_id))
}
