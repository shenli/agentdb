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

use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::{Id, IdKind, NewAgent, NewArtifact, NewSession, NewTenant};
use agentdb_core::event::ArtifactRef;
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

/// What to do with events whose payload exceeds the inline limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LargeEvents {
    /// Drop them (they belong in an artifact, not an inline event).
    Skip,
    /// Store the payload as a content-addressed artifact and append a small
    /// stub event that references it. Identical payloads dedup to one blob.
    Artifact,
}

#[derive(Default)]
struct Totals {
    sessions: usize,
    events: usize,
    externalized: usize,
    externalized_bytes: u64,
    skipped_oversized: usize,
    errors: usize,
}

pub async fn run(
    runtime: Arc<Runtime>,
    source: Source,
    dir: Option<PathBuf>,
    large: LargeEvents,
) -> Result<()> {
    let dir = resolve_dir(source, dir)?;
    let mut state = State::load(STATE_FILE)?;

    let mut files = Vec::new();
    collect_jsonl(&dir, &mut files)?;
    files.sort();

    let mut totals = Totals::default();
    for file in &files {
        match import_file(&runtime, source, file, &mut state, large, &mut totals).await {
            Ok(n) if n > 0 => {
                totals.sessions += 1;
                totals.events += n;
                // Persist the offset after each file so a crash re-imports at
                // most one in-flight file (the batch path carries no per-event
                // idempotency key).
                state.save()?;
            }
            Ok(_) => {}
            Err(error) => {
                totals.errors += 1;
                eprintln!("skip {}: {error:#}", file.display());
            }
        }
    }
    state.save()?;

    println!(
        "imported {} event(s) across {} session(s) from {} ({} file(s))",
        totals.events,
        totals.sessions,
        dir.display(),
        files.len()
    );
    match large {
        LargeEvents::Artifact if totals.externalized > 0 => println!(
            "externalized {} oversized event(s) to artifacts ({:.1} MiB of content)",
            totals.externalized,
            totals.externalized_bytes as f64 / (1024.0 * 1024.0)
        ),
        LargeEvents::Skip if totals.skipped_oversized > 0 => println!(
            "skipped {} oversized event(s); re-run with `--large-events artifact` to keep them",
            totals.skipped_oversized
        ),
        _ => {}
    }
    if totals.errors > 0 {
        println!("{} file(s) had errors (see above)", totals.errors);
    }
    Ok(())
}

// The flush macro's final `batch_bytes = 0` reset is dead on the last call.
#[allow(unused_assignments)]
async fn import_file(
    runtime: &Arc<Runtime>,
    source: Source,
    file: &std::path::Path,
    state: &mut State,
    large: LargeEvents,
    totals: &mut Totals,
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
    let mut last_idx = start;
    // Flush the pending inline batch. Called before an externalized event so
    // event ORDER (and therefore seq order) follows line order across the
    // batched (inline) and single (artifact-ref) append paths.
    macro_rules! flush_batch {
        ($idx:expr) => {
            if !batch.is_empty() {
                let n = batch.len();
                runtime
                    .append_batch(std::mem::take(&mut batch))
                    .await
                    .map_err(|error| anyhow!(error.message))
                    .with_context(|| format!("append batch in {}", file.display()))?;
                appended += n;
                batch_bytes = 0;
                state.set_next_line(&key, $idx + 1);
            }
        };
    }
    for (idx, line) in reader.lines().enumerate() {
        if idx < start {
            continue; // already imported on a previous run
        }
        let line = line.with_context(|| format!("read line {idx} of {}", file.display()))?;
        last_idx = idx;
        match classify_line(&line) {
            LineClass::Skip => {}
            LineClass::Oversized(parsed) => match large {
                LargeEvents::Skip => totals.skipped_oversized += 1,
                LargeEvents::Artifact => {
                    // Preserve order: commit the inline events before this one,
                    // then append the externalized event (single append — the
                    // batch path does not carry artifact refs).
                    flush_batch!(idx);
                    let bytes = externalize_event(runtime, &tenant_id, &agent_id, &session_id, parsed)
                        .await
                        .with_context(|| format!("externalize line {idx} of {}", file.display()))?;
                    appended += 1;
                    totals.externalized += 1;
                    totals.externalized_bytes += bytes;
                    state.set_next_line(&key, idx + 1);
                }
            },
            LineClass::Event(parsed) => {
                batch.push(make_req(parsed.kind, parsed.payload));
                batch_bytes += parsed.payload_bytes;
                if batch.len() >= APPEND_CHUNK || batch_bytes >= APPEND_CHUNK_BYTES {
                    flush_batch!(idx);
                }
            }
        }
    }
    flush_batch!(last_idx);
    // Advance past any trailing blank/skipped lines so they aren't re-read.
    state.set_next_line(&key, last_idx + 1);
    Ok(appended)
}

/// Store an oversized event's payload as a content-addressed artifact (identical
/// payloads dedup to one blob via the sha256 idempotency key) and append a small
/// stub event that references it. Returns the stored byte count.
async fn externalize_event(
    runtime: &Arc<Runtime>,
    tenant_id: &Id,
    agent_id: &Id,
    session_id: &Id,
    parsed: agentdb_import::ParsedLine,
) -> Result<u64> {
    let bytes = serde_json::to_vec(&parsed.payload).unwrap_or_default();
    let sha = agentdb_storage::sha256_hex(&bytes);
    let artifact = runtime
        .create_artifact_with_idempotency(
            NewArtifact {
                tenant_id: tenant_id.clone(),
                content_type: Some("application/json".to_string()),
                expected_sha256: None,
            },
            bytes,
            // Content hash as the idempotency key = content-addressed dedup: a
            // payload repeated across a session stores exactly one blob.
            Some(format!("import-sha256-{sha}")),
        )
        .await
        .map_err(|error| anyhow!(error.message))?;

    let stub = serde_json::json!({
        "externalized": true,
        "type": parsed.kind.clone(),
        "artifact_id": artifact.artifact_id.as_str(),
        "bytes": artifact.bytes,
        "sha256": artifact.sha256,
    });
    let req = AppendRequest {
        tenant_id: tenant_id.clone(),
        agent_id: agent_id.clone(),
        session_id: session_id.clone(),
        kind: parsed.kind,
        payload: stub,
        artifact_refs: vec![ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            content_type: artifact.content_type.clone(),
            bytes: Some(artifact.bytes),
            sha256: Some(artifact.sha256.clone()),
        }],
        source_refs: Vec::new(),
        credential_refs: Vec::new(),
        idempotency_key: None,
    };
    runtime
        .append(req)
        .await
        .map_err(|error| anyhow!(error.message))?;
    Ok(artifact.bytes)
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

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_import::ParsedLine;

    async fn seed() -> (Arc<Runtime>, Id, Id, Id) {
        let rt = Runtime::in_memory();
        let t = rt
            .create_tenant(NewTenant {
                external_key: "ext".into(),
                policy: json!({}),
            })
            .await
            .unwrap();
        let a = rt
            .create_agent(NewAgent {
                tenant_id: t.tenant_id.clone(),
                alias: None,
                config: json!({}),
            })
            .await
            .unwrap();
        let s = rt
            .create_session(NewSession {
                tenant_id: t.tenant_id.clone(),
                agent_id: a.agent_id.clone(),
                alias: None,
            })
            .await
            .unwrap();
        (rt.clone(), t.tenant_id, a.agent_id, s.session_id)
    }

    #[tokio::test]
    async fn externalize_stores_a_ref_and_dedups_identical_payloads() {
        let (rt, tid, aid, sid) = seed().await;
        let payload = json!({ "type": "response_item", "big": "y".repeat(70_000) });
        let parsed = || ParsedLine {
            kind: "response_item".into(),
            payload: payload.clone(),
            payload_bytes: 70_000,
        };

        let b1 = externalize_event(&rt, &tid, &aid, &sid, parsed()).await.unwrap();
        let b2 = externalize_event(&rt, &tid, &aid, &sid, parsed()).await.unwrap();
        assert_eq!(b1, b2);

        let events = rt
            .fetch_from_seq(FetchRequest {
                tenant_id: tid.clone(),
                session_id: sid.clone(),
                from_seq: 0,
                limit: 100,
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 2, "two externalized events appended");
        // The stub payload is small and marks the event.
        assert_eq!(events[0].payload["externalized"], true);
        assert_eq!(events[0].payload["type"], "response_item");
        // Both events reference the SAME artifact — identical payloads dedup.
        let a0 = events[0].artifact_refs[0].artifact_id.clone();
        let a1 = events[1].artifact_refs[0].artifact_id.clone();
        assert_eq!(a0.as_str(), a1.as_str(), "identical payloads dedup to one blob");

        // The blob round-trips to the original payload bytes.
        let blob = rt.get_artifact_blob(&tid, &a0).await.unwrap();
        assert_eq!(blob, serde_json::to_vec(&payload).unwrap());
    }
}
