//! KV-backed persistence helper for V2 runtime objects.
//!
//! This layer is intentionally thin: it knows how to encode/decode V2 objects
//! against an `agentdb_storage::KvStore` and nothing more. All sequencing,
//! locking, and business logic stays in `runtime.rs`.
//!
//! Encoding is JSON via `serde`. The key encoders are imported from Task 3
//! (`agentdb_storage::keys`).
//!
//! ## Atomicity contract
//!
//! Multi-key writes use `KvStore::write_batch` so each logical mutation becomes
//! visible atomically. Append commits bundle the event row, the updated session
//! row, and the idempotency record (if any) in one atomic boundary across
//! crashes.
//!
//! ## Hydration
//!
//! `scan_all_tenants`/`scan_all_agents`/`scan_all_sessions` collect all live
//! records under their respective `record/{tenant,agent,session}/` prefixes.
//! Events are NOT preloaded; `scan_events` is invoked lazily by
//! `Runtime::fetch_from_seq` and `append_and_fetch` on-demand.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agentdb_storage::content_crypto::{decode_keyed_envelope, encode_keyed_envelope};
use agentdb_storage::keys::{
    agent_alias_key, agent_record_key, agent_skill_binding_key, artifact_by_sha256_key,
    artifact_record_key, event_by_id_key, event_key, record_by_namespace_key, record_commit_key,
    record_index_outbox_key, record_key, session_alias_key, session_record_key,
    session_segment_key, skill_record_key, skill_unique_key, tenant_alias_key, tenant_record_key,
};
use agentdb_storage::Error as StorageError;
use agentdb_storage::{
    KeyResolution, KvStore, MetadataCryptoAdapter, MetadataCryptoContext, MetadataObjectKind,
    Result as KvResult, WriteOp,
};

use crate::agent::Agent;
use crate::artifact::Artifact;
use crate::event::EventEnvelope;
use crate::idempotency::IdempotencyRecord;
use crate::record::SearchableRecord;
use crate::runtime::{ExportConsumer, ExportLedgerItem, RecordEdge, TenantMetaEntry};
use crate::segment::SessionSegment;
use crate::session::Session;
use crate::skill::{AgentSkillBinding, Skill};
use crate::tenant::Tenant;

type SkillUniqueRecord = ((String, String, String), String);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AppendLogBatch {
    events: Vec<EventEnvelope>,
    session: Option<Session>,
}

#[derive(serde::Serialize)]
struct AppendLogBatchRefs<'a> {
    events: &'a [&'a EventEnvelope],
    session: Option<&'a Session>,
}

fn append_log_key(tenant_id: &str, session_id: &str, first_seq: u64) -> String {
    format!("append_log/{tenant_id}/{session_id}/{first_seq:020}")
}

fn append_log_session_prefix(tenant_id: &str, session_id: &str) -> String {
    format!("append_log/{tenant_id}/{session_id}/")
}

fn append_log_tenant_prefix(tenant_id: &str) -> String {
    format!("append_log/{tenant_id}/")
}

fn export_consumer_key(tenant_id: &str, consumer_name: &str) -> String {
    format!("record/export_consumer/{tenant_id}/{consumer_name}")
}

static EXPORT_LEDGER_PENDING_SEQ: AtomicU64 = AtomicU64::new(1);

fn next_export_ledger_pending_token(object_id: &str) -> String {
    format!(
        "{:020}-{object_id}",
        EXPORT_LEDGER_PENDING_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Export rows are built before they reach the serialized KV writer, but their
/// public cursor must reflect durable write order rather than wall-clock time.
/// `~pending` keys are therefore private write templates. The writer replaces
/// the token with `v2/<tenant-local durable sequence>` immediately before the
/// atomic batch is committed.
fn export_ledger_event_key(event: &EventEnvelope, token: &str) -> String {
    format!(
        "export_ledger/{}/~pending/{token}/event/{}/{}",
        event.tenant_id, event.session_id, event.event_id
    )
}

fn export_ledger_object_key(item: &ExportLedgerItem, token: &str) -> String {
    format!(
        "export_ledger/{}/~pending/{token}/object/{}/{}",
        item.tenant_id, item.object_type, item.object_id
    )
}

fn export_ledger_item_cursor_from_key(key: &[u8]) -> String {
    let raw = std::str::from_utf8(key).expect("export ledger key is utf8");
    format!("ledger:{}", raw.trim_start_matches("export_ledger/"))
}

fn export_ledger_key_from_item_cursor(tenant_id: &str, cursor: &str) -> Option<String> {
    let raw = cursor.strip_prefix("ledger:")?;
    if raw != tenant_id && !raw.starts_with(&format!("{tenant_id}/")) {
        return None;
    }
    Some(format!("export_ledger/{raw}"))
}

fn export_ledger_session_index_key(event: &EventEnvelope, token: &str) -> String {
    format!(
        "export_ledger_by_session/{}/{}/~pending/{token}",
        event.tenant_id, event.session_id
    )
}

fn export_ledger_agent_index_key(event: &EventEnvelope, token: &str) -> String {
    format!(
        "export_ledger_by_agent/{}/{}/~pending/{token}",
        event.tenant_id, event.agent_id
    )
}

fn export_ledger_tenant_prefix(tenant_id: &str) -> String {
    format!("export_ledger/{tenant_id}/")
}

fn export_ledger_session_index_prefix(tenant_id: &str, session_id: &str) -> String {
    format!("export_ledger_by_session/{tenant_id}/{session_id}/")
}

fn export_ledger_agent_index_prefix(tenant_id: &str, agent_id: &str) -> String {
    format!("export_ledger_by_agent/{tenant_id}/{agent_id}/")
}

fn export_ledger_high_watermark_key(tenant_id: &str) -> String {
    format!("export_ledger_high_watermark/{tenant_id}")
}

fn export_ledger_sequence_key(tenant_id: &str) -> String {
    format!("export_ledger_sequence/{tenant_id}")
}

fn record_edge_key(tenant_id: &str, edge_id: &str) -> String {
    format!("record/searchable_edge/{tenant_id}/{edge_id}")
}

fn tenant_meta_key(tenant_id: &str, key: &str) -> String {
    format!("record/tenant_meta/{tenant_id}/{key}")
}

fn export_ledger_cursor_at_or_before(cursor: &str, from_cursor: Option<&str>) -> bool {
    from_cursor.is_some_and(|from| from.starts_with("ledger:") && cursor <= from)
}

fn export_ledger_object_id_from_key(key: &[u8]) -> Option<&str> {
    let raw = std::str::from_utf8(key).ok()?;
    let parts = raw.split('/').collect::<Vec<_>>();
    if parts.len() == 7 && parts[2] == "v2" && matches!(parts[4], "event" | "object") {
        return Some(parts[6]);
    }
    if parts.len() == 7 && parts[5] == "event" {
        return Some(parts[6]);
    }
    if parts.len() == 6 {
        return Some(parts[5]);
    }
    None
}

fn export_ledger_tenant_from_key(key: &[u8]) -> Option<&str> {
    let raw = std::str::from_utf8(key).ok()?;
    let mut parts = raw.split('/');
    let _namespace = parts.next()?;
    parts.next()
}

#[derive(Default)]
struct ExportLedgerSequencer {
    last_by_tenant: Mutex<BTreeMap<String, u64>>,
}

impl ExportLedgerSequencer {
    fn write(&self, kv: &dyn KvStore, ops: Vec<WriteOp>) -> KvResult<()> {
        let mut last_by_tenant = self
            .last_by_tenant
            .lock()
            .expect("export ledger sequencer lock poisoned");
        let (ops, committed_sequences) = finalize_export_ledger_ops(kv, &last_by_tenant, ops)?;
        kv.write_batch(ops)?;
        for (tenant_id, sequence) in committed_sequences {
            let cached = last_by_tenant.entry(tenant_id).or_default();
            *cached = (*cached).max(sequence);
        }
        Ok(())
    }
}

fn finalize_export_ledger_ops(
    kv: &dyn KvStore,
    last_by_tenant: &BTreeMap<String, u64>,
    mut ops: Vec<WriteOp>,
) -> KvResult<(Vec<WriteOp>, BTreeMap<String, u64>)> {
    let mut assignments = BTreeMap::<String, u64>::new();
    let mut highest_by_tenant = BTreeMap::<String, u64>::new();
    let mut current_by_tenant = BTreeMap::<String, u64>::new();

    for op in &ops {
        let WriteOp::Put { key, .. } = op else {
            continue;
        };
        let Some((tenant_id, token)) = pending_export_primary_identity(key) else {
            continue;
        };
        if assignments.contains_key(token) {
            continue;
        }
        let current = match current_by_tenant.get(tenant_id).copied() {
            Some(current) => current,
            None => {
                let stored = kv
                    .get(export_ledger_sequence_key(tenant_id).as_bytes())?
                    .map(|raw| {
                        raw.as_slice()
                            .try_into()
                            .map(u64::from_be_bytes)
                            .map_err(|_| {
                                StorageError::Other(format!(
                                    "export ledger sequence for tenant {tenant_id} is invalid"
                                ))
                            })
                    })
                    .transpose()?
                    .unwrap_or(0);
                // A tablet snapshot, replicated write, or former leader can
                // advance the durable sequence without touching this local
                // process cache. Reconcile on every pending batch so a node
                // that becomes leader again never reuses an old sequence.
                stored.max(last_by_tenant.get(tenant_id).copied().unwrap_or(0))
            }
        };
        let next = current.checked_add(1).ok_or_else(|| {
            StorageError::Other(format!(
                "export ledger sequence exhausted for tenant {tenant_id}"
            ))
        })?;
        current_by_tenant.insert(tenant_id.to_string(), next);
        assignments.insert(token.to_string(), next);
        highest_by_tenant.insert(tenant_id.to_string(), next);
    }

    if assignments.is_empty() {
        let committed_sequences = export_ledger_sequence_puts(&ops)?;
        return Ok((ops, committed_sequences));
    }

    for op in &mut ops {
        match op {
            WriteOp::Put { key, value } => {
                *key = rewrite_pending_export_bytes(key, &assignments);
                if value.starts_with(b"export_ledger/") || value.starts_with(b"ledger:") {
                    *value = rewrite_pending_export_bytes(value, &assignments);
                }
            }
            WriteOp::Delete { key } => {
                *key = rewrite_pending_export_bytes(key, &assignments);
            }
        }
    }
    for (tenant_id, sequence) in highest_by_tenant {
        ops.push(WriteOp::Put {
            key: export_ledger_sequence_key(&tenant_id).into_bytes(),
            value: sequence.to_be_bytes().to_vec(),
        });
    }
    let committed_sequences = export_ledger_sequence_puts(&ops)?;
    Ok((ops, committed_sequences))
}

fn export_ledger_sequence_puts(ops: &[WriteOp]) -> KvResult<BTreeMap<String, u64>> {
    let mut sequences = BTreeMap::<String, u64>::new();
    for op in ops {
        let WriteOp::Put { key, value } = op else {
            continue;
        };
        let Some(tenant_id) = key.strip_prefix(b"export_ledger_sequence/") else {
            continue;
        };
        let tenant_id = std::str::from_utf8(tenant_id)
            .map_err(|_| StorageError::Other("export ledger sequence tenant is not utf8".into()))?;
        let sequence = value
            .as_slice()
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| {
                StorageError::Other(format!(
                    "export ledger sequence for tenant {tenant_id} is invalid"
                ))
            })?;
        sequences
            .entry(tenant_id.to_string())
            .and_modify(|current| *current = (*current).max(sequence))
            .or_insert(sequence);
    }
    Ok(sequences)
}

fn pending_export_primary_identity(key: &[u8]) -> Option<(&str, &str)> {
    let raw = std::str::from_utf8(key).ok()?;
    let mut parts = raw.split('/');
    if parts.next()? != "export_ledger" {
        return None;
    }
    let tenant_id = parts.next()?;
    if parts.next()? != "~pending" {
        return None;
    }
    Some((tenant_id, parts.next()?))
}

fn rewrite_pending_export_bytes(bytes: &[u8], assignments: &BTreeMap<String, u64>) -> Vec<u8> {
    let Ok(mut text) = std::str::from_utf8(bytes).map(str::to_string) else {
        return bytes.to_vec();
    };
    for (token, sequence) in assignments {
        text = text.replace(
            &format!("/~pending/{token}"),
            &format!("/v2/{sequence:020}"),
        );
    }
    text.into_bytes()
}

fn export_item_from_event(event: &EventEnvelope) -> ExportLedgerItem {
    ExportLedgerItem {
        op: "insert".into(),
        object_type: "event".into(),
        tenant_id: event.tenant_id.clone(),
        agent_id: Some(event.agent_id.clone()),
        session_id: Some(event.session_id.clone()),
        object_id: event.event_id.as_str().to_string(),
        seq: Some(event.seq),
        created_at_ms: event.created_at_ms,
        data: serde_json::to_value(event).expect("event serializes"),
        tombstone: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn export_item_from_value(
    object_type: &str,
    tenant_id: crate::Id,
    agent_id: Option<crate::Id>,
    session_id: Option<crate::Id>,
    object_id: String,
    created_at_ms: u64,
    deleted_at_ms: Option<u64>,
    data: serde_json::Value,
) -> ExportLedgerItem {
    let tombstone = deleted_at_ms.map(|deleted_at_ms| {
        serde_json::json!({
            "deleted_at_ms": deleted_at_ms,
            "reason": "soft_delete"
        })
    });
    ExportLedgerItem {
        op: if tombstone.is_some() {
            "delete".into()
        } else {
            "upsert".into()
        },
        object_type: object_type.into(),
        tenant_id,
        agent_id,
        session_id,
        object_id,
        seq: None,
        created_at_ms: deleted_at_ms.unwrap_or(created_at_ms),
        data,
        tombstone,
    }
}

fn export_fork_edge_item(
    child: &Session,
    parent_session_id: crate::Id,
    fork_point_seq: u64,
) -> ExportLedgerItem {
    ExportLedgerItem {
        op: "insert".into(),
        object_type: "fork_edge".into(),
        tenant_id: child.tenant_id.clone(),
        agent_id: Some(child.agent_id.clone()),
        // Index by the parent session so a parent-scoped export consumer can
        // discover the child branch without subscribing to every child session.
        session_id: Some(parent_session_id.clone()),
        object_id: child.session_id.as_str().to_string(),
        seq: None,
        created_at_ms: child.created_at_ms,
        data: serde_json::json!({
            "parent_session_id": parent_session_id,
            "child_session_id": child.session_id,
            "fork_point_seq": fork_point_seq,
            "created_at_ms": child.created_at_ms,
        }),
        tombstone: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn export_tombstone_item(
    object_type: &str,
    tenant_id: crate::Id,
    agent_id: Option<crate::Id>,
    session_id: Option<crate::Id>,
    object_id: String,
    deleted_at_ms: u64,
    reason: &str,
    data: serde_json::Value,
) -> ExportLedgerItem {
    ExportLedgerItem {
        op: "delete".into(),
        object_type: object_type.into(),
        tenant_id,
        agent_id,
        session_id,
        object_id,
        seq: None,
        created_at_ms: deleted_at_ms,
        data,
        tombstone: Some(serde_json::json!({
            "deleted_at_ms": deleted_at_ms,
            "reason": reason
        })),
    }
}

fn export_tombstone_item_from_event(
    event: &EventEnvelope,
    deleted_at_ms: u64,
    reason: &str,
) -> ExportLedgerItem {
    ExportLedgerItem {
        op: "delete".into(),
        object_type: "event".into(),
        tenant_id: event.tenant_id.clone(),
        agent_id: Some(event.agent_id.clone()),
        session_id: Some(event.session_id.clone()),
        object_id: event.event_id.as_str().to_string(),
        seq: Some(event.seq),
        created_at_ms: deleted_at_ms,
        data: serde_json::to_value(event).expect("event serializes"),
        tombstone: Some(serde_json::json!({
            "deleted_at_ms": deleted_at_ms,
            "reason": reason
        })),
    }
}

static APPEND_PERSIST_PROFILE: OnceLock<Option<AppendPersistProfile>> = OnceLock::new();
static ADAPTIVE_WRITE_PROFILE: OnceLock<Option<AdaptiveWriteProfile>> = OnceLock::new();

struct AppendPersistProfile {
    interval_ms: u64,
    last_log_ms: AtomicU64,
    batches: AtomicU64,
    ops: AtomicU64,
    bytes: AtomicU64,
    build_nanos: AtomicU64,
    write_nanos: AtomicU64,
}

impl AppendPersistProfile {
    fn from_env() -> Option<Self> {
        let interval_ms = std::env::var("AGENTDB_APPEND_PERSIST_PROFILE_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)?;
        Some(Self {
            interval_ms,
            last_log_ms: AtomicU64::new(now_millis()),
            batches: AtomicU64::new(0),
            ops: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            build_nanos: AtomicU64::new(0),
            write_nanos: AtomicU64::new(0),
        })
    }

    fn record(&self, op_count: usize, bytes: usize, build_nanos: u128, write_nanos: u128) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.ops.fetch_add(op_count as u64, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.build_nanos.fetch_add(
            build_nanos.min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        self.write_nanos.fetch_add(
            write_nanos.min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );

        let now = now_millis();
        let last = self.last_log_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < self.interval_ms {
            return;
        }
        if self
            .last_log_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let batches = self.batches.swap(0, Ordering::Relaxed);
        if batches == 0 {
            return;
        }
        let ops = self.ops.swap(0, Ordering::Relaxed);
        let bytes = self.bytes.swap(0, Ordering::Relaxed);
        let build_nanos = self.build_nanos.swap(0, Ordering::Relaxed);
        let write_nanos = self.write_nanos.swap(0, Ordering::Relaxed);
        tracing::info!(
            batches,
            ops,
            bytes,
            avg_ops = ops as f64 / batches as f64,
            avg_bytes = bytes as f64 / batches as f64,
            avg_build_us = build_nanos as f64 / batches as f64 / 1_000.0,
            avg_write_us = write_nanos as f64 / batches as f64 / 1_000.0,
            "append persist profile"
        );
    }
}

fn append_persist_profile() -> Option<&'static AppendPersistProfile> {
    APPEND_PERSIST_PROFILE
        .get_or_init(AppendPersistProfile::from_env)
        .as_ref()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn write_ops_size(ops: &[WriteOp]) -> usize {
    ops.iter()
        .map(|op| match op {
            WriteOp::Put { key, value } => key.len() + value.len(),
            WriteOp::Delete { key } => key.len(),
        })
        .sum()
}

struct AdaptiveWriteBatcher {
    kv: Arc<dyn KvStore + Send + Sync>,
    export_ledger_sequencer: Arc<ExportLedgerSequencer>,
    // Option so Drop can release the sender (ending the writer loop) before
    // joining the writer thread; None only during teardown.
    tx: Option<mpsc::Sender<AdaptiveWriteRequest>>,
    queued: Arc<AtomicU64>,
    writer_busy: Arc<AtomicU64>,
    direct_fast_path: bool,
    writer: std::sync::Mutex<Option<thread::JoinHandle<()>>>,
}

impl Drop for AdaptiveWriteBatcher {
    fn drop(&mut self) {
        // Deterministic shutdown: close the channel so the writer loop
        // drains and exits, then join so the KV handle held by the thread
        // is released before the batcher's owner (and, in tests, the
        // backing temp directory) goes away.
        self.tx = None;
        if let Ok(mut writer) = self.writer.lock() {
            if let Some(handle) = writer.take() {
                let _ = handle.join();
            }
        }
    }
}

struct AdaptiveWriteRequest {
    ops: Vec<WriteOp>,
    enqueued_at: Instant,
    done: AdaptiveWriteCompletion,
    // Captured at submit time: the writer thread has no task-local tablet
    // context, so the flush re-enters this scope per group.
    tablet_id: Option<i64>,
}

enum AdaptiveWriteCompletion {
    Async(tokio::sync::oneshot::Sender<KvResult<()>>),
    Blocking(mpsc::Sender<KvResult<()>>),
}

impl AdaptiveWriteCompletion {
    fn send(self, result: &KvResult<()>) {
        let send_result = match result {
            Ok(()) => Ok(()),
            Err(err) => Err(StorageError::Other(format!(
                "adaptive write batch failed: {err}"
            ))),
        };
        match self {
            AdaptiveWriteCompletion::Async(done) => {
                let _ = done.send(send_result);
            }
            AdaptiveWriteCompletion::Blocking(done) => {
                let _ = done.send(send_result);
            }
        }
    }
}

struct AdaptiveWriteProfile {
    interval_ms: u64,
    last_log_ms: AtomicU64,
    direct_batches: AtomicU64,
    grouped_batches: AtomicU64,
    requests: AtomicU64,
    ops: AtomicU64,
    queue_nanos: AtomicU64,
    write_nanos: AtomicU64,
}

impl AdaptiveWriteProfile {
    fn from_env() -> Option<Self> {
        let interval_ms = std::env::var("AGENTDB_APPEND_PATH_PROFILE_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)?;
        Some(Self {
            interval_ms,
            last_log_ms: AtomicU64::new(now_millis()),
            direct_batches: AtomicU64::new(0),
            grouped_batches: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            ops: AtomicU64::new(0),
            queue_nanos: AtomicU64::new(0),
            write_nanos: AtomicU64::new(0),
        })
    }

    fn record_direct(&self, op_count: usize, write_nanos: u128) {
        self.direct_batches.fetch_add(1, Ordering::Relaxed);
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.ops.fetch_add(op_count as u64, Ordering::Relaxed);
        self.record_common(0, write_nanos);
    }

    fn record_grouped(
        &self,
        request_count: usize,
        op_count: usize,
        queue_nanos: u128,
        write_nanos: u128,
    ) {
        self.grouped_batches.fetch_add(1, Ordering::Relaxed);
        self.requests
            .fetch_add(request_count as u64, Ordering::Relaxed);
        self.ops.fetch_add(op_count as u64, Ordering::Relaxed);
        self.record_common(queue_nanos, write_nanos);
    }

    fn record_common(&self, queue_nanos: u128, write_nanos: u128) {
        self.queue_nanos.fetch_add(
            queue_nanos.min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        self.write_nanos.fetch_add(
            write_nanos.min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );

        let now = now_millis();
        let last = self.last_log_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < self.interval_ms {
            return;
        }
        if self
            .last_log_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let direct_batches = self.direct_batches.swap(0, Ordering::Relaxed);
        let grouped_batches = self.grouped_batches.swap(0, Ordering::Relaxed);
        let batches = direct_batches + grouped_batches;
        if batches == 0 {
            return;
        }
        let requests = self.requests.swap(0, Ordering::Relaxed);
        let ops = self.ops.swap(0, Ordering::Relaxed);
        let queue_nanos = self.queue_nanos.swap(0, Ordering::Relaxed);
        let write_nanos = self.write_nanos.swap(0, Ordering::Relaxed);
        tracing::info!(
            direct_batches,
            grouped_batches,
            requests,
            ops,
            avg_requests_per_batch = requests as f64 / batches as f64,
            avg_ops_per_batch = ops as f64 / batches as f64,
            avg_queue_us = queue_nanos as f64 / requests.max(1) as f64 / 1_000.0,
            avg_write_us = write_nanos as f64 / batches as f64 / 1_000.0,
            "adaptive write profile"
        );
    }
}

fn adaptive_write_profile() -> Option<&'static AdaptiveWriteProfile> {
    ADAPTIVE_WRITE_PROFILE
        .get_or_init(AdaptiveWriteProfile::from_env)
        .as_ref()
}

impl AdaptiveWriteBatcher {
    fn from_env(
        kv: Arc<dyn KvStore + Send + Sync>,
        export_ledger_sequencer: Arc<ExportLedgerSequencer>,
    ) -> Option<Arc<Self>> {
        if env_flag_disabled("AGENTDB_APPEND_GROUP_COMMIT") {
            return None;
        }
        let sync_writes = agentdb_storage::rocksdb_sync_writes_enabled();
        let max_ops = std::env::var("AGENTDB_APPEND_GROUP_COMMIT_MAX_OPS")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(4096);
        let wait_us = std::env::var("AGENTDB_APPEND_GROUP_COMMIT_US")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            // No unconditional wait: an idle append pays only its own fsync.
            .unwrap_or(0);
        let backlog_wait_us = std::env::var("AGENTDB_APPEND_GROUP_COMMIT_BACKLOG_US")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            // Coalesce only when a backlog already exists. In fsync mode a
            // 2ms window amortizes the WAL sync across every queued session;
            // in non-durable mode a tiny pause is enough to batch bursts.
            .unwrap_or(if sync_writes { 2_000 } else { 5 });
        Some(Arc::new(Self::new(
            kv,
            export_ledger_sequencer,
            max_ops,
            wait_us,
            backlog_wait_us,
            !sync_writes && wait_us == 0,
        )))
    }

    fn new(
        kv: Arc<dyn KvStore + Send + Sync>,
        export_ledger_sequencer: Arc<ExportLedgerSequencer>,
        max_ops: usize,
        wait_us: u64,
        backlog_wait_us: u64,
        direct_fast_path: bool,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<AdaptiveWriteRequest>();
        let queued = Arc::new(AtomicU64::new(0));
        let writer_busy = Arc::new(AtomicU64::new(0));
        let writer = thread::Builder::new()
            .name("agentdb-adaptive-write".into())
            .spawn({
                let kv = kv.clone();
                let export_ledger_sequencer = export_ledger_sequencer.clone();
                let queued = queued.clone();
                let writer_busy = writer_busy.clone();
                move || {
                    adaptive_write_loop(
                        kv,
                        export_ledger_sequencer,
                        rx,
                        queued,
                        writer_busy,
                        max_ops,
                        wait_us,
                        backlog_wait_us,
                    )
                }
            });
        Self {
            kv,
            export_ledger_sequencer,
            tx: Some(tx),
            queued,
            writer_busy,
            direct_fast_path,
            writer: std::sync::Mutex::new(writer.ok()),
        }
    }

    async fn submit(&self, ops: Vec<WriteOp>) -> KvResult<()> {
        if self.direct_fast_path
            && self.queued.load(Ordering::Relaxed) == 0
            && self
                .writer_busy
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            let op_count = ops.len();
            let write_started = Instant::now();
            let result = self.export_ledger_sequencer.write(self.kv.as_ref(), ops);
            let write_nanos = write_started.elapsed().as_nanos();
            self.writer_busy.store(0, Ordering::Release);
            if let Some(profile) = adaptive_write_profile() {
                profile.record_direct(op_count, write_nanos);
            }
            return result;
        }

        let (done, rx) = tokio::sync::oneshot::channel();
        self.queued.fetch_add(1, Ordering::Relaxed);
        self.tx
            .as_ref()
            .expect("adaptive write batcher used after teardown")
            .send(AdaptiveWriteRequest {
                ops,
                enqueued_at: Instant::now(),
                done: AdaptiveWriteCompletion::Async(done),
                tablet_id: crate::runtime::current_runtime_tablet_id(),
            })
            .map_err(|request| {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                drop(request);
                StorageError::Other("adaptive write worker stopped".into())
            })?;
        rx.await
            .map_err(|_| StorageError::Other("adaptive write worker dropped result".into()))?
    }

    fn submit_blocking(&self, ops: Vec<WriteOp>) -> KvResult<()> {
        if self.direct_fast_path
            && self.queued.load(Ordering::Relaxed) == 0
            && self
                .writer_busy
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            let op_count = ops.len();
            let write_started = Instant::now();
            let result = self.export_ledger_sequencer.write(self.kv.as_ref(), ops);
            let write_nanos = write_started.elapsed().as_nanos();
            self.writer_busy.store(0, Ordering::Release);
            if let Some(profile) = adaptive_write_profile() {
                profile.record_direct(op_count, write_nanos);
            }
            return result;
        }

        let (done, rx) = mpsc::channel();
        self.queued.fetch_add(1, Ordering::Relaxed);
        self.tx
            .as_ref()
            .expect("adaptive write batcher used after teardown")
            .send(AdaptiveWriteRequest {
                ops,
                enqueued_at: Instant::now(),
                done: AdaptiveWriteCompletion::Blocking(done),
                tablet_id: crate::runtime::current_runtime_tablet_id(),
            })
            .map_err(|request| {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                drop(request);
                StorageError::Other("adaptive write worker stopped".into())
            })?;
        rx.recv()
            .map_err(|_| StorageError::Other("adaptive write worker dropped result".into()))?
    }
}

fn env_flag_disabled(name: &str) -> bool {
    std::env::var(name)
        .map(|raw| {
            let raw = raw.trim().to_ascii_lowercase();
            matches!(raw.as_str(), "0" | "false" | "off" | "disabled")
        })
        .unwrap_or(false)
}

#[allow(clippy::too_many_arguments)]
fn adaptive_write_loop(
    kv: Arc<dyn KvStore + Send + Sync>,
    export_ledger_sequencer: Arc<ExportLedgerSequencer>,
    rx: mpsc::Receiver<AdaptiveWriteRequest>,
    queued: Arc<AtomicU64>,
    writer_busy: Arc<AtomicU64>,
    max_ops: usize,
    wait_us: u64,
    backlog_wait_us: u64,
) {
    while let Ok(first) = rx.recv() {
        let mut requests = vec![first];
        queued.fetch_sub(1, Ordering::Relaxed);
        let mut op_count = requests[0].ops.len();
        if wait_us > 0 {
            thread::sleep(Duration::from_micros(wait_us));
        } else if backlog_wait_us > 0 && queued.load(Ordering::Relaxed) > 0 {
            thread::sleep(Duration::from_micros(backlog_wait_us));
        }

        while op_count < max_ops {
            match rx.try_recv() {
                Ok(request) => {
                    queued.fetch_sub(1, Ordering::Relaxed);
                    op_count += request.ops.len();
                    requests.push(request);
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        let request_count = requests.len();

        while writer_busy
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            thread::yield_now();
        }
        let queue_nanos = requests
            .iter()
            .map(|request| request.enqueued_at.elapsed().as_nanos())
            .sum::<u128>();

        // One flush per tablet scope: the KV handle may be a tablet router
        // that resolves the destination from the task-local tablet id, so
        // ops from different tablets must never share a write_batch.
        // BTreeMap keeps a deterministic flush order; arrival order is
        // preserved within each tablet.
        let mut groups: std::collections::BTreeMap<
            Option<i64>,
            (Vec<WriteOp>, Vec<AdaptiveWriteCompletion>),
        > = std::collections::BTreeMap::new();
        for request in requests {
            let group = groups.entry(request.tablet_id).or_default();
            group.0.extend(request.ops);
            group.1.push(request.done);
        }

        let write_started = Instant::now();
        for (tablet_id, (ops, completions)) in groups {
            let result = crate::runtime::with_runtime_tablet_scope_sync(tablet_id, || {
                export_ledger_sequencer.write(kv.as_ref(), ops)
            });
            for done in completions {
                done.send(&result);
            }
        }
        let write_nanos = write_started.elapsed().as_nanos();
        writer_busy.store(0, Ordering::Release);
        if let Some(profile) = adaptive_write_profile() {
            profile.record_grouped(request_count, op_count, queue_nanos, write_nanos);
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecordIndexMutationOp {
    Upsert,
    Delete,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct RecordIndexMutation {
    pub tenant_id: String,
    pub record_id: String,
    pub commit_seq: u64,
    pub op: RecordIndexMutationOp,
}

fn key_from_parts(key_id: &Option<String>, key_version: &Option<String>) -> Option<KeyResolution> {
    key_id.as_ref().map(|key_id| KeyResolution {
        key_id: key_id.clone(),
        key_version: key_version.clone(),
    })
}

pub(crate) struct Persist {
    pub kv: Arc<dyn KvStore + Send + Sync>,
    metadata_crypto: Arc<dyn MetadataCryptoAdapter>,
    export_ledger_sequencer: Arc<ExportLedgerSequencer>,
    adaptive_write_batcher: Option<Arc<AdaptiveWriteBatcher>>,
}

impl Persist {
    pub(crate) fn ensure_hydration_prefix_within_limits(
        &self,
        label: &str,
        prefix: &[u8],
        max_rows: usize,
        max_bytes: u64,
    ) -> KvResult<(usize, u64)> {
        let mut rows = 0_usize;
        let mut bytes = 0_u64;
        for result in self.kv.scan_prefix(prefix)? {
            let (key, value) = result?;
            rows = rows.saturating_add(1);
            bytes = bytes.saturating_add(
                u64::try_from(key.len().saturating_add(value.len())).unwrap_or(u64::MAX),
            );
            if rows > max_rows {
                return Err(StorageError::Other(format!(
                    "startup hydration limit exceeded for {label}: more than {max_rows} rows; raise the matching AGENTDB_STARTUP_MAX_* setting only after sizing memory"
                )));
            }
            if bytes > max_bytes {
                return Err(StorageError::Other(format!(
                    "startup hydration byte limit exceeded while scanning {label}: more than {max_bytes} key/value bytes; raise AGENTDB_STARTUP_MAX_HYDRATED_BYTES only after sizing memory"
                )));
            }
        }
        Ok((rows, bytes))
    }

    pub fn with_metadata_crypto(
        kv: Arc<dyn KvStore + Send + Sync>,
        metadata_crypto: Arc<dyn MetadataCryptoAdapter>,
    ) -> Self {
        let export_ledger_sequencer = Arc::new(ExportLedgerSequencer::default());
        let adaptive_write_batcher =
            AdaptiveWriteBatcher::from_env(kv.clone(), export_ledger_sequencer.clone());
        Self {
            kv,
            metadata_crypto,
            export_ledger_sequencer,
            adaptive_write_batcher,
        }
    }

    fn encrypt_metadata(
        &self,
        context: &MetadataCryptoContext<'_>,
        plaintext: &[u8],
    ) -> KvResult<Vec<u8>> {
        let ciphertext = self
            .metadata_crypto
            .encrypt(context, plaintext)
            .map_err(|e| {
                StorageError::Other(format!(
                    "metadata encrypt failed for {} {}: {e}",
                    context.object_kind.as_str(),
                    context.object_id
                ))
            })?;
        Ok(match context.key.as_ref() {
            Some(key) => encode_keyed_envelope(key, &ciphertext),
            None => ciphertext,
        })
    }

    fn decrypt_metadata(
        &self,
        context: &MetadataCryptoContext<'_>,
        ciphertext: &[u8],
    ) -> KvResult<Vec<u8>> {
        let decoded = decode_keyed_envelope(ciphertext)
            .map_err(|e| StorageError::Other(format!("metadata envelope decode failed: {e}")))?;
        let (recorded_key, ciphertext) = match decoded {
            Some((key, ciphertext)) => (Some(key), ciphertext),
            None => (None, ciphertext.to_vec()),
        };
        let context = MetadataCryptoContext {
            tenant_id: context.tenant_id,
            object_kind: context.object_kind,
            object_id: context.object_id,
            key: recorded_key.or_else(|| context.key.clone()),
        };
        self.metadata_crypto
            .decrypt(&context, &ciphertext)
            .map_err(|e| {
                StorageError::Other(format!(
                    "metadata decrypt failed for {} {}: {e}",
                    context.object_kind.as_str(),
                    context.object_id
                ))
            })
    }

    fn artifact_context<'a>(&self, a: &'a Artifact) -> MetadataCryptoContext<'a> {
        let key = key_from_parts(&a.metadata_key_id, &a.metadata_key_version);
        MetadataCryptoContext {
            tenant_id: a.tenant_id.as_str(),
            object_kind: MetadataObjectKind::Artifact,
            object_id: a.artifact_id.as_str(),
            key,
        }
    }

    fn event_context<'a>(&self, e: &'a EventEnvelope) -> MetadataCryptoContext<'a> {
        let key = key_from_parts(&e.metadata_key_id, &e.metadata_key_version);
        MetadataCryptoContext {
            tenant_id: e.tenant_id.as_str(),
            object_kind: MetadataObjectKind::Event,
            object_id: e.session_id.as_str(),
            key,
        }
    }

    fn event_context_from_key<'a>(
        &self,
        tenant_id: &'a str,
        session_id: &'a str,
        _seq: u64,
    ) -> MetadataCryptoContext<'a> {
        MetadataCryptoContext {
            tenant_id,
            object_kind: MetadataObjectKind::Event,
            object_id: session_id,
            key: None,
        }
    }

    fn export_ledger_context<'a>(
        &self,
        tenant_id: &'a str,
        object_id: &'a str,
    ) -> MetadataCryptoContext<'a> {
        MetadataCryptoContext {
            tenant_id,
            object_kind: MetadataObjectKind::Event,
            object_id,
            key: None,
        }
    }

    // ----- single-record writes -----
    //
    // Each `put_*` writes the primary record AND, if an alias / external_key
    // is set, an alias index row in the same atomic batch. Hydration scans
    // both prefixes (`record/{kind}/`, `record/{kind}_alias/`) at startup so
    // the in-memory uniqueness map reflects on-disk state.

    pub fn put_tenant(&self, t: &Tenant) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(5);
        ops.push(WriteOp::Put {
            key: tenant_record_key(t.tenant_id.as_str()).into_bytes(),
            value: serde_json::to_vec(t).expect("tenant serializes"),
        });
        if !t.external_key.is_empty() {
            ops.push(WriteOp::Put {
                key: tenant_alias_key(&t.external_key).into_bytes(),
                value: t.tenant_id.as_str().as_bytes().to_vec(),
            });
        }
        ops.extend(self.export_ledger_item_put_ops(export_item_from_value(
            "tenant",
            t.tenant_id.clone(),
            None,
            None,
            t.tenant_id.as_str().to_string(),
            t.created_at_ms,
            t.deleted_at_ms,
            serde_json::to_value(t).expect("tenant serializes"),
        ))?);
        self.write_ops(ops)
    }

    pub fn purge_tenant(&self, t: &Tenant, reason: &str, deleted_at_ms: u64) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
        ops.push(WriteOp::Delete {
            key: tenant_record_key(t.tenant_id.as_str()).into_bytes(),
        });
        if !t.external_key.is_empty() {
            ops.push(WriteOp::Delete {
                key: tenant_alias_key(&t.external_key).into_bytes(),
            });
        }
        ops.extend(self.export_ledger_item_put_ops(export_tombstone_item(
            "tenant",
            t.tenant_id.clone(),
            None,
            None,
            t.tenant_id.as_str().to_string(),
            deleted_at_ms,
            reason,
            serde_json::to_value(t).expect("tenant serializes"),
        ))?);
        self.write_ops(ops)
    }

    pub fn put_agent(&self, a: &Agent) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(5);
        ops.push(WriteOp::Put {
            key: agent_record_key(a.tenant_id.as_str(), a.agent_id.as_str()).into_bytes(),
            value: serde_json::to_vec(a).expect("agent serializes"),
        });
        if let Some(alias) = a.alias.as_deref() {
            if !alias.is_empty() {
                ops.push(WriteOp::Put {
                    key: agent_alias_key(a.tenant_id.as_str(), alias).into_bytes(),
                    value: a.agent_id.as_str().as_bytes().to_vec(),
                });
            }
        }
        ops.extend(self.export_ledger_item_put_ops(export_item_from_value(
            "agent",
            a.tenant_id.clone(),
            Some(a.agent_id.clone()),
            None,
            a.agent_id.as_str().to_string(),
            a.created_at_ms,
            a.deleted_at_ms,
            serde_json::to_value(a).expect("agent serializes"),
        ))?);
        self.write_ops(ops)
    }

    pub fn purge_agent(&self, a: &Agent, reason: &str, deleted_at_ms: u64) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
        ops.push(WriteOp::Delete {
            key: agent_record_key(a.tenant_id.as_str(), a.agent_id.as_str()).into_bytes(),
        });
        if let Some(alias) = a.alias.as_deref() {
            if !alias.is_empty() {
                ops.push(WriteOp::Delete {
                    key: agent_alias_key(a.tenant_id.as_str(), alias).into_bytes(),
                });
            }
        }
        ops.extend(self.export_ledger_item_put_ops(export_tombstone_item(
            "agent",
            a.tenant_id.clone(),
            Some(a.agent_id.clone()),
            None,
            a.agent_id.as_str().to_string(),
            deleted_at_ms,
            reason,
            serde_json::to_value(a).expect("agent serializes"),
        ))?);
        self.write_ops(ops)
    }

    /// Atomically write an artifact's primary record AND the
    /// `artifact_by_sha256` secondary index in one batch. The sha256 index
    /// has no V2.1 lookup API; it's populated so a future dedup pass lands
    /// without rewriting this code path.
    pub fn put_artifact(&self, a: &Artifact) -> KvResult<()> {
        self.write_ops(self.put_artifact_ops(a)?)
    }

    pub async fn put_artifact_async(&self, a: &Artifact) -> KvResult<()> {
        self.write_ops_async(self.put_artifact_ops(a)?).await
    }

    fn put_artifact_ops(&self, a: &Artifact) -> KvResult<Vec<WriteOp>> {
        let artifact_context = self.artifact_context(a);
        let artifact_value = self.encrypt_metadata(
            &artifact_context,
            &serde_json::to_vec(a).expect("artifact serializes"),
        )?;
        let mut ops = vec![
            WriteOp::Put {
                key: artifact_record_key(a.tenant_id.as_str(), a.artifact_id.as_str()).into_bytes(),
                value: artifact_value,
            },
            WriteOp::Put {
                key: artifact_by_sha256_key(a.tenant_id.as_str(), &a.sha256).into_bytes(),
                value: a.artifact_id.as_str().as_bytes().to_vec(),
            },
        ];
        ops.extend(self.export_ledger_item_put_ops(export_item_from_value(
            "artifact",
            a.tenant_id.clone(),
            None,
            None,
            a.artifact_id.as_str().to_string(),
            a.created_at_ms,
            a.deleted_at_ms,
            serde_json::to_value(a).expect("artifact serializes"),
        ))?);
        Ok(ops)
    }

    pub fn purge_artifact(&self, a: &Artifact, reason: &str, deleted_at_ms: u64) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
            WriteOp::Delete {
                key: artifact_record_key(a.tenant_id.as_str(), a.artifact_id.as_str()).into_bytes(),
            },
            WriteOp::Delete {
                key: artifact_by_sha256_key(a.tenant_id.as_str(), &a.sha256).into_bytes(),
            },
        ]
        .into_iter()
        .chain(self.export_ledger_item_put_ops(export_tombstone_item(
            "artifact",
            a.tenant_id.clone(),
            None,
            None,
            a.artifact_id.as_str().to_string(),
            deleted_at_ms,
            reason,
            serde_json::to_value(a).expect("artifact serializes"),
        ))?)
        .collect();
        self.write_ops(ops)
    }

    fn put_session_ops(&self, s: &Session) -> KvResult<Vec<WriteOp>> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(5);
        ops.push(WriteOp::Put {
            key: session_record_key(s.tenant_id.as_str(), s.session_id.as_str()).into_bytes(),
            value: serde_json::to_vec(s).expect("session serializes"),
        });
        if let Some(alias) = s.alias.as_deref() {
            if !alias.is_empty() {
                ops.push(WriteOp::Put {
                    key: session_alias_key(s.tenant_id.as_str(), alias).into_bytes(),
                    value: s.session_id.as_str().as_bytes().to_vec(),
                });
            }
        }
        ops.extend(self.export_ledger_item_put_ops(export_item_from_value(
            "session",
            s.tenant_id.clone(),
            Some(s.agent_id.clone()),
            Some(s.session_id.clone()),
            s.session_id.as_str().to_string(),
            s.created_at_ms,
            s.deleted_at_ms,
            serde_json::to_value(s).expect("session serializes"),
        ))?);
        Ok(ops)
    }

    pub fn put_session(&self, s: &Session) -> KvResult<()> {
        self.write_ops(self.put_session_ops(s)?)
    }

    pub fn put_forked_session(&self, s: &Session) -> KvResult<()> {
        let mut ops = self.put_session_ops(s)?;
        if let Some(parent) = s.forked_from.as_ref() {
            ops.extend(self.export_ledger_item_put_ops(export_fork_edge_item(
                s,
                parent.session_id.clone(),
                parent.at_seq,
            ))?);
        }
        self.write_ops(ops)
    }

    pub fn put_session_segment(&self, segment: &SessionSegment) -> KvResult<()> {
        self.write_ops(vec![WriteOp::Put {
            key: session_segment_key(
                segment.tenant_id.as_str(),
                segment.session_id.as_str(),
                segment.segment_id.as_str(),
            )
            .into_bytes(),
            value: serde_json::to_vec(segment).expect("session segment serializes"),
        }])
    }

    pub fn purge_session(&self, s: &Session, reason: &str, deleted_at_ms: u64) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
        ops.push(WriteOp::Delete {
            key: session_record_key(s.tenant_id.as_str(), s.session_id.as_str()).into_bytes(),
        });
        if let Some(alias) = s.alias.as_deref() {
            if !alias.is_empty() {
                ops.push(WriteOp::Delete {
                    key: session_alias_key(s.tenant_id.as_str(), alias).into_bytes(),
                });
            }
        }
        ops.extend(self.export_ledger_item_put_ops(export_tombstone_item(
            "session",
            s.tenant_id.clone(),
            Some(s.agent_id.clone()),
            Some(s.session_id.clone()),
            s.session_id.as_str().to_string(),
            deleted_at_ms,
            reason,
            serde_json::to_value(s).expect("session serializes"),
        ))?);
        self.write_ops(ops)
    }

    pub fn purge_session_events(
        &self,
        tenant_id: &str,
        session_id: &str,
        reason: &str,
        deleted_at_ms: u64,
    ) -> KvResult<usize> {
        self.delete_session_events_inner(tenant_id, session_id, Some((reason, deleted_at_ms)))
    }

    fn delete_session_events_inner(
        &self,
        tenant_id: &str,
        session_id: &str,
        tombstone: Option<(&str, u64)>,
    ) -> KvResult<usize> {
        let prefix = format!("event/{tenant_id}/{session_id}/");
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let mut ops: Vec<WriteOp> = Vec::new();
        let mut count = 0;
        let mut tombstoned_event_ids = BTreeSet::new();
        for res in iter {
            let (k, v) = res?;
            let context = self.event_context_from_key(tenant_id, session_id, 0);
            let plaintext = self.decrypt_metadata(&context, &v)?;
            let env: EventEnvelope =
                serde_json::from_slice(&plaintext).expect("event envelope decodes");
            if let Some((reason, deleted_at_ms)) = tombstone {
                if tombstoned_event_ids.insert(env.event_id.as_str().to_string()) {
                    ops.extend(self.export_ledger_item_put_ops(
                        export_tombstone_item_from_event(&env, deleted_at_ms, reason),
                    )?);
                }
            }
            ops.push(WriteOp::Delete { key: k });
            ops.push(WriteOp::Delete {
                key: event_by_id_key(tenant_id, env.event_id.as_str()).into_bytes(),
            });
            count += 1;
        }
        let log_prefix = append_log_session_prefix(tenant_id, session_id);
        for res in self.kv.scan_prefix(log_prefix.as_bytes())? {
            let (k, v) = res?;
            let batch = self.decode_append_log_batch(tenant_id, session_id, &v)?;
            for event in &batch.events {
                if let Some((reason, deleted_at_ms)) = tombstone {
                    if tombstoned_event_ids.insert(event.event_id.as_str().to_string()) {
                        ops.extend(self.export_ledger_item_put_ops(
                            export_tombstone_item_from_event(event, deleted_at_ms, reason),
                        )?);
                    }
                }
                ops.push(WriteOp::Delete {
                    key: event_by_id_key(tenant_id, event.event_id.as_str()).into_bytes(),
                });
                count += 1;
            }
            ops.push(WriteOp::Delete { key: k });
        }
        if !ops.is_empty() {
            self.write_ops(ops)?;
        }
        Ok(count)
    }

    pub fn delete_events_from_seq(
        &self,
        tenant_id: &str,
        session_id: &str,
        from_seq: u64,
    ) -> KvResult<usize> {
        let prefix = format!("event/{tenant_id}/{session_id}/");
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let mut ops: Vec<WriteOp> = Vec::new();
        let mut count = 0;
        for res in iter {
            let (k, v) = res?;
            let context = self.event_context_from_key(tenant_id, session_id, 0);
            let plaintext = self.decrypt_metadata(&context, &v)?;
            let env: EventEnvelope =
                serde_json::from_slice(&plaintext).expect("event envelope decodes");
            if env.seq < from_seq {
                continue;
            }
            ops.push(WriteOp::Delete { key: k });
            ops.push(WriteOp::Delete {
                key: event_by_id_key(tenant_id, env.event_id.as_str()).into_bytes(),
            });
            count += 1;
        }
        let log_prefix = append_log_session_prefix(tenant_id, session_id);
        for res in self.kv.scan_prefix(log_prefix.as_bytes())? {
            let (k, v) = res?;
            let batch = self.decode_append_log_batch(tenant_id, session_id, &v)?;
            let mut kept = Vec::new();
            let mut deleted = Vec::new();
            for event in batch.events {
                if event.seq < from_seq {
                    kept.push(event);
                } else {
                    deleted.push(event);
                }
            }
            if deleted.is_empty() {
                continue;
            }
            for event in &deleted {
                ops.push(WriteOp::Delete {
                    key: event_by_id_key(tenant_id, event.event_id.as_str()).into_bytes(),
                });
            }
            count += deleted.len();
            ops.push(WriteOp::Delete { key: k });
            if !kept.is_empty() {
                let first_seq = kept[0].seq;
                let rewritten = AppendLogBatch {
                    events: kept,
                    session: None,
                };
                let value = self.encode_append_log_batch(tenant_id, session_id, &rewritten)?;
                ops.push(WriteOp::Put {
                    key: append_log_key(tenant_id, session_id, first_seq).into_bytes(),
                    value,
                });
            }
        }
        if !ops.is_empty() {
            self.write_ops(ops)?;
        }
        Ok(count)
    }

    /// Atomic multi-write for the append commit path. Bundles the event row,
    /// the `event_by_id` secondary index row, the optional session checkpoint,
    /// and (optionally) the idempotency record so that on crash recovery each
    /// committed event is durable with its lookup/idempotency metadata.
    pub async fn append_batch_async(
        &self,
        event: &EventEnvelope,
        session: Option<&Session>,
        idem: Option<(&str, &IdempotencyRecord)>,
    ) -> KvResult<()> {
        let Some(profile) = append_persist_profile() else {
            let ops = self.append_batch_ops(event, session, idem)?;
            if let Some(batcher) = &self.adaptive_write_batcher {
                return batcher.submit(ops).await;
            }
            return self.write_ops(ops);
        };

        let build_started = Instant::now();
        let ops = self.append_batch_ops(event, session, idem)?;
        let build_nanos = build_started.elapsed().as_nanos();
        let op_count = ops.len();
        let bytes = write_ops_size(&ops);
        let write_started = Instant::now();
        let result = if let Some(batcher) = &self.adaptive_write_batcher {
            batcher.submit(ops).await
        } else {
            self.write_ops(ops)
        };
        let write_nanos = write_started.elapsed().as_nanos();
        profile.record(op_count, bytes, build_nanos, write_nanos);
        result
    }

    pub async fn append_many_batch_async(
        &self,
        events: &[EventEnvelope],
        session: Option<&Session>,
    ) -> KvResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        let Some(profile) = append_persist_profile() else {
            let ops = self.append_many_batch_ops(events, session)?;
            if let Some(batcher) = &self.adaptive_write_batcher {
                return batcher.submit(ops).await;
            }
            return self.write_ops(ops);
        };

        let build_started = Instant::now();
        let ops = self.append_many_batch_ops(events, session)?;
        let build_nanos = build_started.elapsed().as_nanos();
        let op_count = ops.len();
        let bytes = write_ops_size(&ops);
        let write_started = Instant::now();
        let result = if let Some(batcher) = &self.adaptive_write_batcher {
            batcher.submit(ops).await
        } else {
            self.write_ops(ops)
        };
        let write_nanos = write_started.elapsed().as_nanos();
        profile.record(op_count, bytes, build_nanos, write_nanos);
        result
    }

    pub async fn append_many_log_refs_async(
        &self,
        events: &[&EventEnvelope],
        session: Option<&Session>,
    ) -> KvResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        let Some(profile) = append_persist_profile() else {
            let ops = self.append_many_log_ref_ops(events, session)?;
            if let Some(batcher) = &self.adaptive_write_batcher {
                return batcher.submit(ops).await;
            }
            return self.write_ops(ops);
        };

        let build_started = Instant::now();
        let ops = self.append_many_log_ref_ops(events, session)?;
        let build_nanos = build_started.elapsed().as_nanos();
        let op_count = ops.len();
        let bytes = write_ops_size(&ops);
        let write_started = Instant::now();
        let result = if let Some(batcher) = &self.adaptive_write_batcher {
            batcher.submit(ops).await
        } else {
            self.write_ops(ops)
        };
        let write_nanos = write_started.elapsed().as_nanos();
        profile.record(op_count, bytes, build_nanos, write_nanos);
        result
    }

    fn append_many_batch_ops(
        &self,
        events: &[EventEnvelope],
        session: Option<&Session>,
    ) -> KvResult<Vec<WriteOp>> {
        let mut ops = Vec::with_capacity(events.len() * 2 + usize::from(session.is_some()));
        for event in events {
            ops.extend(self.append_batch_ops(event, None, None)?);
        }
        if let Some(session) = session {
            ops.push(WriteOp::Put {
                key: session_record_key(session.tenant_id.as_str(), session.session_id.as_str())
                    .into_bytes(),
                value: serde_json::to_vec(session).expect("session serializes"),
            });
        }
        Ok(ops)
    }

    fn append_many_log_ref_ops(
        &self,
        events: &[&EventEnvelope],
        session: Option<&Session>,
    ) -> KvResult<Vec<WriteOp>> {
        let first = events
            .first()
            .expect("append_many_log_ref_ops requires non-empty events");
        let batch = AppendLogBatchRefs { events, session };
        let mut ops = Vec::with_capacity(events.len() * 4 + 1);
        ops.push(WriteOp::Put {
            key: append_log_key(
                first.tenant_id.as_str(),
                first.session_id.as_str(),
                first.seq,
            )
            .into_bytes(),
            value: self.encode_append_log_batch_refs(
                first.tenant_id.as_str(),
                first.session_id.as_str(),
                &batch,
            )?,
        });
        for event in events {
            ops.extend(self.export_ledger_event_ops(event)?);
        }
        Ok(ops)
    }

    pub(crate) fn append_batch_ops(
        &self,
        event: &EventEnvelope,
        session: Option<&Session>,
        idem: Option<(&str, &IdempotencyRecord)>,
    ) -> KvResult<Vec<WriteOp>> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(7);
        let ek = event_key(
            event.tenant_id.as_str(),
            event.session_id.as_str(),
            event.seq,
        );
        let event_context = self.event_context(event);
        let event_value = self.encrypt_metadata(
            &event_context,
            &serde_json::to_vec(event).expect("event serializes"),
        )?;
        ops.push(WriteOp::Put {
            key: ek.into_bytes(),
            value: event_value,
        });
        ops.extend(self.export_ledger_event_ops(event)?);
        if let Some(session) = session {
            let sk = session_record_key(session.tenant_id.as_str(), session.session_id.as_str());
            ops.push(WriteOp::Put {
                key: sk.into_bytes(),
                value: serde_json::to_vec(session).expect("session serializes"),
            });
        }
        // Secondary index: event_by_id -> {session_id, seq}. Lets a caller
        // look up an event by its event_id without scanning the session.
        let by_id = event_by_id_key(event.tenant_id.as_str(), event.event_id.as_str());
        ops.push(WriteOp::Put {
            key: by_id.into_bytes(),
            value: serde_json::to_vec(&serde_json::json!({
                "session_id": event.session_id.as_str(),
                "seq": event.seq,
            }))
            .expect("event_by_id pointer serializes"),
        });
        if let Some((full_key, rec)) = idem {
            // Persist under a stable "record/idempotency/<full_key>" namespace so
            // the persisted scope is independent of the in-memory composite-key
            // formatter. The full key composed by Runtime is already
            // tenant-scoped: "{tenant}::append::{session}::{user_key}".
            let key = format!("record/idempotency/{full_key}");
            ops.push(WriteOp::Put {
                key: key.into_bytes(),
                value: serde_json::to_vec(rec).expect("idempotency record serializes"),
            });
        }
        Ok(ops)
    }

    fn export_ledger_event_ops(&self, event: &EventEnvelope) -> KvResult<Vec<WriteOp>> {
        let token = next_export_ledger_pending_token(event.event_id.as_str());
        Ok(vec![
            self.export_ledger_event_put_op(event, &token)?,
            self.export_ledger_session_index_put_op(event, &token),
            self.export_ledger_agent_index_put_op(event, &token),
            self.export_ledger_high_watermark_put_op(event, &token),
        ])
    }

    fn export_ledger_event_put_op(&self, event: &EventEnvelope, token: &str) -> KvResult<WriteOp> {
        let item = export_item_from_event(event);
        let event_context =
            self.export_ledger_context(event.tenant_id.as_str(), event.event_id.as_str());
        let value = self.encrypt_metadata(
            &event_context,
            &serde_json::to_vec(&item).expect("export ledger item serializes"),
        )?;
        Ok(WriteOp::Put {
            key: export_ledger_event_key(event, token).into_bytes(),
            value,
        })
    }

    fn export_ledger_high_watermark_put_op(&self, event: &EventEnvelope, token: &str) -> WriteOp {
        let key = export_ledger_event_key(event, token);
        let cursor = format!("ledger:{}", key.trim_start_matches("export_ledger/"));
        WriteOp::Put {
            key: export_ledger_high_watermark_key(event.tenant_id.as_str()).into_bytes(),
            value: cursor.into_bytes(),
        }
    }

    fn export_ledger_session_index_put_op(&self, event: &EventEnvelope, token: &str) -> WriteOp {
        WriteOp::Put {
            key: export_ledger_session_index_key(event, token).into_bytes(),
            value: export_ledger_event_key(event, token).into_bytes(),
        }
    }

    fn export_ledger_agent_index_put_op(&self, event: &EventEnvelope, token: &str) -> WriteOp {
        WriteOp::Put {
            key: export_ledger_agent_index_key(event, token).into_bytes(),
            value: export_ledger_event_key(event, token).into_bytes(),
        }
    }

    fn export_ledger_item_put_ops(&self, item: ExportLedgerItem) -> KvResult<Vec<WriteOp>> {
        let token = next_export_ledger_pending_token(&item.object_id);
        let key = export_ledger_object_key(&item, &token);
        let cursor = format!("ledger:{}", key.trim_start_matches("export_ledger/"));
        let context = self.export_ledger_context(item.tenant_id.as_str(), &item.object_id);
        let value = self.encrypt_metadata(
            &context,
            &serde_json::to_vec(&item).expect("export ledger item serializes"),
        )?;
        let mut ops = vec![
            WriteOp::Put {
                key: key.as_bytes().to_vec(),
                value,
            },
            WriteOp::Put {
                key: export_ledger_high_watermark_key(item.tenant_id.as_str()).into_bytes(),
                value: cursor.into_bytes(),
            },
        ];
        if let Some(session_id) = item.session_id.as_ref() {
            ops.push(WriteOp::Put {
                key: format!(
                    "export_ledger_by_session/{}/{}/~pending/{token}",
                    item.tenant_id, session_id
                )
                .into_bytes(),
                value: key.as_bytes().to_vec(),
            });
        }
        if let Some(agent_id) = item.agent_id.as_ref() {
            ops.push(WriteOp::Put {
                key: format!(
                    "export_ledger_by_agent/{}/{}/~pending/{token}",
                    item.tenant_id, agent_id
                )
                .into_bytes(),
                value: key.into_bytes(),
            });
        }
        Ok(ops)
    }

    /// Resolve an `event_id` to its `(session_id, seq)` location via the
    /// `event_by_id` secondary index. Returns `None` if no row exists.
    pub fn lookup_event_by_id(
        &self,
        tenant_id: &str,
        event_id: &str,
    ) -> KvResult<Option<(String, u64)>> {
        let raw = self
            .kv
            .get(event_by_id_key(tenant_id, event_id).as_bytes())?;
        let Some(bytes) = raw else { return Ok(None) };
        let v: serde_json::Value =
            serde_json::from_slice(&bytes).expect("event_by_id pointer decodes");
        let session_id = v["session_id"]
            .as_str()
            .expect("event_by_id pointer has session_id")
            .to_string();
        let seq = v["seq"].as_u64().expect("event_by_id pointer has seq");
        Ok(Some((session_id, seq)))
    }

    fn encode_append_log_batch(
        &self,
        tenant_id: &str,
        session_id: &str,
        batch: &AppendLogBatch,
    ) -> KvResult<Vec<u8>> {
        let context = self.event_context_from_key(tenant_id, session_id, 0);
        self.encrypt_metadata(
            &context,
            &serde_json::to_vec(batch).expect("append log batch serializes"),
        )
    }

    fn encode_append_log_batch_refs(
        &self,
        tenant_id: &str,
        session_id: &str,
        batch: &AppendLogBatchRefs<'_>,
    ) -> KvResult<Vec<u8>> {
        let context = self.event_context_from_key(tenant_id, session_id, 0);
        self.encrypt_metadata(
            &context,
            &serde_json::to_vec(batch).expect("append log batch serializes"),
        )
    }

    fn decode_append_log_batch(
        &self,
        tenant_id: &str,
        session_id: &str,
        value: &[u8],
    ) -> KvResult<AppendLogBatch> {
        let context = self.event_context_from_key(tenant_id, session_id, 0);
        let plaintext = self.decrypt_metadata(&context, value)?;
        Ok(serde_json::from_slice(&plaintext).expect("append log batch decodes"))
    }

    fn append_log_session_id_from_key<'a>(tenant_id: &str, key: &'a [u8]) -> Option<&'a str> {
        let raw = std::str::from_utf8(key).ok()?;
        let prefix = append_log_tenant_prefix(tenant_id);
        let rest = raw.strip_prefix(prefix.as_str())?;
        let (session_id, _) = rest.split_once('/')?;
        Some(session_id)
    }

    pub fn lookup_event_by_id_in_append_log(
        &self,
        tenant_id: &str,
        event_id: &str,
    ) -> KvResult<Option<(String, u64)>> {
        let prefix = append_log_tenant_prefix(tenant_id);
        for res in self.kv.scan_prefix(prefix.as_bytes())? {
            let (k, v) = res?;
            let Some(session_id) = Self::append_log_session_id_from_key(tenant_id, &k) else {
                continue;
            };
            let batch = self.decode_append_log_batch(tenant_id, session_id, &v)?;
            for event in batch.events {
                if event.event_id.as_str() == event_id {
                    return Ok(Some((session_id.to_string(), event.seq)));
                }
            }
        }
        Ok(None)
    }

    pub fn latest_append_log_session(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> KvResult<Option<Session>> {
        let prefix = append_log_session_prefix(tenant_id, session_id);
        let mut latest: Option<(u64, Session)> = None;
        for res in self.kv.scan_prefix(prefix.as_bytes())? {
            let (_k, v) = res?;
            let batch = self.decode_append_log_batch(tenant_id, session_id, &v)?;
            let Some(session) = batch.session else {
                continue;
            };
            let last_seq = batch.events.last().map(|event| event.seq).unwrap_or(0);
            if latest.as_ref().is_none_or(|(seq, _)| last_seq >= *seq) {
                latest = Some((last_seq, session));
            }
        }
        Ok(latest.map(|(_, session)| session))
    }

    // ----- reads -----

    pub fn scan_events(
        &self,
        tenant_id: &str,
        session_id: &str,
        from_seq: u64,
        limit: usize,
    ) -> KvResult<Vec<EventEnvelope>> {
        let mut merged: BTreeMap<u64, EventEnvelope> = BTreeMap::new();
        let prefix = format!("event/{tenant_id}/{session_id}/");
        let start = event_key(tenant_id, session_id, from_seq);
        let iter = self
            .kv
            .scan_prefix_from(prefix.as_bytes(), start.as_bytes())?;
        for res in iter {
            let (_k, v) = res?;
            let context = self.event_context_from_key(tenant_id, session_id, 0);
            let plaintext = self.decrypt_metadata(&context, &v)?;
            let env: EventEnvelope =
                serde_json::from_slice(&plaintext).expect("event envelope decodes");
            if env.seq < from_seq {
                continue;
            }
            merged.insert(env.seq, env);
            if merged.len() >= limit {
                break;
            }
        }

        let log_prefix = append_log_session_prefix(tenant_id, session_id);
        for res in self.kv.scan_prefix(log_prefix.as_bytes())? {
            let (_k, v) = res?;
            let batch = self.decode_append_log_batch(tenant_id, session_id, &v)?;
            for event in batch.events {
                if event.seq < from_seq {
                    continue;
                }
                merged.insert(event.seq, event);
            }
        }

        Ok(merged.into_values().take(limit).collect())
    }

    pub fn get_event(
        &self,
        tenant_id: &str,
        session_id: &str,
        seq: u64,
    ) -> KvResult<Option<EventEnvelope>> {
        let Some(value) = self
            .kv
            .get(event_key(tenant_id, session_id, seq).as_bytes())?
        else {
            return Ok(None);
        };
        let context = self.event_context_from_key(tenant_id, session_id, 0);
        let plaintext = self.decrypt_metadata(&context, &value)?;
        let event: EventEnvelope =
            serde_json::from_slice(&plaintext).expect("event envelope decodes");
        Ok(Some(event))
    }

    /// Visit durable export rows without materializing the whole matching
    /// backlog. Returning `true` from `visitor` stops the scan.
    pub(crate) fn visit_export_ledger_items<F>(
        &self,
        tenant_id: &str,
        agent_id: Option<&str>,
        session_id: Option<&str>,
        from_cursor: Option<&str>,
        mut visitor: F,
    ) -> KvResult<()>
    where
        F: FnMut(String, ExportLedgerItem) -> bool,
    {
        if let Some(session_id) = session_id {
            let prefix = export_ledger_session_index_prefix(tenant_id, session_id);
            return self.visit_export_ledger_index(tenant_id, &prefix, from_cursor, visitor);
        }
        if let Some(agent_id) = agent_id {
            let prefix = export_ledger_agent_index_prefix(tenant_id, agent_id);
            return self.visit_export_ledger_index(tenant_id, &prefix, from_cursor, visitor);
        }

        let prefix = export_ledger_tenant_prefix(tenant_id);
        let start_key =
            from_cursor.and_then(|cursor| export_ledger_key_from_item_cursor(tenant_id, cursor));
        let iter = match start_key.as_deref() {
            Some(start) => self
                .kv
                .scan_prefix_from(prefix.as_bytes(), start.as_bytes())?,
            None => self.kv.scan_prefix(prefix.as_bytes())?,
        };
        for res in iter {
            let (key, value) = res?;
            let cursor = export_ledger_item_cursor_from_key(&key);
            if export_ledger_cursor_at_or_before(&cursor, from_cursor) {
                continue;
            }
            let Some(object_id) = export_ledger_object_id_from_key(&key) else {
                continue;
            };
            let context = self.export_ledger_context(tenant_id, object_id);
            let plaintext = self.decrypt_metadata(&context, &value)?;
            let item: ExportLedgerItem =
                serde_json::from_slice(&plaintext).expect("export ledger item decodes");
            if visitor(cursor, item) {
                break;
            }
        }
        Ok(())
    }

    fn visit_export_ledger_index<F>(
        &self,
        tenant_id: &str,
        prefix: &str,
        from_cursor: Option<&str>,
        mut visitor: F,
    ) -> KvResult<()>
    where
        F: FnMut(String, ExportLedgerItem) -> bool,
    {
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        for res in iter {
            let (_index_key, ledger_key) = res?;
            let cursor = export_ledger_item_cursor_from_key(&ledger_key);
            if export_ledger_cursor_at_or_before(&cursor, from_cursor) {
                continue;
            }
            let Some(value) = self.kv.get(&ledger_key)? else {
                continue;
            };
            let Some(object_id) = export_ledger_object_id_from_key(&ledger_key) else {
                continue;
            };
            let Some(ledger_tenant_id) = export_ledger_tenant_from_key(&ledger_key) else {
                continue;
            };
            if ledger_tenant_id != tenant_id {
                continue;
            }
            let context = self.export_ledger_context(tenant_id, object_id);
            let plaintext = self.decrypt_metadata(&context, &value)?;
            let item: ExportLedgerItem =
                serde_json::from_slice(&plaintext).expect("export ledger item decodes");
            if visitor(cursor, item) {
                break;
            }
        }
        Ok(())
    }

    pub fn get_export_ledger_item(
        &self,
        tenant_id: &str,
        cursor: &str,
    ) -> KvResult<Option<ExportLedgerItem>> {
        let Some(key) = export_ledger_key_from_item_cursor(tenant_id, cursor) else {
            return Ok(None);
        };
        let Some(value) = self.kv.get(key.as_bytes())? else {
            return Ok(None);
        };
        let Some(object_id) = export_ledger_object_id_from_key(key.as_bytes()) else {
            return Ok(None);
        };
        let context = self.export_ledger_context(tenant_id, object_id);
        let plaintext = self.decrypt_metadata(&context, &value)?;
        let item = serde_json::from_slice(&plaintext).expect("export ledger item decodes");
        Ok(Some(item))
    }

    pub fn export_ledger_high_watermark(&self, tenant_id: &str) -> KvResult<Option<String>> {
        let Some(value) = self
            .kv
            .get(export_ledger_high_watermark_key(tenant_id).as_bytes())?
        else {
            return Ok(None);
        };
        Ok(Some(
            String::from_utf8(value).expect("export ledger high watermark is utf8"),
        ))
    }

    pub fn export_ledger_oldest_cursor(&self, tenant_id: &str) -> KvResult<Option<String>> {
        let prefix = export_ledger_tenant_prefix(tenant_id);
        let mut iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let Some(first) = iter.next() else {
            return Ok(None);
        };
        let (key, _) = first?;
        Ok(Some(export_ledger_item_cursor_from_key(&key)))
    }

    /// Return the highest persisted event seq for `(tenant_id, session_id)`,
    /// or `None` if no events exist. Used by hydration to reconcile
    /// `Session::next_seq` against on-disk reality.
    pub fn max_event_seq(&self, tenant_id: &str, session_id: &str) -> KvResult<Option<u64>> {
        let prefix = format!("event/{tenant_id}/{session_id}/");
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let mut max_seq: Option<u64> = None;
        for res in iter {
            let (_k, v) = res?;
            let context = self.event_context_from_key(tenant_id, session_id, 0);
            let plaintext = self.decrypt_metadata(&context, &v)?;
            let env: EventEnvelope =
                serde_json::from_slice(&plaintext).expect("event envelope decodes");
            max_seq = Some(match max_seq {
                Some(m) if m >= env.seq => m,
                _ => env.seq,
            });
        }
        let log_prefix = append_log_session_prefix(tenant_id, session_id);
        for res in self.kv.scan_prefix(log_prefix.as_bytes())? {
            let (_k, v) = res?;
            let batch = self.decode_append_log_batch(tenant_id, session_id, &v)?;
            for event in batch.events {
                max_seq = Some(match max_seq {
                    Some(m) if m >= event.seq => m,
                    _ => event.seq,
                });
            }
        }
        Ok(max_seq)
    }

    fn get_json_row<T: serde::de::DeserializeOwned>(&self, key: String) -> KvResult<Option<T>> {
        match self.kv.get(key.as_bytes())? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(|err| {
                agentdb_storage::Error::Other(format!("row at {key} does not decode: {err}"))
            })?)),
            None => Ok(None),
        }
    }

    fn get_string_row(&self, key: String) -> KvResult<Option<String>> {
        Ok(self
            .kv
            .get(key.as_bytes())?
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
    }

    // Point reads for the cache-miss fallback path: the in-memory maps are a
    // cache over these rows, not the authority. Deleted rows are returned
    // as-is (soft-delete tombstones) so callers' liveness filters apply.
    pub fn get_tenant_row(&self, tenant_id: &str) -> KvResult<Option<Tenant>> {
        self.get_json_row(tenant_record_key(tenant_id))
    }

    pub fn get_agent_row(&self, tenant_id: &str, agent_id: &str) -> KvResult<Option<Agent>> {
        self.get_json_row(agent_record_key(tenant_id, agent_id))
    }

    pub fn get_session_row(&self, tenant_id: &str, session_id: &str) -> KvResult<Option<Session>> {
        self.get_json_row(session_record_key(tenant_id, session_id))
    }

    pub fn get_artifact_row(
        &self,
        tenant_id: &str,
        artifact_id: &str,
    ) -> KvResult<Option<Artifact>> {
        // Artifact metadata rows are envelope-encrypted; mirror the
        // hydration decode rather than parsing the raw bytes as JSON.
        let key = artifact_record_key(tenant_id, artifact_id);
        let Some(bytes) = self.kv.get(key.as_bytes())? else {
            return Ok(None);
        };
        let context = MetadataCryptoContext {
            tenant_id,
            object_kind: MetadataObjectKind::Artifact,
            object_id: artifact_id,
            key: None,
        };
        let plaintext = self.decrypt_metadata(&context, &bytes)?;
        Ok(Some(serde_json::from_slice(&plaintext).map_err(|err| {
            agentdb_storage::Error::Other(format!("artifact row at {key} does not decode: {err}"))
        })?))
    }

    pub fn get_record_row(
        &self,
        tenant_id: &str,
        record_id: &str,
    ) -> KvResult<Option<SearchableRecord>> {
        self.get_json_row(record_key(tenant_id, record_id))
    }

    pub fn scan_tenant_records(&self, tenant_id: &str) -> KvResult<Vec<SearchableRecord>> {
        let prefix = format!("record/searchable/{tenant_id}/");
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            out.push(serde_json::from_slice(&v).map_err(|err| {
                agentdb_storage::Error::Other(format!("record row does not decode: {err}"))
            })?);
        }
        Ok(out)
    }

    pub fn get_tenant_alias_row(&self, external_key: &str) -> KvResult<Option<String>> {
        self.get_string_row(tenant_alias_key(external_key))
    }

    pub fn get_agent_alias_row(&self, tenant_id: &str, alias: &str) -> KvResult<Option<String>> {
        self.get_string_row(agent_alias_key(tenant_id, alias))
    }

    pub fn get_session_alias_row(&self, tenant_id: &str, alias: &str) -> KvResult<Option<String>> {
        self.get_string_row(session_alias_key(tenant_id, alias))
    }

    // Tenant-scoped enumeration for the KV-authoritative read path. The
    // tenant-first key layout makes each of these a single bounded prefix
    // scan; they are the enumeration counterpart of the point reads above.
    pub fn scan_tenant_agents(&self, tenant_id: &str) -> KvResult<Vec<Agent>> {
        let prefix = format!("record/agent/{tenant_id}/");
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            out.push(serde_json::from_slice(&v).map_err(|err| {
                agentdb_storage::Error::Other(format!("agent row does not decode: {err}"))
            })?);
        }
        Ok(out)
    }

    pub fn scan_tenant_sessions(&self, tenant_id: &str) -> KvResult<Vec<Session>> {
        let prefix = format!("record/session/{tenant_id}/");
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            out.push(serde_json::from_slice(&v).map_err(|err| {
                agentdb_storage::Error::Other(format!("session row does not decode: {err}"))
            })?);
        }
        Ok(out)
    }

    pub fn scan_tenant_artifacts(&self, tenant_id: &str) -> KvResult<Vec<Artifact>> {
        let prefix = format!("record/artifact/{tenant_id}/");
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix = std::str::from_utf8(&k[b"record/artifact/".len()..])
                .map_err(|_| agentdb_storage::Error::Other("artifact key is not utf8".into()))?;
            let mut split = suffix.splitn(2, '/');
            let key_tenant = split.next().unwrap_or_default();
            let artifact_id = split.next().unwrap_or_default();
            let context = MetadataCryptoContext {
                tenant_id: key_tenant,
                object_kind: MetadataObjectKind::Artifact,
                object_id: artifact_id,
                key: None,
            };
            let plaintext = self.decrypt_metadata(&context, &v)?;
            out.push(serde_json::from_slice(&plaintext).map_err(|err| {
                agentdb_storage::Error::Other(format!("artifact row does not decode: {err}"))
            })?);
        }
        Ok(out)
    }

    pub fn scan_all_tenants(&self) -> KvResult<Vec<Tenant>> {
        // Note: the prefix ends in '/', so this does NOT pick up sibling
        // namespaces like `record/tenant_alias/...` because their bytes after
        // `record/tenant` are `_` not `/`.
        let iter = self.kv.scan_prefix(b"record/tenant/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let t: Tenant = serde_json::from_slice(&v).expect("tenant decodes");
            out.push(t);
        }
        Ok(out)
    }

    pub fn scan_all_agents(&self) -> KvResult<Vec<Agent>> {
        let iter = self.kv.scan_prefix(b"record/agent/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let a: Agent = serde_json::from_slice(&v).expect("agent decodes");
            out.push(a);
        }
        Ok(out)
    }

    pub fn scan_all_sessions(&self) -> KvResult<Vec<Session>> {
        let iter = self.kv.scan_prefix(b"record/session/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let s: Session = serde_json::from_slice(&v).expect("session decodes");
            out.push(s);
        }
        Ok(out)
    }

    /// Hydrate all live artifact metadata records. The `record/artifact/`
    /// prefix is disjoint from the `record/artifact_by_sha256/` index prefix
    /// (the byte after `record/artifact` is `_` for the index, `/` for the
    /// record), so this scan does NOT pick up index entries.
    pub fn scan_all_artifacts(&self) -> KvResult<Vec<Artifact>> {
        let prefix = b"record/artifact/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix =
                std::str::from_utf8(&k[prefix.len()..]).expect("artifact key suffix is utf8");
            let mut split = suffix.splitn(2, '/');
            let tenant_id = split.next().expect("tenant_id segment");
            let artifact_id = split.next().expect("artifact_id segment");
            let context = MetadataCryptoContext {
                tenant_id,
                object_kind: MetadataObjectKind::Artifact,
                object_id: artifact_id,
                key: None,
            };
            let plaintext = self.decrypt_metadata(&context, &v)?;
            let a: Artifact = serde_json::from_slice(&plaintext).expect("artifact decodes");
            out.push(a);
        }
        Ok(out)
    }

    /// Scan tenant alias rows, yielding `(external_key, tenant_id)` pairs.
    /// Used at hydration to rebuild the in-memory uniqueness map.
    pub fn scan_all_tenant_aliases(&self) -> KvResult<Vec<(String, String)>> {
        let prefix = b"record/tenant_alias/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let external_key = std::str::from_utf8(&k[prefix.len()..])
                .expect("tenant_alias key suffix is utf8")
                .to_string();
            let tenant_id = std::str::from_utf8(&v)
                .expect("tenant_alias value is utf8")
                .to_string();
            out.push((external_key, tenant_id));
        }
        Ok(out)
    }

    /// Scan agent alias rows, yielding `((tenant_id, alias), agent_id)`.
    pub fn scan_all_agent_aliases(&self) -> KvResult<Vec<((String, String), String)>> {
        let prefix = b"record/agent_alias/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix =
                std::str::from_utf8(&k[prefix.len()..]).expect("agent_alias key suffix is utf8");
            // suffix shape: "{tenant_id}/{alias}"
            let mut split = suffix.splitn(2, '/');
            let tenant = split.next().expect("tenant_id segment").to_string();
            let alias = split.next().expect("alias segment").to_string();
            let agent_id = std::str::from_utf8(&v)
                .expect("agent_alias value is utf8")
                .to_string();
            out.push(((tenant, alias), agent_id));
        }
        Ok(out)
    }

    /// Scan session alias rows, yielding `((tenant_id, alias), session_id)`.
    pub fn scan_all_session_aliases(&self) -> KvResult<Vec<((String, String), String)>> {
        let prefix = b"record/session_alias/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix =
                std::str::from_utf8(&k[prefix.len()..]).expect("session_alias key suffix is utf8");
            let mut split = suffix.splitn(2, '/');
            let tenant = split.next().expect("tenant_id segment").to_string();
            let alias = split.next().expect("alias segment").to_string();
            let session_id = std::str::from_utf8(&v)
                .expect("session_alias value is utf8")
                .to_string();
            out.push(((tenant, alias), session_id));
        }
        Ok(out)
    }

    pub fn scan_all_idempotency(&self) -> KvResult<Vec<(String, IdempotencyRecord)>> {
        let prefix = b"record/idempotency/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            // Strip the "record/idempotency/" prefix to recover the original
            // composite key Runtime uses in its in-memory store.
            let full_key = std::str::from_utf8(&k[prefix.len()..])
                .expect("idempotency key is utf8")
                .to_string();
            let rec: IdempotencyRecord =
                serde_json::from_slice(&v).expect("idempotency record decodes");
            out.push((full_key, rec));
        }
        Ok(out)
    }

    pub fn get_idempotency(&self, full_key: &str) -> KvResult<Option<IdempotencyRecord>> {
        let Some(raw) = self
            .kv
            .get(format!("record/idempotency/{full_key}").as_bytes())?
        else {
            return Ok(None);
        };
        let rec: IdempotencyRecord = serde_json::from_slice(&raw)
            .map_err(|err| StorageError::Other(format!("idempotency record decodes: {err}")))?;
        Ok(Some(rec))
    }

    pub async fn put_export_consumer_async(&self, consumer: &ExportConsumer) -> KvResult<()> {
        self.write_ops_async(vec![WriteOp::Put {
            key: export_consumer_key(consumer.tenant_id.as_str(), consumer.consumer_name.as_str())
                .into_bytes(),
            value: serde_json::to_vec(consumer).expect("export consumer serializes"),
        }])
        .await
    }

    pub fn scan_all_export_consumers(&self) -> KvResult<Vec<ExportConsumer>> {
        let prefix = b"record/export_consumer/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let consumer: ExportConsumer =
                serde_json::from_slice(&v).expect("export consumer decodes");
            out.push(consumer);
        }
        Ok(out)
    }

    /// Atomic write of a V2.2 searchable record's primary row plus the
    /// `record_by_namespace` secondary index entry. Both rows are written
    /// in one batch so a crash either reveals the new record + index
    /// pair or neither — no half-state where a record is reachable by id
    /// but missing from a namespace prefix scan (or vice versa).
    pub fn put_record_with_commit_seq(
        &self,
        r: &SearchableRecord,
        commit_seq: Option<u64>,
    ) -> KvResult<()> {
        self.write_ops(self.put_record_with_commit_seq_ops(r, commit_seq)?)
    }

    pub async fn put_record_with_commit_seq_async(
        &self,
        r: &SearchableRecord,
        commit_seq: Option<u64>,
    ) -> KvResult<()> {
        self.write_ops_async(self.put_record_with_commit_seq_ops(r, commit_seq)?)
            .await
    }

    pub(crate) fn put_record_with_commit_seq_ops(
        &self,
        r: &SearchableRecord,
        commit_seq: Option<u64>,
    ) -> KvResult<Vec<WriteOp>> {
        let mut ops: Vec<WriteOp> = vec![
            WriteOp::Put {
                key: record_key(r.tenant_id.as_str(), r.record_id.as_str()).into_bytes(),
                value: serde_json::to_vec(r).expect("record serializes"),
            },
            WriteOp::Put {
                key: record_by_namespace_key(
                    r.tenant_id.as_str(),
                    &r.namespace_path(),
                    r.record_id.as_str(),
                )
                .into_bytes(),
                value: r.record_id.as_str().as_bytes().to_vec(),
            },
        ]
        .into_iter()
        .chain(commit_seq.into_iter().flat_map(|seq| {
            let op = if r.deleted_at_ms.is_some() {
                RecordIndexMutationOp::Delete
            } else {
                RecordIndexMutationOp::Upsert
            };
            let mutation = RecordIndexMutation {
                tenant_id: r.tenant_id.as_str().to_string(),
                record_id: r.record_id.as_str().to_string(),
                commit_seq: seq,
                op,
            };
            [
                WriteOp::Put {
                    key: record_commit_key(r.tenant_id.as_str(), r.record_id.as_str()).into_bytes(),
                    value: seq.to_be_bytes().to_vec(),
                },
                WriteOp::Put {
                    key: record_index_outbox_key(r.tenant_id.as_str(), seq, r.record_id.as_str())
                        .into_bytes(),
                    value: serde_json::to_vec(&mutation).expect("record index mutation serializes"),
                },
            ]
        }))
        .collect();
        ops.extend(self.export_ledger_item_put_ops(export_item_from_value(
            "record",
            r.tenant_id.clone(),
            None,
            None,
            r.record_id.as_str().to_string(),
            r.created_at_ms,
            r.deleted_at_ms,
            serde_json::to_value(r).expect("record serializes"),
        ))?);
        Ok(ops)
    }

    pub(crate) fn write_ops(&self, ops: Vec<WriteOp>) -> KvResult<()> {
        if let Some(batcher) = &self.adaptive_write_batcher {
            batcher.submit_blocking(ops)
        } else {
            self.export_ledger_sequencer.write(self.kv.as_ref(), ops)
        }
    }

    pub(crate) async fn write_ops_async(&self, ops: Vec<WriteOp>) -> KvResult<()> {
        if let Some(batcher) = &self.adaptive_write_batcher {
            batcher.submit(ops).await
        } else {
            self.write_ops(ops)
        }
    }

    pub fn purge_record(
        &self,
        r: &SearchableRecord,
        reason: &str,
        deleted_at_ms: u64,
    ) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = vec![
            WriteOp::Delete {
                key: record_key(r.tenant_id.as_str(), r.record_id.as_str()).into_bytes(),
            },
            WriteOp::Delete {
                key: record_by_namespace_key(
                    r.tenant_id.as_str(),
                    &r.namespace_path(),
                    r.record_id.as_str(),
                )
                .into_bytes(),
            },
        ];
        ops.extend(self.export_ledger_item_put_ops(export_tombstone_item(
            "record",
            r.tenant_id.clone(),
            None,
            None,
            r.record_id.as_str().to_string(),
            deleted_at_ms,
            reason,
            serde_json::to_value(r).expect("record serializes"),
        ))?);
        self.write_ops(ops)
    }

    /// Atomic write of a V2.3 skill record plus its
    /// `(tenant_id, name, version)` uniqueness index.
    pub fn put_skill(&self, skill: &Skill) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = vec![
            WriteOp::Put {
                key: skill_record_key(skill.tenant_id.as_str(), skill.skill_id.as_str())
                    .into_bytes(),
                value: serde_json::to_vec(skill).expect("skill serializes"),
            },
            WriteOp::Put {
                key: skill_unique_key(skill.tenant_id.as_str(), &skill.name, &skill.version)
                    .into_bytes(),
                value: skill.skill_id.as_str().as_bytes().to_vec(),
            },
        ];
        ops.extend(self.export_ledger_item_put_ops(export_item_from_value(
            "skill",
            skill.tenant_id.clone(),
            None,
            None,
            skill.skill_id.as_str().to_string(),
            skill.created_at_ms,
            skill.deleted_at_ms,
            serde_json::to_value(skill).expect("skill serializes"),
        ))?);
        self.write_ops(ops)
    }

    pub fn purge_skill(&self, skill: &Skill, reason: &str, deleted_at_ms: u64) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = vec![
            WriteOp::Delete {
                key: skill_record_key(skill.tenant_id.as_str(), skill.skill_id.as_str())
                    .into_bytes(),
            },
            WriteOp::Delete {
                key: skill_unique_key(skill.tenant_id.as_str(), &skill.name, &skill.version)
                    .into_bytes(),
            },
        ];
        ops.extend(self.export_ledger_item_put_ops(export_tombstone_item(
            "skill",
            skill.tenant_id.clone(),
            None,
            None,
            skill.skill_id.as_str().to_string(),
            deleted_at_ms,
            reason,
            serde_json::to_value(skill).expect("skill serializes"),
        ))?);
        self.write_ops(ops)
    }

    pub fn put_agent_skill_binding(&self, binding: &AgentSkillBinding) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = vec![WriteOp::Put {
            key: agent_skill_binding_key(
                binding.tenant_id.as_str(),
                binding.agent_id.as_str(),
                binding.skill_id.as_str(),
            )
            .into_bytes(),
            value: serde_json::to_vec(binding).expect("agent skill binding serializes"),
        }];
        ops.extend(self.export_ledger_item_put_ops(export_item_from_value(
            "skill_binding",
            binding.tenant_id.clone(),
            Some(binding.agent_id.clone()),
            None,
            format!("{}:{}", binding.agent_id, binding.skill_id),
            binding.bound_at_ms,
            binding.deleted_at_ms,
            serde_json::to_value(binding).expect("agent skill binding serializes"),
        ))?);
        self.write_ops(ops)
    }

    pub fn purge_agent_skill_binding(
        &self,
        binding: &AgentSkillBinding,
        reason: &str,
        deleted_at_ms: u64,
    ) -> KvResult<()> {
        let mut ops = vec![WriteOp::Delete {
            key: agent_skill_binding_key(
                binding.tenant_id.as_str(),
                binding.agent_id.as_str(),
                binding.skill_id.as_str(),
            )
            .into_bytes(),
        }];
        ops.extend(self.export_ledger_item_put_ops(export_tombstone_item(
            "skill_binding",
            binding.tenant_id.clone(),
            Some(binding.agent_id.clone()),
            None,
            format!("{}:{}", binding.agent_id, binding.skill_id),
            deleted_at_ms,
            reason,
            serde_json::to_value(binding).expect("agent skill binding serializes"),
        ))?);
        self.write_ops(ops)
    }

    pub fn put_idempotency(&self, full_key: &str, rec: &IdempotencyRecord) -> KvResult<()> {
        self.write_ops(vec![WriteOp::Put {
            key: format!("record/idempotency/{full_key}").into_bytes(),
            value: serde_json::to_vec(rec).expect("idempotency record serializes"),
        }])
    }

    pub fn delete_idempotency(&self, full_key: &str) -> KvResult<()> {
        self.write_ops(vec![WriteOp::Delete {
            key: format!("record/idempotency/{full_key}").into_bytes(),
        }])
    }

    /// Look up a record by primary key (tenant + record_id). Returns
    /// `None` if the row is absent. Callers are responsible for
    /// soft-delete and expiration filtering.
    ///
    /// Currently unused by the V2.2a runtime (which serves
    /// `get_record` from the in-memory cache hydrated at startup);
    /// kept for the V2.2b HTTP layer that will need on-demand reads
    /// against the KV store.
    #[allow(dead_code)]
    pub fn get_record(
        &self,
        tenant_id: &str,
        record_id: &str,
    ) -> KvResult<Option<SearchableRecord>> {
        let raw = self.kv.get(record_key(tenant_id, record_id).as_bytes())?;
        let Some(bytes) = raw else { return Ok(None) };
        let r: SearchableRecord = serde_json::from_slice(&bytes).expect("record decodes");
        Ok(Some(r))
    }

    /// Hydrate every searchable record from the KV store. The
    /// `record/searchable/` prefix is byte-disjoint from the secondary
    /// index prefix `record/searchable_by_namespace/` (the byte after
    /// `record/searchable` is `_` for the index but `/` for the record
    /// prefix), so this scan does NOT pick up index entries.
    pub fn scan_all_records(&self) -> KvResult<Vec<SearchableRecord>> {
        let iter = self.kv.scan_prefix(b"record/searchable/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let r: SearchableRecord = serde_json::from_slice(&v).expect("record decodes");
            out.push(r);
        }
        Ok(out)
    }

    pub fn scan_all_record_commit_seqs(&self) -> KvResult<Vec<(String, u64)>> {
        let prefix = b"record/searchable_commit/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix =
                std::str::from_utf8(&k[prefix.len()..]).expect("record_commit key suffix is utf8");
            let mut split = suffix.splitn(2, '/');
            let _tenant_id = split.next().expect("tenant_id segment");
            let record_id = split.next().expect("record_id segment").to_string();
            if v.len() == 8 {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&v);
                out.push((record_id, u64::from_be_bytes(bytes)));
            }
        }
        Ok(out)
    }

    pub fn scan_all_record_index_mutations(&self) -> KvResult<Vec<RecordIndexMutation>> {
        let prefix = b"record/searchable_index_outbox/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let mutation: RecordIndexMutation =
                serde_json::from_slice(&v).expect("record index mutation decodes");
            out.push(mutation);
        }
        Ok(out)
    }

    pub fn put_record_edge(&self, edge: &RecordEdge) -> KvResult<()> {
        self.write_ops(vec![WriteOp::Put {
            key: record_edge_key(edge.tenant_id.as_str(), edge.edge_id.as_str()).into_bytes(),
            value: serde_json::to_vec(edge).expect("record edge serializes"),
        }])
    }

    pub fn scan_all_record_edges(&self) -> KvResult<Vec<RecordEdge>> {
        let iter = self.kv.scan_prefix(b"record/searchable_edge/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let edge: RecordEdge = serde_json::from_slice(&v).expect("record edge decodes");
            out.push(edge);
        }
        Ok(out)
    }

    pub fn put_tenant_meta(&self, entry: &TenantMetaEntry) -> KvResult<()> {
        self.write_ops(vec![WriteOp::Put {
            key: tenant_meta_key(entry.tenant_id.as_str(), &entry.key).into_bytes(),
            value: serde_json::to_vec(entry).expect("tenant meta entry serializes"),
        }])
    }

    pub fn scan_all_tenant_meta(&self) -> KvResult<Vec<TenantMetaEntry>> {
        let iter = self.kv.scan_prefix(b"record/tenant_meta/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let entry: TenantMetaEntry =
                serde_json::from_slice(&v).expect("tenant meta entry decodes");
            out.push(entry);
        }
        Ok(out)
    }

    pub fn scan_all_skills(&self) -> KvResult<Vec<Skill>> {
        let iter = self.kv.scan_prefix(b"record/skill/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let skill: Skill = serde_json::from_slice(&v).expect("skill decodes");
            out.push(skill);
        }
        Ok(out)
    }

    pub fn scan_all_skill_uniques(&self) -> KvResult<Vec<SkillUniqueRecord>> {
        let prefix = b"record/skill_unique/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix =
                std::str::from_utf8(&k[prefix.len()..]).expect("skill_unique key suffix is utf8");
            let mut split = suffix.splitn(3, '/');
            let tenant = split.next().expect("tenant_id segment").to_string();
            let name = split.next().expect("name segment").to_string();
            let version = split.next().expect("version segment").to_string();
            let skill_id = std::str::from_utf8(&v)
                .expect("skill_unique value is utf8")
                .to_string();
            out.push(((tenant, name, version), skill_id));
        }
        Ok(out)
    }

    pub fn scan_all_agent_skill_bindings(&self) -> KvResult<Vec<AgentSkillBinding>> {
        let iter = self.kv.scan_prefix(b"record/agent_skill/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let binding: AgentSkillBinding =
                serde_json::from_slice(&v).expect("agent skill binding decodes");
            out.push(binding);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod export_ledger_sequence_tests {
    use super::*;
    use agentdb_storage::InMemoryKvStore;

    #[test]
    fn sequencer_reconciles_a_durable_advance_that_bypassed_its_cache() {
        let kv = InMemoryKvStore::new();
        let sequencer = ExportLedgerSequencer::default();
        sequencer
            .write(
                &kv,
                vec![WriteOp::Put {
                    key: b"export_ledger/ten_test/~pending/a/object/record/rec_a".to_vec(),
                    value: b"first".to_vec(),
                }],
            )
            .unwrap();
        assert!(kv
            .get(b"export_ledger/ten_test/v2/00000000000000000001/object/record/rec_a")
            .unwrap()
            .is_some());

        // Model a replicated apply or snapshot install that advances the
        // durable counter while this process retains its old cached value.
        kv.write_batch(vec![WriteOp::Put {
            key: b"export_ledger_sequence/ten_test".to_vec(),
            value: 9_u64.to_be_bytes().to_vec(),
        }])
        .unwrap();

        sequencer
            .write(
                &kv,
                vec![WriteOp::Put {
                    key: b"export_ledger/ten_test/~pending/b/object/record/rec_b".to_vec(),
                    value: b"second".to_vec(),
                }],
            )
            .unwrap();
        assert!(kv
            .get(b"export_ledger/ten_test/v2/00000000000000000010/object/record/rec_b")
            .unwrap()
            .is_some());
        assert_eq!(
            kv.get(b"export_ledger_sequence/ten_test").unwrap(),
            Some(10_u64.to_be_bytes().to_vec())
        );
    }
}
