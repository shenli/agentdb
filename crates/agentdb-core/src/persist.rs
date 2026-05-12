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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};
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
    tx: mpsc::Sender<AdaptiveWriteRequest>,
    queued: Arc<AtomicU64>,
    writer_busy: Arc<AtomicU64>,
    direct_fast_path: bool,
}

struct AdaptiveWriteRequest {
    ops: Vec<WriteOp>,
    enqueued_at: Instant,
    done: AdaptiveWriteCompletion,
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
    fn from_env(kv: Arc<dyn KvStore + Send + Sync>) -> Option<Arc<Self>> {
        if env_flag_disabled("AGENTDB_APPEND_GROUP_COMMIT") {
            return None;
        }
        let sync_writes = env_flag_enabled("AGENTDB_ROCKSDB_SYNC_WRITES");
        let max_ops = std::env::var("AGENTDB_APPEND_GROUP_COMMIT_MAX_OPS")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(4096);
        let wait_us = std::env::var("AGENTDB_APPEND_GROUP_COMMIT_US")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or_else(|| if sync_writes { 1_000 } else { 0 });
        Some(Arc::new(Self::new(
            kv,
            max_ops,
            wait_us,
            !sync_writes && wait_us == 0,
        )))
    }

    fn new(
        kv: Arc<dyn KvStore + Send + Sync>,
        max_ops: usize,
        wait_us: u64,
        direct_fast_path: bool,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<AdaptiveWriteRequest>();
        let queued = Arc::new(AtomicU64::new(0));
        let writer_busy = Arc::new(AtomicU64::new(0));
        let _ = thread::Builder::new()
            .name("agentdb-adaptive-write".into())
            .spawn({
                let kv = kv.clone();
                let queued = queued.clone();
                let writer_busy = writer_busy.clone();
                move || adaptive_write_loop(kv, rx, queued, writer_busy, max_ops, wait_us)
            });
        Self {
            kv,
            tx,
            queued,
            writer_busy,
            direct_fast_path,
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
            let result = self.kv.write_batch(ops);
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
            .send(AdaptiveWriteRequest {
                ops,
                enqueued_at: Instant::now(),
                done: AdaptiveWriteCompletion::Async(done),
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
            let result = self.kv.write_batch(ops);
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
            .send(AdaptiveWriteRequest {
                ops,
                enqueued_at: Instant::now(),
                done: AdaptiveWriteCompletion::Blocking(done),
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

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|raw| {
            let raw = raw.trim().to_ascii_lowercase();
            matches!(raw.as_str(), "1" | "true" | "on" | "enabled" | "yes")
        })
        .unwrap_or(false)
}

fn adaptive_write_loop(
    kv: Arc<dyn KvStore + Send + Sync>,
    rx: mpsc::Receiver<AdaptiveWriteRequest>,
    queued: Arc<AtomicU64>,
    writer_busy: Arc<AtomicU64>,
    max_ops: usize,
    wait_us: u64,
) {
    while let Ok(first) = rx.recv() {
        let mut requests = vec![first];
        queued.fetch_sub(1, Ordering::Relaxed);
        let mut op_count = requests[0].ops.len();
        if wait_us > 0 {
            thread::sleep(Duration::from_micros(wait_us));
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

        let mut ops = Vec::with_capacity(op_count);
        let mut completions = Vec::with_capacity(requests.len());
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
        for request in requests {
            ops.extend(request.ops);
            completions.push(request.done);
        }

        let write_started = Instant::now();
        let result = kv.write_batch(ops);
        let write_nanos = write_started.elapsed().as_nanos();
        writer_busy.store(0, Ordering::Release);
        if let Some(profile) = adaptive_write_profile() {
            profile.record_grouped(request_count, op_count, queue_nanos, write_nanos);
        }
        for done in completions {
            done.send(&result);
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
    adaptive_write_batcher: Option<Arc<AdaptiveWriteBatcher>>,
}

impl Persist {
    pub fn with_metadata_crypto(
        kv: Arc<dyn KvStore + Send + Sync>,
        metadata_crypto: Arc<dyn MetadataCryptoAdapter>,
    ) -> Self {
        let adaptive_write_batcher = AdaptiveWriteBatcher::from_env(kv.clone());
        Self {
            kv,
            metadata_crypto,
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

    // ----- single-record writes -----
    //
    // Each `put_*` writes the primary record AND, if an alias / external_key
    // is set, an alias index row in the same atomic batch. Hydration scans
    // both prefixes (`record/{kind}/`, `record/{kind}_alias/`) at startup so
    // the in-memory uniqueness map reflects on-disk state.

    pub fn put_tenant(&self, t: &Tenant) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
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
        self.write_ops(ops)
    }

    pub fn delete_tenant(&self, t: &Tenant) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
        ops.push(WriteOp::Delete {
            key: tenant_record_key(t.tenant_id.as_str()).into_bytes(),
        });
        if !t.external_key.is_empty() {
            ops.push(WriteOp::Delete {
                key: tenant_alias_key(&t.external_key).into_bytes(),
            });
        }
        self.write_ops(ops)
    }

    pub fn put_agent(&self, a: &Agent) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
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
        self.write_ops(ops)
    }

    pub fn delete_agent(&self, a: &Agent) -> KvResult<()> {
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
        Ok(vec![
            WriteOp::Put {
                key: artifact_record_key(a.tenant_id.as_str(), a.artifact_id.as_str()).into_bytes(),
                value: artifact_value,
            },
            WriteOp::Put {
                key: artifact_by_sha256_key(a.tenant_id.as_str(), &a.sha256).into_bytes(),
                value: a.artifact_id.as_str().as_bytes().to_vec(),
            },
        ])
    }

    pub fn delete_artifact(&self, a: &Artifact) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
            WriteOp::Delete {
                key: artifact_record_key(a.tenant_id.as_str(), a.artifact_id.as_str()).into_bytes(),
            },
            WriteOp::Delete {
                key: artifact_by_sha256_key(a.tenant_id.as_str(), &a.sha256).into_bytes(),
            },
        ];
        self.write_ops(ops)
    }

    pub fn put_session(&self, s: &Session) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
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

    pub fn delete_session(&self, s: &Session) -> KvResult<()> {
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
        self.write_ops(ops)
    }

    pub fn delete_session_events(&self, tenant_id: &str, session_id: &str) -> KvResult<usize> {
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
        Ok(vec![WriteOp::Put {
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
        }])
    }

    pub(crate) fn append_batch_ops(
        &self,
        event: &EventEnvelope,
        session: Option<&Session>,
        idem: Option<(&str, &IdempotencyRecord)>,
    ) -> KvResult<Vec<WriteOp>> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(4);
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
            if latest.as_ref().map_or(true, |(seq, _)| last_seq >= *seq) {
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
        let ops: Vec<WriteOp> = vec![
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
        Ok(ops)
    }

    pub(crate) fn write_ops(&self, ops: Vec<WriteOp>) -> KvResult<()> {
        if let Some(batcher) = &self.adaptive_write_batcher {
            batcher.submit_blocking(ops)
        } else {
            self.kv.write_batch(ops)
        }
    }

    pub(crate) async fn write_ops_async(&self, ops: Vec<WriteOp>) -> KvResult<()> {
        if let Some(batcher) = &self.adaptive_write_batcher {
            batcher.submit(ops).await
        } else {
            self.write_ops(ops)
        }
    }

    pub fn delete_record(&self, r: &SearchableRecord) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
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
        self.write_ops(ops)
    }

    /// Atomic write of a V2.3 skill record plus its
    /// `(tenant_id, name, version)` uniqueness index.
    pub fn put_skill(&self, skill: &Skill) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
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
        self.write_ops(ops)
    }

    pub fn delete_skill(&self, skill: &Skill) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
            WriteOp::Delete {
                key: skill_record_key(skill.tenant_id.as_str(), skill.skill_id.as_str())
                    .into_bytes(),
            },
            WriteOp::Delete {
                key: skill_unique_key(skill.tenant_id.as_str(), &skill.name, &skill.version)
                    .into_bytes(),
            },
        ];
        self.write_ops(ops)
    }

    pub fn put_agent_skill_binding(&self, binding: &AgentSkillBinding) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![WriteOp::Put {
            key: agent_skill_binding_key(
                binding.tenant_id.as_str(),
                binding.agent_id.as_str(),
                binding.skill_id.as_str(),
            )
            .into_bytes(),
            value: serde_json::to_vec(binding).expect("agent skill binding serializes"),
        }];
        self.write_ops(ops)
    }

    pub fn delete_agent_skill_binding(&self, binding: &AgentSkillBinding) -> KvResult<()> {
        self.write_ops(vec![WriteOp::Delete {
            key: agent_skill_binding_key(
                binding.tenant_id.as_str(),
                binding.agent_id.as_str(),
                binding.skill_id.as_str(),
            )
            .into_bytes(),
        }])
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
