//! Recovery-manifest support: object storage as the recovery root.
//!
//! Raft (or the local WAL in single-node mode) owns the acknowledged write;
//! the bucket owns survivability. A recovery manifest is a point-in-time,
//! full-fidelity dump of the runtime KV state, written as JSON Lines:
//! one self-describing header line followed by one `{"k":..,"v":..}` line
//! per KV pair (base64-encoded), so a bucket plus this file is enough to
//! rebuild a node without the original metadata store.
//!
//! The manifest format is a durable contract, versioned by
//! `RECOVERY_MANIFEST_FORMAT_VERSION`. Import refuses unknown versions and
//! refuses to write into a non-empty store — recovery never silently merges
//! into existing state.
//!
//! Each manifest carries a content-derived `change_id` (independent of its
//! timestamp) and a monotonic `watermark`, so publishing is reconcilable
//! (`RecoveryObjectStore::publish_latest` → `PublishOutcome`): an unchanged
//! snapshot is an idempotent `AlreadyApplied` no-op, and a stale retry is
//! `RejectedStale` rather than clobbering a newer manifest.
//! `read_latest_summary` is the authoritative "what is currently applied"
//! query a worker reconciles an unknown outcome against
//! (`docs/design/distributed-safety-hardening.md` §4).

use std::io;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::kv::{KvStore, WriteOp};

/// Bumped to 2 for the content-derived `change_id` + `watermark` fields.
/// Import still accepts version 1 (those fields default: change_id is
/// recomputed from content, watermark falls back to `created_at_ms`).
pub const RECOVERY_MANIFEST_FORMAT_VERSION: u32 = 2;

/// First line of every recovery manifest. Self-describing on purpose:
/// a reader with only the bucket must be able to identify what it holds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveryManifestHeader {
    pub format: String,
    pub format_version: u32,
    pub created_at_ms: i64,
    pub node_id: Option<String>,
    pub entry_count: u64,
    pub payload_bytes: u64,
    /// Stable id derived from the manifest's CONTENT (not its timestamp),
    /// so two exports of identical state — a retry, a re-run — carry the
    /// same id and can be recognized as the same logical transition. A v1
    /// manifest lacks it; `#[serde(default)]` yields an empty string, which
    /// callers recompute via `change_id_of`.
    #[serde(default)]
    pub change_id: String,
    /// Monotonic marker used to reject a stale publish: a manifest whose
    /// watermark is below the currently-published one must not overwrite
    /// it. Defaults to `created_at_ms` for v1 manifests.
    #[serde(default)]
    pub watermark: u64,
}

impl RecoveryManifestHeader {
    /// The change id this header should carry, recomputed from content.
    /// v1 manifests (empty `change_id`) get it derived on read.
    pub fn effective_change_id(&self) -> String {
        if self.change_id.is_empty() {
            change_id_of(
                self.node_id.as_deref(),
                self.entry_count,
                self.payload_bytes,
            )
        } else {
            self.change_id.clone()
        }
    }

    /// The watermark to compare on publish; v1 falls back to `created_at_ms`.
    pub fn effective_watermark(&self) -> u64 {
        if self.watermark == 0 {
            self.created_at_ms.max(0) as u64
        } else {
            self.watermark
        }
    }
}

/// Content-derived change id for a manifest transition. Deliberately does
/// NOT include the wall-clock `created_at_ms`, so retries of the same
/// logical snapshot share an id. The `entries_digest` is a rolling hash of
/// the serialized entry lines (see `export_recovery_manifest`).
fn change_id_of(node_id: Option<&str>, entry_count: u64, payload_bytes: u64) -> String {
    // `export_recovery_manifest` computes the primary id by additionally
    // folding in a digest of the entries. This node/count/bytes-only form is
    // the fallback for re-deriving a v1 header's id from the header alone
    // (the entries aren't available from a header read).
    let mut hasher = Sha256::new();
    hasher.update(node_id.unwrap_or("").as_bytes());
    hasher.update([0]);
    hasher.update(entry_count.to_be_bytes());
    hasher.update(payload_bytes.to_be_bytes());
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[derive(Debug, Serialize, Deserialize)]
struct RecoveryManifestEntry {
    k: String,
    v: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct RecoveryImportStats {
    pub entries: u64,
    pub payload_bytes: u64,
}

/// The outcome of a reconcilable `publish_latest`, distinguishing the four
/// cases §4 of the hardening plan calls for (the fourth, "unknown", is the
/// `io::Error` returned when the store read/write itself fails).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The manifest was written and is now the published latest.
    Applied {
        uri: String,
        change_id: String,
        watermark: u64,
    },
    /// The published latest already has this change id — nothing to do.
    AlreadyApplied { change_id: String },
    /// The attempted manifest is older than the published one; not written.
    RejectedStale {
        current_watermark: u64,
        attempted_watermark: u64,
    },
}

impl PublishOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            PublishOutcome::Applied { .. } => "applied",
            PublishOutcome::AlreadyApplied { .. } => "already_applied",
            PublishOutcome::RejectedStale { .. } => "rejected_stale",
        }
    }
}

/// Authoritative view of the currently-published manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AppliedManifestSummary {
    pub change_id: String,
    pub watermark: u64,
    pub created_at_ms: i64,
    pub entry_count: u64,
}

/// Serialize a point-in-time snapshot of the entire KV state.
///
/// `max_bytes` bounds the serialized payload (keys + values, pre-encoding);
/// exceeding it fails closed with an actionable error rather than writing a
/// truncated manifest.
pub fn export_recovery_manifest(
    kv: Arc<dyn KvStore + Send + Sync>,
    node_id: Option<&str>,
    created_at_ms: i64,
    max_bytes: u64,
) -> Result<Vec<u8>> {
    let snapshot = kv.snapshot_owned()?;
    let mut lines = Vec::new();
    let mut entry_count = 0u64;
    let mut payload_bytes = 0u64;
    // Rolling content hash over the (deterministically ordered) entries, so
    // the change id addresses the actual snapshot content, not its
    // timestamp.
    let mut entries_hasher = Sha256::new();
    for item in snapshot.scan_prefix(b"")? {
        let (key, value) = item?;
        payload_bytes += (key.len() + value.len()) as u64;
        if payload_bytes > max_bytes {
            return Err(Error::Other(format!(
                "recovery manifest exceeds max_bytes={max_bytes}; raise \
                 recovery.manifest_max_bytes or reduce retained runtime state"
            )));
        }
        entry_count += 1;
        entries_hasher.update((key.len() as u64).to_be_bytes());
        entries_hasher.update(&key);
        entries_hasher.update((value.len() as u64).to_be_bytes());
        entries_hasher.update(&value);
        let entry = RecoveryManifestEntry {
            k: BASE64.encode(&key),
            v: BASE64.encode(&value),
        };
        lines.push(serde_json::to_string(&entry).map_err(|err| Error::Other(err.to_string()))?);
    }
    let entries_digest = entries_hasher.finalize();
    let mut change_hasher = Sha256::new();
    change_hasher.update(node_id.unwrap_or("").as_bytes());
    change_hasher.update([0]);
    change_hasher.update(entry_count.to_be_bytes());
    change_hasher.update(payload_bytes.to_be_bytes());
    change_hasher.update(entries_digest);
    let change_id = hex(&change_hasher.finalize());
    let header = RecoveryManifestHeader {
        format: "agentdb-recovery-manifest".to_string(),
        format_version: RECOVERY_MANIFEST_FORMAT_VERSION,
        created_at_ms,
        node_id: node_id.map(str::to_string),
        entry_count,
        payload_bytes,
        change_id,
        watermark: created_at_ms.max(0) as u64,
    };
    let mut out = serde_json::to_string(&header).map_err(|err| Error::Other(err.to_string()))?;
    for line in lines {
        out.push('\n');
        out.push_str(&line);
    }
    out.push('\n');
    Ok(out.into_bytes())
}

/// Import a recovery manifest into an empty KV store.
pub fn import_recovery_manifest(kv: &dyn KvStore, data: &[u8]) -> Result<RecoveryImportStats> {
    let text = std::str::from_utf8(data)
        .map_err(|_| Error::Other("recovery manifest is not valid UTF-8".into()))?;
    let mut lines = text.lines();
    let header_line = lines
        .next()
        .ok_or_else(|| Error::Other("recovery manifest is empty".into()))?;
    let header: RecoveryManifestHeader = serde_json::from_str(header_line)
        .map_err(|err| Error::Other(format!("invalid recovery manifest header: {err}")))?;
    if header.format != "agentdb-recovery-manifest" {
        return Err(Error::Other(format!(
            "not a recovery manifest (format: {})",
            header.format
        )));
    }
    if header.format_version == 0 || header.format_version > RECOVERY_MANIFEST_FORMAT_VERSION {
        return Err(Error::Other(format!(
            "unsupported recovery manifest format_version {} (this build supports up to {})",
            header.format_version, RECOVERY_MANIFEST_FORMAT_VERSION
        )));
    }
    if kv.scan_prefix(b"")?.next().is_some() {
        return Err(Error::Other(
            "refusing to import a recovery manifest into a non-empty store; \
             point the import at a fresh data directory"
                .into(),
        ));
    }

    let mut stats = RecoveryImportStats::default();
    let mut batch = Vec::with_capacity(1024);
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let entry: RecoveryManifestEntry = serde_json::from_str(line)
            .map_err(|err| Error::Other(format!("invalid recovery manifest entry: {err}")))?;
        let key = BASE64
            .decode(entry.k.as_bytes())
            .map_err(|err| Error::Other(format!("invalid manifest key encoding: {err}")))?;
        let value = BASE64
            .decode(entry.v.as_bytes())
            .map_err(|err| Error::Other(format!("invalid manifest value encoding: {err}")))?;
        stats.entries += 1;
        stats.payload_bytes += (key.len() + value.len()) as u64;
        batch.push(WriteOp::Put { key, value });
        if batch.len() >= 1024 {
            kv.write_batch(std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        kv.write_batch(batch)?;
    }
    if stats.entries != header.entry_count {
        return Err(Error::Other(format!(
            "recovery manifest is truncated: header declares {} entries, found {}",
            header.entry_count, stats.entries
        )));
    }
    Ok(stats)
}

/// Read only the header of a manifest, without importing it.
pub fn read_recovery_manifest_header(data: &[u8]) -> Result<RecoveryManifestHeader> {
    let text = std::str::from_utf8(data)
        .map_err(|_| Error::Other("recovery manifest is not valid UTF-8".into()))?;
    let header_line = text
        .lines()
        .next()
        .ok_or_else(|| Error::Other("recovery manifest is empty".into()))?;
    serde_json::from_str(header_line)
        .map_err(|err| Error::Other(format!("invalid recovery manifest header: {err}")))
}

/// Bucket-side layout for recovery manifests:
/// `{prefix}/recovery/manifest-latest.jsonl` is the stable pointer read by
/// restore; `{prefix}/recovery/history/manifest-{created_at_ms}.jsonl` keeps
/// a bounded-by-lifecycle history for point-in-time inspection.
pub struct RecoveryObjectStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl RecoveryObjectStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into().trim_matches('/').to_string();
        Self { store, prefix }
    }

    fn path(&self, suffix: &str) -> ObjectPath {
        let mut path = String::new();
        if !self.prefix.is_empty() {
            path.push_str(&self.prefix);
            path.push('/');
        }
        path.push_str("recovery/");
        path.push_str(suffix);
        ObjectPath::from(path)
    }

    pub fn latest_uri(&self) -> String {
        self.path("manifest-latest.jsonl").to_string()
    }

    /// Upload a manifest: history object first, then the latest pointer, so
    /// a reader of `manifest-latest.jsonl` never observes a manifest whose
    /// history copy is missing.
    ///
    /// Unconditional overwrite — prefer `publish_latest`, which is
    /// idempotent and rejects a stale republish. Retained for callers that
    /// have already decided to publish.
    pub async fn put_latest(&self, bytes: &[u8], created_at_ms: i64) -> io::Result<String> {
        let history = self.path(&format!("history/manifest-{created_at_ms}.jsonl"));
        self.store
            .put(&history, bytes.to_vec().into())
            .await
            .map_err(|err| io::Error::other(format!("recovery manifest upload failed: {err}")))?;
        let latest = self.path("manifest-latest.jsonl");
        self.store
            .put(&latest, bytes.to_vec().into())
            .await
            .map_err(|err| io::Error::other(format!("recovery manifest upload failed: {err}")))?;
        Ok(latest.to_string())
    }

    /// Reconcilable publish: identify the transition by its content-derived
    /// `change_id` and gate it on the `watermark` so retries and stale
    /// re-publishes are safe.
    ///
    /// - The currently-published manifest carries the same `change_id` →
    ///   `AlreadyApplied` (idempotent no-op; the object is left untouched).
    /// - The new manifest's `watermark` is below the published one →
    ///   `RejectedStale` (a late retry must not clobber newer state).
    /// - Otherwise the history object is written first, then the latest
    ///   pointer → `Applied`.
    ///
    /// `bytes` must be the manifest whose header is `header` (as produced by
    /// `export_recovery_manifest`).
    pub async fn publish_latest(
        &self,
        bytes: &[u8],
        header: &RecoveryManifestHeader,
    ) -> io::Result<PublishOutcome> {
        if let Some(current) = self.read_latest_summary().await? {
            if current.change_id == header.effective_change_id() {
                return Ok(PublishOutcome::AlreadyApplied {
                    change_id: current.change_id,
                });
            }
            if header.effective_watermark() < current.watermark {
                return Ok(PublishOutcome::RejectedStale {
                    current_watermark: current.watermark,
                    attempted_watermark: header.effective_watermark(),
                });
            }
        }
        let uri = self.put_latest(bytes, header.created_at_ms).await?;
        Ok(PublishOutcome::Applied {
            uri,
            change_id: header.effective_change_id(),
            watermark: header.effective_watermark(),
        })
    }

    /// The authoritative summary of the currently-published manifest —
    /// its change id and watermark — so a worker or operator can reconcile
    /// an unknown outcome (did my publish land?) with one header read.
    /// `Ok(None)` when no manifest has been published yet.
    pub async fn read_latest_summary(&self) -> io::Result<Option<AppliedManifestSummary>> {
        let bytes = match self.get_latest().await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        let header = read_recovery_manifest_header(&bytes).map_err(|err| {
            io::Error::other(format!("recovery manifest header unreadable: {err}"))
        })?;
        Ok(Some(AppliedManifestSummary {
            change_id: header.effective_change_id(),
            watermark: header.effective_watermark(),
            created_at_ms: header.created_at_ms,
            entry_count: header.entry_count,
        }))
    }

    pub async fn get_latest(&self) -> io::Result<Vec<u8>> {
        let latest = self.path("manifest-latest.jsonl");
        let result = self.store.get(&latest).await.map_err(|err| match err {
            object_store::Error::NotFound { .. } => io::Error::new(
                io::ErrorKind::NotFound,
                format!("no recovery manifest found at {latest}"),
            ),
            other => io::Error::other(format!("recovery manifest read failed: {other}")),
        })?;
        let bytes = result
            .bytes()
            .await
            .map_err(|err| io::Error::other(format!("recovery manifest read failed: {err}")))?;
        Ok(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::InMemoryKvStore;

    fn seeded_store() -> Arc<dyn KvStore + Send + Sync> {
        let kv = InMemoryKvStore::default();
        kv.put(b"tenant/ten_1", b"{\"name\":\"a\"}").unwrap();
        kv.put(b"session/ten_1/ses_1", b"{\"seq\":3}").unwrap();
        kv.put(b"event/ten_1/ses_1/000001", b"{\"payload\":1}")
            .unwrap();
        Arc::new(kv)
    }

    #[test]
    fn export_import_roundtrip_preserves_every_pair() {
        let src = seeded_store();
        let bytes = export_recovery_manifest(src.clone(), Some("node-a"), 42, u64::MAX).unwrap();

        let header = read_recovery_manifest_header(&bytes).unwrap();
        assert_eq!(header.format_version, RECOVERY_MANIFEST_FORMAT_VERSION);
        assert_eq!(header.entry_count, 3);
        assert_eq!(header.node_id.as_deref(), Some("node-a"));

        let dst = InMemoryKvStore::default();
        let stats = import_recovery_manifest(&dst, &bytes).unwrap();
        assert_eq!(stats.entries, 3);

        let src_rows: Vec<_> = src.scan_prefix(b"").unwrap().map(|r| r.unwrap()).collect();
        let dst_rows: Vec<_> = dst.scan_prefix(b"").unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(src_rows, dst_rows);
    }

    #[test]
    fn import_refuses_non_empty_store() {
        let src = seeded_store();
        let bytes = export_recovery_manifest(src, None, 1, u64::MAX).unwrap();
        let dst = InMemoryKvStore::default();
        dst.put(b"existing", b"row").unwrap();
        let err = import_recovery_manifest(&dst, &bytes).unwrap_err();
        assert!(err.to_string().contains("non-empty"));
    }

    #[test]
    fn import_refuses_unknown_format_version() {
        let src = seeded_store();
        let bytes = export_recovery_manifest(src, None, 1, u64::MAX).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let bumped = text.replacen(
            &format!("\"format_version\":{RECOVERY_MANIFEST_FORMAT_VERSION}"),
            "\"format_version\":999",
            1,
        );
        let err =
            import_recovery_manifest(&InMemoryKvStore::default(), bumped.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("format_version 999"));
    }

    #[test]
    fn change_id_is_content_derived_and_timestamp_independent() {
        let src = seeded_store();
        // Same content, different export timestamps → same change id.
        let a = read_recovery_manifest_header(
            &export_recovery_manifest(src.clone(), Some("node-a"), 100, u64::MAX).unwrap(),
        )
        .unwrap();
        let b = read_recovery_manifest_header(
            &export_recovery_manifest(src.clone(), Some("node-a"), 999, u64::MAX).unwrap(),
        )
        .unwrap();
        assert_eq!(a.change_id, b.change_id, "same content → same change id");
        assert!(!a.change_id.is_empty());
        assert_eq!(a.watermark, 100);
        assert_eq!(b.watermark, 999);

        // Different content → different change id.
        let other = InMemoryKvStore::default();
        other.put(b"tenant/ten_2", b"{}").unwrap();
        let c = read_recovery_manifest_header(
            &export_recovery_manifest(Arc::new(other), Some("node-a"), 100, u64::MAX).unwrap(),
        )
        .unwrap();
        assert_ne!(a.change_id, c.change_id);
    }

    #[tokio::test]
    async fn publish_is_idempotent_and_rejects_stale() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let recovery = RecoveryObjectStore::new(store.clone(), "agentdb");
        let src = seeded_store();

        assert!(recovery.read_latest_summary().await.unwrap().is_none());

        let v1 = export_recovery_manifest(src.clone(), Some("n"), 100, u64::MAX).unwrap();
        let h1 = read_recovery_manifest_header(&v1).unwrap();
        assert!(matches!(
            recovery.publish_latest(&v1, &h1).await.unwrap(),
            PublishOutcome::Applied { .. }
        ));

        // Re-publishing the identical content is an idempotent no-op.
        let again = export_recovery_manifest(src.clone(), Some("n"), 150, u64::MAX).unwrap();
        let again_h = read_recovery_manifest_header(&again).unwrap();
        assert!(matches!(
            recovery.publish_latest(&again, &again_h).await.unwrap(),
            PublishOutcome::AlreadyApplied { .. }
        ));

        // New content advances the manifest.
        let mutated = InMemoryKvStore::default();
        mutated.put(b"tenant/ten_9", b"{}").unwrap();
        let v2 = export_recovery_manifest(Arc::new(mutated), Some("n"), 200, u64::MAX).unwrap();
        let h2 = read_recovery_manifest_header(&v2).unwrap();
        assert!(matches!(
            recovery.publish_latest(&v2, &h2).await.unwrap(),
            PublishOutcome::Applied { .. }
        ));
        assert_eq!(
            recovery
                .read_latest_summary()
                .await
                .unwrap()
                .unwrap()
                .watermark,
            200
        );

        // A stale retry (older watermark, different content) must not clobber
        // the newer published manifest.
        let mut stale = h1.clone();
        stale.change_id = "deadbeef".into();
        stale.watermark = 50;
        let outcome = recovery.publish_latest(&v1, &stale).await.unwrap();
        assert!(
            matches!(outcome, PublishOutcome::RejectedStale { .. }),
            "expected RejectedStale, got {outcome:?}"
        );
        // Latest is still the v2 content (watermark 200).
        assert_eq!(
            recovery
                .read_latest_summary()
                .await
                .unwrap()
                .unwrap()
                .watermark,
            200
        );
    }

    #[test]
    fn import_detects_truncated_manifest() {
        let src = seeded_store();
        let bytes = export_recovery_manifest(src, None, 1, u64::MAX).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let mut lines: Vec<&str> = text.lines().collect();
        lines.pop();
        let truncated = lines.join("\n");
        let err = import_recovery_manifest(&InMemoryKvStore::default(), truncated.as_bytes())
            .unwrap_err();
        assert!(err.to_string().contains("truncated"));
    }

    #[test]
    fn export_fails_closed_past_max_bytes() {
        let src = seeded_store();
        let err = export_recovery_manifest(src, None, 1, 8).unwrap_err();
        assert!(err.to_string().contains("max_bytes"));
    }

    #[tokio::test]
    async fn object_store_roundtrip_and_latest_pointer() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let recovery = RecoveryObjectStore::new(store.clone(), "agentdb");

        assert!(recovery.get_latest().await.is_err());
        recovery.put_latest(b"first", 100).await.unwrap();
        recovery.put_latest(b"second", 200).await.unwrap();
        assert_eq!(recovery.get_latest().await.unwrap(), b"second");

        let history = recovery.path("history/manifest-100.jsonl");
        assert!(store.head(&history).await.is_ok());
    }

    /// §4 crash matrix: a publish writes the history object first, then the
    /// latest pointer. A crash BETWEEN those two writes (history present,
    /// pointer not advanced) must leave the previously-published manifest
    /// intact — a reader never sees a broken or half-advanced latest — and a
    /// retry must complete the transition. We simulate the crash by writing
    /// only the history object out of band, leaving the pointer untouched.
    #[tokio::test]
    async fn interrupted_publish_leaves_previous_latest_intact() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let recovery = RecoveryObjectStore::new(store.clone(), "agentdb");
        let src = seeded_store();

        // Publish v1 fully.
        let v1 = export_recovery_manifest(src.clone(), Some("n"), 100, u64::MAX).unwrap();
        let h1 = read_recovery_manifest_header(&v1).unwrap();
        recovery.publish_latest(&v1, &h1).await.unwrap();
        let v1_change = h1.effective_change_id();

        // Prepare a different v2 and simulate a crash mid-publish: only its
        // history object lands; the latest pointer is never advanced.
        let other = InMemoryKvStore::default();
        other.put(b"tenant/ten_2", b"{}").unwrap();
        let v2 = export_recovery_manifest(Arc::new(other), Some("n"), 200, u64::MAX).unwrap();
        let h2 = read_recovery_manifest_header(&v2).unwrap();
        store
            .put(
                &recovery.path("history/manifest-200.jsonl"),
                v2.clone().into(),
            )
            .await
            .unwrap();

        // The authoritative latest is still v1 — no dangling/half-applied
        // pointer.
        let summary = recovery.read_latest_summary().await.unwrap().unwrap();
        assert_eq!(summary.change_id, v1_change);
        assert_eq!(summary.watermark, 100);

        // A retry completes the transition to v2 (the stray history object is
        // simply overwritten, and the pointer advances).
        assert!(matches!(
            recovery.publish_latest(&v2, &h2).await.unwrap(),
            PublishOutcome::Applied { .. }
        ));
        assert_eq!(
            recovery
                .read_latest_summary()
                .await
                .unwrap()
                .unwrap()
                .watermark,
            200
        );
    }
}
