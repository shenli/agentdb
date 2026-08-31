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

use std::io;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::kv::{KvStore, WriteOp};

pub const RECOVERY_MANIFEST_FORMAT_VERSION: u32 = 1;

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
        let entry = RecoveryManifestEntry {
            k: BASE64.encode(&key),
            v: BASE64.encode(&value),
        };
        lines.push(serde_json::to_string(&entry).map_err(|err| Error::Other(err.to_string()))?);
    }
    let header = RecoveryManifestHeader {
        format: "agentdb-recovery-manifest".to_string(),
        format_version: RECOVERY_MANIFEST_FORMAT_VERSION,
        created_at_ms,
        node_id: node_id.map(str::to_string),
        entry_count,
        payload_bytes,
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
    if header.format_version != RECOVERY_MANIFEST_FORMAT_VERSION {
        return Err(Error::Other(format!(
            "unsupported recovery manifest format_version {} (this build supports {})",
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
        let bumped = text.replacen("\"format_version\":1", "\"format_version\":999", 1);
        let err =
            import_recovery_manifest(&InMemoryKvStore::default(), bumped.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("format_version 999"));
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
}
