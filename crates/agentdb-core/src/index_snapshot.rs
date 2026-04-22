//! M16 Phase D — persistent index snapshots.
//!
//! Serializes `PerBoxIndexes` (text BM25 + HNSW per-path entries +
//! agent-events index) to an object-storage-backed BSON blob so a
//! restarting process can avoid the ~33-minute cold-start rebuild that
//! the BoTE identified as M16's primary motivation.
//!
//! Layout on object storage:
//!
//! ```text
//! indexes/{box_id:04x}/snap-{hlc_physical_ns:016x}.bson
//! ```
//!
//! Hex keys are fixed-width so alphabetical listing is numeric order,
//! and the snapshot with the highest hex name is the newest — the same
//! convention segments already use.
//!
//! Design choice: snapshot the **normalized vector entries**, not the
//! HNSW graph. The graph rebuilds lazily on the first query at each
//! path once `BRUTE_FORCE_THRESHOLD` is crossed — so recovery cost is
//! `O(paths × threshold)` graph-builds during warm-up, not on the hot
//! path of `indexes_built`. This keeps the snapshot format stable
//! across `hnsw_rs` backend evolutions (the serialized format is
//! library-version-dependent; our serialized format is not).

use std::sync::Arc;

use agentdb_index::{TextSnapshot, VectorSnapshot};
use agentdb_storage::BoxId;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::hlc::HlcTimestamp;
use crate::indexes::PerBoxIndexes;

pub const INDEX_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Fully materialized index snapshot. What goes on the wire to/from
/// object storage. Deliberately self-contained: a recovering process
/// needs only this blob plus the HLC cutoff to reconstruct a
/// `PerBoxIndexes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSnapshot {
    pub schema_version: u32,
    pub box_id: BoxId,
    /// Cutoff: every event with `hlc_ts <= hlc_ts_at_snapshot` is
    /// captured here. Recovery replays events whose `hlc_ts >` this
    /// value on top.
    pub hlc_ts_at_snapshot: HlcTimestamp,
    pub text: TextSnapshot,
    pub vector: VectorSnapshot,
    pub agent_events: std::collections::BTreeMap<String, Vec<(u64, u64)>>,
    pub indexed: u64,
}

/// Compute the canonical object-storage path for a snapshot.
pub fn snapshot_path(box_id: BoxId, hlc_ts: HlcTimestamp) -> String {
    format!(
        "indexes/{:04x}/snap-{:016x}.bson",
        box_id, hlc_ts.physical_ns
    )
}

/// Object-storage prefix used for listing snapshots of a given box.
pub fn snapshot_prefix(box_id: BoxId) -> String {
    format!("indexes/{:04x}/", box_id)
}

/// Capture a live `PerBoxIndexes` as a serializable snapshot.
pub fn snapshot_indexes(
    indexes: &PerBoxIndexes,
    box_id: BoxId,
    hlc_ts: HlcTimestamp,
) -> IndexSnapshot {
    IndexSnapshot {
        schema_version: INDEX_SNAPSHOT_SCHEMA_VERSION,
        box_id,
        hlc_ts_at_snapshot: hlc_ts,
        text: indexes.text.to_snapshot(),
        vector: indexes.vector.to_snapshot(),
        agent_events: indexes
            .agent_events
            .iter()
            .map(|(actor, docs)| {
                (
                    actor.clone(),
                    docs.iter()
                        .map(|d| (d.session_internal_id, d.seq))
                        .collect(),
                )
            })
            .collect(),
        indexed: indexes.indexed,
    }
}

/// Rebuild a `PerBoxIndexes` from a snapshot. The HNSW graphs are not
/// pre-built; the first query at a path crossing
/// `BRUTE_FORCE_THRESHOLD` rebuilds that path's graph from its
/// restored entries.
pub fn restore_indexes(snap: &IndexSnapshot) -> Result<PerBoxIndexes> {
    if snap.schema_version != INDEX_SNAPSHOT_SCHEMA_VERSION {
        return Err(Error::Invalid {
            field: None,
            message: format!(
                "unsupported index snapshot schema version {}, expected {}",
                snap.schema_version, INDEX_SNAPSHOT_SCHEMA_VERSION
            ),
        });
    }
    let text = agentdb_index::text::InMemoryInverted::from_snapshot(&snap.text);
    // Honor the per-env ef knob on reload so a restart respects current
    // config rather than a stale value captured at snapshot time.
    let vector = agentdb_index::vector::HnswVectorIndex::from_snapshot(
        &snap.vector,
        read_ef_from_env(),
    );
    let agent_events = snap
        .agent_events
        .iter()
        .map(|(actor, docs)| {
            (
                actor.clone(),
                docs.iter()
                    .map(|(s, q)| agentdb_index::DocId::new(*s, *q))
                    .collect(),
            )
        })
        .collect();
    Ok(PerBoxIndexes::from_parts(
        text,
        vector,
        agent_events,
        snap.indexed,
    ))
}

fn read_ef_from_env() -> usize {
    std::env::var("AGENTDB_HNSW_EF")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(agentdb_index::vector::DEFAULT_EF)
}

/// Write a snapshot to object storage. Uses BSON (same format as
/// segments) — large but portable and self-describing.
pub async fn write_snapshot(
    store: &Arc<dyn ObjectStore>,
    snapshot: &IndexSnapshot,
) -> Result<String> {
    let path = snapshot_path(snapshot.box_id, snapshot.hlc_ts_at_snapshot);
    let doc = bson::serialize_to_document(snapshot).map_err(|e| Error::Invalid {
        field: None,
        message: format!("serialize IndexSnapshot: {e}"),
    })?;
    let bytes = bson::serialize_to_vec(&doc).map_err(|e| Error::Invalid {
        field: None,
        message: format!("encode IndexSnapshot bson: {e}"),
    })?;
    let op = ObjectPath::from(path.as_str());
    store
        .put(&op, bytes.into())
        .await
        .map_err(|e| Error::Invalid {
            field: None,
            message: format!("object_store put IndexSnapshot: {e}"),
        })?;
    Ok(path)
}

/// Read a snapshot from object storage.
pub async fn read_snapshot(store: &Arc<dyn ObjectStore>, path: &str) -> Result<IndexSnapshot> {
    let op = ObjectPath::from(path);
    let get = store.get(&op).await.map_err(|e| Error::Invalid {
        field: None,
        message: format!("object_store get IndexSnapshot: {e}"),
    })?;
    let bytes = get.bytes().await.map_err(|e| Error::Invalid {
        field: None,
        message: format!("object_store read IndexSnapshot: {e}"),
    })?;
    let doc: bson::Document = bson::deserialize_from_slice(&bytes).map_err(|e| Error::Invalid {
        field: None,
        message: format!("bson decode IndexSnapshot: {e}"),
    })?;
    let snap: IndexSnapshot = bson::deserialize_from_document(doc).map_err(|e| Error::Invalid {
        field: None,
        message: format!("deserialize IndexSnapshot: {e}"),
    })?;
    if snap.schema_version != INDEX_SNAPSHOT_SCHEMA_VERSION {
        return Err(Error::Invalid {
            field: None,
            message: format!(
                "unsupported index snapshot schema version {}, expected {}",
                snap.schema_version, INDEX_SNAPSHOT_SCHEMA_VERSION
            ),
        });
    }
    Ok(snap)
}

/// List snapshot paths for a box, newest first (by HLC physical ns).
pub async fn list_snapshots(
    store: &Arc<dyn ObjectStore>,
    box_id: BoxId,
) -> Result<Vec<String>> {
    use futures_util::StreamExt;

    let prefix = snapshot_prefix(box_id);
    let prefix_path = ObjectPath::from(prefix.as_str());
    let mut stream = store.list(Some(&prefix_path));
    let mut paths: Vec<String> = Vec::new();
    while let Some(res) = stream.next().await {
        let meta = res.map_err(|e| Error::Invalid {
            field: None,
            message: format!("object_store list: {e}"),
        })?;
        paths.push(meta.location.to_string());
    }
    // Lexicographic descending == newest first (hex HLC is fixed-width).
    paths.sort_by(|a, b| b.cmp(a));
    Ok(paths)
}

/// The highest-HLC snapshot path for a box, or `None` if no snapshots
/// exist.
pub async fn latest_snapshot_path(
    store: &Arc<dyn ObjectStore>,
    box_id: BoxId,
) -> Result<Option<String>> {
    Ok(list_snapshots(store, box_id).await?.into_iter().next())
}

/// Delete snapshots older than the `retain` most recent. No-op when the
/// count is under the retention bar. Called from the periodic job so
/// object storage doesn't accumulate snapshots forever.
pub async fn prune_old_snapshots(
    store: &Arc<dyn ObjectStore>,
    box_id: BoxId,
    retain: usize,
) -> Result<usize> {
    let paths = list_snapshots(store, box_id).await?;
    if paths.len() <= retain {
        return Ok(0);
    }
    let mut removed = 0;
    for p in paths.iter().skip(retain) {
        let op = ObjectPath::from(p.as_str());
        if let Err(e) = store.delete(&op).await {
            tracing::warn!(path = %p, error = %e, "index_snapshot prune: delete failed");
            continue;
        }
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn mem_store() -> (TempDir, Arc<dyn ObjectStore>) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        (dir, store)
    }

    #[test]
    fn path_is_fixed_width_hex() {
        let p = snapshot_path(
            3,
            HlcTimestamp {
                physical_ns: 0x1234_5678,
                logical: 7,
            },
        );
        assert_eq!(p, "indexes/0003/snap-0000000012345678.bson");
    }

    #[tokio::test]
    async fn snapshot_write_read_round_trip() {
        let (_d, store) = mem_store();
        let mut indexes = PerBoxIndexes::new();
        // Inject a trivially-reconstructible event so BM25 + actor index
        // end up with content to roundtrip.
        let ev = crate::types::Event {
            seq: 1,
            ts: 0,
            hlc_ts: HlcTimestamp::ZERO,
            actor_id: Some("agent-x".into()),
            payload: serde_json::json!({"text": "hello world", "embedding": [1.0_f32, 0.0, 0.0]}),
        };
        indexes.index_event(42, &ev);

        let hlc = HlcTimestamp {
            physical_ns: 999,
            logical: 0,
        };
        let snap = snapshot_indexes(&indexes, 7, hlc);
        let written_path = write_snapshot(&store, &snap).await.unwrap();

        let snap2 = read_snapshot(&store, &written_path).await.unwrap();
        assert_eq!(snap2.schema_version, INDEX_SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(snap2.box_id, 7);
        assert_eq!(snap2.hlc_ts_at_snapshot, hlc);
        assert_eq!(snap2.indexed, 1);

        let restored = restore_indexes(&snap2).unwrap();
        // Text query "hello" finds the one event
        use agentdb_index::text::TextIndex;
        let hits = restored.text.search("payload.text", "hello", 10);
        assert_eq!(hits.len(), 1, "restored text index should serve query");
        // Agent index preserved
        assert_eq!(restored.agent_events.get("agent-x").map(|v| v.len()), Some(1));
    }

    #[tokio::test]
    async fn latest_snapshot_picks_highest_hlc() {
        let (_d, store) = mem_store();
        let indexes = PerBoxIndexes::new();

        // Three snapshots with increasing HLC. The latest helper should
        // pick the newest.
        for ns in [100u64, 500, 300] {
            let snap = snapshot_indexes(
                &indexes,
                2,
                HlcTimestamp {
                    physical_ns: ns,
                    logical: 0,
                },
            );
            write_snapshot(&store, &snap).await.unwrap();
        }
        let latest = latest_snapshot_path(&store, 2).await.unwrap().unwrap();
        assert!(
            latest.contains(&format!("{:016x}", 500u64)),
            "expected latest to be the 500ns snapshot; got {latest}"
        );
    }

    #[tokio::test]
    async fn prune_old_snapshots_retains_n_newest() {
        let (_d, store) = mem_store();
        let indexes = PerBoxIndexes::new();
        for ns in 1..=5u64 {
            let snap = snapshot_indexes(
                &indexes,
                3,
                HlcTimestamp {
                    physical_ns: ns * 100,
                    logical: 0,
                },
            );
            write_snapshot(&store, &snap).await.unwrap();
        }
        let removed = prune_old_snapshots(&store, 3, 2).await.unwrap();
        assert_eq!(removed, 3, "should remove 5 - 2 = 3 older snapshots");

        let remaining = list_snapshots(&store, 3).await.unwrap();
        assert_eq!(remaining.len(), 2);
        // Newest two remain — 500 and 400.
        assert!(remaining[0].contains(&format!("{:016x}", 500u64)));
        assert!(remaining[1].contains(&format!("{:016x}", 400u64)));
    }

    #[tokio::test]
    async fn unsupported_schema_version_is_rejected_on_read() {
        let (_d, store) = mem_store();
        let indexes = PerBoxIndexes::new();
        let mut snap = snapshot_indexes(&indexes, 5, HlcTimestamp::ZERO);
        snap.schema_version = 99;
        let path = write_snapshot(&store, &snap).await.unwrap();
        let err = read_snapshot(&store, &path).await.unwrap_err();
        assert!(err.to_string().contains("schema version"));
    }
}
