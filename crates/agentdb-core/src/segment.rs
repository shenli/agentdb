use std::sync::Arc;
use std::time::Duration;

use bson::Document;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::hedge::{hedged, Winner};
use crate::metrics::BoxMetrics;
use crate::types::{Event, Tier};

/// On-disk segment schema version. Incremented whenever the binary layout
/// of a `SegmentFile` changes incompatibly. Readers reject any other
/// value; AgentDB is pre-1.0 so earlier on-disk layouts are not supported.
///
/// v2 (current): events carry `hlc_ts`. Bumped in M14.
pub const SEGMENT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentFile {
    pub schema_version: u32,
    pub session_internal_id: u64,
    pub events: Vec<Event>,
}

/// Build the object-store path for a session's segment. Fixed-width hex so
/// alphabetical listings match numeric ordering.
pub fn segment_path(
    box_id: u16,
    tenant_internal_id: u64,
    session_internal_id: u64,
    epoch_ms: u64,
) -> String {
    format!(
        "boxes/{:04x}/cold/{:016x}/{:016x}/segment-{}.bson",
        box_id, tenant_internal_id, session_internal_id, epoch_ms
    )
}

pub async fn write_segment(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    segment: &SegmentFile,
) -> Result<()> {
    let doc = bson::serialize_to_document(segment).map_err(|e| Error::Invalid {
        field: None,
        message: e.to_string(),
    })?;
    let bytes = bson::serialize_to_vec(&doc).map_err(|e| Error::Invalid {
        field: None,
        message: e.to_string(),
    })?;
    let p = ObjectPath::from(path);
    store
        .put(&p, bytes.into())
        .await
        .map_err(|e| Error::Invalid {
            field: None,
            message: format!("object_store put: {e}"),
        })?;
    Ok(())
}

pub async fn read_segment(store: &Arc<dyn ObjectStore>, path: &str) -> Result<SegmentFile> {
    let p = ObjectPath::from(path);
    let get = store.get(&p).await.map_err(|e| Error::Invalid {
        field: None,
        message: format!("object_store get: {e}"),
    })?;
    let bytes = get.bytes().await.map_err(|e| Error::Invalid {
        field: None,
        message: format!("object_store read: {e}"),
    })?;
    let doc: Document = bson::deserialize_from_slice(&bytes).map_err(|e| Error::Invalid {
        field: None,
        message: format!("bson decode: {e}"),
    })?;
    let seg: SegmentFile = bson::deserialize_from_document(doc).map_err(|e| Error::Invalid {
        field: None,
        message: e.to_string(),
    })?;
    if seg.schema_version != SEGMENT_SCHEMA_VERSION {
        return Err(Error::Invalid {
            field: None,
            message: format!(
                "unsupported segment schema version {}, expected {}",
                seg.schema_version, SEGMENT_SCHEMA_VERSION
            ),
        });
    }
    Ok(seg)
}

/// Hedged variant of `read_segment` for tail-latency protection on
/// cold/archive tiers. Fires a primary read; if it hasn't completed
/// within `delay`, fires a secondary read against the same path. First
/// to finish wins; the loser is cancelled via drop.
///
/// Configuration (`hedge_delay_for`):
/// - `Tier::Warm` → hedging disabled (returns the primary result).
/// - `Tier::Cold` → `AGENTDB_HEDGE_COLD_MS` (default 50).
/// - `Tier::Archive` → `AGENTDB_HEDGE_ARCHIVE_MS` (default 200).
///
/// Records `hedged_triggers` / `hedged_primary_wins` /
/// `hedged_secondary_wins` on `BoxMetrics`. Honors the
/// `AGENTDB_HEDGE_ENABLED` kill-switch (`false` bypasses hedging).
#[tracing::instrument(
    name = "read_segment_hedged",
    skip(store, metrics),
    fields(
        tier = ?tier,
        path,
        hedge_triggered = tracing::field::Empty,
        hedge_winner = tracing::field::Empty,
    ),
)]
pub async fn read_segment_hedged(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    tier: Tier,
    metrics: &BoxMetrics,
) -> Result<SegmentFile> {
    if !hedge_enabled() {
        return read_segment(store, path).await;
    }
    let Some(delay) = hedge_delay_for(tier) else {
        return read_segment(store, path).await;
    };
    let store2 = store.clone();
    let path2 = path.to_string();
    let result = hedged(
        read_segment(store, path),
        delay,
        move || async move { read_segment(&store2, &path2).await },
    )
    .await?;
    let span = tracing::Span::current();
    span.record("hedge_triggered", result.triggered);
    if result.triggered {
        metrics.record_hedged_trigger();
        let winner = match result.winner {
            Winner::Primary => {
                metrics.record_hedged_primary_win();
                "primary"
            }
            Winner::Secondary => {
                metrics.record_hedged_secondary_win();
                "secondary"
            }
        };
        span.record("hedge_winner", winner);
    }
    Ok(result.value)
}

/// Per-tier hedge delay, `None` for tiers where hedging is disabled.
fn hedge_delay_for(tier: Tier) -> Option<Duration> {
    match tier {
        Tier::Warm => None,
        Tier::Cold => Some(Duration::from_millis(env_ms(
            "AGENTDB_HEDGE_COLD_MS",
            50,
        ))),
        Tier::Archive => Some(Duration::from_millis(env_ms(
            "AGENTDB_HEDGE_ARCHIVE_MS",
            200,
        ))),
    }
}

fn hedge_enabled() -> bool {
    !matches!(
        std::env::var("AGENTDB_HEDGE_ENABLED").as_deref(),
        Ok("0") | Ok("false") | Ok("FALSE") | Ok("False")
    )
}

fn env_ms(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

pub async fn delete_segment(store: &Arc<dyn ObjectStore>, path: &str) -> Result<()> {
    let p = ObjectPath::from(path);
    store.delete(&p).await.map_err(|e| Error::Invalid {
        field: None,
        message: format!("object_store delete: {e}"),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::HlcTimestamp;
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use tempfile::TempDir;

    fn mem_store() -> (TempDir, Arc<dyn ObjectStore>) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        (dir, store)
    }

    #[tokio::test]
    async fn segment_roundtrip_preserves_events_with_hlc() {
        let (_d, store) = mem_store();
        let hlc = HlcTimestamp {
            physical_ns: 1_700_000_000_111_222_333,
            logical: 4,
        };
        let seg = SegmentFile {
            schema_version: SEGMENT_SCHEMA_VERSION,
            session_internal_id: 7,
            events: vec![
                Event {
                    seq: 1,
                    ts: 100,
                    hlc_ts: hlc,
                    actor_id: None,
                    payload: json!({"text": "hello"}),
                },
                Event {
                    seq: 2,
                    ts: 200,
                    hlc_ts: HlcTimestamp {
                        physical_ns: hlc.physical_ns + 1,
                        logical: 0,
                    },
                    actor_id: Some("agent-builder".into()),
                    payload: json!({"embedding": [1.0, 0.0]}),
                },
            ],
        };
        let path = segment_path(3, 42, 7, 1_700_000_000_000);
        write_segment(&store, &path, &seg).await.unwrap();
        let back = read_segment(&store, &path).await.unwrap();
        assert_eq!(back.schema_version, SEGMENT_SCHEMA_VERSION);
        assert_eq!(back.session_internal_id, 7);
        assert_eq!(back.events.len(), 2);
        assert_eq!(back.events[0].hlc_ts, hlc);
        assert_eq!(back.events[0].payload, json!({"text": "hello"}));
        assert_eq!(back.events[1].payload, json!({"embedding": [1.0, 0.0]}));
    }

    #[tokio::test]
    async fn read_segment_with_unsupported_schema_errors() {
        let (_d, store) = mem_store();
        let seg = SegmentFile {
            schema_version: 99,
            session_internal_id: 1,
            events: vec![],
        };
        let path = segment_path(1, 1, 1, 1);
        write_segment(&store, &path, &seg).await.unwrap();
        let err = read_segment(&store, &path).await.unwrap_err();
        assert!(err.to_string().contains("schema version"));
    }

    #[tokio::test]
    async fn delete_removes_segment() {
        let (_d, store) = mem_store();
        let seg = SegmentFile {
            schema_version: SEGMENT_SCHEMA_VERSION,
            session_internal_id: 1,
            events: vec![],
        };
        let path = segment_path(1, 1, 1, 1);
        write_segment(&store, &path, &seg).await.unwrap();
        delete_segment(&store, &path).await.unwrap();
        assert!(read_segment(&store, &path).await.is_err());
    }

    #[test]
    fn segment_path_is_fixed_width_hex() {
        let p = segment_path(3, 42, 7, 1_700_000_000_000);
        assert_eq!(
            p,
            "boxes/0003/cold/000000000000002a/0000000000000007/segment-1700000000000.bson"
        );
    }
}
