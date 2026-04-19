use std::sync::Arc;

use bson::Document;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::types::Event;

pub const SEGMENT_SCHEMA_V1: u32 = 1;

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
    let doc = bson::serialize_to_document(segment).map_err(|e| Error::Invalid(e.to_string()))?;
    let bytes = bson::serialize_to_vec(&doc).map_err(|e| Error::Invalid(e.to_string()))?;
    let p = ObjectPath::from(path);
    store
        .put(&p, bytes.into())
        .await
        .map_err(|e| Error::Invalid(format!("object_store put: {e}")))?;
    Ok(())
}

pub async fn read_segment(store: &Arc<dyn ObjectStore>, path: &str) -> Result<SegmentFile> {
    let p = ObjectPath::from(path);
    let get = store
        .get(&p)
        .await
        .map_err(|e| Error::Invalid(format!("object_store get: {e}")))?;
    let bytes = get
        .bytes()
        .await
        .map_err(|e| Error::Invalid(format!("object_store read: {e}")))?;
    let doc: Document = bson::deserialize_from_slice(&bytes)
        .map_err(|e| Error::Invalid(format!("bson decode: {e}")))?;
    let seg: SegmentFile =
        bson::deserialize_from_document(doc).map_err(|e| Error::Invalid(e.to_string()))?;
    if seg.schema_version != SEGMENT_SCHEMA_V1 {
        return Err(Error::Invalid(format!(
            "unknown segment schema version {}",
            seg.schema_version
        )));
    }
    Ok(seg)
}

pub async fn delete_segment(store: &Arc<dyn ObjectStore>, path: &str) -> Result<()> {
    let p = ObjectPath::from(path);
    store
        .delete(&p)
        .await
        .map_err(|e| Error::Invalid(format!("object_store delete: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn segment_roundtrip_preserves_events() {
        let (_d, store) = mem_store();
        let seg = SegmentFile {
            schema_version: SEGMENT_SCHEMA_V1,
            session_internal_id: 7,
            events: vec![
                Event {
                    seq: 1,
                    ts: 100,
                    payload: json!({"text": "hello"}),
                },
                Event {
                    seq: 2,
                    ts: 200,
                    payload: json!({"embedding": [1.0, 0.0]}),
                },
            ],
        };
        let path = segment_path(3, 42, 7, 1_700_000_000_000);
        write_segment(&store, &path, &seg).await.unwrap();
        let back = read_segment(&store, &path).await.unwrap();
        assert_eq!(back.session_internal_id, 7);
        assert_eq!(back.events.len(), 2);
        assert_eq!(back.events[0].payload, json!({"text": "hello"}));
        assert_eq!(back.events[1].payload, json!({"embedding": [1.0, 0.0]}));
    }

    #[tokio::test]
    async fn read_segment_with_unknown_schema_errors() {
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
            schema_version: SEGMENT_SCHEMA_V1,
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
