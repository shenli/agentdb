use std::sync::Arc;

use agentdb_index::text::{InMemoryInverted, TextIndex};
use agentdb_index::vector::{InMemoryFlatVec, VectorIndex};
use agentdb_index::DocId;
use agentdb_storage::{BoxId, KvStore};

use crate::error::Result;
use crate::primitives::scan_session_events;
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::types::Event;

/// Per-box index state: one text index + one vector index, both in-memory.
pub struct PerBoxIndexes {
    pub text: InMemoryInverted,
    pub vector: InMemoryFlatVec,
    /// Number of events indexed (for diagnostics).
    pub indexed: u64,
}

impl Default for PerBoxIndexes {
    fn default() -> Self {
        Self::new()
    }
}

impl PerBoxIndexes {
    pub fn new() -> Self {
        Self {
            text: InMemoryInverted::new(),
            vector: InMemoryFlatVec::new(),
            indexed: 0,
        }
    }

    /// Scan every session in `box_id`, pull all events, index them.
    pub fn build(
        &mut self,
        kv: &Arc<dyn KvStore>,
        tenants: &TenantRegistry,
        sessions: &SessionRegistry,
        box_id: BoxId,
    ) -> Result<()> {
        let tenant_externals = tenants.list_in_box(box_id)?;
        for tenant_ext in tenant_externals {
            let Some(tenant) = tenants.lookup(box_id, &tenant_ext)? else {
                continue;
            };
            let session_externals = sessions.list_in_tenant(box_id, tenant.internal_id)?;
            for session_ext in session_externals {
                let Some(session) = sessions.lookup(box_id, tenant.internal_id, &session_ext)?
                else {
                    continue;
                };
                for event in scan_session_events(kv, &session)? {
                    self.index_event(session.internal_id, &event);
                }
            }
        }
        Ok(())
    }

    /// Index one event under the indexes' by-path structure. The top-level
    /// prefix is `"payload"` so callers use natural paths like
    /// `"payload.text"` and `"payload.embedding"`.
    fn index_event(&mut self, session_internal_id: u64, event: &Event) {
        let doc_id = DocId::new(session_internal_id, event.seq);
        self.walk(doc_id, "payload", &event.payload);
        self.indexed += 1;
    }

    fn walk(&mut self, doc_id: DocId, path: &str, value: &serde_json::Value) {
        match value {
            serde_json::Value::String(s) => self.text.insert(path, doc_id, s),
            serde_json::Value::Array(a) => {
                if let Some(v) = as_float_vec(a) {
                    if (2..=2048).contains(&v.len()) {
                        let _ = self.vector.insert(path, doc_id, &v);
                    }
                }
            }
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    let child = format!("{path}.{k}");
                    self.walk(doc_id, &child, v);
                }
            }
            _ => {}
        }
    }
}

fn as_float_vec(a: &[serde_json::Value]) -> Option<Vec<f32>> {
    let mut out = Vec::with_capacity(a.len());
    for v in a {
        let n = v.as_f64()?;
        out.push(n as f32);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::RocksKvStore;
    use serde_json::json;
    use tempfile::TempDir;

    async fn setup_with_events() -> (
        TempDir,
        TempDir,
        Arc<dyn KvStore>,
        Arc<crate::BoxRegistry>,
        TenantRegistry,
        SessionRegistry,
    ) {
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(crate::BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = TenantRegistry::new(kv.clone());
        let sessions = SessionRegistry::new(kv.clone());
        (dir, obj_dir, kv, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn build_indexes_text_from_payload_text_field() {
        let (_dir, _obj_dir, kv, registry, tenants, sessions) = setup_with_events().await;
        crate::primitives::append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"text": "refund policy for shipping"}),
            0,
            0,
        )
        .await
        .unwrap();
        let box_id = registry.route_tenant("t-1").id;
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &tenants, &sessions, box_id).unwrap();
        let hits = idx.text.search("payload.text", "refund", 10);
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn build_indexes_vector_from_payload_embedding_array() {
        let (_dir, _obj_dir, kv, registry, tenants, sessions) = setup_with_events().await;
        crate::primitives::append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"embedding": [1.0, 0.0, 0.0, 0.0]}),
            0,
            0,
        )
        .await
        .unwrap();
        let box_id = registry.route_tenant("t-1").id;
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &tenants, &sessions, box_id).unwrap();
        let hits = idx
            .vector
            .knn("payload.embedding", &[1.0, 0.0, 0.0, 0.0], 10);
        assert_eq!(hits.len(), 1);
        assert!(hits[0].1 > 0.99);
    }

    #[tokio::test]
    async fn build_indexes_counts_across_sessions_and_tenants() {
        let (_dir, _obj_dir, kv, registry, tenants, sessions) = setup_with_events().await;
        for i in 0..3 {
            crate::primitives::append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"text": format!("hello {i}")}),
                0,
                i,
            )
            .await
            .unwrap();
        }
        let box_id = registry.route_tenant("t-1").id;
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &tenants, &sessions, box_id).unwrap();
        assert_eq!(idx.indexed, 3);
    }
}
