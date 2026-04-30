//! Idempotency record store for V2 mutating operations.
//!
//! Scope is `(tenant_id, operation, target, idempotency_key)`. The full key is
//! composed by `Runtime::append` (and future tasks for artifact/record/skill/
//! policy mutations) before consulting this store.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::event::{ArtifactRef, SourceRef};
use crate::id::Id;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub payload_hash: u64,
    pub result_json: String,
    #[serde(default)]
    pub created_at_ms: u64,
}

#[derive(Default, Debug)]
pub struct IdempotencyStore {
    inner: Mutex<HashMap<String, IdempotencyRecord>>,
}

impl IdempotencyStore {
    pub fn lookup(&self, key: &str) -> Option<IdempotencyRecord> {
        self.inner.lock().unwrap().get(key).cloned()
    }
    pub fn put(&self, key: String, record: IdempotencyRecord) {
        self.inner.lock().unwrap().insert(key, record);
    }
    pub fn remove(&self, key: &str) {
        self.inner.lock().unwrap().remove(key);
    }
    pub fn entries(&self) -> Vec<(String, IdempotencyRecord)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    /// Bulk-load records (used by `Runtime::with_kv` to hydrate the in-memory
    /// idempotency table from persisted state at startup).
    pub fn extend<I>(&self, records: I)
    where
        I: IntoIterator<Item = (String, IdempotencyRecord)>,
    {
        let mut g = self.inner.lock().unwrap();
        for (k, v) in records {
            g.insert(k, v);
        }
    }
}

/// Hash the full append-request shape for idempotency-conflict detection.
///
/// We must include every field the caller can vary — not just `payload` —
/// because a retry that changes `kind`, `artifact_refs`, `source_refs`, or
/// the (tenant, agent, session) routing tuple is a *different* request that
/// must surface as a conflict, not silently replay the original commit.
///
/// The implementation builds a canonical JSON object and hashes its string
/// form. JSON object key order is fixed by the literal here, so identical
/// inputs produce identical hashes.
pub fn hash_request(
    tenant_id: &Id,
    agent_id: &Id,
    session_id: &Id,
    kind: &str,
    payload: &serde_json::Value,
    artifact_refs: &[ArtifactRef],
    source_refs: &[SourceRef],
) -> u64 {
    use std::hash::{Hash, Hasher};
    let canon = serde_json::json!({
        "tenant_id": tenant_id.as_str(),
        "agent_id": agent_id.as_str(),
        "session_id": session_id.as_str(),
        "kind": kind,
        "payload": payload,
        "artifact_refs": artifact_refs,
        "source_refs": source_refs,
    });
    let mut h = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(&canon).unwrap().hash(&mut h);
    h.finish()
}

/// Hash an arbitrary canonical JSON value for non-append idempotent
/// operations. Callers should build the value with every field that affects
/// the mutation result.
pub fn hash_json(value: &serde_json::Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(value).unwrap().hash(&mut h);
    h.finish()
}
