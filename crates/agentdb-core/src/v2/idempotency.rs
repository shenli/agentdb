//! Idempotency record store for V2 mutating operations.
//!
//! Scope is `(tenant_id, operation, target, idempotency_key)`. The full key is
//! composed by `Runtime::append` (and future tasks for artifact/record/skill/
//! policy mutations) before consulting this store.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct IdempotencyRecord {
    pub payload_hash: u64,
    pub result_json: String,
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
}

/// Hash a JSON payload for idempotency-conflict detection. Two requests with
/// the same idempotency key but different payloads must produce different
/// hashes; same payload must produce same hash.
pub fn hash_payload(payload: &serde_json::Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(payload).unwrap().hash(&mut h);
    h.finish()
}
