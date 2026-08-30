//! Idempotency record store for V2 mutating operations.
//!
//! Scope is `(tenant_id, operation, target, idempotency_key)`. The full key is
//! composed by `Runtime::append` (and future tasks for artifact/record/skill/
//! policy mutations) before consulting this store.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::event::{ArtifactRef, CredentialRef, SourceRef};
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
    puts_since_eviction: std::sync::atomic::AtomicU64,
}

/// Retention knobs for the in-memory idempotency table. Every cross-component
/// map is a quota site: without a TTL and cap this is an unbounded queue that
/// grows with every keyed mutation for the life of the process.
pub const DEFAULT_IDEMPOTENCY_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
pub const DEFAULT_IDEMPOTENCY_MAX_ENTRIES: usize = 1_000_000;
const EVICTION_EVERY_N_PUTS: u64 = 4_096;

pub fn idempotency_ttl_ms() -> u64 {
    std::env::var("AGENTDB_IDEMPOTENCY_TTL_MS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_IDEMPOTENCY_TTL_MS)
}

pub fn idempotency_max_entries() -> usize {
    std::env::var("AGENTDB_IDEMPOTENCY_MAX_ENTRIES")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_IDEMPOTENCY_MAX_ENTRIES)
}

impl IdempotencyStore {
    pub fn lookup(&self, key: &str) -> Option<IdempotencyRecord> {
        self.inner.lock().unwrap().get(key).cloned()
    }
    pub fn put(&self, key: String, record: IdempotencyRecord) {
        self.inner.lock().unwrap().insert(key, record);
        let puts = self
            .puts_since_eviction
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if puts != 0 && puts % EVICTION_EVERY_N_PUTS == 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.evict(now_ms, idempotency_ttl_ms(), idempotency_max_entries());
        }
    }

    /// Drop expired records, then enforce the entry cap by evicting the
    /// oldest survivors. Returns the keys removed so the caller can also
    /// delete the corresponding durable rows.
    pub fn evict(&self, now_ms: u64, ttl_ms: u64, max_entries: usize) -> Vec<String> {
        let mut g = self.inner.lock().unwrap();
        let mut removed = Vec::new();
        g.retain(|key, record| {
            let expired = now_ms.saturating_sub(record.created_at_ms) > ttl_ms;
            if expired {
                removed.push(key.clone());
            }
            !expired
        });
        if g.len() > max_entries {
            let mut by_age: Vec<(u64, String)> = g
                .iter()
                .map(|(key, record)| (record.created_at_ms, key.clone()))
                .collect();
            by_age.sort_unstable();
            for (_, key) in by_age.into_iter().take(g.len() - max_entries) {
                g.remove(&key);
                removed.push(key);
            }
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
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

    /// Replace all in-memory idempotency records during runtime rehydration.
    pub fn replace<I>(&self, records: I)
    where
        I: IntoIterator<Item = (String, IdempotencyRecord)>,
    {
        let mut g = self.inner.lock().unwrap();
        g.clear();
        for (k, v) in records {
            g.insert(k, v);
        }
    }
}

/// Hash the full append-request shape for idempotency-conflict detection.
///
/// We must include every field the caller can vary — not just `payload` —
/// because a retry that changes `kind`, refs, or the (tenant, agent, session)
/// routing tuple is a *different* request that must surface as a conflict, not
/// silently replay the original commit.
///
/// The implementation builds a canonical JSON object and hashes its string
/// form. JSON object key order is fixed by the literal here, so identical
/// inputs produce identical hashes.
#[allow(clippy::too_many_arguments)]
pub fn hash_request(
    tenant_id: &Id,
    agent_id: &Id,
    session_id: &Id,
    kind: &str,
    payload: &serde_json::Value,
    artifact_refs: &[ArtifactRef],
    source_refs: &[SourceRef],
    credential_refs: &[CredentialRef],
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
        "credential_refs": credential_refs,
    });
    let mut h = siphasher::sip::SipHasher13::new();
    serde_json::to_string(&canon).unwrap().hash(&mut h);
    h.finish()
}

/// Hash an arbitrary canonical JSON value for non-append idempotent
/// operations. Callers should build the value with every field that affects
/// the mutation result.
pub fn hash_json(value: &serde_json::Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = siphasher::sip::SipHasher13::new();
    serde_json::to_string(value).unwrap().hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_hash_algorithm_is_pinned_to_legacy_default_hasher_values() {
        use std::hash::{Hash, Hasher};

        let mut hasher = siphasher::sip::SipHasher13::new();
        "hello".hash(&mut hasher);
        assert_eq!(hasher.finish(), 16_156_531_084_128_653_017);

        assert_eq!(
            hash_json(&serde_json::json!({"a": 1})),
            14_886_008_067_637_593_304
        );
    }

    #[test]
    fn evict_removes_expired_and_enforces_cap() {
        let store = IdempotencyStore::default();
        for i in 0..10u64 {
            store.put(
                format!("key-{i}"),
                IdempotencyRecord {
                    payload_hash: i,
                    result_json: "{}".into(),
                    created_at_ms: i * 100,
                },
            );
        }
        // TTL pass: records older than 500ms at now=1000 (created <= 400).
        let removed = store.evict(1_000, 500, 100);
        assert_eq!(removed.len(), 5);
        assert_eq!(store.len(), 5);
        assert!(store.lookup("key-4").is_none());
        assert!(store.lookup("key-5").is_some());

        // Cap pass: shrink to the 2 newest survivors.
        let removed = store.evict(1_000, u64::MAX, 2);
        assert_eq!(removed.len(), 3);
        assert_eq!(store.len(), 2);
        assert!(store.lookup("key-8").is_some());
        assert!(store.lookup("key-9").is_some());
    }
}
