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
//! `append_batch` is the only multi-key write and uses `KvStore::write_batch`
//! so the event row, the updated session row, and the idempotency record (if
//! any) all become visible together. This preserves the Task 9 invariant —
//! idempotency record is committed in the same atomic boundary as the event +
//! session — across crashes.
//!
//! ## Hydration
//!
//! `scan_all_tenants`/`scan_all_agents`/`scan_all_sessions` collect all live
//! records under their respective `record/{tenant,agent,session}/` prefixes.
//! Events are NOT preloaded; `scan_events` is invoked lazily by
//! `Runtime::fetch_from_seq` and `append_and_fetch` on-demand.

use std::sync::Arc;

use agentdb_storage::keys::{
    agent_record_key, event_key, session_record_key, tenant_record_key,
};
use agentdb_storage::{KvStore, Result as KvResult, WriteOp};

use crate::agent::Agent;
use crate::event::EventEnvelope;
use crate::idempotency::IdempotencyRecord;
use crate::session::Session;
use crate::tenant::Tenant;

pub(crate) struct V2Persist {
    pub kv: Arc<dyn KvStore + Send + Sync>,
}

impl V2Persist {
    pub fn new(kv: Arc<dyn KvStore + Send + Sync>) -> Self {
        Self { kv }
    }

    // ----- single-record writes -----

    pub fn put_tenant(&self, t: &Tenant) -> KvResult<()> {
        let key = tenant_record_key(t.tenant_id.as_str());
        let value = serde_json::to_vec(t).expect("tenant serializes");
        self.kv.put(key.as_bytes(), &value)
    }

    pub fn put_agent(&self, a: &Agent) -> KvResult<()> {
        let key = agent_record_key(a.tenant_id.as_str(), a.agent_id.as_str());
        let value = serde_json::to_vec(a).expect("agent serializes");
        self.kv.put(key.as_bytes(), &value)
    }

    pub fn put_session(&self, s: &Session) -> KvResult<()> {
        let key = session_record_key(s.tenant_id.as_str(), s.session_id.as_str());
        let value = serde_json::to_vec(s).expect("session serializes");
        self.kv.put(key.as_bytes(), &value)
    }

    /// Atomic multi-write for the append commit path. Bundles the event row,
    /// the updated session row, and (optionally) the idempotency record so
    /// that on crash recovery either all three are visible or none are.
    pub fn append_batch(
        &self,
        event: &EventEnvelope,
        session: &Session,
        idem: Option<(&str, &IdempotencyRecord)>,
    ) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(3);
        let ek = event_key(event.tenant_id.as_str(), event.session_id.as_str(), event.seq);
        ops.push(WriteOp::Put {
            key: ek.into_bytes(),
            value: serde_json::to_vec(event).expect("event serializes"),
        });
        let sk = session_record_key(session.tenant_id.as_str(), session.session_id.as_str());
        ops.push(WriteOp::Put {
            key: sk.into_bytes(),
            value: serde_json::to_vec(session).expect("session serializes"),
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
        self.kv.write_batch(ops)
    }

    // ----- reads -----

    pub fn scan_events(
        &self,
        tenant_id: &str,
        session_id: &str,
        from_seq: u64,
        limit: usize,
    ) -> KvResult<Vec<EventEnvelope>> {
        // KvStore::scan_prefix uses its argument as both the starting key AND
        // the prefix-filter, so we cannot seek to `event_key(.., from_seq)`
        // and still see later keys in the same session — they would not start
        // with the seq-specific stem. Scan the full session-scoped prefix and
        // filter the lower bound client-side. For V2.0 sessions are small.
        let prefix = format!("event/{tenant_id}/{session_id}/");
        let iter = self.kv.scan_prefix(prefix.as_bytes())?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let env: EventEnvelope =
                serde_json::from_slice(&v).expect("event envelope decodes");
            if env.seq < from_seq {
                continue;
            }
            out.push(env);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
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
            let env: EventEnvelope =
                serde_json::from_slice(&v).expect("event envelope decodes");
            max_seq = Some(match max_seq {
                Some(m) if m >= env.seq => m,
                _ => env.seq,
            });
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
}
