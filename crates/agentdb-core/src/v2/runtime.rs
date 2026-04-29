//! V2 runtime — atomic append, idempotency, basic in-memory state.
//!
//! Task 11 will replace the in-memory state with KV-backed persistence; the
//! public API surface (Runtime, AppendRequest, etc.) is meant to stay stable
//! across that transition.

use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

use crate::v2::id::Id;
use crate::v2::tenant::{Tenant, NewTenant};
use crate::v2::agent::{Agent, NewAgent};
use crate::v2::session::{Session, NewSession};
use crate::v2::event::{EventEnvelope, NewEvent, ArtifactRef, SourceRef};
use crate::v2::errors::{V2Error, ErrorCode};
use crate::v2::idempotency::{IdempotencyStore, IdempotencyRecord, hash_payload};

pub type V2Result<T> = Result<T, V2Error>;

#[derive(Debug, Clone)]
pub struct AppendRequest {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub session_id: Id,
    pub kind: String,
    pub payload: serde_json::Value,
    pub artifact_refs: Vec<ArtifactRef>,
    pub source_refs: Vec<SourceRef>,
    pub idempotency_key: Option<String>,
}

impl AppendRequest {
    /// Convenience for tests and simple call sites that already have references
    /// to the live tenant/agent/session.
    pub fn simple(t: &Tenant, a: &Agent, s: &Session, kind: &str, payload: serde_json::Value) -> Self {
        Self {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            session_id: s.session_id.clone(),
            kind: kind.into(),
            payload,
            artifact_refs: vec![],
            source_refs: vec![],
            idempotency_key: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub tenant_id: Id,
    pub session_id: Id,
    pub from_seq: u64,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendResponse {
    pub event_id: Id,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendAndFetchResponse {
    pub event_id: Id,
    pub seq: u64,
    pub recent: Vec<EventEnvelope>,
}

#[derive(Default, Debug)]
struct State {
    tenants: BTreeMap<String, Tenant>,
    agents: BTreeMap<String, Agent>,
    sessions: BTreeMap<String, Session>,
    /// Keyed by (session_id, seq) for ordered scan.
    events: BTreeMap<(String, u64), EventEnvelope>,
}

pub struct Runtime {
    state: Mutex<State>,
    idem: IdempotencyStore,
}

impl Runtime {
    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            idem: IdempotencyStore::default(),
        })
    }

    pub async fn create_tenant(&self, req: NewTenant) -> V2Result<Tenant> {
        let t = Tenant::create(req);
        self.state.lock().await.tenants.insert(t.tenant_id.as_str().to_string(), t.clone());
        Ok(t)
    }

    pub async fn create_agent(&self, req: NewAgent) -> V2Result<Agent> {
        let mut s = self.state.lock().await;
        if !s.tenants.contains_key(req.tenant_id.as_str()) {
            return Err(V2Error::new(ErrorCode::TenantNotFound, format!("{}", req.tenant_id)));
        }
        let a = Agent::create(req);
        s.agents.insert(a.agent_id.as_str().to_string(), a.clone());
        Ok(a)
    }

    pub async fn create_session(&self, req: NewSession) -> V2Result<Session> {
        let mut s = self.state.lock().await;
        if !s.agents.contains_key(req.agent_id.as_str()) {
            return Err(V2Error::new(ErrorCode::AgentNotFound, format!("{}", req.agent_id)));
        }
        // Tenant existence is implicit via agent ownership for V2.0; cross-check
        // is added in V2.4 when policy attachment validates the relationship.
        let sess = Session::create(req);
        s.sessions.insert(sess.session_id.as_str().to_string(), sess.clone());
        Ok(sess)
    }

    /// Test-only convenience that creates a tenant + agent + session in one shot.
    pub async fn bootstrap_test_session(&self) -> (Tenant, Agent, Session) {
        let t = self.create_tenant(NewTenant {
            external_key: "test".into(),
            policy: serde_json::json!({}),
        }).await.unwrap();
        let a = self.create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        }).await.unwrap();
        let s = self.create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        }).await.unwrap();
        (t, a, s)
    }

    pub async fn append(&self, req: AppendRequest) -> V2Result<AppendResponse> {
        // Compose the full idempotency scope key BEFORE locking state, so we can
        // short-circuit cheaply on retry.
        let idem_full_key = req.idempotency_key.as_ref().map(|k| {
            format!("{}::{}::{}::{}", req.tenant_id, "append", req.session_id, k)
        });

        // Optimistic check before locking — fast path for retries that find a
        // committed result without contending on the state mutex.
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                return verify_idem_replay(&rec, &req.payload);
            }
        }

        let mut s = self.state.lock().await;

        // Re-check under the state lock to close the concurrent-append race:
        // another task could have committed our key between the optimistic check
        // and the lock acquisition above.
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                drop(s);
                return verify_idem_replay(&rec, &req.payload);
            }
        }

        let (seq, policy_version) = {
            let session = s.sessions.get_mut(req.session_id.as_str()).ok_or_else(|| {
                V2Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id))
            })?;
            let seq = session.next_seq;
            let pv = session.policy_version;
            session.next_seq += 1;
            session.last_active_at_ms = super::now_ms();
            (seq, pv)
        };

        let envelope = EventEnvelope::create(NewEvent {
            tenant_id: req.tenant_id.clone(),
            agent_id: req.agent_id.clone(),
            session_id: req.session_id.clone(),
            seq,
            kind: req.kind.clone(),
            payload: req.payload.clone(),
            artifact_refs: req.artifact_refs.clone(),
            source_refs: req.source_refs.clone(),
            idempotency_key: req.idempotency_key.clone(),
            policy_version,
        });
        let event_id = envelope.event_id.clone();
        let key = (req.session_id.as_str().to_string(), seq);
        s.events.insert(key, envelope);

        let resp = AppendResponse { event_id, seq };

        // Record the idempotency result while still holding the state lock so
        // concurrent appends with the same key serialize cleanly: any racing
        // caller will observe this record on its under-lock re-check and replay
        // instead of committing a second event. Lock order is (state -> idem)
        // and the idempotency store never reaches back into state, so there is
        // no deadlock risk despite holding both briefly.
        if let Some(full) = idem_full_key {
            self.idem.put(
                full,
                IdempotencyRecord {
                    payload_hash: hash_payload(&req.payload),
                    result_json: serde_json::to_string(&resp).unwrap(),
                },
            );
        }
        drop(s);

        Ok(resp)
    }

    /// Append an event and return it together with up to `last` recent events
    /// from the session in ascending seq order.
    ///
    /// # Parameters
    /// - `last`: maximum number of recent events to return.
    /// - `max_bytes`: **reserved**; not enforced until V2.4 introduces budgeted
    ///   context. Callers should pass a sensible cap anyway since it will become
    ///   load-bearing without an API change.
    pub async fn append_and_fetch(
        &self,
        req: AppendRequest,
        last: usize,
        _max_bytes: usize,
    ) -> V2Result<AppendAndFetchResponse> {
        // TODO(V2.4): enforce max_bytes
        let session_id = req.session_id.clone();
        let appended = self.append(req).await?;

        let s = self.state.lock().await;
        let lo = (session_id.as_str().to_string(), 0);
        let hi = (session_id.as_str().to_string(), u64::MAX);
        let mut recent: Vec<EventEnvelope> = s
            .events
            .range(lo..=hi)
            .rev()
            .take(last)
            .map(|(_, v)| v.clone())
            .collect();
        recent.reverse();

        Ok(AppendAndFetchResponse {
            event_id: appended.event_id,
            seq: appended.seq,
            recent,
        })
    }

    /// Return events from `from_seq` (inclusive) up to `limit` count, in
    /// ascending seq order. Returns `SessionNotFound` if the session does not
    /// exist. Returns an empty vec if `from_seq` is past the session's last
    /// committed seq.
    pub async fn fetch_from_seq(&self, req: FetchRequest) -> V2Result<Vec<EventEnvelope>> {
        let s = self.state.lock().await;
        if !s.sessions.contains_key(req.session_id.as_str()) {
            return Err(V2Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id)));
        }
        let lo = (req.session_id.as_str().to_string(), req.from_seq);
        let hi = (req.session_id.as_str().to_string(), u64::MAX);
        Ok(s.events.range(lo..=hi).take(req.limit).map(|(_, v)| v.clone()).collect())
    }
}

/// Verify a stored idempotency record matches the incoming payload and replay
/// the original response, or surface a conflict if the payload differs.
fn verify_idem_replay(
    rec: &IdempotencyRecord,
    payload: &serde_json::Value,
) -> V2Result<AppendResponse> {
    let h = hash_payload(payload);
    if rec.payload_hash != h {
        return Err(V2Error::new(
            ErrorCode::IdempotencyConflict,
            "payload differs for the same idempotency key",
        ));
    }
    let resp: AppendResponse = serde_json::from_str(&rec.result_json).unwrap();
    Ok(resp)
}
