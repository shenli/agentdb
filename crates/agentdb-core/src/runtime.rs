//! V2 runtime — atomic append, idempotency, in-memory state with optional
//! KV-backed persistence (Task 11).
//!
//! `Runtime::in_memory()` keeps the original ephemeral behavior (used by every
//! pre-Task-11 unit/integration test). `Runtime::with_kv(kv)` mirrors every
//! mutation into the supplied `KvStore` and hydrates from it on startup, so
//! sessions, events, and idempotency records survive restarts.
//!
//! Atomicity contract for `append` (per Task 9 fix): the event row, the
//! updated session row, and the idempotency record (if present) commit
//! together. With KV this is implemented via `KvStore::write_batch`; with
//! in-memory it's the state mutex.

use std::collections::BTreeMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use agentdb_storage::content::{ContentBackend, InMemoryContentBackend};
use agentdb_storage::KvStore;

use crate::agent::{Agent, NewAgent};
use crate::artifact::{Artifact, NewArtifact};
use crate::errors::{Error, ErrorCode, Result};
use crate::event::{ArtifactRef, EventEnvelope, NewEvent, SourceRef};
use crate::id::Id;
use crate::idempotency::{hash_request, IdempotencyRecord, IdempotencyStore};
use crate::persist::Persist;
use crate::session::{NewSession, Session};
use crate::tenant::{NewTenant, Tenant};

/// V2.1 hardcoded artifact size cap. Picked to fit comfortably in memory
/// since the content backend reads/writes the whole blob in one shot.
/// TODO(V2.4): drive this from the tenant policy instead.
const MAX_ARTIFACT_BYTES: u64 = 16 * 1024 * 1024;

/// V2.1 hardcoded ceiling on the serialized JSON size of an inline event
/// payload. Anything bigger should be stored as an artifact and referenced via
/// `artifact_refs`; the suggestion attached to `EventTooLarge` points callers
/// at that path.
/// TODO(V2.4): drive this from the tenant policy instead.
const MAX_INLINE_EVENT_BYTES: usize = 64 * 1024;

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
    /// Keyed by artifact_id_str. Hydrated at startup from
    /// `Persist::scan_all_artifacts`; updated on every `create_artifact`.
    artifacts: BTreeMap<String, Artifact>,
    /// Keyed by (session_id, seq) for ordered scan. Only populated in the
    /// in-memory backend; the KV backend reads events back from storage on
    /// demand instead of carrying a duplicate copy in memory.
    events: BTreeMap<(String, u64), EventEnvelope>,
    /// Maps `event_id -> (session_id_str, seq)` for in-memory `event_by_id`
    /// lookup. Only populated in the in-memory backend (the KV backend
    /// resolves through the `event_by_id` secondary index in storage).
    /// Reset on every restart, which is fine — the in-memory backend
    /// doesn't persist anyway.
    event_by_id: BTreeMap<String, (String, u64)>,
    /// Alias / external_key uniqueness maps. Each entry is the canonical
    /// "is this alias claimed?" lookup; on `create_*` we check the map under
    /// the state lock and reject collisions with `AliasConflict`.
    ///
    /// Tenant alias scope is global (external_key is a global identifier).
    /// Agent and session alias scopes are tenant-local — encoded as
    /// "{tenant_id}::{alias}" so we can use a flat BTreeMap.
    tenant_aliases: BTreeMap<String, String>,
    agent_aliases: BTreeMap<String, String>,
    session_aliases: BTreeMap<String, String>,
}

fn scoped_alias_key(tenant_id: &str, alias: &str) -> String {
    format!("{tenant_id}::{alias}")
}

pub struct Runtime {
    state: Mutex<State>,
    idem: IdempotencyStore,
    persist: Option<Persist>,
    content: Arc<dyn ContentBackend>,
}

impl Runtime {
    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            idem: IdempotencyStore::default(),
            persist: None,
            content: Arc::new(InMemoryContentBackend::default()),
        })
    }

    /// Build a Runtime backed by `kv` for metadata and `content` for artifact
    /// bytes. Hydrates tenants, agents, sessions, artifacts, and idempotency
    /// records from storage at startup; events are NOT preloaded and are read
    /// on demand via `fetch_from_seq`.
    ///
    /// Reconciliation: for every hydrated session, the highest persisted event
    /// seq is checked and `next_seq` is bumped past it if needed. This is a
    /// belt-and-suspenders defense against partial-batch states that
    /// theoretically shouldn't exist (the append path uses `write_batch` which
    /// the RocksDB backend honors atomically) but is cheap to do and makes
    /// post-restart correctness independent of backend-specific batch
    /// guarantees.
    ///
    /// Artifact metadata is hydrated; the bytes themselves stay in `content`
    /// and are read on demand via `get_artifact_blob`.
    pub fn with_kv(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
    ) -> Arc<Self> {
        let persist = Persist::new(kv);

        let mut state = State::default();
        for t in persist.scan_all_tenants().expect("hydrate tenants") {
            state.tenants.insert(t.tenant_id.as_str().to_string(), t);
        }
        for a in persist.scan_all_agents().expect("hydrate agents") {
            state.agents.insert(a.agent_id.as_str().to_string(), a);
        }
        for mut s in persist.scan_all_sessions().expect("hydrate sessions") {
            // Reconcile next_seq against persisted events.
            let max_seq = persist
                .max_event_seq(s.tenant_id.as_str(), s.session_id.as_str())
                .expect("hydrate session events");
            if let Some(m) = max_seq {
                if s.next_seq <= m {
                    s.next_seq = m + 1;
                }
            }
            state.sessions.insert(s.session_id.as_str().to_string(), s);
        }
        for art in persist.scan_all_artifacts().expect("hydrate artifacts") {
            state
                .artifacts
                .insert(art.artifact_id.as_str().to_string(), art);
        }
        // Rebuild alias-uniqueness maps from on-disk alias rows so a restarted
        // runtime keeps enforcing the same constraints. We scan the alias
        // prefixes directly rather than walking the records; alias rows are
        // the source of truth for "this name is taken".
        for (external_key, tenant_id) in persist
            .scan_all_tenant_aliases()
            .expect("hydrate tenant aliases")
        {
            state.tenant_aliases.insert(external_key, tenant_id);
        }
        for ((tenant, alias), agent_id) in persist
            .scan_all_agent_aliases()
            .expect("hydrate agent aliases")
        {
            state
                .agent_aliases
                .insert(scoped_alias_key(&tenant, &alias), agent_id);
        }
        for ((tenant, alias), session_id) in persist
            .scan_all_session_aliases()
            .expect("hydrate session aliases")
        {
            state
                .session_aliases
                .insert(scoped_alias_key(&tenant, &alias), session_id);
        }

        let idem = IdempotencyStore::default();
        idem.extend(
            persist
                .scan_all_idempotency()
                .expect("hydrate idempotency records"),
        );

        Arc::new(Self {
            state: Mutex::new(state),
            idem,
            persist: Some(persist),
            content,
        })
    }

    /// Look up a tenant by id. Returns `TenantNotFound` if the id doesn't
     /// match a live (non-soft-deleted) tenant.
    pub async fn get_tenant(&self, tenant_id: &Id) -> Result<Tenant> {
        let s = self.state.lock().await;
        s.tenants
            .get(tenant_id.as_str())
            .filter(|t| t.deleted_at_ms.is_none())
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::TenantNotFound, format!("{}", tenant_id)))
    }

    /// Look up an agent by id, scoped to the given tenant. Returns
    /// `AgentNotFound` if the id is unknown or belongs to a different tenant.
    pub async fn get_agent(&self, tenant_id: &Id, agent_id: &Id) -> Result<Agent> {
        let s = self.state.lock().await;
        s.agents
            .get(agent_id.as_str())
            .filter(|a| a.deleted_at_ms.is_none() && a.tenant_id.as_str() == tenant_id.as_str())
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::AgentNotFound, format!("{}", agent_id)))
    }

    /// Look up a session by id, scoped to the given tenant. Returns
    /// `SessionNotFound` if the id is unknown or belongs to a different tenant.
    pub async fn get_session(&self, tenant_id: &Id, session_id: &Id) -> Result<Session> {
        let s = self.state.lock().await;
        s.sessions
            .get(session_id.as_str())
            .filter(|sess| {
                sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
            })
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::SessionNotFound, format!("{}", session_id)))
    }

    pub async fn create_tenant(&self, req: NewTenant) -> Result<Tenant> {
        let t = {
            let mut s = self.state.lock().await;
            // External keys are tenant-name slots; a duplicate is an
            // unrecoverable client error (not a retry), so we surface it as
            // AliasConflict rather than IdempotencyConflict.
            if !req.external_key.is_empty()
                && s.tenant_aliases.contains_key(&req.external_key)
            {
                return Err(Error::new(
                    ErrorCode::AliasConflict,
                    format!("tenant external_key already in use: {}", req.external_key),
                ));
            }
            let t = Tenant::create(req);
            s.tenants.insert(t.tenant_id.as_str().to_string(), t.clone());
            if !t.external_key.is_empty() {
                s.tenant_aliases
                    .insert(t.external_key.clone(), t.tenant_id.as_str().to_string());
            }
            t
        };
        if let Some(p) = &self.persist {
            p.put_tenant(&t).map_err(internal)?;
        }
        Ok(t)
    }

    pub async fn create_agent(&self, req: NewAgent) -> Result<Agent> {
        let a = {
            let mut s = self.state.lock().await;
            if !s.tenants.contains_key(req.tenant_id.as_str()) {
                return Err(Error::new(ErrorCode::TenantNotFound, format!("{}", req.tenant_id)));
            }
            // Agent alias is unique within (tenant, alias). Two tenants can
            // both have an agent named "rep-1".
            if let Some(alias) = req.alias.as_deref() {
                if !alias.is_empty() {
                    let key = scoped_alias_key(req.tenant_id.as_str(), alias);
                    if s.agent_aliases.contains_key(&key) {
                        return Err(Error::new(
                            ErrorCode::AliasConflict,
                            format!("agent alias already in use in tenant: {alias}"),
                        ));
                    }
                }
            }
            let a = Agent::create(req);
            s.agents.insert(a.agent_id.as_str().to_string(), a.clone());
            if let Some(alias) = a.alias.as_deref() {
                if !alias.is_empty() {
                    s.agent_aliases.insert(
                        scoped_alias_key(a.tenant_id.as_str(), alias),
                        a.agent_id.as_str().to_string(),
                    );
                }
            }
            a
        };
        if let Some(p) = &self.persist {
            p.put_agent(&a).map_err(internal)?;
        }
        Ok(a)
    }

    pub async fn create_session(&self, req: NewSession) -> Result<Session> {
        let sess = {
            let mut s = self.state.lock().await;
            // Verify the agent exists AND belongs to the requesting tenant. We
            // collapse "agent unknown" and "agent belongs to another tenant"
            // into the same `AgentNotFound` response so a caller in tenant A
            // can't probe tenant B's agent ids by error-code differential.
            let agent = s
                .agents
                .get(req.agent_id.as_str())
                .filter(|a| a.deleted_at_ms.is_none())
                .ok_or_else(|| {
                    Error::new(ErrorCode::AgentNotFound, format!("{}", req.agent_id))
                })?;
            if agent.tenant_id.as_str() != req.tenant_id.as_str() {
                return Err(Error::new(
                    ErrorCode::AgentNotFound,
                    format!("{}", req.agent_id),
                ));
            }
            // Session alias is unique within (tenant, alias).
            if let Some(alias) = req.alias.as_deref() {
                if !alias.is_empty() {
                    let key = scoped_alias_key(req.tenant_id.as_str(), alias);
                    if s.session_aliases.contains_key(&key) {
                        return Err(Error::new(
                            ErrorCode::AliasConflict,
                            format!("session alias already in use in tenant: {alias}"),
                        ));
                    }
                }
            }
            let sess = Session::create(req);
            s.sessions.insert(sess.session_id.as_str().to_string(), sess.clone());
            if let Some(alias) = sess.alias.as_deref() {
                if !alias.is_empty() {
                    s.session_aliases.insert(
                        scoped_alias_key(sess.tenant_id.as_str(), alias),
                        sess.session_id.as_str().to_string(),
                    );
                }
            }
            sess
        };
        if let Some(p) = &self.persist {
            p.put_session(&sess).map_err(internal)?;
        }
        Ok(sess)
    }

    /// Create an artifact: write the bytes to the content backend, persist
    /// the metadata, return the metadata record. Tenant must exist.
    ///
    /// On success the bytes are durable (or in-memory equivalent) and the
    /// metadata is committed atomically with the secondary `by_sha256` index.
    ///
    /// Atomicity caveat: the content write and the metadata write are
    /// **separate** persistence steps. If the metadata write fails after the
    /// content write succeeds we have an orphan blob; V2.4 GC will reap it
    /// (TODO: link the reaper plan once it lands). This task accepts the
    /// orphan window in exchange for a simpler V2.1 surface.
    pub async fn create_artifact(
        &self,
        req: NewArtifact,
        bytes: Vec<u8>,
    ) -> Result<Artifact> {
        // Reject oversized blobs before touching the content backend so a
        // misbehaving caller can't fill disk by streaming a huge body.
        let len = bytes.len() as u64;
        if len > MAX_ARTIFACT_BYTES {
            return Err(Error::new(
                ErrorCode::ArtifactTooLarge,
                format!("artifact body {len} bytes exceeds cap {MAX_ARTIFACT_BYTES}"),
            ));
        }

        // Validate tenant exists. We hold the lock only long enough to look
        // up the tenant; the content I/O happens after we drop it.
        {
            let s = self.state.lock().await;
            if !s
                .tenants
                .get(req.tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none())
            {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", req.tenant_id),
                ));
            }
        }

        // Mint the id BEFORE the put so the content backend path is stable
        // regardless of how long the put takes.
        let artifact_id = Id::new(crate::id::IdKind::Artifact);
        let put = self
            .content
            .put(req.tenant_id.as_str(), artifact_id.as_str(), &bytes)
            .await
            .map_err(|e| {
                Error::new(ErrorCode::Internal, format!("content put failed: {e}"))
            })?;

        // Verify caller-provided sha256 against the digest the backend
        // computed while writing. On mismatch we best-effort delete the
        // freshly-written blob to avoid orphaning it; ignore the delete
        // error since the caller is already getting an error and the GC
        // reaper will clean up later.
        if let Some(expected) = req.expected_sha256.as_deref() {
            if !expected.eq_ignore_ascii_case(&put.sha256_hex) {
                let _ = self
                    .content
                    .delete(req.tenant_id.as_str(), artifact_id.as_str())
                    .await;
                return Err(Error::new(
                    ErrorCode::HashMismatch,
                    format!(
                        "expected sha256 {expected} but content hashed to {}",
                        put.sha256_hex
                    ),
                ));
            }
        }

        // Build the record with the freshly-minted id rather than relying on
        // Artifact::from_put to mint one (it would mint a different id from
        // the one we already used to write the blob).
        let artifact = Artifact {
            artifact_id: artifact_id.clone(),
            tenant_id: req.tenant_id,
            content_type: req.content_type,
            bytes: put.bytes,
            sha256: put.sha256_hex,
            created_at_ms: crate::now_ms(),
            deleted_at_ms: None,
            raw_purged_at_ms: None,
        };

        // Persist metadata, then update the in-memory cache. If persist
        // fails the bytes are an orphan (see atomicity caveat above).
        if let Some(p) = &self.persist {
            p.put_artifact(&artifact).map_err(internal)?;
        }
        let mut s = self.state.lock().await;
        s.artifacts
            .insert(artifact.artifact_id.as_str().to_string(), artifact.clone());
        Ok(artifact)
    }

    /// Look up artifact metadata, scoped to `tenant_id`. Returns
    /// `ArtifactNotFound` if the id is unknown, soft-deleted, or owned by a
    /// different tenant — we collapse those cases (mirroring the
    /// agent/session lookups) so the caller in tenant A cannot probe
    /// tenant B's artifact ids by error-code differential.
    pub async fn get_artifact(
        &self,
        tenant_id: &Id,
        artifact_id: &Id,
    ) -> Result<Artifact> {
        let s = self.state.lock().await;
        s.artifacts
            .get(artifact_id.as_str())
            .filter(|a| {
                a.deleted_at_ms.is_none() && a.tenant_id.as_str() == tenant_id.as_str()
            })
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::ArtifactNotFound, format!("{}", artifact_id)))
    }

    /// Read the bytes for an artifact. Same scoping rules as `get_artifact`.
    /// Once raw is purged (V2.4 derivative-only lifecycle) the bytes are
    /// gone but the metadata stays — a `get_artifact_blob` against a
    /// purged artifact returns `ArtifactNotFound`.
    pub async fn get_artifact_blob(
        &self,
        tenant_id: &Id,
        artifact_id: &Id,
    ) -> Result<Vec<u8>> {
        let art = self.get_artifact(tenant_id, artifact_id).await?;
        if art.raw_purged_at_ms.is_some() {
            return Err(Error::new(
                ErrorCode::ArtifactNotFound,
                format!("{}", artifact_id),
            ));
        }
        // TODO(V2.4): re-verify the returned bytes against art.sha256. Skipped
        // in V2.1 to avoid hashing on every read.
        self.content
            .get(tenant_id.as_str(), artifact_id.as_str())
            .await
            .map_err(|e| {
                Error::new(ErrorCode::Internal, format!("content get failed: {e}"))
            })
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

    pub async fn append(&self, req: AppendRequest) -> Result<AppendResponse> {
        // Compose the full idempotency scope key BEFORE locking state, so we can
        // short-circuit cheaply on retry.
        let idem_full_key = req.idempotency_key.as_ref().map(|k| {
            format!("{}::{}::{}::{}", req.tenant_id, "append", req.session_id, k)
        });

        // Optimistic check before locking — fast path for retries that find a
        // committed result without contending on the state mutex.
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                return verify_idem_replay(&rec, &req);
            }
        }

        // Inline payload size guard. Done after the idempotency short-circuit
        // (a retry that already committed should replay, not re-validate) but
        // before we lock state, so a misbehaving caller can't churn the mutex
        // by hammering oversized payloads. The MAX_INLINE_EVENT_BYTES limit is
        // hardcoded for V2.1; V2.4 will pull it from the tenant policy.
        let payload_len = serde_json::to_vec(&req.payload)
            .map(|v| v.len())
            .unwrap_or(usize::MAX);
        if payload_len > MAX_INLINE_EVENT_BYTES {
            return Err(Error::new(
                ErrorCode::EventTooLarge,
                format!(
                    "inline event payload {payload_len} bytes exceeds {MAX_INLINE_EVENT_BYTES} byte limit"
                ),
            )
            .with_suggestion(
                "store the payload as an artifact and reference it via artifact_refs",
            ));
        }

        let mut s = self.state.lock().await;

        // Re-check under the state lock to close the concurrent-append race:
        // another task could have committed our key between the optimistic check
        // and the lock acquisition above.
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                drop(s);
                return verify_idem_replay(&rec, &req);
            }
        }

        // Validate session + agent membership without mutating yet, so the
        // artifact_refs check below can run in the right error-code order
        // (session/agent mismatches surface as their dedicated NotFound
        // before we ever look at artifacts).
        {
            let session = s.sessions.get(req.session_id.as_str()).ok_or_else(|| {
                Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id))
            })?;
            // Tenant isolation: a session id from tenant B is "not found" from
            // tenant A's perspective, otherwise an attacker who learns a
            // cross-tenant session id could append events into another
            // tenant's session (and worse: those events would persist under
            // the attacker's tenant key prefix, splitting the session).
            if session.tenant_id.as_str() != req.tenant_id.as_str() {
                return Err(Error::new(
                    ErrorCode::SessionNotFound,
                    format!("{}", req.session_id),
                ));
            }
            // Agent must match the session's recorded agent. We surface this
            // as `AgentNotFound` (more precise than reusing SessionNotFound)
            // so callers get an actionable error.
            if session.agent_id.as_str() != req.agent_id.as_str() {
                return Err(Error::new(
                    ErrorCode::AgentNotFound,
                    format!("{}", req.agent_id),
                ));
            }
        }

        // Validate every artifact_ref points to a live artifact owned by the
        // same tenant before we mutate session state. We collapse "wrong
        // kind", "unknown id", "soft-deleted", and "owned by another tenant"
        // into a single `ArtifactNotFound` (mirroring `get_artifact`) so a
        // caller in tenant A can't probe tenant B's artifact ids by
        // error-code differential.
        for art_ref in &req.artifact_refs {
            let known = (art_ref.artifact_id.kind() == crate::id::IdKind::Artifact)
                && s.artifacts
                    .get(art_ref.artifact_id.as_str())
                    .is_some_and(|a| {
                        a.deleted_at_ms.is_none()
                            && a.tenant_id.as_str() == req.tenant_id.as_str()
                    });
            if !known {
                return Err(Error::new(
                    ErrorCode::ArtifactNotFound,
                    format!("{}", art_ref.artifact_id),
                ));
            }
        }

        let (seq, policy_version, session_snapshot) = {
            // Re-acquire mutable access to the session row to mint the seq
            // and bump `last_active_at_ms`. The validation above already
            // confirmed the session is the right tenant + agent; no
            // additional checks needed here.
            let session = s
                .sessions
                .get_mut(req.session_id.as_str())
                .expect("session presence checked above");
            let seq = session.next_seq;
            let pv = session.policy_version;
            session.next_seq += 1;
            session.last_active_at_ms = crate::now_ms();
            (seq, pv, session.clone())
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

        let resp = AppendResponse { event_id, seq };

        // Build the idempotency record up-front so both the in-memory and KV
        // commit paths can use it. The hash covers the entire request shape
        // (not just payload) so that a retry under the same idempotency key
        // with a different `kind`, agent, session, or refs is detected as a
        // conflict rather than silently replaying the original result.
        let idem_record = idem_full_key.as_ref().map(|_| IdempotencyRecord {
            payload_hash: hash_request(
                &req.tenant_id,
                &req.agent_id,
                &req.session_id,
                &req.kind,
                &req.payload,
                &req.artifact_refs,
                &req.source_refs,
            ),
            result_json: serde_json::to_string(&resp).unwrap(),
        });

        // Atomic commit. With persist=Some, we use KvStore::write_batch to bind
        // event + session + idempotency record into a single durable write so
        // the Task 9 invariant (idempotency record commit is in the same atomic
        // boundary as the event+session commit) survives crashes.
        if let Some(p) = &self.persist {
            let pair = idem_full_key
                .as_ref()
                .zip(idem_record.as_ref())
                .map(|(k, r)| (k.as_str(), r));
            // If the batch fails we must NOT have mutated visible state. We've
            // already touched session.next_seq above, so on failure we roll it
            // back before returning an error.
            if let Err(e) = p.append_batch(&envelope, &session_snapshot, pair) {
                if let Some(sess) = s.sessions.get_mut(req.session_id.as_str()) {
                    sess.next_seq = seq;
                    // last_active_at_ms is monotonic-ish; leaving it advanced
                    // is harmless and reverting would require capturing the
                    // pre-image. Skip for simplicity.
                }
                drop(s);
                return Err(internal(e));
            }
        } else {
            // In-memory backend: keep the events map populated, and
            // mirror the `event_by_id` secondary-index entry that the
            // KV backend writes via `append_batch`. Without this the
            // in-memory `lookup_event_by_id` would always return None,
            // which is asymmetric with the KV path.
            let key = (req.session_id.as_str().to_string(), seq);
            s.event_by_id.insert(
                envelope.event_id.as_str().to_string(),
                (envelope.session_id.as_str().to_string(), envelope.seq),
            );
            s.events.insert(key, envelope);
        }

        // Record the idempotency result while still holding the state lock so
        // concurrent appends with the same key serialize cleanly: any racing
        // caller will observe this record on its under-lock re-check and replay
        // instead of committing a second event. Lock order is (state -> idem)
        // and the idempotency store never reaches back into state, so there is
        // no deadlock risk despite holding both briefly.
        if let (Some(full), Some(rec)) = (idem_full_key, idem_record) {
            self.idem.put(full, rec);
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
    ) -> Result<AppendAndFetchResponse> {
        // TODO(V2.4): enforce max_bytes
        let tenant_id = req.tenant_id.clone();
        let session_id = req.session_id.clone();
        let appended = self.append(req).await?;

        // KV-backed: scan the session's events from storage so we don't need to
        // also keep an in-memory copy. We bound the scan with `last` by reading
        // from seq 0 with a high enough limit; for V2.0 sessions are small.
        if let Some(p) = &self.persist {
            let mut all = p
                .scan_events(tenant_id.as_str(), session_id.as_str(), 0, usize::MAX)
                .map_err(internal)?;
            // Tail of length `last`, in ascending seq order.
            if all.len() > last {
                let drop_n = all.len() - last;
                all.drain(0..drop_n);
            }
            return Ok(AppendAndFetchResponse {
                event_id: appended.event_id,
                seq: appended.seq,
                recent: all,
            });
        }

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

    /// Resolve an `event_id` to its `(session_id, seq)` location.
    ///
    /// KV-backed runtime resolves through the persisted `event_by_id`
    /// secondary index. In-memory runtime resolves through an in-process
    /// map populated on `append`; the map is reset on every restart, which
    /// is fine — the in-memory backend doesn't persist anyway.
    ///
    /// In both branches the lookup is tenant-scoped: an event_id created
    /// under tenant A is `None` to a caller passing tenant B, so a probe
    /// for cross-tenant event ids cannot leak existence.
    pub async fn lookup_event_by_id(
        &self,
        tenant_id: &Id,
        event_id: &Id,
    ) -> Result<Option<(Id, u64)>> {
        if let Some(p) = &self.persist {
            // KV-backed: persist is the single source of truth. The
            // KV `event_by_id` key is itself tenant-scoped, so a
            // mismatched tenant simply won't find the row.
            let raw = p
                .lookup_event_by_id(tenant_id.as_str(), event_id.as_str())
                .map_err(internal)?;
            let Some((sid, seq)) = raw else { return Ok(None) };
            let session_id = Id::parse(&sid)
                .map_err(|e| Error::new(ErrorCode::Internal, e.to_string()))?;
            return Ok(Some((session_id, seq)));
        }

        // In-memory backend: walk the event into its session, confirm
        // the session belongs to the requesting tenant.
        let s = self.state.lock().await;
        let Some((sid_str, seq)) = s.event_by_id.get(event_id.as_str()) else {
            return Ok(None);
        };
        let Some(sess) = s.sessions.get(sid_str.as_str()) else {
            // Session was deleted out from under the event_by_id
            // entry; treat as not-found rather than leak the orphan.
            return Ok(None);
        };
        if sess.tenant_id.as_str() != tenant_id.as_str() {
            return Ok(None);
        }
        Ok(Some((sess.session_id.clone(), *seq)))
    }

    /// Return events from `from_seq` (inclusive) up to `limit` count, in
    /// ascending seq order. Returns `SessionNotFound` if the session does not
    /// exist. Returns an empty vec if `from_seq` is past the session's last
    /// committed seq.
    pub async fn fetch_from_seq(&self, req: FetchRequest) -> Result<Vec<EventEnvelope>> {
        let s = self.state.lock().await;
        let session = s.sessions.get(req.session_id.as_str()).ok_or_else(|| {
            Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id))
        })?;
        // tenant_id on FetchRequest finally becomes load-bearing for the KV
        // backend's tenant-scoped key encoding. Refuse mismatches outright so
        // callers can't sidestep tenancy by passing the wrong tenant.
        if session.tenant_id.as_str() != req.tenant_id.as_str() {
            return Err(Error::new(
                ErrorCode::SessionNotFound,
                format!("{}", req.session_id),
            ));
        }

        if let Some(p) = &self.persist {
            // Drop the lock before the KV scan to avoid blocking other appends.
            drop(s);
            return p
                .scan_events(
                    req.tenant_id.as_str(),
                    req.session_id.as_str(),
                    req.from_seq,
                    req.limit,
                )
                .map_err(internal);
        }
        let lo = (req.session_id.as_str().to_string(), req.from_seq);
        let hi = (req.session_id.as_str().to_string(), u64::MAX);
        Ok(s.events.range(lo..=hi).take(req.limit).map(|(_, v)| v.clone()).collect())
    }
}

/// Verify a stored idempotency record matches the incoming request and replay
/// the original response, or surface a conflict if any field that affects
/// the commit differs.
fn verify_idem_replay(
    rec: &IdempotencyRecord,
    req: &AppendRequest,
) -> Result<AppendResponse> {
    let h = hash_request(
        &req.tenant_id,
        &req.agent_id,
        &req.session_id,
        &req.kind,
        &req.payload,
        &req.artifact_refs,
        &req.source_refs,
    );
    if rec.payload_hash != h {
        return Err(Error::new(
            ErrorCode::IdempotencyConflict,
            "request differs for the same idempotency key",
        ));
    }
    let resp: AppendResponse = serde_json::from_str(&rec.result_json).unwrap();
    Ok(resp)
}

/// Map a storage-layer error into a V2 `Internal` error. The persist layer
/// returns `agentdb_storage::Error`; surfacing it as `Internal` keeps the
/// Error code surface stable.
fn internal(e: agentdb_storage::Error) -> Error {
    Error::new(ErrorCode::Internal, e.to_string())
}
