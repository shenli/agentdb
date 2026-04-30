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
    agent_alias_key, agent_record_key, agent_skill_binding_key, artifact_by_sha256_key,
    artifact_record_key, event_by_id_key, event_key, record_by_namespace_key, record_key,
    session_alias_key, session_record_key, skill_record_key, skill_unique_key, tenant_alias_key,
    tenant_record_key,
};
use agentdb_storage::{KvStore, Result as KvResult, WriteOp};

use crate::agent::Agent;
use crate::artifact::Artifact;
use crate::event::EventEnvelope;
use crate::idempotency::IdempotencyRecord;
use crate::record::SearchableRecord;
use crate::session::Session;
use crate::skill::{AgentSkillBinding, Skill};
use crate::tenant::Tenant;

pub(crate) struct Persist {
    pub kv: Arc<dyn KvStore + Send + Sync>,
}

impl Persist {
    pub fn new(kv: Arc<dyn KvStore + Send + Sync>) -> Self {
        Self { kv }
    }

    // ----- single-record writes -----
    //
    // Each `put_*` writes the primary record AND, if an alias / external_key
    // is set, an alias index row in the same atomic batch. Hydration scans
    // both prefixes (`record/{kind}/`, `record/{kind}_alias/`) at startup so
    // the in-memory uniqueness map reflects on-disk state.

    pub fn put_tenant(&self, t: &Tenant) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
        ops.push(WriteOp::Put {
            key: tenant_record_key(t.tenant_id.as_str()).into_bytes(),
            value: serde_json::to_vec(t).expect("tenant serializes"),
        });
        if !t.external_key.is_empty() {
            ops.push(WriteOp::Put {
                key: tenant_alias_key(&t.external_key).into_bytes(),
                value: t.tenant_id.as_str().as_bytes().to_vec(),
            });
        }
        self.kv.write_batch(ops)
    }

    pub fn put_agent(&self, a: &Agent) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
        ops.push(WriteOp::Put {
            key: agent_record_key(a.tenant_id.as_str(), a.agent_id.as_str()).into_bytes(),
            value: serde_json::to_vec(a).expect("agent serializes"),
        });
        if let Some(alias) = a.alias.as_deref() {
            if !alias.is_empty() {
                ops.push(WriteOp::Put {
                    key: agent_alias_key(a.tenant_id.as_str(), alias).into_bytes(),
                    value: a.agent_id.as_str().as_bytes().to_vec(),
                });
            }
        }
        self.kv.write_batch(ops)
    }

    /// Atomically write an artifact's primary record AND the
    /// `artifact_by_sha256` secondary index in one batch. The sha256 index
    /// has no V2.1 lookup API; it's populated so a future dedup pass lands
    /// without rewriting this code path.
    pub fn put_artifact(&self, a: &Artifact) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
            WriteOp::Put {
                key: artifact_record_key(a.tenant_id.as_str(), a.artifact_id.as_str()).into_bytes(),
                value: serde_json::to_vec(a).expect("artifact serializes"),
            },
            WriteOp::Put {
                key: artifact_by_sha256_key(a.tenant_id.as_str(), &a.sha256).into_bytes(),
                value: a.artifact_id.as_str().as_bytes().to_vec(),
            },
        ];
        self.kv.write_batch(ops)
    }

    pub fn delete_artifact(&self, a: &Artifact) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
            WriteOp::Delete {
                key: artifact_record_key(a.tenant_id.as_str(), a.artifact_id.as_str()).into_bytes(),
            },
            WriteOp::Delete {
                key: artifact_by_sha256_key(a.tenant_id.as_str(), &a.sha256).into_bytes(),
            },
        ];
        self.kv.write_batch(ops)
    }

    pub fn put_session(&self, s: &Session) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(2);
        ops.push(WriteOp::Put {
            key: session_record_key(s.tenant_id.as_str(), s.session_id.as_str()).into_bytes(),
            value: serde_json::to_vec(s).expect("session serializes"),
        });
        if let Some(alias) = s.alias.as_deref() {
            if !alias.is_empty() {
                ops.push(WriteOp::Put {
                    key: session_alias_key(s.tenant_id.as_str(), alias).into_bytes(),
                    value: s.session_id.as_str().as_bytes().to_vec(),
                });
            }
        }
        self.kv.write_batch(ops)
    }

    /// Atomic multi-write for the append commit path. Bundles the event row,
    /// the updated session row, the `event_by_id` secondary index row, and
    /// (optionally) the idempotency record so that on crash recovery either
    /// all of them are visible or none are.
    pub fn append_batch(
        &self,
        event: &EventEnvelope,
        session: &Session,
        idem: Option<(&str, &IdempotencyRecord)>,
    ) -> KvResult<()> {
        let mut ops: Vec<WriteOp> = Vec::with_capacity(4);
        let ek = event_key(
            event.tenant_id.as_str(),
            event.session_id.as_str(),
            event.seq,
        );
        ops.push(WriteOp::Put {
            key: ek.into_bytes(),
            value: serde_json::to_vec(event).expect("event serializes"),
        });
        let sk = session_record_key(session.tenant_id.as_str(), session.session_id.as_str());
        ops.push(WriteOp::Put {
            key: sk.into_bytes(),
            value: serde_json::to_vec(session).expect("session serializes"),
        });
        // Secondary index: event_by_id -> {session_id, seq}. Lets a caller
        // look up an event by its event_id without scanning the session.
        let by_id = event_by_id_key(event.tenant_id.as_str(), event.event_id.as_str());
        ops.push(WriteOp::Put {
            key: by_id.into_bytes(),
            value: serde_json::to_vec(&serde_json::json!({
                "session_id": event.session_id.as_str(),
                "seq": event.seq,
            }))
            .expect("event_by_id pointer serializes"),
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

    /// Resolve an `event_id` to its `(session_id, seq)` location via the
    /// `event_by_id` secondary index. Returns `None` if no row exists.
    pub fn lookup_event_by_id(
        &self,
        tenant_id: &str,
        event_id: &str,
    ) -> KvResult<Option<(String, u64)>> {
        let raw = self
            .kv
            .get(event_by_id_key(tenant_id, event_id).as_bytes())?;
        let Some(bytes) = raw else { return Ok(None) };
        let v: serde_json::Value =
            serde_json::from_slice(&bytes).expect("event_by_id pointer decodes");
        let session_id = v["session_id"]
            .as_str()
            .expect("event_by_id pointer has session_id")
            .to_string();
        let seq = v["seq"].as_u64().expect("event_by_id pointer has seq");
        Ok(Some((session_id, seq)))
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
            let env: EventEnvelope = serde_json::from_slice(&v).expect("event envelope decodes");
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
            let env: EventEnvelope = serde_json::from_slice(&v).expect("event envelope decodes");
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

    /// Hydrate all live artifact metadata records. The `record/artifact/`
    /// prefix is disjoint from the `record/artifact_by_sha256/` index prefix
    /// (the byte after `record/artifact` is `_` for the index, `/` for the
    /// record), so this scan does NOT pick up index entries.
    pub fn scan_all_artifacts(&self) -> KvResult<Vec<Artifact>> {
        let iter = self.kv.scan_prefix(b"record/artifact/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let a: Artifact = serde_json::from_slice(&v).expect("artifact decodes");
            out.push(a);
        }
        Ok(out)
    }

    /// Scan tenant alias rows, yielding `(external_key, tenant_id)` pairs.
    /// Used at hydration to rebuild the in-memory uniqueness map.
    pub fn scan_all_tenant_aliases(&self) -> KvResult<Vec<(String, String)>> {
        let prefix = b"record/tenant_alias/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let external_key = std::str::from_utf8(&k[prefix.len()..])
                .expect("tenant_alias key suffix is utf8")
                .to_string();
            let tenant_id = std::str::from_utf8(&v)
                .expect("tenant_alias value is utf8")
                .to_string();
            out.push((external_key, tenant_id));
        }
        Ok(out)
    }

    /// Scan agent alias rows, yielding `((tenant_id, alias), agent_id)`.
    pub fn scan_all_agent_aliases(&self) -> KvResult<Vec<((String, String), String)>> {
        let prefix = b"record/agent_alias/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix =
                std::str::from_utf8(&k[prefix.len()..]).expect("agent_alias key suffix is utf8");
            // suffix shape: "{tenant_id}/{alias}"
            let mut split = suffix.splitn(2, '/');
            let tenant = split.next().expect("tenant_id segment").to_string();
            let alias = split.next().expect("alias segment").to_string();
            let agent_id = std::str::from_utf8(&v)
                .expect("agent_alias value is utf8")
                .to_string();
            out.push(((tenant, alias), agent_id));
        }
        Ok(out)
    }

    /// Scan session alias rows, yielding `((tenant_id, alias), session_id)`.
    pub fn scan_all_session_aliases(&self) -> KvResult<Vec<((String, String), String)>> {
        let prefix = b"record/session_alias/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix =
                std::str::from_utf8(&k[prefix.len()..]).expect("session_alias key suffix is utf8");
            let mut split = suffix.splitn(2, '/');
            let tenant = split.next().expect("tenant_id segment").to_string();
            let alias = split.next().expect("alias segment").to_string();
            let session_id = std::str::from_utf8(&v)
                .expect("session_alias value is utf8")
                .to_string();
            out.push(((tenant, alias), session_id));
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

    /// Atomic write of a V2.2 searchable record's primary row plus the
    /// `record_by_namespace` secondary index entry. Both rows are written
    /// in one batch so a crash either reveals the new record + index
    /// pair or neither — no half-state where a record is reachable by id
    /// but missing from a namespace prefix scan (or vice versa).
    pub fn put_record(&self, r: &SearchableRecord) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
            WriteOp::Put {
                key: record_key(r.tenant_id.as_str(), r.record_id.as_str()).into_bytes(),
                value: serde_json::to_vec(r).expect("record serializes"),
            },
            WriteOp::Put {
                key: record_by_namespace_key(
                    r.tenant_id.as_str(),
                    &r.namespace_path(),
                    r.record_id.as_str(),
                )
                .into_bytes(),
                value: r.record_id.as_str().as_bytes().to_vec(),
            },
        ];
        self.kv.write_batch(ops)
    }

    pub fn delete_record(&self, r: &SearchableRecord) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
            WriteOp::Delete {
                key: record_key(r.tenant_id.as_str(), r.record_id.as_str()).into_bytes(),
            },
            WriteOp::Delete {
                key: record_by_namespace_key(
                    r.tenant_id.as_str(),
                    &r.namespace_path(),
                    r.record_id.as_str(),
                )
                .into_bytes(),
            },
        ];
        self.kv.write_batch(ops)
    }

    /// Atomic write of a V2.3 skill record plus its
    /// `(tenant_id, name, version)` uniqueness index.
    pub fn put_skill(&self, skill: &Skill) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![
            WriteOp::Put {
                key: skill_record_key(skill.tenant_id.as_str(), skill.skill_id.as_str())
                    .into_bytes(),
                value: serde_json::to_vec(skill).expect("skill serializes"),
            },
            WriteOp::Put {
                key: skill_unique_key(skill.tenant_id.as_str(), &skill.name, &skill.version)
                    .into_bytes(),
                value: skill.skill_id.as_str().as_bytes().to_vec(),
            },
        ];
        self.kv.write_batch(ops)
    }

    pub fn put_agent_skill_binding(&self, binding: &AgentSkillBinding) -> KvResult<()> {
        let ops: Vec<WriteOp> = vec![WriteOp::Put {
            key: agent_skill_binding_key(
                binding.tenant_id.as_str(),
                binding.agent_id.as_str(),
                binding.skill_id.as_str(),
            )
            .into_bytes(),
            value: serde_json::to_vec(binding).expect("agent skill binding serializes"),
        }];
        self.kv.write_batch(ops)
    }

    pub fn put_idempotency(&self, full_key: &str, rec: &IdempotencyRecord) -> KvResult<()> {
        self.kv.write_batch(vec![WriteOp::Put {
            key: format!("record/idempotency/{full_key}").into_bytes(),
            value: serde_json::to_vec(rec).expect("idempotency record serializes"),
        }])
    }

    /// Look up a record by primary key (tenant + record_id). Returns
    /// `None` if the row is absent. Callers are responsible for
    /// soft-delete and expiration filtering.
    ///
    /// Currently unused by the V2.2a runtime (which serves
    /// `get_record` from the in-memory cache hydrated at startup);
    /// kept for the V2.2b HTTP layer that will need on-demand reads
    /// against the KV store.
    #[allow(dead_code)]
    pub fn get_record(
        &self,
        tenant_id: &str,
        record_id: &str,
    ) -> KvResult<Option<SearchableRecord>> {
        let raw = self.kv.get(record_key(tenant_id, record_id).as_bytes())?;
        let Some(bytes) = raw else { return Ok(None) };
        let r: SearchableRecord = serde_json::from_slice(&bytes).expect("record decodes");
        Ok(Some(r))
    }

    /// Hydrate every searchable record from the KV store. The
    /// `record/searchable/` prefix is byte-disjoint from the secondary
    /// index prefix `record/searchable_by_namespace/` (the byte after
    /// `record/searchable` is `_` for the index but `/` for the record
    /// prefix), so this scan does NOT pick up index entries.
    pub fn scan_all_records(&self) -> KvResult<Vec<SearchableRecord>> {
        let iter = self.kv.scan_prefix(b"record/searchable/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let r: SearchableRecord = serde_json::from_slice(&v).expect("record decodes");
            out.push(r);
        }
        Ok(out)
    }

    pub fn scan_all_skills(&self) -> KvResult<Vec<Skill>> {
        let iter = self.kv.scan_prefix(b"record/skill/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let skill: Skill = serde_json::from_slice(&v).expect("skill decodes");
            out.push(skill);
        }
        Ok(out)
    }

    pub fn scan_all_skill_uniques(&self) -> KvResult<Vec<((String, String, String), String)>> {
        let prefix = b"record/skill_unique/";
        let iter = self.kv.scan_prefix(prefix)?;
        let mut out = Vec::new();
        for res in iter {
            let (k, v) = res?;
            let suffix =
                std::str::from_utf8(&k[prefix.len()..]).expect("skill_unique key suffix is utf8");
            let mut split = suffix.splitn(3, '/');
            let tenant = split.next().expect("tenant_id segment").to_string();
            let name = split.next().expect("name segment").to_string();
            let version = split.next().expect("version segment").to_string();
            let skill_id = std::str::from_utf8(&v)
                .expect("skill_unique value is utf8")
                .to_string();
            out.push(((tenant, name, version), skill_id));
        }
        Ok(out)
    }

    pub fn scan_all_agent_skill_bindings(&self) -> KvResult<Vec<AgentSkillBinding>> {
        let iter = self.kv.scan_prefix(b"record/agent_skill/")?;
        let mut out = Vec::new();
        for res in iter {
            let (_k, v) = res?;
            let binding: AgentSkillBinding =
                serde_json::from_slice(&v).expect("agent skill binding decodes");
            out.push(binding);
        }
        Ok(out)
    }
}
