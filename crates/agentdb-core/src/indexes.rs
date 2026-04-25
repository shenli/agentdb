use std::collections::BTreeMap;
use std::sync::Arc;

use agentdb_index::text::{InMemoryInverted, TextIndex};
use agentdb_index::vector::{HnswVectorIndex, VectorIndex};
use agentdb_index::DocId;
use agentdb_storage::{BoxId, KvStore, Namespace};
use object_store::ObjectStore;

use crate::error::Result;
use crate::primitives::DEFAULT_BRANCH_ID;
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::segment::read_segment;
use crate::types::{Event, Tier};

/// Per-box index state: one text index, one vector index, and a small
/// by-agent index — all in-memory.
pub struct PerBoxIndexes {
    pub text: InMemoryInverted,
    pub vector: HnswVectorIndex,
    /// `actor_id` → sorted `DocId`s of events that carry that actor_id.
    /// Built at lazy-init time alongside the other indexes. `BTreeMap`
    /// rather than `HashMap` so the iteration order is stable for tests
    /// and the memory overhead of the map is predictable at high agent
    /// cardinality.
    pub agent_events: BTreeMap<String, Vec<DocId>>,
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
            vector: HnswVectorIndex::from_env(),
            agent_events: BTreeMap::new(),
            indexed: 0,
        }
    }

    /// Construct from parts — used by `index_snapshot::restore_indexes`
    /// (M16 Phase E) to rebuild `PerBoxIndexes` from a deserialized
    /// snapshot.
    pub fn from_parts(
        text: InMemoryInverted,
        vector: HnswVectorIndex,
        agent_events: BTreeMap<String, Vec<DocId>>,
        indexed: u64,
    ) -> Self {
        Self {
            text,
            vector,
            agent_events,
            indexed,
        }
    }

    /// DocIds of events attributed to `actor_id`. Empty if the agent is
    /// unknown or has no events.
    pub fn docs_by_agent(&self, actor_id: &str) -> &[DocId] {
        self.agent_events
            .get(actor_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Scan every session in `box_id`, pull all events, index them.
    pub async fn build(
        &mut self,
        kv: &Arc<dyn KvStore>,
        store: &Arc<dyn ObjectStore>,
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
                let events: Vec<Event> = match session.tier {
                    Tier::Warm => {
                        // `PerBoxIndexes::build` runs at lazy-init time under the box's
                        // `indexes` write lock. It reads every event once to populate the
                        // index and is not itself a per-request read path; routing it
                        // through a ReadCtx would be meaningless because there's no snapshot
                        // to anchor to — the build sees whatever KV state it finds.
                        let prefix = agentdb_storage::prefix_for_session_branch(
                            Namespace::Events,
                            session.box_id,
                            session.tenant_internal_id,
                            session.internal_id,
                            DEFAULT_BRANCH_ID,
                        );
                        let mut out: Vec<Event> = Vec::new();
                        for res in kv.scan_prefix(&prefix)? {
                            let (_k, v) = res?;
                            let ev: Event = serde_json::from_slice(&v)?;
                            out.push(ev);
                        }
                        out
                    }
                    // M26 Phase 4: Archive is metadata-only —
                    // its segment lives at the same path it
                    // had as Cold (see `tiering::cold_to_archive`).
                    // Read it the same way so indexed retrieval
                    // is archive-complete: text + vector queries
                    // no longer silently drop archived data.
                    Tier::Cold | Tier::Archive => {
                        if let Some(path) = &session.segment_path {
                            match read_segment(store, path).await {
                                Ok(seg) => seg.events,
                                Err(err) => {
                                    tracing::warn!(
                                        ?err,
                                        session = %session.external_id,
                                        tier = ?session.tier,
                                        "skipping session with unreadable segment"
                                    );
                                    continue;
                                }
                            }
                        } else {
                            continue;
                        }
                    }
                };
                for event in events {
                    self.index_event(session.internal_id, &event);
                }
            }
        }
        Ok(())
    }

    /// Like `build`, but only indexes events whose `hlc_ts` is strictly
    /// greater than `cutoff`. Used by M16 Phase E recovery: after
    /// `from_snapshot(…)`, replay any events that were appended after
    /// the snapshot's HLC cutoff.
    pub async fn replay_events_after(
        &mut self,
        kv: &Arc<dyn KvStore>,
        store: &Arc<dyn ObjectStore>,
        tenants: &TenantRegistry,
        sessions: &SessionRegistry,
        box_id: BoxId,
        cutoff: crate::hlc::HlcTimestamp,
    ) -> Result<u64> {
        let mut replayed = 0u64;
        let tenant_externals = tenants.list_in_box(box_id)?;
        for tenant_ext in tenant_externals {
            let Some(tenant) = tenants.lookup(box_id, &tenant_ext)? else {
                continue;
            };
            let session_externals = sessions.list_in_tenant(box_id, tenant.internal_id)?;
            for session_ext in session_externals {
                let Some(session) =
                    sessions.lookup(box_id, tenant.internal_id, &session_ext)?
                else {
                    continue;
                };
                let events: Vec<Event> = match session.tier {
                    Tier::Warm => {
                        let prefix = agentdb_storage::prefix_for_session_branch(
                            Namespace::Events,
                            session.box_id,
                            session.tenant_internal_id,
                            session.internal_id,
                            DEFAULT_BRANCH_ID,
                        );
                        let mut out: Vec<Event> = Vec::new();
                        for res in kv.scan_prefix(&prefix)? {
                            let (_k, v) = res?;
                            let ev: Event = serde_json::from_slice(&v)?;
                            out.push(ev);
                        }
                        out
                    }
                    // M26 Phase 4: archive uses the same segment
                    // as cold (transition is metadata-only). The
                    // post-cutoff filter below still applies, so
                    // events older than the snapshot's HLC don't
                    // get re-indexed.
                    Tier::Cold | Tier::Archive => {
                        if let Some(path) = &session.segment_path {
                            match read_segment(store, path).await {
                                Ok(seg) => seg.events,
                                Err(_) => continue,
                            }
                        } else {
                            continue;
                        }
                    }
                };
                for event in events {
                    if event.hlc_ts > cutoff {
                        self.index_event(session.internal_id, &event);
                        replayed += 1;
                    }
                }
            }
        }
        Ok(replayed)
    }

    /// Index one event under the indexes' by-path structure. The top-level
    /// prefix is `"payload"` so callers use natural paths like
    /// `"payload.text"` and `"payload.embedding"`.
    ///
    /// Called from the write path (`Box_::index_event`, M16 Phase C) for
    /// incremental maintenance; also called from `build()` for initial
    /// lazy rebuild.
    pub fn index_event(&mut self, session_internal_id: u64, event: &Event) {
        let doc_id = DocId::new(session_internal_id, event.seq);
        self.walk(doc_id, "payload", &event.payload);
        if let Some(actor_id) = event.actor_id.as_ref() {
            self.agent_events
                .entry(actor_id.clone())
                .or_default()
                .push(doc_id);
        }
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
        let store = registry.object_store().clone();
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &store, &tenants, &sessions, box_id)
            .await
            .unwrap();
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
        let store = registry.object_store().clone();
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &store, &tenants, &sessions, box_id)
            .await
            .unwrap();
        let hits = idx
            .vector
            .knn("payload.embedding", &[1.0, 0.0, 0.0, 0.0], 10);
        assert_eq!(hits.len(), 1);
        assert!(hits[0].1 > 0.99);
    }

    #[tokio::test]
    async fn build_indexes_archived_session_via_segment_so_text_query_still_returns_it() {
        // M26 Phase 4 contract: an archived session's events
        // remain queryable via the in-memory text index. Pre-
        // Phase-4, `PerBoxIndexes::build` skipped Tier::Archive
        // entirely and unhinted text queries quietly lost
        // archived data. Here we drive the full
        // warm → cold → archive transition, rebuild the index,
        // and assert the text term still hits.
        let (_dir, _obj_dir, kv, registry, tenants, sessions) = setup_with_events().await;
        crate::primitives::append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-arc",
            "s-arc",
            json!({"text": "archive should still be queryable"}),
            0,
            10,
        )
        .await
        .unwrap();

        // Pull the session and walk it warm → cold → archive.
        // cold_transition writes the segment; cold_to_archive
        // is a metadata-only relabel.
        let box_ = registry.route_tenant("t-arc");
        let tenant = tenants.get_or_mint(box_.id, "t-arc", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-arc")
            .unwrap()
            .unwrap();
        let guard = box_.tenant_write_lock("t-arc").lock_owned().await;
        crate::tiering::cold_transition(
            registry.kv(),
            registry.object_store(),
            &sessions,
            &guard,
            &mut session,
        )
        .await
        .unwrap();
        crate::tiering::cold_to_archive(&sessions, &guard, &mut session)
            .await
            .unwrap();
        drop(guard);
        assert_eq!(session.tier, Tier::Archive);
        assert!(session.segment_path.is_some());

        // Rebuild the index from scratch and verify the text
        // search returns the archived event.
        let store = registry.object_store().clone();
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &store, &tenants, &sessions, box_.id)
            .await
            .unwrap();
        let hits = idx.text.search("payload.text", "queryable", 10);
        assert_eq!(
            hits.len(),
            1,
            "text query must still find the archived event; got {hits:?}"
        );
        assert_eq!(idx.indexed, 1);
    }

    #[tokio::test]
    async fn replay_indexes_archived_session_so_text_query_still_returns_it() {
        // M26 Phase 4: same contract for the snapshot-replay
        // path used by `index_snapshot::restore_indexes`.
        // Pre-Phase-4, `replay_events_after` skipped Archive,
        // so a server restart with an archived session +
        // post-snapshot events on it would silently miss them.
        let (_dir, _obj_dir, kv, registry, tenants, sessions) = setup_with_events().await;
        crate::primitives::append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-arc",
            "s-arc",
            json!({"text": "replay-after-archive marker"}),
            0,
            10,
        )
        .await
        .unwrap();

        let box_ = registry.route_tenant("t-arc");
        let tenant = tenants.get_or_mint(box_.id, "t-arc", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-arc")
            .unwrap()
            .unwrap();
        let guard = box_.tenant_write_lock("t-arc").lock_owned().await;
        crate::tiering::cold_transition(
            registry.kv(),
            registry.object_store(),
            &sessions,
            &guard,
            &mut session,
        )
        .await
        .unwrap();
        crate::tiering::cold_to_archive(&sessions, &guard, &mut session)
            .await
            .unwrap();
        drop(guard);

        let store = registry.object_store().clone();
        let mut idx = PerBoxIndexes::new();
        // HLC cutoff = ZERO so every archived event is past
        // the cutoff and replays.
        let replayed = idx
            .replay_events_after(
                &kv,
                &store,
                &tenants,
                &sessions,
                box_.id,
                crate::hlc::HlcTimestamp::ZERO,
            )
            .await
            .unwrap();
        assert_eq!(replayed, 1);
        assert!(!idx
            .text
            .search("payload.text", "marker", 10)
            .is_empty());
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
        let store = registry.object_store().clone();
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &store, &tenants, &sessions, box_id)
            .await
            .unwrap();
        assert_eq!(idx.indexed, 3);
    }

    #[tokio::test]
    async fn append_and_fetch_keeps_the_live_index_current() {
        // M16 Phase C regression: previously events appended after
        // the initial lazy-build were NOT indexed until a server
        // restart. With Box_::index_event wired into the write path,
        // every append should make its content immediately searchable.
        let (_dir, _obj_dir, _kv, registry, tenants, sessions) = setup_with_events().await;

        // Seed one event to trigger the initial (empty-at-start) index build.
        crate::primitives::append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-live",
            "s-live",
            json!({"text": "seed"}),
            0,
            0,
        )
        .await
        .unwrap();

        // Append a second event AFTER the first one built the index.
        // In the pre-M16 world, this second event would be absent from
        // the index until the server restarted.
        crate::primitives::append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-live",
            "s-live",
            json!({"text": "freshly_indexed_token"}),
            0,
            0,
        )
        .await
        .unwrap();

        // Pull the indexes via the Box_'s read guard — no rebuild.
        let box_ = registry.route_tenant("t-live");
        let guard = box_
            .indexes_built(
                registry.kv(),
                registry.object_store(),
                &tenants,
                &sessions,
            )
            .await
            .unwrap();
        let idx = guard.as_ref().expect("indexes built");
        let hits = idx
            .text
            .search("payload.text", "freshly_indexed_token", 10);
        assert!(
            !hits.is_empty(),
            "second event's text must be live-indexed; hits={:?}",
            hits
        );

        // Metric assertion: events_indexed should have advanced twice.
        let snap = box_.metrics.snapshot();
        assert!(
            snap.events_indexed >= 2,
            "events_indexed should count both appends; got {}",
            snap.events_indexed
        );
    }
}
