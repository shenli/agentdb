//! Warm-session data handoff between two local boxes.
//!
//! Every shed / shard-reassignment PR since M20 has left
//! the same gap: reassigning a shard moves future routing
//! but the already-written events stay on the old tablet.
//! Reads routed to the new tablet miss them.
//!
//! This module closes the gap for warm sessions — which
//! is the common case — via a single atomic KV write
//! batch. Cold/archive sessions are explicitly refused
//! because their events live in object-store segments
//! whose paths embed `box_id`; moving those needs a copy
//! operation that isn't atomic with the KV batch and
//! belongs in a separate PR.
//!
//! **Intra-process only.** All boxes in the same process
//! share one `KvStore`, so the rekey is a same-store
//! operation. Cross-node handoff would need snapshot
//! shipping; that's future work.
//!
//! **Not concurrency-safe against active writers.** The
//! migration scans the source event prefix and builds a
//! batch; a writer that appends during the scan window
//! may land an event on the source AFTER the scan and
//! survive the Delete (because it wasn't seen). Callers
//! should quiesce the session (operator-level: don't
//! migrate a session you're actively writing to). We
//! don't hold the per-tenant write lock because migration
//! is potentially long.

use std::sync::Arc;

use agentdb_storage::{
    encode_key, prefix_for_session_branch, KvStore, Namespace, WriteOp,
};

use crate::box_runtime::BoxRegistry;
use crate::error::{Error, Resource, Result};
use crate::meta::{key_session_by_external, key_session_record};
use crate::primitives::DEFAULT_BRANCH_ID;
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::types::{Event, Tier};

/// Outcome of [`migrate_warm_session`]. `events_migrated` is
/// the count of Events-namespace rows moved; `from_box_id`
/// and `to_box_id` echo the resolved inputs so callers can
/// log the full move.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MigratedSession {
    pub events_migrated: u64,
    pub from_box_id: u16,
    pub to_box_id: u16,
    pub source_session_internal_id: u64,
    pub dest_session_internal_id: u64,
}

/// Move a warm session's events, record, and lookup from
/// whatever box currently owns it to `to_box_id`. Atomic at
/// the KV level (single `write_batch`). Refuses non-warm
/// sessions.
///
/// Resolves the source box by scanning every box currently
/// in the registry for a matching tenant+session — the
/// session lookup keys embed `box_id`, so a "which box owns
/// this session?" query is a linear scan at prototype
/// scale (dozens of boxes).
#[allow(clippy::too_many_arguments)]
pub async fn migrate_warm_session(
    registry: &BoxRegistry,
    kv: &Arc<dyn KvStore>,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    tenant_external: &str,
    session_external: &str,
    to_box_id: u16,
    now_ms: u64,
) -> Result<MigratedSession> {
    // Find the source box by scanning current boxes. A real
    // deployment could cache this but the N is small and the
    // admin endpoint doesn't fire often.
    let boxes = registry.boxes_snapshot();
    let mut source_info: Option<(u16, crate::types::Tenant, crate::types::Session)> = None;
    for b in &boxes {
        if let Some(tenant) = tenants.lookup(b.id, tenant_external)? {
            if let Some(session) = sessions.lookup(b.id, tenant.internal_id, session_external)? {
                source_info = Some((b.id, tenant, session));
                break;
            }
        }
    }
    let Some((from_box_id, src_tenant, src_session)) = source_info else {
        return Err(Error::NotFound {
            resource: Resource::Session,
            id: session_external.to_string(),
        });
    };

    if from_box_id == to_box_id {
        // Idempotent no-op: session already on destination.
        return Ok(MigratedSession {
            events_migrated: 0,
            from_box_id,
            to_box_id,
            source_session_internal_id: src_session.internal_id,
            dest_session_internal_id: src_session.internal_id,
        });
    }

    if src_session.tier != Tier::Warm {
        return Err(Error::Invalid {
            field: Some("tier".into()),
            message: format!(
                "session {session_external} is {:?}; only Warm sessions can be migrated \
                 (cold/archive segments embed box_id in object-store paths — see plan doc)",
                src_session.tier
            ),
        });
    }

    // Refuse to migrate a fork parent. The parent's child
    // sessions carry `forked_from.parent_session_internal_id`
    // pointing into the source's session registry; moving the
    // parent without the children would break those pointers.
    // Detecting fork-parents at prototype scale: scan every
    // session in the source tenant and check forked_from.
    let externals_in_tenant = sessions.list_in_tenant(from_box_id, src_tenant.internal_id)?;
    for other_external in &externals_in_tenant {
        if other_external == session_external {
            continue;
        }
        if let Some(s) = sessions.lookup(from_box_id, src_tenant.internal_id, other_external)? {
            if let Some(fref) = &s.forked_from {
                if fref.parent_session_internal_id == src_session.internal_id {
                    return Err(Error::Invalid {
                        field: Some("forked_from".into()),
                        message: format!(
                            "session {session_external} has forked children (e.g. {other_external}); \
                             cross-box fork-parent migration is not supported"
                        ),
                    });
                }
            }
        }
    }

    // Mint / resolve tenant and session on the destination
    // box. `get_or_mint_with_fork` is idempotent — if an
    // operator already minted on dest for some reason we
    // pick up the existing id.
    let dest_tenant = tenants.get_or_mint(to_box_id, tenant_external, now_ms)?;
    let dest_session = sessions.get_or_mint_with_fork(
        to_box_id,
        dest_tenant.internal_id,
        session_external,
        now_ms,
        src_session.forked_from.clone(),
    )?;

    // Scan the source's Events prefix and build a rekey
    // batch. Collect (seq, value) so we can also replay
    // into the dest box's index after the KV batch lands.
    let src_prefix = prefix_for_session_branch(
        Namespace::Events,
        from_box_id,
        src_tenant.internal_id,
        src_session.internal_id,
        DEFAULT_BRANCH_ID,
    );
    let mut ops: Vec<WriteOp> = Vec::new();
    let mut replayed: Vec<(u64, Vec<u8>)> = Vec::new();
    for item in kv.scan_prefix(&src_prefix)? {
        let (old_key, value) = item?;
        // old_key is the full 31-byte key; last 8 bytes =
        // seq. Parse it to rebuild the new key and later
        // deserialize the Event into the dest index.
        if old_key.len() != 31 {
            // Defensive: a malformed key in the scan would
            // otherwise be silently dropped. Skip and log.
            tracing::warn!(
                ?old_key,
                "handoff: encountered malformed event key in source prefix; skipping"
            );
            continue;
        }
        let seq = u64::from_be_bytes(old_key[23..31].try_into().unwrap());
        let new_key = encode_key(
            Namespace::Events,
            to_box_id,
            dest_tenant.internal_id,
            dest_session.internal_id,
            DEFAULT_BRANCH_ID,
            seq,
        );
        replayed.push((seq, value.clone()));
        ops.push(WriteOp::Put {
            key: new_key,
            value,
        });
        ops.push(WriteOp::Delete { key: old_key });
    }

    // Update dest session record with source metadata
    // (last_active_at, next_seq carry over). The
    // get_or_mint_with_fork above created a *new* fresh
    // record; replace its fields with source's so reads
    // resume at the right seq.
    let migrated_session = crate::types::Session {
        internal_id: dest_session.internal_id,
        external_id: dest_session.external_id.clone(),
        tenant_internal_id: dest_tenant.internal_id,
        box_id: to_box_id,
        created_at: src_session.created_at,
        last_active_at: src_session.last_active_at,
        tier: src_session.tier,
        next_seq: src_session.next_seq,
        segment_path: src_session.segment_path.clone(),
        forked_from: src_session.forked_from.clone(),
        primary_agent_id: src_session.primary_agent_id.clone(),
    };
    let migrated_bytes = serde_json::to_vec(&migrated_session).map_err(Error::from)?;
    ops.push(WriteOp::Put {
        key: key_session_record(to_box_id, dest_tenant.internal_id, dest_session.internal_id),
        value: migrated_bytes,
    });

    // Delete the source session lookup + record so reads
    // against from_box no longer resolve. (The source's
    // next_session_id counter stays put — it's a one-way
    // monotonic.)
    ops.push(WriteOp::Delete {
        key: key_session_by_external(
            from_box_id,
            src_tenant.internal_id,
            session_external,
        ),
    });
    ops.push(WriteOp::Delete {
        key: key_session_record(from_box_id, src_tenant.internal_id, src_session.internal_id),
    });

    let events_migrated = replayed.len() as u64;

    // Apply atomically. If this fails, no state changed.
    kv.write_batch(ops)?;

    // Replay events into the dest box's index so
    // session-filtered reads return them. Best-effort: the
    // dest box's lazy-build path will rebuild the full
    // index from KV on first access anyway, so a failure
    // here is recoverable.
    if let Some(dest_box) = registry.box_by_id(to_box_id) {
        for (_seq, bytes) in replayed {
            if let Ok(event) = serde_json::from_slice::<Event>(&bytes) {
                if let Err(err) = dest_box
                    .index_event(
                        kv,
                        registry.object_store(),
                        tenants,
                        sessions,
                        dest_session.internal_id,
                        &event,
                    )
                    .await
                {
                    tracing::warn!(
                        %err,
                        tablet_id = to_box_id,
                        session = session_external,
                        "handoff: per-event index replay failed; dest index will lazy-rebuild"
                    );
                    break;
                }
            }
        }
    }

    Ok(MigratedSession {
        events_migrated,
        from_box_id,
        to_box_id,
        source_session_internal_id: src_session.internal_id,
        dest_session_internal_id: dest_session.internal_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::append_and_fetch;
    use agentdb_storage::prefix_for_session_branch;
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn setup() -> (
        TempDir,
        TempDir,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (dir, obj_dir, reg, tenants, sessions)
    }

    async fn write_events(
        reg: &Arc<BoxRegistry>,
        tenants: &Arc<TenantRegistry>,
        sessions: &Arc<SessionRegistry>,
        session_id: &str,
        n: u64,
    ) {
        for i in 1..=n {
            let payload = serde_json::json!({ "i": i });
            append_and_fetch(
                reg,
                tenants,
                sessions,
                "t-1",
                session_id,
                payload,
                0,
                1_700_000_000_000 + i,
            )
            .await
            .unwrap();
        }
    }

    fn source_box_for(reg: &Arc<BoxRegistry>, session_id: &str) -> u16 {
        // With no shard override attached, route_session
        // degrades to route_tenant, i.e. tenant-hash routing.
        reg.route_session("t-1", session_id).id
    }

    #[tokio::test]
    async fn migrates_events_atomically_and_dest_can_read_them() {
        let (_d, _o, reg, tenants, sessions) = setup();
        let session_id = "s-migrate";
        write_events(&reg, &tenants, &sessions, session_id, 20).await;

        let from_box = source_box_for(&reg, session_id);
        // Pick a different box as destination.
        let to_box = (from_box + 1) % reg.num_boxes();
        let result = migrate_warm_session(
            &reg,
            reg.kv(),
            &tenants,
            &sessions,
            "t-1",
            session_id,
            to_box,
            1_800_000_000_000,
        )
        .await
        .unwrap();
        assert_eq!(result.events_migrated, 20);
        assert_eq!(result.from_box_id, from_box);
        assert_eq!(result.to_box_id, to_box);

        // Source should no longer hold the session.
        let src_tenant = tenants.lookup(from_box, "t-1").unwrap();
        if let Some(src_tenant) = src_tenant {
            let src_session = sessions
                .lookup(from_box, src_tenant.internal_id, session_id)
                .unwrap();
            assert!(
                src_session.is_none(),
                "source should no longer have the session after migration"
            );
        }

        // Destination reads every event in order. Scan the
        // Events prefix directly since fetch_last_n needs a
        // full ReadCtx the unit test doesn't construct.
        let dest_tenant = tenants.lookup(to_box, "t-1").unwrap().unwrap();
        let dest_session = sessions
            .lookup(to_box, dest_tenant.internal_id, session_id)
            .unwrap()
            .unwrap();
        let prefix = prefix_for_session_branch(
            Namespace::Events,
            to_box,
            dest_tenant.internal_id,
            dest_session.internal_id,
            DEFAULT_BRANCH_ID,
        );
        let mut events: Vec<Event> = Vec::new();
        for res in reg.kv().scan_prefix(&prefix).unwrap() {
            let (_k, v) = res.unwrap();
            events.push(serde_json::from_slice(&v).unwrap());
        }
        assert_eq!(events.len(), 20);
        // scan_prefix returns in key order, so seq is ascending.
        for (i, ev) in events.iter().enumerate() {
            assert_eq!(ev.seq, (i as u64) + 1);
        }
    }

    #[tokio::test]
    async fn migrate_to_same_box_is_idempotent_no_op() {
        let (_d, _o, reg, tenants, sessions) = setup();
        let session_id = "s-noop";
        write_events(&reg, &tenants, &sessions, session_id, 5).await;
        let from_box = source_box_for(&reg, session_id);
        let result = migrate_warm_session(
            &reg,
            reg.kv(),
            &tenants,
            &sessions,
            "t-1",
            session_id,
            from_box,
            1_800_000_000_000,
        )
        .await
        .unwrap();
        assert_eq!(result.events_migrated, 0);
        assert_eq!(result.from_box_id, from_box);
        assert_eq!(result.to_box_id, from_box);
        // Events still there on source.
        let tenant = tenants.lookup(from_box, "t-1").unwrap().unwrap();
        let session = sessions
            .lookup(from_box, tenant.internal_id, session_id)
            .unwrap()
            .unwrap();
        let prefix = prefix_for_session_branch(
            Namespace::Events,
            from_box,
            tenant.internal_id,
            session.internal_id,
            DEFAULT_BRANCH_ID,
        );
        let count = reg.kv().scan_prefix(&prefix).unwrap().count();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn migrate_nonexistent_session_returns_not_found() {
        let (_d, _o, reg, tenants, sessions) = setup();
        let err = migrate_warm_session(
            &reg,
            reg.kv(),
            &tenants,
            &sessions,
            "t-1",
            "does-not-exist",
            0,
            1_800_000_000_000,
        )
        .await
        .unwrap_err();
        match err {
            Error::NotFound { resource, .. } => {
                assert!(matches!(resource, Resource::Session));
            }
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn migrate_refuses_cold_session() {
        // Flip the session record's tier to Cold by
        // rewriting the record directly. Saves us the
        // tenant-write-lock ceremony needed to call
        // cold_transition, while exercising the same
        // refuse-non-Warm branch in migrate_warm_session.
        let (_d, _o, reg, tenants, sessions) = setup();
        let session_id = "s-cold";
        write_events(&reg, &tenants, &sessions, session_id, 3).await;
        let from_box = source_box_for(&reg, session_id);
        let tenant = tenants.lookup(from_box, "t-1").unwrap().unwrap();
        let mut session = sessions
            .lookup(from_box, tenant.internal_id, session_id)
            .unwrap()
            .unwrap();
        session.tier = Tier::Cold;
        session.segment_path = Some(format!(
            "boxes/{:04x}/cold/{:016x}/{:016x}/segment-fake.bson",
            from_box, tenant.internal_id, session.internal_id,
        ));
        let record_bytes = serde_json::to_vec(&session).unwrap();
        reg.kv()
            .write_batch(vec![WriteOp::Put {
                key: key_session_record(from_box, tenant.internal_id, session.internal_id),
                value: record_bytes,
            }])
            .unwrap();

        // Migration must refuse.
        let to_box = (from_box + 1) % reg.num_boxes();
        let err = migrate_warm_session(
            &reg,
            reg.kv(),
            &tenants,
            &sessions,
            "t-1",
            session_id,
            to_box,
            1_800_000_000_000,
        )
        .await
        .unwrap_err();
        match err {
            Error::Invalid { field, message } => {
                assert_eq!(field.as_deref(), Some("tier"));
                assert!(message.contains("Warm"));
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }
}
