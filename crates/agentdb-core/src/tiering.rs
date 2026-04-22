use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_storage::{encode_key, KvStore, Namespace, WriteOp};
use object_store::ObjectStore;
use tokio::sync::OwnedMutexGuard;

use crate::error::{Error, Result};
use crate::primitives::DEFAULT_BRANCH_ID;
use crate::registry::SessionRegistry;
use crate::segment::{
    delete_segment, read_segment, segment_path, write_segment, SegmentFile, SEGMENT_SCHEMA_VERSION,
};
use crate::types::{Session, Tier};

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Move a warm session to cold: write a segment on the object store, then
/// delete the session's warm events and mark the metadata.
///
/// Precondition: caller holds the tenant's write lock (demonstrated by the
/// `OwnedMutexGuard` reference). `session.tier` must be `Warm`.
pub async fn cold_transition(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    _lock: &OwnedMutexGuard<()>,
    session: &mut Session,
) -> Result<()> {
    if session.tier != Tier::Warm {
        return Err(Error::Invalid {
            field: Some("tier".into()),
            message: format!(
                "cold_transition: session {} is not Warm (tier={:?})",
                session.external_id, session.tier
            ),
        });
    }
    // `cold_transition` runs under the tenant's write lock, so a
    // live-KV prefix scan here is equivalent to a snapshot taken at
    // lock-acquire time — no concurrent writer on the same tenant can
    // mutate this session's event range. Writers on *other* tenants
    // can proceed in parallel but operate on disjoint key prefixes.
    // Not routed through a ReadCtx because this is a writer path, not a
    // reader, and the caller doesn't have one to hand down.
    let events: Vec<crate::types::Event> = {
        let prefix = agentdb_storage::prefix_for_session_branch(
            Namespace::Events,
            session.box_id,
            session.tenant_internal_id,
            session.internal_id,
            DEFAULT_BRANCH_ID,
        );
        let mut out = Vec::new();
        for res in kv.scan_prefix(&prefix)? {
            let (_k, v) = res?;
            let ev: crate::types::Event = serde_json::from_slice(&v)?;
            out.push(ev);
        }
        out
    };
    let path = segment_path(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        now_millis(),
    );

    // (1) Write segment. If this fails we return before touching any other state.
    let seg = SegmentFile {
        schema_version: SEGMENT_SCHEMA_VERSION,
        session_internal_id: session.internal_id,
        events: events.clone(),
    };
    write_segment(store, &path, &seg).await?;

    // (2) Delete warm events + write updated metadata as a single batch so
    // either the session flips to cold AND events are gone, or neither.
    let mut ops: Vec<WriteOp> = Vec::with_capacity(events.len() + 1);
    for ev in &events {
        ops.push(WriteOp::Delete {
            key: encode_key(
                Namespace::Events,
                session.box_id,
                session.tenant_internal_id,
                session.internal_id,
                DEFAULT_BRANCH_ID,
                ev.seq,
            ),
        });
    }
    session.tier = Tier::Cold;
    session.segment_path = Some(path.clone());
    let session_record_key = crate::meta::key_session_record(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
    );
    let session_record_bytes = serde_json::to_vec(session)?;
    ops.push(WriteOp::Put {
        key: session_record_key,
        value: session_record_bytes,
    });
    if let Err(e) = kv.write_batch(ops) {
        let _ = delete_segment(store, &path).await;
        session.tier = Tier::Warm;
        session.segment_path = None;
        return Err(e.into());
    }
    let _ = sessions; // see docs on reactivate
    Ok(())
}

/// Restore a cold session's events to warm, then delete the segment.
///
/// Precondition: caller holds the tenant's write lock. `session.tier` must be `Cold`.
pub async fn reactivate(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    _lock: &OwnedMutexGuard<()>,
    session: &mut Session,
) -> Result<()> {
    if session.tier != Tier::Cold {
        return Err(Error::Invalid {
            field: Some("tier".into()),
            message: format!(
                "reactivate: session {} is not Cold (tier={:?})",
                session.external_id, session.tier
            ),
        });
    }
    let Some(path) = session.segment_path.clone() else {
        return Err(Error::Invalid {
            field: Some("tier".into()),
            message: format!(
                "reactivate: cold session {} has no segment_path",
                session.external_id
            ),
        });
    };
    let seg = read_segment(store, &path).await?;

    let mut ops: Vec<WriteOp> = Vec::with_capacity(seg.events.len() + 1);
    for ev in &seg.events {
        let key = encode_key(
            Namespace::Events,
            session.box_id,
            session.tenant_internal_id,
            session.internal_id,
            DEFAULT_BRANCH_ID,
            ev.seq,
        );
        let value = serde_json::to_vec(ev)?;
        ops.push(WriteOp::Put { key, value });
    }
    session.tier = Tier::Warm;
    session.segment_path = None;
    let session_record_key = crate::meta::key_session_record(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
    );
    let session_record_bytes = serde_json::to_vec(session)?;
    ops.push(WriteOp::Put {
        key: session_record_key,
        value: session_record_bytes,
    });
    kv.write_batch(ops)?;
    let _ = sessions; // signature retained for future archive transitions
                      // M9: segment deletion deferred to SegmentGc. reactivate must not
                      // delete inline because any open ReadCtx that captured this session
                      // as Cold still needs to read from the segment path it saw.
    Ok(())
}

/// Relabel a cold session as archived. Metadata-only: the segment
/// stays where it is. The caller holds the box write lock.
///
/// Returns an error only if the session isn't currently `Cold`.
pub async fn cold_to_archive(
    sessions: &crate::registry::SessionRegistry,
    _guard: &tokio::sync::OwnedMutexGuard<()>,
    session: &mut crate::types::Session,
) -> crate::error::Result<()> {
    if session.tier != crate::types::Tier::Cold {
        return Err(crate::error::Error::Invalid {
            field: Some("tier".into()),
            message: format!(
                "session {} is not cold (is {:?}); cannot archive",
                session.external_id, session.tier
            ),
        });
    }
    session.tier = crate::types::Tier::Archive;
    sessions.save(session)?;
    Ok(())
}

/// Un-archive: relabel as cold. Metadata-only. Caller holds the box
/// write lock. From cold, a subsequent `reactivate` call can pull
/// events back to warm.
pub async fn archive_to_cold(
    sessions: &crate::registry::SessionRegistry,
    _guard: &tokio::sync::OwnedMutexGuard<()>,
    session: &mut crate::types::Session,
) -> crate::error::Result<()> {
    if session.tier != crate::types::Tier::Archive {
        return Err(crate::error::Error::Invalid {
            field: Some("tier".into()),
            message: format!(
                "session {} is not archived (is {:?}); cannot un-archive",
                session.external_id, session.tier
            ),
        });
    }
    session.tier = crate::types::Tier::Cold;
    sessions.save(session)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::append_and_fetch;
    use crate::registry::{SessionRegistry, TenantRegistry};
    use crate::BoxRegistry;
    use agentdb_storage::RocksKvStore;
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct Ctx {
        _kv_dir: TempDir,
        _obj_dir: TempDir,
        registry: Arc<BoxRegistry>,
        tenants: Arc<TenantRegistry>,
        sessions: Arc<SessionRegistry>,
    }

    async fn seed(text: &str, tag: &str) -> (Ctx, Session) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            tag,
            json!({"text": text}),
            0,
            100,
        )
        .await
        .unwrap();
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let session = sessions
            .lookup(box_.id, tenant.internal_id, tag)
            .unwrap()
            .unwrap();
        (
            Ctx {
                _kv_dir: kv_dir,
                _obj_dir: obj_dir,
                registry,
                tenants,
                sessions,
            },
            session,
        )
    }

    #[tokio::test]
    async fn cold_transition_writes_segment_and_deletes_warm_events() {
        let (ctx, mut session) = seed("hello cold world", "s-1").await;
        let box_ = ctx.registry.route_tenant("t-1");
        let guard = box_.tenant_write_lock("t-1").lock_owned().await;
        cold_transition(
            ctx.registry.kv(),
            ctx.registry.object_store(),
            &ctx.sessions,
            &guard,
            &mut session,
        )
        .await
        .unwrap();
        drop(guard);

        assert_eq!(session.tier, Tier::Cold);
        assert!(session.segment_path.is_some());

        let read_ctx = crate::read_ctx::ReadCtx::open(&box_, ctx.registry.kv()).unwrap();
        let warm = crate::primitives::scan_session_events(&read_ctx, &session).unwrap();
        assert!(warm.is_empty());

        let seg = read_segment(
            ctx.registry.object_store(),
            session.segment_path.as_deref().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(seg.events.len(), 1);
        assert_eq!(
            seg.events[0].payload.get("text").and_then(|v| v.as_str()),
            Some("hello cold world")
        );
    }

    #[tokio::test]
    async fn cold_transition_rejects_non_warm_session() {
        let (ctx, mut session) = seed("t", "s").await;
        session.tier = Tier::Cold;
        let box_ = ctx.registry.route_tenant("t-1");
        let guard = box_.tenant_write_lock("t-1").lock_owned().await;
        let err = cold_transition(
            ctx.registry.kv(),
            ctx.registry.object_store(),
            &ctx.sessions,
            &guard,
            &mut session,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not Warm"));
    }

    #[tokio::test]
    async fn reactivate_restores_warm_events_and_keeps_segment_for_gc() {
        let (ctx, mut session) = seed("round trip", "s-2").await;
        let box_ = ctx.registry.route_tenant("t-1");
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            cold_transition(
                ctx.registry.kv(),
                ctx.registry.object_store(),
                &ctx.sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        let path_before = session.segment_path.clone().unwrap();

        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            reactivate(
                ctx.registry.kv(),
                ctx.registry.object_store(),
                &ctx.sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }

        assert_eq!(session.tier, Tier::Warm);
        assert!(session.segment_path.is_none());

        let read_ctx = crate::read_ctx::ReadCtx::open(&box_, ctx.registry.kv()).unwrap();
        let warm = crate::primitives::scan_session_events(&read_ctx, &session).unwrap();
        assert_eq!(warm.len(), 1);

        // After M9, reactivate no longer deletes the segment; GC handles it.
        assert!(read_segment(ctx.registry.object_store(), &path_before)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn reactivate_leaves_segment_on_disk_for_gc() {
        let (ctx, mut session) = seed("round trip", "s-2").await;
        let box_ = ctx.registry.route_tenant("t-1");
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            cold_transition(
                ctx.registry.kv(),
                ctx.registry.object_store(),
                &ctx.sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        let path = session.segment_path.clone().unwrap();
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            reactivate(
                ctx.registry.kv(),
                ctx.registry.object_store(),
                &ctx.sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        // After M9 change: segment is still on disk; GC will reap it later.
        assert!(
            read_segment(ctx.registry.object_store(), &path)
                .await
                .is_ok(),
            "segment should still exist after reactivate"
        );
    }

    #[tokio::test]
    async fn reactivate_rejects_non_cold_session() {
        let (ctx, mut session) = seed("t", "s").await;
        let box_ = ctx.registry.route_tenant("t-1");
        let guard = box_.tenant_write_lock("t-1").lock_owned().await;
        let err = reactivate(
            ctx.registry.kv(),
            ctx.registry.object_store(),
            &ctx.sessions,
            &guard,
            &mut session,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not Cold"));
    }

    #[allow(dead_code)]
    fn _use_ctx(ctx: &Ctx) {
        let _ = (&ctx.registry, &ctx.tenants);
    }

    #[tokio::test]
    async fn cold_to_archive_flips_tier_only() {
        use crate::types::Tier;
        let (ctx, mut session) = seed("archive me", "s-arch-1").await;
        let box_ = ctx.registry.route_tenant("t-1");
        // First transition to Cold
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            cold_transition(
                ctx.registry.kv(),
                ctx.registry.object_store(),
                &ctx.sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        assert_eq!(session.tier, Tier::Cold);
        let segment_path_before = session.segment_path.clone();
        assert!(segment_path_before.is_some());

        // Now archive
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            cold_to_archive(&ctx.sessions, &guard, &mut session)
                .await
                .unwrap();
        }

        assert_eq!(session.tier, Tier::Archive);
        // Segment path must be unchanged — metadata-only operation
        assert_eq!(session.segment_path, segment_path_before);
    }

    #[tokio::test]
    async fn archive_to_cold_flips_tier_only() {
        use crate::types::Tier;
        let (ctx, mut session) = seed("unarchive me", "s-arch-2").await;
        let box_ = ctx.registry.route_tenant("t-1");
        // Transition Warm -> Cold -> Archive
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            cold_transition(
                ctx.registry.kv(),
                ctx.registry.object_store(),
                &ctx.sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            cold_to_archive(&ctx.sessions, &guard, &mut session)
                .await
                .unwrap();
        }
        assert_eq!(session.tier, Tier::Archive);
        let segment_path_before = session.segment_path.clone();
        assert!(segment_path_before.is_some());

        // Un-archive back to Cold
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            archive_to_cold(&ctx.sessions, &guard, &mut session)
                .await
                .unwrap();
        }

        assert_eq!(session.tier, Tier::Cold);
        // Segment path must be unchanged — metadata-only operation
        assert_eq!(session.segment_path, segment_path_before);
    }

    #[tokio::test]
    async fn cold_to_archive_rejects_non_cold() {
        // A freshly seeded session is Warm; cold_to_archive should return Err
        let (ctx, mut session) = seed("reject me", "s-arch-3").await;
        let box_ = ctx.registry.route_tenant("t-1");
        let guard = box_.tenant_write_lock("t-1").lock_owned().await;
        let err = cold_to_archive(&ctx.sessions, &guard, &mut session)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not cold"),
            "unexpected error: {err}"
        );
    }
}
