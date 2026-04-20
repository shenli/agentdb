use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_storage::{encode_key, KvStore, Namespace, WriteOp};
use object_store::ObjectStore;
use tokio::sync::MutexGuard;

use crate::error::{Error, Result};
use crate::primitives::DEFAULT_BRANCH_ID;
use crate::registry::SessionRegistry;
use crate::segment::{
    delete_segment, read_segment, segment_path, write_segment, SegmentFile, SEGMENT_SCHEMA_V1,
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
/// Precondition: caller holds the box's `write_lock` (demonstrated by the
/// `MutexGuard` reference). `session.tier` must be `Warm`.
pub async fn cold_transition(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    _lock: &MutexGuard<'_, ()>,
    session: &mut Session,
) -> Result<()> {
    if session.tier != Tier::Warm {
        return Err(Error::Invalid(format!(
            "cold_transition: session {} is not Warm (tier={:?})",
            session.external_id, session.tier
        )));
    }
    // Inlined pending T7 refactor to route through ReadCtx. cold_transition
    // runs under the box's write_lock so there are no concurrent appends to
    // this session's key space; reading directly from the live KV here is
    // equivalent to reading from a snapshot opened at lock-acquire time.
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
        schema_version: SEGMENT_SCHEMA_V1,
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
/// Precondition: caller holds the box's `write_lock`. `session.tier` must be `Cold`.
pub async fn reactivate(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    _lock: &MutexGuard<'_, ()>,
    session: &mut Session,
) -> Result<()> {
    if session.tier != Tier::Cold {
        return Err(Error::Invalid(format!(
            "reactivate: session {} is not Cold (tier={:?})",
            session.external_id, session.tier
        )));
    }
    let Some(path) = session.segment_path.clone() else {
        return Err(Error::Invalid(format!(
            "reactivate: cold session {} has no segment_path",
            session.external_id
        )));
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
        let guard = box_.write_lock.lock().await;
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

        let read_ctx =
            crate::read_ctx::ReadCtx::open(&box_, ctx.registry.kv()).unwrap();
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
        let guard = box_.write_lock.lock().await;
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
            let guard = box_.write_lock.lock().await;
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
            let guard = box_.write_lock.lock().await;
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

        let read_ctx =
            crate::read_ctx::ReadCtx::open(&box_, ctx.registry.kv()).unwrap();
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
            let guard = box_.write_lock.lock().await;
            cold_transition(ctx.registry.kv(), ctx.registry.object_store(),
                &ctx.sessions, &guard, &mut session).await.unwrap();
        }
        let path = session.segment_path.clone().unwrap();
        {
            let guard = box_.write_lock.lock().await;
            reactivate(ctx.registry.kv(), ctx.registry.object_store(),
                &ctx.sessions, &guard, &mut session).await.unwrap();
        }
        // After M9 change: segment is still on disk; GC will reap it later.
        assert!(
            read_segment(ctx.registry.object_store(), &path).await.is_ok(),
            "segment should still exist after reactivate"
        );
    }

    #[tokio::test]
    async fn reactivate_rejects_non_cold_session() {
        let (ctx, mut session) = seed("t", "s").await;
        let box_ = ctx.registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
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
}
