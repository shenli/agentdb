use tokio::sync::MutexGuard;

use crate::box_runtime::BoxRegistry;
use crate::error::{Error, Result};
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::types::{ForkRef, Session};

/// Create a new session whose history begins as a copy-on-write snapshot of
/// the parent's events at `parent.next_seq - 1` (inclusive).
///
/// - Must be called while holding the box write lock (demonstrated by the
///   `MutexGuard` reference).
/// - Parent must exist. Child's external id must be free.
/// - Both sessions live under the same tenant/box as the parent; fork never
///   crosses box boundaries.
#[allow(clippy::too_many_arguments)]
pub fn fork_session(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    _lock: &MutexGuard<'_, ()>,
    tenant_external: &str,
    parent_session_external: &str,
    new_session_external: &str,
    now_millis: u64,
) -> Result<Session> {
    let box_ = registry.route_tenant(tenant_external);
    let tenant = tenants
        .lookup(box_.id, tenant_external)?
        .ok_or_else(|| Error::NotFound(format!("tenant {tenant_external}")))?;
    box_.admit(tenant.internal_id).map_err(|d| Error::TooManyRequests {
        scope: d.scope.as_str().into(),
    })?;
    let parent = sessions
        .lookup(box_.id, tenant.internal_id, parent_session_external)?
        .ok_or_else(|| Error::NotFound(format!("parent session {parent_session_external}")))?;

    if sessions
        .lookup(box_.id, tenant.internal_id, new_session_external)?
        .is_some()
    {
        return Err(Error::Invalid(format!(
            "target session {new_session_external} already exists"
        )));
    }

    let fork_ref = ForkRef {
        parent_session_internal_id: parent.internal_id,
        parent_seq_at_fork: parent.next_seq,
    };
    let child = sessions.get_or_mint_with_fork(
        box_.id,
        tenant.internal_id,
        new_session_external,
        now_millis,
        Some(fork_ref),
    )?;
    box_.metrics.record_fork();
    box_.metrics.bump_tier(child.tier);
    let _ = registry.kv(); // reserved for future fork-lineage persistence
    Ok(child)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::append_and_fetch;
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn ctx() -> (
        TempDir,
        TempDir,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (kv_dir, obj_dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn fork_creates_child_with_parent_pointer() {
        let (_k, _o, registry, tenants, sessions) = ctx().await;
        for i in 1..=3u64 {
            append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "parent",
                json!({"i": i}),
                0,
                i * 10,
            )
            .await
            .unwrap();
        }
        let box_ = registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        let child = fork_session(
            &registry, &tenants, &sessions, &guard, "t-1", "parent", "child", 1000,
        )
        .unwrap();
        drop(guard);

        assert!(child.forked_from.is_some());
        let fork = child.forked_from.as_ref().unwrap();
        assert_eq!(fork.parent_seq_at_fork, 4);
        assert_eq!(child.next_seq, 1);
    }

    #[tokio::test]
    async fn fork_with_missing_parent_errors() {
        let (_k, _o, registry, tenants, sessions) = ctx().await;
        tenants
            .get_or_mint(registry.route_tenant("t-1").id, "t-1", 0)
            .unwrap();
        let box_ = registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        let err = fork_session(
            &registry, &tenants, &sessions, &guard, "t-1", "nope", "child", 0,
        )
        .unwrap_err();
        assert!(err.to_string().contains("parent session"));
    }

    #[tokio::test]
    async fn fork_with_existing_child_errors() {
        let (_k, _o, registry, tenants, sessions) = ctx().await;
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "parent",
            json!({}),
            0,
            1,
        )
        .await
        .unwrap();
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "taken",
            json!({}),
            0,
            1,
        )
        .await
        .unwrap();
        let box_ = registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        let err = fork_session(
            &registry, &tenants, &sessions, &guard, "t-1", "parent", "taken", 0,
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }
}
