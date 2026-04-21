use std::sync::Arc;
use std::time::Duration;

use agentdb_storage::KvStore;
use object_store::ObjectStore;
use tokio::task::JoinHandle;

use crate::box_runtime::Box_;
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::tiering::cold_transition;
use crate::types::{Session, Tier};

#[derive(Debug, Clone, Copy)]
pub struct LifecycleConfig {
    pub tick: Duration,
    pub t_cold: Duration,
    pub t_archive: Duration,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(2),
            t_cold: Duration::from_secs(60),
            t_archive: Duration::from_secs(600),
        }
    }
}

/// Callback invoked after each successful tier transition performed
/// by the lifecycle sweep. `None` at `LifecycleEngine::start` call
/// site = no-op. Signature: `(box_, fresh_session, from_tier, to_tier)`.
pub type TierChangeHook = Arc<dyn Fn(&Arc<Box_>, Session, Tier, Tier) + Send + Sync>;

pub struct LifecycleEngine {
    _handle: JoinHandle<()>,
}

impl LifecycleEngine {
    pub fn start(
        kv: Arc<dyn KvStore>,
        store: Arc<dyn ObjectStore>,
        tenants: Arc<TenantRegistry>,
        sessions: Arc<SessionRegistry>,
        box_: Arc<Box_>,
        config: LifecycleConfig,
        hook: Option<TierChangeHook>,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(err) =
                    sweep(&kv, &store, &tenants, &sessions, &box_, config, hook.as_ref()).await
                {
                    tracing::warn!(box_id = box_.id, ?err, "lifecycle sweep errored");
                }
            }
        });
        Self { _handle: handle }
    }
}

async fn sweep(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    tenants: &Arc<TenantRegistry>,
    sessions: &Arc<SessionRegistry>,
    box_: &Arc<Box_>,
    config: LifecycleConfig,
    hook: Option<&TierChangeHook>,
) -> crate::error::Result<()> {
    let tenant_externals = tenants.list_in_box(box_.id)?;
    let now_ms = now_millis();
    for tenant_ext in tenant_externals {
        let Some(tenant) = tenants.lookup(box_.id, &tenant_ext)? else {
            continue;
        };
        let session_externals = sessions.list_in_tenant(box_.id, tenant.internal_id)?;

        // Pass 1: warm → cold
        let cold_threshold_ms = config.t_cold.as_millis() as u64;
        for session_ext in &session_externals {
            let Some(session) = sessions.lookup(box_.id, tenant.internal_id, session_ext)? else {
                continue;
            };
            if session.tier != Tier::Warm {
                continue;
            }
            if now_ms.saturating_sub(session.last_active_at) < cold_threshold_ms {
                continue;
            }
            let guard = box_.write_lock.lock().await;
            let Some(mut fresh) = sessions.lookup(box_.id, tenant.internal_id, session_ext)?
            else {
                continue;
            };
            if fresh.tier != Tier::Warm {
                continue;
            }
            if now_ms.saturating_sub(fresh.last_active_at) < cold_threshold_ms {
                continue;
            }
            match cold_transition(kv, store, sessions, &guard, &mut fresh).await {
                Ok(()) => {
                    let _ = session;
                    box_.metrics.record_tier_transition();
                    box_.metrics.drop_tier(Tier::Warm);
                    box_.metrics.bump_tier(Tier::Cold);
                    if let Some(h) = hook {
                        h(box_, fresh.clone(), Tier::Warm, Tier::Cold);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        box_id = box_.id,
                        session = %session_ext,
                        ?err,
                        "cold_transition failed"
                    );
                }
            }
        }

        // Pass 2: cold → archive
        let archive_threshold_ms = config.t_archive.as_millis() as u64;
        for session_ext in &session_externals {
            let Some(session) = sessions.lookup(box_.id, tenant.internal_id, session_ext)? else {
                continue;
            };
            if session.tier != Tier::Cold {
                continue;
            }
            if now_ms.saturating_sub(session.last_active_at) < archive_threshold_ms {
                continue;
            }
            let guard = box_.write_lock.lock().await;
            let Some(mut fresh) = sessions.lookup(box_.id, tenant.internal_id, session_ext)?
            else {
                continue;
            };
            if fresh.tier != Tier::Cold {
                continue;
            }
            if now_ms.saturating_sub(fresh.last_active_at) < archive_threshold_ms {
                continue;
            }
            match crate::tiering::cold_to_archive(sessions, &guard, &mut fresh).await {
                Ok(()) => {
                    box_.metrics.record_tier_transition();
                    box_.metrics.drop_tier(Tier::Cold);
                    box_.metrics.bump_tier(Tier::Archive);
                    if let Some(h) = hook {
                        h(box_, fresh.clone(), Tier::Cold, Tier::Archive);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        box_id = box_.id,
                        session = %session_ext,
                        ?err,
                        "cold_to_archive failed"
                    );
                }
            }
        }
    }
    Ok(())
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
    use tokio::time::{sleep, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn engine_demotes_idle_warm_session_to_cold_on_tick() {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));

        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"text": "to be cold"}),
            0,
            1, // ancient last_active_at so threshold is trivially met
        )
        .await
        .unwrap();

        let box_ = registry.route_tenant("t-1");
        let _engine = LifecycleEngine::start(
            kv.clone(),
            store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_.clone(),
            LifecycleConfig {
                tick: Duration::from_millis(50),
                t_cold: Duration::from_millis(10),
                // Use Duration::MAX so archive never triggers during this test.
                t_archive: Duration::MAX,
            },
            None,
        );

        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut seen_cold = false;
        for _ in 0..40 {
            if let Some(s) = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap() {
                if s.tier == Tier::Cold {
                    seen_cold = true;
                    break;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(seen_cold, "session never transitioned to cold");
    }

    #[tokio::test]
    async fn sweep_demotes_warm_session_direct() {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));

        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-warm",
            json!({"text": "to be cold"}),
            0,
            1,
        )
        .await
        .unwrap();

        let box_ = registry.route_tenant("t-1");
        let config = LifecycleConfig {
            tick: Duration::from_secs(60),
            t_cold: Duration::from_millis(10),
            // Duration::MAX ensures archive never triggers for this warm->cold test.
            t_archive: Duration::MAX,
        };
        sweep(&kv, &store, &tenants, &sessions, &box_, config, None)
            .await
            .unwrap();

        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let s = sessions
            .lookup(box_.id, tenant.internal_id, "s-warm")
            .unwrap()
            .unwrap();
        assert_eq!(s.tier, Tier::Cold, "session should have been demoted to cold");
    }

    #[tokio::test]
    async fn sweep_archives_cold_session_past_t_archive() {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));

        // Seed a session so the box and tenant exist
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-arch",
            json!({"text": "to be archived"}),
            0,
            1, // ancient last_active_at
        )
        .await
        .unwrap();

        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

        // Manually transition to Cold first (cold_to_archive requires Cold tier)
        {
            let mut session = sessions
                .lookup(box_.id, tenant.internal_id, "s-arch")
                .unwrap()
                .unwrap();
            let guard = box_.write_lock.lock().await;
            crate::tiering::cold_transition(
                &kv,
                &store,
                &sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
            // Override last_active_at to be ancient so archive threshold is met
            session.last_active_at = 1;
            sessions.save(&session).unwrap();
        }

        // Verify the session is now Cold
        let session = sessions
            .lookup(box_.id, tenant.internal_id, "s-arch")
            .unwrap()
            .unwrap();
        assert_eq!(session.tier, Tier::Cold);

        // Run sweep with a very short t_archive threshold
        let config = LifecycleConfig {
            tick: Duration::from_secs(60),
            t_cold: Duration::from_millis(10),
            t_archive: Duration::from_millis(10),
        };
        sweep(&kv, &store, &tenants, &sessions, &box_, config, None)
            .await
            .unwrap();

        // Assert session is now Archive
        let after = sessions
            .lookup(box_.id, tenant.internal_id, "s-arch")
            .unwrap()
            .unwrap();
        assert_eq!(after.tier, Tier::Archive, "session should have been archived");
    }
}
