use std::sync::Arc;

use agentdb_storage::{encode_key, KvStore, Namespace, WriteOp};
use tokio::sync::MutexGuard;

use crate::error::Result;
use crate::meta::key_session_record;
use crate::registry::SessionRegistry;
use crate::types::{Event, Session};

/// Default branch id for sessions that have never been forked. Fork arrives in M6.
pub const DEFAULT_BRANCH_ID: u32 = 0;

/// Append one event to a session, atomically bumping its `next_seq` and
/// `last_active_at`. Must be called while holding the box's write lock
/// (demonstrated by the MutexGuard reference).
pub fn append_event(
    kv: &Arc<dyn KvStore>,
    _lock: &MutexGuard<'_, ()>,
    session: &mut Session,
    payload: crate::types::Document,
    now_millis: u64,
) -> Result<Event> {
    let seq = session.next_seq;
    let event = Event {
        seq,
        ts: now_millis,
        payload,
    };
    let event_key = encode_key(
        Namespace::Events,
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
        seq,
    );
    let event_bytes = serde_json::to_vec(&event)?;
    session.next_seq = seq + 1;
    session.last_active_at = now_millis;
    let session_bytes = serde_json::to_vec(session)?;
    let session_key = key_session_record(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
    );
    kv.write_batch(vec![
        WriteOp::Put {
            key: event_key,
            value: event_bytes,
        },
        WriteOp::Put {
            key: session_key,
            value: session_bytes,
        },
    ])?;
    Ok(event)
}

/// Fetch the last `n` events of `session`'s default branch in seq order
/// (oldest-to-newest within the returned slice).
///
/// Streams through the prefix scan with a bounded ring buffer so memory is
/// O(n), not O(session size). Events arrive in ascending seq order because
/// the key layout sorts by seq last.
pub fn fetch_last_n(kv: &Arc<dyn KvStore>, session: &Session, n: usize) -> Result<Vec<Event>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let prefix = agentdb_storage::prefix_for_session_branch(
        Namespace::Events,
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
    );
    let mut ring: std::collections::VecDeque<Event> = std::collections::VecDeque::with_capacity(n);
    for res in kv.scan_prefix(&prefix)? {
        let (_k, v) = res?;
        let ev: Event = serde_json::from_slice(&v)?;
        if ring.len() == n {
            ring.pop_front();
        }
        ring.push_back(ev);
    }
    Ok(ring.into())
}

/// Scan all events of a session in seq order (no bound). Used by wire-protocol
/// `find`. Streams through RocksDB's prefix iterator; eager-collect because the
/// caller typically applies a filter and limit.
pub fn scan_session_events(kv: &Arc<dyn KvStore>, session: &Session) -> Result<Vec<Event>> {
    let prefix = agentdb_storage::prefix_for_session_branch(
        Namespace::Events,
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
    );
    let mut all: Vec<Event> = Vec::new();
    for res in kv.scan_prefix(&prefix)? {
        let (_k, v) = res?;
        let ev: Event = serde_json::from_slice(&v)?;
        all.push(ev);
    }
    Ok(all)
}

use crate::box_runtime::BoxRegistry;
use crate::registry::TenantRegistry;

#[derive(Debug)]
pub struct AppendFetchOutput {
    pub new_seq: u64,
    pub recent: Vec<Event>,
    pub session: Session,
}

#[allow(clippy::too_many_arguments)]
pub async fn append_and_fetch(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    tenant_external: &str,
    session_external: &str,
    payload: crate::types::Document,
    fetch_last_n_: usize,
    now_millis: u64,
) -> Result<AppendFetchOutput> {
    let box_ = registry.route_tenant(tenant_external);
    let guard = box_.write_lock.lock().await;
    let kv = registry.kv().clone();
    let tenant = tenants.get_or_mint(box_.id, tenant_external, now_millis)?;
    let mut session =
        sessions.get_or_mint(box_.id, tenant.internal_id, session_external, now_millis)?;
    let event = append_event(&kv, &guard, &mut session, payload, now_millis)?;
    drop(guard);
    let recent = fetch_last_n(&kv, &session, fetch_last_n_)?;
    Ok(AppendFetchOutput {
        new_seq: event.seq,
        recent,
        session,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::box_runtime::BoxRegistry;
    use crate::registry::{SessionRegistry, TenantRegistry};
    use agentdb_storage::RocksKvStore;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    fn setup() -> (TempDir, Arc<dyn KvStore>, TenantRegistry, SessionRegistry) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let tenants = TenantRegistry::new(kv.clone());
        let sessions = SessionRegistry::new(kv.clone());
        (dir, kv, tenants, sessions)
    }

    // Test-only helper replacing the former `open_and_append` free function.
    #[allow(clippy::too_many_arguments)]
    async fn open_and_append(
        kv: Arc<dyn KvStore>,
        sessions: &SessionRegistry,
        box_lock: &tokio::sync::Mutex<()>,
        box_id: agentdb_storage::BoxId,
        tenant_id: agentdb_storage::TenantId,
        session_external: &str,
        payload: crate::types::Document,
        now_millis: u64,
    ) -> Result<(Event, Session)> {
        let guard = box_lock.lock().await;
        let mut session = sessions.get_or_mint(box_id, tenant_id, session_external, now_millis)?;
        let event = append_event(&kv, &guard, &mut session, payload, now_millis)?;
        Ok((event, session))
    }

    fn context() -> (TempDir, BoxRegistry, TenantRegistry, SessionRegistry) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = BoxRegistry::new(kv.clone(), 4).unwrap();
        let tenants = TenantRegistry::new(kv.clone());
        let sessions = SessionRegistry::new(kv);
        (dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn append_increments_seq_and_persists_event() {
        let (_dir, kv, tenants, sessions) = setup();
        let lock = Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 10).unwrap();

        let (e1, s1) = open_and_append(
            kv.clone(),
            &sessions,
            &lock,
            0,
            t.internal_id,
            "s-1",
            json!({"k": 1}),
            100,
        )
        .await
        .unwrap();
        assert_eq!(e1.seq, 1);
        assert_eq!(s1.next_seq, 2);
        assert_eq!(s1.last_active_at, 100);

        let (e2, s2) = open_and_append(
            kv.clone(),
            &sessions,
            &lock,
            0,
            t.internal_id,
            "s-1",
            json!({"k": 2}),
            200,
        )
        .await
        .unwrap();
        assert_eq!(e2.seq, 2);
        assert_eq!(s2.next_seq, 3);
        assert_eq!(s2.last_active_at, 200);
    }

    #[tokio::test]
    async fn event_is_readable_at_expected_key() {
        let (_dir, kv, tenants, sessions) = setup();
        let lock = Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 1).unwrap();
        let (_e, s) = open_and_append(
            kv.clone(),
            &sessions,
            &lock,
            0,
            t.internal_id,
            "s-1",
            json!({"hello": "world"}),
            50,
        )
        .await
        .unwrap();

        let k = encode_key(
            Namespace::Events,
            s.box_id,
            s.tenant_internal_id,
            s.internal_id,
            DEFAULT_BRANCH_ID,
            1,
        );
        let bytes = kv.get(&k).unwrap().unwrap();
        let ev: Event = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(ev.seq, 1);
        assert_eq!(ev.payload, json!({"hello": "world"}));
    }

    #[tokio::test]
    async fn fetch_last_n_returns_tail_in_order() {
        let (_dir, kv, tenants, sessions) = setup();
        let lock = Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 0).unwrap();
        for i in 1..=5u64 {
            open_and_append(
                kv.clone(),
                &sessions,
                &lock,
                0,
                t.internal_id,
                "s-1",
                json!({"i": i}),
                i,
            )
            .await
            .unwrap();
        }
        let session = sessions.lookup(0, t.internal_id, "s-1").unwrap().unwrap();

        let last_3 = fetch_last_n(&kv, &session, 3).unwrap();
        assert_eq!(last_3.len(), 3);
        assert_eq!(last_3[0].seq, 3);
        assert_eq!(last_3[1].seq, 4);
        assert_eq!(last_3[2].seq, 5);
    }

    #[tokio::test]
    async fn fetch_last_n_larger_than_size_returns_all_in_order() {
        let (_dir, kv, tenants, sessions) = setup();
        let lock = Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 0).unwrap();
        for i in 1..=2u64 {
            open_and_append(
                kv.clone(),
                &sessions,
                &lock,
                0,
                t.internal_id,
                "s-1",
                json!({"i": i}),
                i,
            )
            .await
            .unwrap();
        }
        let session = sessions.lookup(0, t.internal_id, "s-1").unwrap().unwrap();
        let all = fetch_last_n(&kv, &session, 100).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].seq, 1);
        assert_eq!(all[1].seq, 2);
    }

    #[test]
    fn fetch_last_n_zero_returns_empty() {
        let (_dir, kv, _tenants, _sessions) = setup();
        let session = Session {
            internal_id: 1,
            external_id: "s".into(),
            tenant_internal_id: 1,
            box_id: 0,
            created_at: 0,
            last_active_at: 0,
            tier: crate::types::Tier::Warm,
            next_seq: 1,
        };
        let out = fetch_last_n(&kv, &session, 0).unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn append_and_fetch_returns_new_seq_and_recent_events() {
        let (_dir, registry, tenants, sessions) = context();
        let out = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"a": 1}),
            5,
            10,
        )
        .await
        .unwrap();
        assert_eq!(out.new_seq, 1);
        assert_eq!(out.recent.len(), 1);
        assert_eq!(out.recent[0].payload, json!({"a": 1}));

        let out2 = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"a": 2}),
            5,
            20,
        )
        .await
        .unwrap();
        assert_eq!(out2.new_seq, 2);
        assert_eq!(out2.recent.len(), 2);
        assert_eq!(out2.recent[1].payload, json!({"a": 2}));
    }

    #[tokio::test]
    async fn append_and_fetch_limits_recent_to_n() {
        let (_dir, registry, tenants, sessions) = context();
        for i in 1..=5u64 {
            append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"i": i}),
                2,
                i,
            )
            .await
            .unwrap();
        }
        let out = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"final": true}),
            2,
            100,
        )
        .await
        .unwrap();
        assert_eq!(out.new_seq, 6);
        assert_eq!(out.recent.len(), 2);
        assert_eq!(out.recent[0].seq, 5);
        assert_eq!(out.recent[1].seq, 6);
    }

    #[tokio::test]
    async fn different_tenants_do_not_interfere() {
        let (_dir, registry, tenants, sessions) = context();
        let a = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-A",
            "s-1",
            json!({"owner": "A"}),
            5,
            10,
        )
        .await
        .unwrap();
        let b = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-B",
            "s-1",
            json!({"owner": "B"}),
            5,
            20,
        )
        .await
        .unwrap();
        assert_eq!(a.new_seq, 1);
        assert_eq!(b.new_seq, 1);
        assert_ne!(a.recent, b.recent);
    }
}
