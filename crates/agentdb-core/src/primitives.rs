use std::sync::Arc;

use agentdb_storage::{encode_key, KvStore, Namespace, WriteOp};
use tokio::sync::MutexGuard;

use crate::error::{Error, Result};
use crate::hlc::HlcTimestamp;
use crate::meta::key_session_record;
use crate::registry::SessionRegistry;
use crate::types::{Event, Session, Tier};

/// Default branch id for sessions that have never been forked. Fork arrives in M6.
pub const DEFAULT_BRANCH_ID: u32 = 0;

/// Append one event to a session, atomically bumping its `next_seq` and
/// `last_active_at`. Must be called while holding the box's write lock
/// (demonstrated by the MutexGuard reference).
///
/// `hlc_ts` should be allocated from the process-global `HlcClock` before
/// calling — see `append_and_fetch` for the wired-up call site. Callers
/// that do not yet have an HLC wired (test fixtures, migration tools) may
/// pass `HlcTimestamp::ZERO`; this is allowed but should not happen on
/// the hot path.
///
/// `actor_id` is free-form agent attribution; optional per `agent-db-v2.md` §2.3.
#[allow(clippy::too_many_arguments)]
pub fn append_event(
    kv: &Arc<dyn KvStore>,
    _lock: &MutexGuard<'_, ()>,
    session: &mut Session,
    payload: crate::types::Document,
    now_millis: u64,
    hlc_ts: HlcTimestamp,
    actor_id: Option<String>,
) -> Result<Event> {
    let seq = session.next_seq;
    let event = Event {
        seq,
        ts: now_millis,
        hlc_ts,
        actor_id,
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
///
/// Reads through the pinned snapshot held by `ctx`. Any writes committed
/// after the ReadCtx opened are invisible. As a belt-and-suspenders guard
/// we also drop events with `seq >= session.next_seq` in case a stale
/// session record was passed in from outside the snapshot.
pub fn fetch_last_n(
    ctx: &crate::read_ctx::ReadCtx,
    session: &Session,
    n: usize,
) -> Result<Vec<Event>> {
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
    let cutoff = ctx.hlc_cutoff();
    let mut ring: std::collections::VecDeque<Event> = std::collections::VecDeque::with_capacity(n);
    for res in ctx.scan_prefix(&prefix)? {
        let (_k, v) = res?;
        let ev: Event = serde_json::from_slice(&v)?;
        if ev.seq >= session.next_seq {
            continue;
        }
        if ev.hlc_ts > cutoff {
            continue;
        }
        if ring.len() == n {
            ring.pop_front();
        }
        ring.push_back(ev);
    }
    Ok(ring.into())
}

/// Scan all events of a session in seq order (no bound). Used by wire-protocol
/// `find`. Streams through the pinned snapshot's prefix iterator; eager-collect
/// because the caller typically applies a filter and limit.
///
/// The `seq < session.next_seq` guard protects against stale session records
/// passed in from outside the snapshot. Under normal use the snapshot filters
/// newer writes already.
pub fn scan_session_events(
    ctx: &crate::read_ctx::ReadCtx,
    session: &Session,
) -> Result<Vec<Event>> {
    let prefix = agentdb_storage::prefix_for_session_branch(
        Namespace::Events,
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
    );
    let cutoff = ctx.hlc_cutoff();
    let mut all: Vec<Event> = Vec::new();
    for res in ctx.scan_prefix(&prefix)? {
        let (_k, v) = res?;
        let ev: Event = serde_json::from_slice(&v)?;
        // Belt-and-suspenders: guard against stale session records passed in
        // from outside the snapshot. Under normal use the snapshot filters
        // newer writes already.
        if ev.seq >= session.next_seq {
            continue;
        }
        if ev.hlc_ts > cutoff {
            continue;
        }
        all.push(ev);
    }
    Ok(all)
}

use object_store::ObjectStore;

use crate::box_runtime::BoxRegistry;
use crate::registry::TenantRegistry;

/// Scan every event of a session regardless of tier, honoring the
/// current `ReadCtx`'s HLC cutoff. Unifies warm-KV, cold-segment, and
/// archive-segment reads behind a single call — so that "reading an
/// archived session" and "reading a cold session" differ only in
/// latency, not in semantics (M17).
///
/// - `Tier::Warm` → KV prefix scan via the pinned snapshot.
/// - `Tier::Cold` / `Tier::Archive` → `read_segment_hedged` against
///   the session's `segment_path`, with per-tier hedge delay.
///
/// A cold/archive session with no `segment_path` (shouldn't happen
/// under normal lifecycle transitions) surfaces as empty rather than
/// an error — the caller already tolerates an empty tier in the
/// pre-M17 code paths.
#[tracing::instrument(
    name = "scan_session_all_tiers",
    skip_all,
    fields(
        session = %session.external_id,
        tier = ?session.tier,
        next_seq = session.next_seq,
    ),
)]
pub async fn scan_session_all_tiers(
    ctx: &crate::read_ctx::ReadCtx,
    store: &Arc<dyn ObjectStore>,
    session: &Session,
) -> Result<Vec<Event>> {
    match session.tier {
        Tier::Warm => scan_session_events(ctx, session),
        Tier::Cold | Tier::Archive => {
            let Some(path) = session.segment_path.as_deref() else {
                return Ok(Vec::new());
            };
            let seg =
                match crate::segment::read_segment_hedged(store, path, session.tier, ctx.metrics())
                    .await
                {
                    Ok(s) => s,
                    // A concurrent `reactivate` / GC could have deleted
                    // the segment between the metadata read and this
                    // object-store fetch. Treat as empty rather than
                    // propagating; matches the existing fork-parent
                    // cold branch in `fetch_last_n_merged`.
                    Err(_) => return Ok(Vec::new()),
                };
            let cutoff = ctx.hlc_cutoff();
            Ok(seg
                .events
                .into_iter()
                .filter(|e| e.seq < session.next_seq && e.hlc_ts <= cutoff)
                .collect())
        }
    }
}

/// Fork+tier-aware version of `fetch_last_n`.
///
/// Non-forked session: returns the last N events of the session itself.
/// Forked session: merges parent events (with `seq < parent_seq_at_fork`)
/// read from the parent's tier-appropriate source — warm reads from
/// `cf_events`, cold reads the segment via `object_store`, archive is treated
/// as empty. The merged set is sorted by `ts` and truncated to the last N.
#[tracing::instrument(
    name = "fetch_last_n_merged",
    skip_all,
    fields(
        session = %session.external_id,
        tier = ?session.tier,
        fork = session.forked_from.is_some(),
        n,
    ),
)]
pub async fn fetch_last_n_merged(
    ctx: &crate::read_ctx::ReadCtx,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    session: &Session,
    n: usize,
) -> Result<Vec<Event>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let own = fetch_last_n(ctx, session, n)?;
    let Some(fork) = session.forked_from.as_ref() else {
        return Ok(own);
    };
    let Some(parent) = ctx.session_by_internal(
        sessions,
        session.tenant_internal_id,
        fork.parent_session_internal_id,
    )?
    else {
        return Ok(own);
    };
    let cutoff = ctx.hlc_cutoff();
    // M17: unify cold/archive reads via the tier-aware helper. Archive
    // no longer shortcircuits to empty; the segment lookup is the same
    // as cold-tier read, with the (longer) archive hedge delay.
    let parent_events = scan_session_all_tiers(ctx, store, &parent).await?;
    let mut all: Vec<Event> = own;
    all.extend(
        parent_events
            .into_iter()
            .filter(|e| e.seq < fork.parent_seq_at_fork && e.hlc_ts <= cutoff),
    );
    all.sort_by_key(|e| e.ts);
    let start = all.len().saturating_sub(n);
    Ok(all.split_off(start))
}

#[derive(Debug)]
pub struct AppendFetchOutput {
    pub new_seq: u64,
    pub recent: Vec<Event>,
    pub session: Session,
}

/// Append one event and return the last N events of the session in a
/// single atomic request. The `actor_id` for the event is `None`; use
/// [`append_and_fetch_with_actor`] for agent-attributed writes.
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
    append_and_fetch_with_actor(
        registry,
        tenants,
        sessions,
        tenant_external,
        session_external,
        payload,
        None,
        fetch_last_n_,
        now_millis,
    )
    .await
}

/// Like [`append_and_fetch`] but attaches an `actor_id` (agent
/// identifier) to the event. See `agent-db-v2.md` §2.3.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    name = "append_and_fetch",
    skip_all,
    fields(
        tenant = tenant_external,
        session = session_external,
        actor = actor_id.as_deref().unwrap_or(""),
        fetch_last_n = fetch_last_n_,
        seq = tracing::field::Empty,
        hlc_physical_ns = tracing::field::Empty,
    ),
)]
pub async fn append_and_fetch_with_actor(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    tenant_external: &str,
    session_external: &str,
    payload: crate::types::Document,
    actor_id: Option<String>,
    fetch_last_n_: usize,
    now_millis: u64,
) -> Result<AppendFetchOutput> {
    let box_ = registry.route_tenant(tenant_external);
    // M21: lease-alive check. If this box has a lease configured
    // (MetaStore-backed registry) and it has expired, fail fast so
    // the caller can retry or be routed to the new owner. When no
    // lease is configured (common path in tests + single-node dev),
    // the check is a cheap `Option::is_none` and we skip to the
    // existing fault path unchanged.
    if let Some(lease) = box_.lease() {
        if !lease.is_live() {
            box_.metrics.record_lease_lost();
            return Err(Error::LeaseLost {
                box_id: box_.id,
                epoch: lease.current_epoch(),
            });
        }
    }
    // Fault injection check — if an error fault is active, short-circuit.
    if box_.apply_fault().await {
        box_.metrics.record_fault();
        return Err(Error::FaultInjected {
            box_id: box_.id,
            // Fault is active (apply_fault returned true); we know the
            // kind is Error because only Error faults short-circuit.
            fault_kind: crate::box_runtime::FaultKind::Error,
        });
    }
    // Admission control: per-box gate always, per-tenant gate only for
    // already-minted tenants. `get_or_mint` is not safe to call outside
    // the write_lock (it has a TOCTOU window on the "is this a new
    // tenant?" check), so the first-ever request for a new tenant uses
    // a sentinel id of 0 which skips the per-tenant gate. Subsequent
    // requests for that tenant will hit both gates.
    let admit_tenant_id = tenants
        .lookup(box_.id, tenant_external)?
        .map(|t| t.internal_id)
        .unwrap_or(0);
    box_.admit(admit_tenant_id)
        .map_err(|d| Error::TooManyRequests {
            scope: d.scope,
            retry_after_ms: 1000,
        })?;
    // Soft quota check on warm sessions.
    {
        let q = *box_.quota.read();
        if let Some(max) = q.max_warm_sessions {
            let g = box_.metrics.gauges.read().clone();
            if g.warm_sessions >= max {
                box_.metrics.record_quota_hit();
                return Err(Error::Invalid {
                    field: Some("max_warm_sessions".into()),
                    message: format!("warm_sessions cap {max} reached on box {}", box_.id),
                });
            }
        }
    }
    let guard = box_.write_lock.lock().await;
    let kv = registry.kv().clone();
    let tenant = tenants.get_or_mint(box_.id, tenant_external, now_millis)?;
    let newly_minted = sessions
        .lookup(box_.id, tenant.internal_id, session_external)?
        .is_none();
    let mut session =
        sessions.get_or_mint(box_.id, tenant.internal_id, session_external, now_millis)?;
    if newly_minted {
        box_.metrics.bump_tier(session.tier);
    }
    // Allocate the HLC inside the session lock, just before `seq`, so that
    // (seq, hlc_ts) within one session is strictly monotonic. See
    // `docs/superpowers/notes/2026-04-21-hlc-clock-decision.md` §3.3.
    let hlc_ts = registry.hlc().now();
    let event = append_event(
        &kv,
        &guard,
        &mut session,
        payload,
        now_millis,
        hlc_ts,
        actor_id,
    )?;
    // Record HLC + seq on the current span for observability.
    tracing::Span::current()
        .record("seq", event.seq)
        .record("hlc_physical_ns", hlc_ts.physical_ns);
    box_.metrics.record_append();
    // M16 Phase C: keep the live index current. Lock order is always
    // `write_lock -> indexes.write` — this call obeys it because we
    // still hold `guard` (the write_lock) and `Box_::index_event` takes
    // only the indexes write lock internally.
    box_.index_event(
        &kv,
        registry.object_store(),
        tenants,
        sessions,
        session.internal_id,
        &event,
    )
    .await?;
    drop(guard);
    // M9: read through a freshly opened ReadCtx so fetch sees a consistent
    // snapshot including the write we just committed. The snapshot opens
    // AFTER the box write lock is dropped, so RocksDB picks up our batch.
    let ctx = crate::read_ctx::ReadCtx::open(&box_, registry.kv())?;
    let recent = fetch_last_n_merged(
        &ctx,
        registry.object_store(),
        sessions,
        &session,
        fetch_last_n_,
    )
    .await?;
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
        let event = append_event(
            &kv,
            &guard,
            &mut session,
            payload,
            now_millis,
            HlcTimestamp::ZERO,
            None,
        )?;
        Ok((event, session))
    }

    fn context() -> (
        TempDir,
        TempDir,
        BoxRegistry,
        TenantRegistry,
        SessionRegistry,
    ) {
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap(),
        );
        let registry = BoxRegistry::new(kv.clone(), store, 4).unwrap();
        let tenants = TenantRegistry::new(kv.clone());
        let sessions = SessionRegistry::new(kv);
        (dir, obj_dir, registry, tenants, sessions)
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
    async fn appends_in_a_session_are_monotonic_in_both_seq_and_hlc() {
        // `append_and_fetch` assigns HLC inside the session lock, right
        // before `seq`. The invariant is: for any session's events, both
        // fields strictly increase.
        let (_dir, _obj_dir, registry, tenants, sessions) = context();
        let mut outputs = Vec::new();
        for i in 1..=20u64 {
            let out = append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-mono",
                "s-mono",
                json!({"i": i}),
                0,
                i * 10,
            )
            .await
            .unwrap();
            outputs.push(out);
        }

        // seq increases by 1 each time.
        for (i, out) in outputs.iter().enumerate() {
            assert_eq!(
                out.new_seq,
                (i + 1) as u64,
                "seq at index {} not monotonic",
                i
            );
        }

        // Pull events back and verify their hlc_ts is strictly increasing.
        let box_ = registry.route_tenant("t-mono");
        let tenant = tenants.get_or_mint(box_.id, "t-mono", 0).unwrap();
        let session = sessions
            .lookup(box_.id, tenant.internal_id, "s-mono")
            .unwrap()
            .unwrap();
        let ctx = crate::read_ctx::ReadCtx::open(&box_, registry.kv()).unwrap();
        let events = fetch_last_n(&ctx, &session, 100).unwrap();
        assert_eq!(events.len(), 20);
        for pair in events.windows(2) {
            assert!(
                pair[0].hlc_ts < pair[1].hlc_ts,
                "hlc_ts not strictly monotonic across appends: {:?} then {:?}",
                pair[0].hlc_ts,
                pair[1].hlc_ts
            );
        }

        // Every event's hlc_ts is non-zero — real wall-clock-backed HLC,
        // not the Phase-B placeholder ZERO.
        for ev in &events {
            assert_ne!(
                ev.hlc_ts,
                crate::hlc::HlcTimestamp::ZERO,
                "event seq {} got placeholder HLC — did append_and_fetch regress?",
                ev.seq
            );
        }
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

    // Minimal helper for low-level tests that only have a standalone `kv`
    // and tenant/session registries: mints an ephemeral Box_ via a
    // throwaway BoxRegistry so we can open a ReadCtx.
    fn ephemeral_box_ctx(kv: &Arc<dyn KvStore>) -> (TempDir, crate::read_ctx::ReadCtx) {
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;
        let obj_dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = BoxRegistry::new(kv.clone(), store, 1).unwrap();
        let box_ = reg.box_by_id(0).unwrap();
        let ctx = crate::read_ctx::ReadCtx::open(&box_, kv).unwrap();
        (obj_dir, ctx)
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

        let (_obj, ctx) = ephemeral_box_ctx(&kv);
        let last_3 = fetch_last_n(&ctx, &session, 3).unwrap();
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
        let (_obj, ctx) = ephemeral_box_ctx(&kv);
        let all = fetch_last_n(&ctx, &session, 100).unwrap();
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
            segment_path: None,
            forked_from: None,
            primary_agent_id: None,
        };
        let (_obj, ctx) = ephemeral_box_ctx(&kv);
        let out = fetch_last_n(&ctx, &session, 0).unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn scan_session_events_via_read_ctx_is_isolated_from_new_appends() {
        use crate::read_ctx::ReadCtx;
        let (_dir, _obj_dir, registry, tenants, sessions) = context();
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"i": 1}),
            0,
            10,
        )
        .await
        .unwrap();
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let session_at_open = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();

        let ctx = ReadCtx::open(&box_, registry.kv()).unwrap();

        // Write more events after the ReadCtx opens.
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"i": 2}),
            0,
            20,
        )
        .await
        .unwrap();
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"i": 3}),
            0,
            30,
        )
        .await
        .unwrap();

        let events = scan_session_events(&ctx, &session_at_open).unwrap();
        assert_eq!(
            events.len(),
            1,
            "snapshot should see only events visible at open"
        );
        assert_eq!(events[0].seq, 1);
    }

    #[tokio::test]
    async fn fetch_last_n_merged_via_read_ctx_isolates_post_snapshot_writes() {
        use crate::read_ctx::ReadCtx;
        let (_dir, _obj_dir, registry, tenants, sessions) = context();

        // Parent: 3 events.
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
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

        // Fork child before snapshot.
        let parent = sessions
            .lookup(box_.id, tenant.internal_id, "parent")
            .unwrap()
            .unwrap();
        let fork_ref = crate::types::ForkRef {
            parent_session_internal_id: parent.internal_id,
            parent_seq_at_fork: parent.next_seq,
        };
        sessions
            .get_or_mint_with_fork(box_.id, tenant.internal_id, "child", 1000, Some(fork_ref))
            .unwrap();
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "child",
            json!({"c": 1}),
            0,
            100,
        )
        .await
        .unwrap();

        let ctx = ReadCtx::open(&box_, registry.kv()).unwrap();

        // After snapshot open: two more writes (parent + child) invisible to ctx.
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "parent",
            json!({"i": 99}),
            0,
            200,
        )
        .await
        .unwrap();
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "child",
            json!({"c": 2}),
            0,
            210,
        )
        .await
        .unwrap();

        let child_at_open = ctx
            .session(&sessions, tenant.internal_id, "child")
            .unwrap()
            .unwrap();
        let got = fetch_last_n_merged(&ctx, registry.object_store(), &sessions, &child_at_open, 10)
            .await
            .unwrap();
        // Expect: 3 parent events (ts 10, 20, 30) + 1 child event (ts 100) = 4.
        // No post-snapshot writes: parent's seq 99 (ts 200) / child's seq 2 (ts 210).
        assert_eq!(got.len(), 4);
        assert_eq!(got.iter().filter(|e| e.ts == 200).count(), 0);
        assert_eq!(got.iter().filter(|e| e.ts == 210).count(), 0);
    }

    #[tokio::test]
    async fn append_and_fetch_returns_new_seq_and_recent_events() {
        let (_dir, _obj_dir, registry, tenants, sessions) = context();
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
        let (_dir, _obj_dir, registry, tenants, sessions) = context();
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
    async fn fetch_last_n_merged_on_forked_session_pulls_parent_tail() {
        use crate::read_ctx::ReadCtx;

        let (_dir, _obj_dir, registry, tenants, sessions) = context();
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
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let parent = sessions
            .lookup(box_.id, tenant.internal_id, "parent")
            .unwrap()
            .unwrap();
        let fork_ref = crate::types::ForkRef {
            parent_session_internal_id: parent.internal_id,
            parent_seq_at_fork: 4,
        };
        sessions
            .get_or_mint_with_fork(box_.id, tenant.internal_id, "child", 1000, Some(fork_ref))
            .unwrap();

        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "child",
            json!({"c": 1}),
            0,
            100,
        )
        .await
        .unwrap();
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "child",
            json!({"c": 2}),
            0,
            110,
        )
        .await
        .unwrap();

        let child = sessions
            .lookup(box_.id, tenant.internal_id, "child")
            .unwrap()
            .unwrap();
        let ctx = ReadCtx::open(&box_, registry.kv()).unwrap();
        let got = fetch_last_n_merged(&ctx, registry.object_store(), &sessions, &child, 4)
            .await
            .unwrap();
        let tss: Vec<u64> = got.iter().map(|e| e.ts).collect();
        assert_eq!(tss, vec![20, 30, 100, 110]);
    }

    #[tokio::test]
    async fn fetch_last_n_merged_on_archived_parent_still_pulls_parent_tail() {
        // M17 core regression: previously the `Tier::Archive` branch
        // short-circuited to empty and the child's fetch_last_n would
        // miss parent events. Now archive reads through the cold
        // segment just like cold tier.
        use crate::read_ctx::ReadCtx;
        let (_dir, _obj_dir, registry, tenants, sessions) = context();
        for i in 1..=3u64 {
            append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-arc",
                "parent",
                json!({"i": i}),
                0,
                i * 10,
            )
            .await
            .unwrap();
        }
        let box_ = registry.route_tenant("t-arc");
        let tenant = tenants.get_or_mint(box_.id, "t-arc", 0).unwrap();
        let mut parent = sessions
            .lookup(box_.id, tenant.internal_id, "parent")
            .unwrap()
            .unwrap();
        let fork_ref = crate::types::ForkRef {
            parent_session_internal_id: parent.internal_id,
            parent_seq_at_fork: 4,
        };
        sessions
            .get_or_mint_with_fork(box_.id, tenant.internal_id, "child", 1000, Some(fork_ref))
            .unwrap();

        // Transition parent to Cold (writes segment + flips tier),
        // then directly to Archive (metadata flip; segment stays put).
        {
            let guard = box_.write_lock.lock().await;
            crate::tiering::cold_transition(
                registry.kv(),
                registry.object_store(),
                &sessions,
                &guard,
                &mut parent,
            )
            .await
            .unwrap();
        }
        parent.tier = crate::types::Tier::Archive;
        sessions.save(&parent).unwrap();

        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-arc",
            "child",
            json!({"c": 1}),
            0,
            100,
        )
        .await
        .unwrap();

        let child = sessions
            .lookup(box_.id, tenant.internal_id, "child")
            .unwrap()
            .unwrap();
        let ctx = ReadCtx::open(&box_, registry.kv()).unwrap();
        let got = fetch_last_n_merged(&ctx, registry.object_store(), &sessions, &child, 10)
            .await
            .unwrap();
        // Parent events (seq 1..=3, with parent_seq_at_fork=4) plus
        // child seq 1. All four ts values must be present regardless
        // of the parent's Archive tier.
        let tss: std::collections::BTreeSet<u64> = got.iter().map(|e| e.ts).collect();
        assert_eq!(
            tss,
            std::collections::BTreeSet::from([10, 20, 30, 100]),
            "archived parent must still surface all pre-fork events"
        );
    }

    #[tokio::test]
    async fn different_tenants_do_not_interfere() {
        let (_dir, _obj_dir, registry, tenants, sessions) = context();
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
