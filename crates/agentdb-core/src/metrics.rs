use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Per-box counters and gauges. All writes are non-blocking via atomics; the
/// single `RwLock` protects the tier gauge bundle.
#[derive(Debug, Default)]
pub struct BoxMetrics {
    pub inserts: AtomicU64,
    pub queries_find: AtomicU64,
    pub queries_aggregate: AtomicU64,
    pub appends: AtomicU64,
    pub forks: AtomicU64,
    pub tier_transitions: AtomicU64,
    pub quota_hits: AtomicU64,
    pub faults_injected: AtomicU64,
    // M9 additions:
    pub open_snapshots: AtomicU64,               // gauge
    pub segments_pinned_by_snapshots: AtomicU64, // gauge, sampled from pin registry
    pub segment_gc_reclaimed: AtomicU64,         // counter
    pub segment_gc_skipped_pinned: AtomicU64,    // counter
    // M12 additions:
    pub webhook_fires: AtomicU64,
    pub webhook_failures: AtomicU64,
    // M15 hedged-read additions:
    /// Count of segment reads where the secondary (hedge) request was
    /// fired because the primary did not finish within the per-tier
    /// delay. A subset of these are `hedged_secondary_wins`.
    pub hedged_triggers: AtomicU64,
    /// Count of hedged reads where the primary completed first *after*
    /// the secondary had been fired — primary was slow but still beat
    /// the hedge.
    pub hedged_primary_wins: AtomicU64,
    /// Count of hedged reads where the secondary completed first. These
    /// are the cases where hedging actually bought latency.
    pub hedged_secondary_wins: AtomicU64,
    // M16 persistent+incremental indexing:
    /// Count of events inserted into the live index via the write
    /// path. Does not include events processed during a lazy initial
    /// rebuild; only counts post-build increments.
    pub events_indexed: AtomicU64,
    /// Count of successful index snapshot uploads to object storage.
    pub index_snapshots_written: AtomicU64,
    /// Count of failed index snapshot uploads (logged + retried next tick).
    pub index_snapshot_failures: AtomicU64,
    // M21 lease management:
    /// Successful lease renewals (including the initial acquire).
    pub lease_renewals: AtomicU64,
    /// Failed renewal attempts (meta unreachable, epoch conflict, etc.).
    pub lease_renewal_failures: AtomicU64,
    /// Lease observed lost — renewal failed AND the local lease
    /// expiry has passed wall clock.
    pub lease_lost: AtomicU64,
    /// Current lease epoch, gauge-style. Zero means no lease held.
    pub lease_epoch: AtomicU64,
    // M23 admission semaphore:
    /// Count of write-path requests that had to wait on the per-box
    /// admission semaphore because all permits were in use. Sustained
    /// growth means the configured capacity is biting and either the
    /// workload needs `AGENTDB_ADMISSION_CAPACITY` raised or the
    /// cluster needs more boxes.
    pub admission_waits_total: AtomicU64,
    // M20 multi-node forwarding:
    /// Count of writes this node has forwarded to a peer node
    /// because the target tablet is owned elsewhere. Counted
    /// on the originating node's box 0 (admin-chosen attribution
    /// since the actual work happens on the remote box).
    pub forwarded_writes_total: AtomicU64,
    /// Forwarded writes that the peer rejected (non-2xx
    /// response) or that failed to connect. Helps operators
    /// diagnose partial multi-node partitions.
    pub forwarded_write_failures_total: AtomicU64,
    pub gauges: RwLock<GaugeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GaugeSnapshot {
    pub warm_sessions: u64,
    pub cold_sessions: u64,
    pub archive_sessions: u64,
}

impl BoxMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_query_find(&self) {
        self.queries_find.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_query_aggregate(&self) {
        self.queries_aggregate.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_append(&self) {
        self.appends.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_fork(&self) {
        self.forks.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_tier_transition(&self) {
        self.tier_transitions.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_quota_hit(&self) {
        self.quota_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_fault(&self) {
        self.faults_injected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_open_snapshots(&self) {
        self.open_snapshots.fetch_add(1, Ordering::Relaxed);
    }
    pub fn drop_open_snapshot(&self) {
        // Saturating: never decrement below zero. `fetch_update` loops on
        // CAS failure so concurrent drops can't silently lose decrements.
        let _ = self
            .open_snapshots
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                if v == 0 {
                    None // stop: nothing to decrement
                } else {
                    Some(v - 1)
                }
            });
    }
    pub fn set_segments_pinned(&self, n: u64) {
        self.segments_pinned_by_snapshots
            .store(n, Ordering::Relaxed);
    }
    pub fn record_segment_gc_reclaimed(&self) {
        self.segment_gc_reclaimed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_segment_gc_skipped_pinned(&self) {
        self.segment_gc_skipped_pinned
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_webhook_fire(&self) {
        self.webhook_fires.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_webhook_failure(&self) {
        self.webhook_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Secondary (hedge) request was fired because primary did not
    /// finish within the per-tier delay.
    pub fn record_hedged_trigger(&self) {
        self.hedged_triggers.fetch_add(1, Ordering::Relaxed);
    }
    /// Primary completed first after secondary had been fired.
    pub fn record_hedged_primary_win(&self) {
        self.hedged_primary_wins.fetch_add(1, Ordering::Relaxed);
    }
    /// Secondary completed first — hedging bought latency.
    pub fn record_hedged_secondary_win(&self) {
        self.hedged_secondary_wins.fetch_add(1, Ordering::Relaxed);
    }

    /// Event reached the incremental index via the write path.
    pub fn record_event_indexed(&self) {
        self.events_indexed.fetch_add(1, Ordering::Relaxed);
    }
    /// Index snapshot successfully uploaded to object storage.
    pub fn record_index_snapshot_written(&self) {
        self.index_snapshots_written.fetch_add(1, Ordering::Relaxed);
    }
    /// Index snapshot attempt failed; incremented by the periodic job.
    pub fn record_index_snapshot_failure(&self) {
        self.index_snapshot_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lease_renewal(&self) {
        self.lease_renewals.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_lease_renewal_failure(&self) {
        self.lease_renewal_failures.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_lease_lost(&self) {
        self.lease_lost.fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_lease_epoch(&self, epoch: i64) {
        self.lease_epoch.store(epoch as u64, Ordering::Relaxed);
    }

    /// Admission semaphore forced the caller to wait for a permit.
    pub fn record_admission_wait(&self) {
        self.admission_waits_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_tier(&self, tier: crate::types::Tier) {
        let mut g = self.gauges.write();
        match tier {
            crate::types::Tier::Warm => g.warm_sessions += 1,
            crate::types::Tier::Cold => g.cold_sessions += 1,
            crate::types::Tier::Archive => g.archive_sessions += 1,
        }
    }
    pub fn drop_tier(&self, tier: crate::types::Tier) {
        let mut g = self.gauges.write();
        match tier {
            crate::types::Tier::Warm => g.warm_sessions = g.warm_sessions.saturating_sub(1),
            crate::types::Tier::Cold => g.cold_sessions = g.cold_sessions.saturating_sub(1),
            crate::types::Tier::Archive => {
                g.archive_sessions = g.archive_sessions.saturating_sub(1)
            }
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            inserts: self.inserts.load(Ordering::Relaxed),
            queries_find: self.queries_find.load(Ordering::Relaxed),
            queries_aggregate: self.queries_aggregate.load(Ordering::Relaxed),
            appends: self.appends.load(Ordering::Relaxed),
            forks: self.forks.load(Ordering::Relaxed),
            tier_transitions: self.tier_transitions.load(Ordering::Relaxed),
            quota_hits: self.quota_hits.load(Ordering::Relaxed),
            faults_injected: self.faults_injected.load(Ordering::Relaxed),
            open_snapshots: self.open_snapshots.load(Ordering::Relaxed),
            segments_pinned_by_snapshots: self.segments_pinned_by_snapshots.load(Ordering::Relaxed),
            segment_gc_reclaimed: self.segment_gc_reclaimed.load(Ordering::Relaxed),
            segment_gc_skipped_pinned: self.segment_gc_skipped_pinned.load(Ordering::Relaxed),
            webhook_fires: self.webhook_fires.load(Ordering::Relaxed),
            webhook_failures: self.webhook_failures.load(Ordering::Relaxed),
            hedged_triggers: self.hedged_triggers.load(Ordering::Relaxed),
            hedged_primary_wins: self.hedged_primary_wins.load(Ordering::Relaxed),
            hedged_secondary_wins: self.hedged_secondary_wins.load(Ordering::Relaxed),
            events_indexed: self.events_indexed.load(Ordering::Relaxed),
            index_snapshots_written: self.index_snapshots_written.load(Ordering::Relaxed),
            index_snapshot_failures: self.index_snapshot_failures.load(Ordering::Relaxed),
            lease_renewals: self.lease_renewals.load(Ordering::Relaxed),
            lease_renewal_failures: self.lease_renewal_failures.load(Ordering::Relaxed),
            lease_lost: self.lease_lost.load(Ordering::Relaxed),
            lease_epoch: self.lease_epoch.load(Ordering::Relaxed),
            admission_waits_total: self.admission_waits_total.load(Ordering::Relaxed),
            forwarded_writes_total: self.forwarded_writes_total.load(Ordering::Relaxed),
            forwarded_write_failures_total: self
                .forwarded_write_failures_total
                .load(Ordering::Relaxed),
            gauges: self.gauges.read().clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct MetricsSnapshot {
    pub inserts: u64,
    pub queries_find: u64,
    pub queries_aggregate: u64,
    pub appends: u64,
    pub forks: u64,
    pub tier_transitions: u64,
    pub quota_hits: u64,
    pub faults_injected: u64,
    pub open_snapshots: u64,
    pub segments_pinned_by_snapshots: u64,
    pub segment_gc_reclaimed: u64,
    pub segment_gc_skipped_pinned: u64,
    pub webhook_fires: u64,
    pub webhook_failures: u64,
    pub hedged_triggers: u64,
    pub hedged_primary_wins: u64,
    pub hedged_secondary_wins: u64,
    pub events_indexed: u64,
    pub index_snapshots_written: u64,
    pub index_snapshot_failures: u64,
    pub lease_renewals: u64,
    pub lease_renewal_failures: u64,
    pub lease_lost: u64,
    pub lease_epoch: u64,
    pub admission_waits_total: u64,
    pub forwarded_writes_total: u64,
    pub forwarded_write_failures_total: u64,
    pub gauges: GaugeSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Tier;

    #[test]
    fn counters_increment() {
        let m = BoxMetrics::new();
        m.record_append();
        m.record_append();
        m.record_fork();
        let s = m.snapshot();
        assert_eq!(s.appends, 2);
        assert_eq!(s.forks, 1);
    }

    #[test]
    fn webhook_counters_increment() {
        let m = BoxMetrics::new();
        m.record_webhook_fire();
        m.record_webhook_fire();
        m.record_webhook_failure();
        let s = m.snapshot();
        assert_eq!(s.webhook_fires, 2);
        assert_eq!(s.webhook_failures, 1);
    }

    #[test]
    fn tier_gauges_reflect_bump_and_drop() {
        let m = BoxMetrics::new();
        m.bump_tier(Tier::Warm);
        m.bump_tier(Tier::Warm);
        m.bump_tier(Tier::Cold);
        m.drop_tier(Tier::Warm);
        let s = m.snapshot();
        assert_eq!(s.gauges.warm_sessions, 1);
        assert_eq!(s.gauges.cold_sessions, 1);
        assert_eq!(s.gauges.archive_sessions, 0);
    }

    #[test]
    fn drop_tier_saturates_at_zero() {
        let m = BoxMetrics::new();
        m.drop_tier(Tier::Warm);
        assert_eq!(m.snapshot().gauges.warm_sessions, 0);
    }

    #[test]
    fn open_snapshots_gauge_bump_and_drop() {
        let m = BoxMetrics::new();
        m.bump_open_snapshots();
        m.bump_open_snapshots();
        m.drop_open_snapshot();
        let s = m.snapshot();
        assert_eq!(s.open_snapshots, 1);
    }

    #[test]
    fn open_snapshots_saturates_at_zero() {
        let m = BoxMetrics::new();
        m.drop_open_snapshot();
        assert_eq!(m.snapshot().open_snapshots, 0);
    }

    #[test]
    fn segment_gc_counters_increment() {
        let m = BoxMetrics::new();
        m.record_segment_gc_reclaimed();
        m.record_segment_gc_reclaimed();
        m.record_segment_gc_skipped_pinned();
        let s = m.snapshot();
        assert_eq!(s.segment_gc_reclaimed, 2);
        assert_eq!(s.segment_gc_skipped_pinned, 1);
    }

    #[test]
    fn segments_pinned_snapshot_reads_back_whatever_was_set() {
        let m = BoxMetrics::new();
        m.set_segments_pinned(3);
        assert_eq!(m.snapshot().segments_pinned_by_snapshots, 3);
        m.set_segments_pinned(0);
        assert_eq!(m.snapshot().segments_pinned_by_snapshots, 0);
    }

    #[test]
    fn drop_open_snapshot_is_race_safe() {
        use std::sync::Arc;
        use std::thread;

        let m = Arc::new(BoxMetrics::new());
        // Bump 1000 times on the main thread.
        for _ in 0..1000 {
            m.bump_open_snapshots();
        }
        // Drop concurrently from 8 threads.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let m = m.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..125 {
                    m.drop_open_snapshot();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 1000 bumps, 8 * 125 = 1000 drops — gauge must land exactly at 0.
        assert_eq!(m.snapshot().open_snapshots, 0);
    }
}
