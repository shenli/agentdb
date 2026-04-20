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
    pub gauges: RwLock<GaugeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
            gauges: self.gauges.read().clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
