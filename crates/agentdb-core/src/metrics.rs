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
}
