//! In-process operational-state registry (metadata self-hosting stage 1).
//!
//! Collects what THIS process observes about raft replicas — progress
//! applied through the ingress, reachability observed by the message
//! outbox, movement catch-up — in memory. The node heartbeat serializes
//! the registry (together with live raft status and storage stats) into
//! one `node_operational` summary row per heartbeat interval. Nothing on
//! a raft send/receive/apply path touches SQL anymore.
//!
//! Contents are ephemeral by design: lost on restart, rebuilt by the next
//! observations within a heartbeat period.

use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaObservation {
    pub applied_index: i64,
    pub match_index: i64,
    pub health: String,
    pub observed_at_ms: i64,
}

#[derive(Default)]
pub struct OperationalRegistry {
    /// Keyed by (tablet_id, control-plane node id of the observed replica).
    replicas: Mutex<BTreeMap<(i64, String), ReplicaObservation>>,
    /// Per-tablet raft peer-id sets, cached from placement-intent rows so
    /// the message ingress validates peers without SQL per message.
    peer_sets: Mutex<BTreeMap<i64, (i64, std::collections::BTreeSet<u64>)>>,
    /// raft node id -> control-plane node id, learned wherever placement
    /// intent is read (registry sync, peer-url refresh). Stable for a
    /// node's lifetime, so no TTL.
    raft_nodes: Mutex<BTreeMap<u64, String>>,
    /// Latest storage/throughput observations for tablets this node stores,
    /// replaced wholesale by the stats sampler each tick.
    tablet_stats: Mutex<Vec<agentdb_meta::node_operational::TabletStatsObservation>>,
}

/// How long a cached peer set answers ingress validation before it must be
/// re-read from placement intent. Membership changes are minutes-scale.
pub const PEER_SET_TTL_MS: i64 = 5_000;

impl OperationalRegistry {
    pub fn record_progress(
        &self,
        tablet_id: i64,
        node_id: &str,
        applied_index: i64,
        match_index: i64,
        now_ms: i64,
    ) {
        let mut replicas = self.replicas.lock().expect("operational registry lock");
        let entry =
            replicas
                .entry((tablet_id, node_id.to_string()))
                .or_insert(ReplicaObservation {
                    applied_index: 0,
                    match_index: 0,
                    health: "healthy".to_string(),
                    observed_at_ms: now_ms,
                });
        entry.applied_index = entry.applied_index.max(applied_index);
        entry.match_index = entry.match_index.max(match_index);
        // Forward progress is itself a health signal.
        entry.health = "healthy".to_string();
        entry.observed_at_ms = now_ms;
    }

    /// `health` is one of `healthy`, `lagging`, `unreachable`.
    pub fn record_health(&self, tablet_id: i64, node_id: &str, health: &str, now_ms: i64) {
        let mut replicas = self.replicas.lock().expect("operational registry lock");
        let entry =
            replicas
                .entry((tablet_id, node_id.to_string()))
                .or_insert(ReplicaObservation {
                    applied_index: 0,
                    match_index: 0,
                    health: "healthy".to_string(),
                    observed_at_ms: now_ms,
                });
        entry.health = health.to_string();
        entry.observed_at_ms = now_ms;
    }

    /// Cached raft peer ids for a tablet, if fresh.
    pub fn cached_peer_set(
        &self,
        tablet_id: i64,
        now_ms: i64,
    ) -> Option<std::collections::BTreeSet<u64>> {
        let peer_sets = self.peer_sets.lock().expect("operational registry lock");
        peer_sets.get(&tablet_id).and_then(|(cached_at, set)| {
            (now_ms.saturating_sub(*cached_at) <= PEER_SET_TTL_MS).then(|| set.clone())
        })
    }

    pub fn store_peer_set(
        &self,
        tablet_id: i64,
        now_ms: i64,
        set: std::collections::BTreeSet<u64>,
    ) {
        self.peer_sets
            .lock()
            .expect("operational registry lock")
            .insert(tablet_id, (now_ms, set));
    }

    pub fn map_raft_node(&self, raft_node_id: u64, node_id: &str) {
        self.raft_nodes
            .lock()
            .expect("operational registry lock")
            .insert(raft_node_id, node_id.to_string());
    }

    pub fn resolve_raft_node(&self, raft_node_id: u64) -> Option<String> {
        self.raft_nodes
            .lock()
            .expect("operational registry lock")
            .get(&raft_node_id)
            .cloned()
    }

    pub fn set_tablet_stats(
        &self,
        stats: Vec<agentdb_meta::node_operational::TabletStatsObservation>,
    ) {
        *self.tablet_stats.lock().expect("operational registry lock") = stats;
    }

    pub fn tablet_stats_snapshot(
        &self,
    ) -> Vec<agentdb_meta::node_operational::TabletStatsObservation> {
        self.tablet_stats
            .lock()
            .expect("operational registry lock")
            .clone()
    }

    /// Drop observations for a tablet this process no longer participates in.
    pub fn forget_tablet(&self, tablet_id: i64) {
        self.replicas
            .lock()
            .expect("operational registry lock")
            .retain(|(tablet, _), _| *tablet != tablet_id);
    }

    /// Snapshot for heartbeat serialization, dropping entries older than
    /// `cutoff_ms` so a replica that stopped being observed ages out of the
    /// node's report instead of being re-asserted forever.
    pub fn snapshot(&self, cutoff_ms: i64) -> Vec<(i64, String, ReplicaObservation)> {
        let mut replicas = self.replicas.lock().expect("operational registry lock");
        replicas.retain(|_, observation| observation.observed_at_ms >= cutoff_ms);
        replicas
            .iter()
            .map(|((tablet_id, node_id), observation)| {
                (*tablet_id, node_id.clone(), observation.clone())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_is_monotone_and_marks_healthy() {
        let registry = OperationalRegistry::default();
        registry.record_progress(1, "node-a", 10, 10, 100);
        registry.record_health(1, "node-a", "unreachable", 150);
        registry.record_progress(1, "node-a", 5, 20, 200);
        let snapshot = registry.snapshot(0);
        assert_eq!(snapshot.len(), 1);
        let (_, _, obs) = &snapshot[0];
        assert_eq!(obs.applied_index, 10, "applied never regresses");
        assert_eq!(obs.match_index, 20);
        assert_eq!(obs.health, "healthy", "new progress clears unreachable");
    }

    #[test]
    fn snapshot_ages_out_stale_entries() {
        let registry = OperationalRegistry::default();
        registry.record_progress(1, "node-a", 10, 10, 100);
        registry.record_progress(2, "node-b", 3, 3, 500);
        assert_eq!(registry.snapshot(200).len(), 1);
        // The stale entry was also evicted, not just filtered.
        assert_eq!(registry.snapshot(0).len(), 1);
    }
}
