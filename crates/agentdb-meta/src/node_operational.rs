//! Per-node operational summaries (metadata self-hosting stage 1).
//!
//! Replaces the per-tablet `tablet_replicas` progress/health writes and the
//! `tablet_stats` table. Each node reports ONE row at heartbeat cadence: a
//! JSON summary of everything it observes about the raft groups it
//! participates in and the tablets it stores. Write rate is O(nodes) per
//! heartbeat interval instead of O(tablets x messages); readers reconstruct
//! the fleet view from the reports.
//!
//! Staleness is handled the same way node liveness is: `reported_at_ms`
//! plus a cutoff. A crashed node's report ages out with its heartbeat, and
//! that is correct — stale operational state is worse than absent state.

use sqlx::AnyPool;

use crate::error::MetaResult;
use crate::tablet_replicas::TabletReplica;
use crate::tablet_stats::{TabletStats, TabletStatsAggregate};

/// What one node knows, reported wholesale each heartbeat.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeOperationalSummary {
    /// Observations about raft replicas. Usually the reporting node's own
    /// replicas; a leader may also report what raft tells it about peers.
    #[serde(default)]
    pub replicas: Vec<ReplicaObservation>,
    /// Storage/throughput observations for tablets this node stores.
    #[serde(default)]
    pub tablet_stats: Vec<TabletStatsObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicaObservation {
    pub tablet_id: i64,
    /// Control-plane node id of the OBSERVED replica (usually the reporter).
    pub node_id: String,
    pub role: String,
    pub health: String,
    pub applied_index: i64,
    pub match_index: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TabletStatsObservation {
    pub tablet_id: i64,
    pub key_count: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub storage_bytes: i64,
    pub applied_index: i64,
    pub sample_interval_ms: i64,
    pub write_ops_delta: i64,
    pub write_ops_per_sec_milli: i64,
    pub compaction_debt_ops: i64,
}

/// Default freshness window for operational reports. Reports older than
/// this are treated as absent (the node stopped heartbeating).
pub const DEFAULT_STALENESS_MS: i64 = 60_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeOperationalReport {
    pub node_id: String,
    pub reported_at_ms: i64,
    pub summary: NodeOperationalSummary,
}

/// Replace the reporting node's summary row.
pub async fn report(
    pool: &AnyPool,
    node_id: &str,
    now_ms: i64,
    summary: &NodeOperationalSummary,
) -> MetaResult<()> {
    let encoded = serde_json::to_string(summary)
        .map_err(|err| crate::error::MetaError::InvalidInput(format!("summary encode: {err}")))?;
    sqlx::query(
        "INSERT INTO node_operational (node_id, reported_at_ms, summary)
         VALUES ($1, $2, $3)
         ON CONFLICT (node_id) DO UPDATE SET
            reported_at_ms = excluded.reported_at_ms,
            summary = excluded.summary",
    )
    .bind(node_id)
    .bind(now_ms)
    .bind(encoded)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<NodeOperationalReport>> {
    let rows: Vec<(String, i64, String)> =
        sqlx::query_as("SELECT node_id, reported_at_ms, summary FROM node_operational")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(node_id, reported_at_ms, raw)| {
            // An undecodable report (rolling upgrade, hand edit) is dropped,
            // not fatal: it regenerates on the node's next heartbeat.
            match serde_json::from_str(&raw) {
                Ok(summary) => Some(NodeOperationalReport {
                    node_id,
                    reported_at_ms,
                    summary,
                }),
                Err(error) => {
                    tracing::warn!(node_id, ?error, "dropping undecodable operational report");
                    None
                }
            }
        })
        .collect())
}

pub async fn delete(pool: &AnyPool, node_id: &str) -> MetaResult<bool> {
    let rows = sqlx::query("DELETE FROM node_operational WHERE node_id = $1")
        .bind(node_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

/// Seed convenience (bootstrap tools and tests): placement intent plus a
/// single observation in one call, mirroring the pre-stage-1
/// `tablet_replicas::upsert` shape. The observation is appended to (or
/// replaced in) the OBSERVED node's own report.
#[allow(clippy::too_many_arguments)]
pub async fn seed_replica(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    role: &str,
    health: &str,
    applied_index: i64,
    match_index: i64,
    now_ms: i64,
) -> MetaResult<()> {
    crate::tablet_replicas::upsert_intent(pool, tablet_id, node_id, role, now_ms).await?;
    let mut summary = summary_for(pool, node_id).await?;
    summary
        .replicas
        .retain(|obs| !(obs.tablet_id == tablet_id && obs.node_id == node_id));
    summary.replicas.push(ReplicaObservation {
        tablet_id,
        node_id: node_id.to_string(),
        role: role.to_string(),
        health: health.to_string(),
        applied_index,
        match_index,
    });
    report(pool, node_id, now_ms, &summary).await
}

/// Seed convenience for stats (bootstrap tools and tests), mirroring the
/// pre-stage-1 `tablet_stats::upsert_with_rate` shape.
#[allow(clippy::too_many_arguments)]
pub async fn seed_tablet_stats(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    key_count: i64,
    key_bytes: i64,
    value_bytes: i64,
    storage_bytes: i64,
    applied_index: i64,
    sample_interval_ms: i64,
    write_ops_delta: i64,
    write_ops_per_sec_milli: i64,
    compaction_debt_ops: i64,
    now_ms: i64,
) -> MetaResult<()> {
    let mut summary = summary_for(pool, node_id).await?;
    summary
        .tablet_stats
        .retain(|obs| obs.tablet_id != tablet_id);
    summary.tablet_stats.push(TabletStatsObservation {
        tablet_id,
        key_count,
        key_bytes,
        value_bytes,
        storage_bytes,
        applied_index,
        sample_interval_ms,
        write_ops_delta,
        write_ops_per_sec_milli,
        compaction_debt_ops,
    });
    report(pool, node_id, now_ms, &summary).await
}

async fn summary_for(pool: &AnyPool, node_id: &str) -> MetaResult<NodeOperationalSummary> {
    Ok(list_all(pool)
        .await?
        .into_iter()
        .find(|report| report.node_id == node_id)
        .map(|report| report.summary)
        .unwrap_or_default())
}

/// Intent joined with observations for every replica of every tablet.
///
/// Rows come from the union of placement-intent rows and fresh
/// observations. Intent supplies `role` (and surfaces `draining` through
/// `health`, preserving the pre-stage-1 vocabulary); observations supply
/// health/progress, defaulting to `unknown`/0 when no fresh report covers
/// a replica.
pub async fn merged_replica_view_all(
    pool: &AnyPool,
    cutoff_ms: i64,
) -> MetaResult<Vec<TabletReplica>> {
    let intent = crate::tablet_replicas::list_intent_all(pool).await?;
    let reports = list_all(pool).await?;
    Ok(merge_intent_and_observations(&intent, &reports, cutoff_ms))
}

pub async fn merged_replica_view_by_tablet(
    pool: &AnyPool,
    tablet_id: i64,
    cutoff_ms: i64,
) -> MetaResult<Vec<TabletReplica>> {
    let intent = crate::tablet_replicas::list_intent_by_tablet(pool, tablet_id).await?;
    let reports = list_all(pool).await?;
    Ok(merge_intent_and_observations(&intent, &reports, cutoff_ms)
        .into_iter()
        .filter(|replica| replica.tablet_id == tablet_id)
        .collect())
}

pub fn merge_intent_and_observations(
    intent: &[crate::tablet_replicas::ReplicaIntent],
    reports: &[NodeOperationalReport],
    cutoff_ms: i64,
) -> Vec<TabletReplica> {
    use std::collections::BTreeMap;
    // Placement intent DEFINES the replica set; observations only enrich
    // it. An observation with no intent row (a just-retired replica still
    // echoing in another node's report) is not a replica.
    let observations: BTreeMap<(i64, String), TabletReplica> = replica_view(reports, cutoff_ms)
        .into_iter()
        .map(|replica| ((replica.tablet_id, replica.node_id.clone()), replica))
        .collect();
    intent
        .iter()
        .map(|row| {
            let observed = observations.get(&(row.tablet_id, row.node_id.clone()));
            let health = if row.state == "draining" {
                "draining".to_string()
            } else {
                observed
                    .map(|replica| replica.health.clone())
                    .unwrap_or_else(|| "unknown".to_string())
            };
            TabletReplica {
                tablet_id: row.tablet_id,
                node_id: row.node_id.clone(),
                role: row.role.clone(),
                health,
                applied_index: observed.map(|replica| replica.applied_index).unwrap_or(0),
                match_index: observed.map(|replica| replica.match_index).unwrap_or(0),
                updated_at_ms: observed
                    .map(|replica| replica.updated_at_ms)
                    .unwrap_or(row.updated_at_ms)
                    .max(row.updated_at_ms),
            }
        })
        .collect()
}

/// Reconstruct the classic per-(tablet, node) replica view from reports.
///
/// Merge rules, in priority order for each `(tablet_id, node_id)` key:
/// self-reports (reporter == observed node) win for role/health/applied;
/// other nodes' observations (e.g. a leader's view of a follower, or
/// outbox-observed unreachability) fill gaps and can only DEGRADE health
/// and RAISE match/applied indexes. Reports older than `cutoff_ms` are
/// ignored entirely.
pub fn replica_view(reports: &[NodeOperationalReport], cutoff_ms: i64) -> Vec<TabletReplica> {
    use std::collections::BTreeMap;
    let mut merged: BTreeMap<(i64, String), TabletReplica> = BTreeMap::new();
    for report in reports {
        if report.reported_at_ms < cutoff_ms {
            continue;
        }
        for obs in &report.summary.replicas {
            let is_self = obs.node_id == report.node_id;
            let replica = merged
                .entry((obs.tablet_id, obs.node_id.clone()))
                .or_insert_with(|| TabletReplica {
                    tablet_id: obs.tablet_id,
                    node_id: obs.node_id.clone(),
                    role: obs.role.clone(),
                    health: obs.health.clone(),
                    applied_index: obs.applied_index,
                    match_index: obs.match_index,
                    updated_at_ms: report.reported_at_ms,
                });
            if is_self {
                replica.role = obs.role.clone();
            }
            // Progress only moves forward across observers; unreachability
            // observed by ANYONE degrades the merged health.
            replica.applied_index = replica.applied_index.max(obs.applied_index);
            replica.match_index = replica.match_index.max(obs.match_index);
            if obs.health == "unreachable" {
                replica.health = "unreachable".to_string();
            }
            replica.updated_at_ms = replica.updated_at_ms.max(report.reported_at_ms);
        }
    }
    merged.into_values().collect()
}

/// Reconstruct the per-(tablet, node) stats view from reports.
pub fn stats_view(reports: &[NodeOperationalReport], cutoff_ms: i64) -> Vec<TabletStats> {
    let mut out = Vec::new();
    for report in reports {
        if report.reported_at_ms < cutoff_ms {
            continue;
        }
        for obs in &report.summary.tablet_stats {
            out.push(TabletStats {
                tablet_id: obs.tablet_id,
                node_id: report.node_id.clone(),
                key_count: obs.key_count,
                key_bytes: obs.key_bytes,
                value_bytes: obs.value_bytes,
                storage_bytes: obs.storage_bytes,
                applied_index: obs.applied_index,
                sample_interval_ms: obs.sample_interval_ms,
                write_ops_delta: obs.write_ops_delta,
                write_ops_per_sec_milli: obs.write_ops_per_sec_milli,
                compaction_debt_ops: obs.compaction_debt_ops,
                updated_at_ms: report.reported_at_ms,
            });
        }
    }
    out.sort_by(|a, b| (a.tablet_id, &a.node_id).cmp(&(b.tablet_id, &b.node_id)));
    out
}

/// Per-tablet aggregates over [`stats_view`], matching the shape the old
/// SQL GROUP BY produced: max-of-sizes (replicas store the same data),
/// count of replicas, max applied index, sum of write deltas.
pub fn stats_aggregates(stats: &[TabletStats]) -> Vec<TabletStatsAggregate> {
    use std::collections::BTreeMap;
    let mut by_tablet: BTreeMap<i64, TabletStatsAggregate> = BTreeMap::new();
    for row in stats {
        let agg = by_tablet
            .entry(row.tablet_id)
            .or_insert_with(|| TabletStatsAggregate {
                tablet_id: row.tablet_id,
                replica_count: 0,
                key_count: 0,
                key_bytes: 0,
                value_bytes: 0,
                storage_bytes: 0,
                applied_index: 0,
                sample_interval_ms: 0,
                write_ops_delta: 0,
                write_ops_per_sec_milli: 0,
                compaction_debt_ops: 0,
                updated_at_ms: 0,
            });
        agg.replica_count += 1;
        agg.key_count = agg.key_count.max(row.key_count);
        agg.key_bytes = agg.key_bytes.max(row.key_bytes);
        agg.value_bytes = agg.value_bytes.max(row.value_bytes);
        agg.storage_bytes = agg.storage_bytes.max(row.storage_bytes);
        agg.applied_index = agg.applied_index.max(row.applied_index);
        agg.sample_interval_ms = agg.sample_interval_ms.max(row.sample_interval_ms);
        agg.write_ops_delta += row.write_ops_delta;
        agg.write_ops_per_sec_milli = agg.write_ops_per_sec_milli.max(row.write_ops_per_sec_milli);
        agg.compaction_debt_ops = agg.compaction_debt_ops.max(row.compaction_debt_ops);
        agg.updated_at_ms = agg.updated_at_ms.max(row.updated_at_ms);
    }
    by_tablet.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(tablet: i64, node: &str, health: &str, applied: i64) -> ReplicaObservation {
        ReplicaObservation {
            tablet_id: tablet,
            node_id: node.into(),
            role: "voter".into(),
            health: health.into(),
            applied_index: applied,
            match_index: applied,
        }
    }

    #[test]
    fn self_report_wins_but_peers_degrade_health_and_raise_indexes() {
        let reports = vec![
            NodeOperationalReport {
                node_id: "node-a".into(),
                reported_at_ms: 100,
                summary: NodeOperationalSummary {
                    replicas: vec![
                        obs(1, "node-a", "healthy", 10),
                        obs(1, "node-b", "healthy", 12),
                    ],
                    tablet_stats: vec![],
                },
            },
            NodeOperationalReport {
                node_id: "node-b".into(),
                reported_at_ms: 110,
                summary: NodeOperationalSummary {
                    replicas: vec![obs(1, "node-b", "healthy", 8)],
                    tablet_stats: vec![],
                },
            },
            NodeOperationalReport {
                node_id: "node-c".into(),
                reported_at_ms: 120,
                summary: NodeOperationalSummary {
                    replicas: vec![obs(1, "node-b", "unreachable", 0)],
                    tablet_stats: vec![],
                },
            },
        ];
        let view = replica_view(&reports, 0);
        let b = view
            .iter()
            .find(|replica| replica.node_id == "node-b")
            .unwrap();
        // Self reported applied=8, but node-a (leader view) saw 12 — max wins.
        assert_eq!(b.applied_index, 12);
        // node-c observed unreachability — degradation wins over self "healthy".
        assert_eq!(b.health, "unreachable");
    }

    #[test]
    fn stale_reports_age_out() {
        let reports = vec![NodeOperationalReport {
            node_id: "node-a".into(),
            reported_at_ms: 100,
            summary: NodeOperationalSummary {
                replicas: vec![obs(1, "node-a", "healthy", 10)],
                tablet_stats: vec![],
            },
        }];
        assert!(replica_view(&reports, 101).is_empty());
        assert_eq!(replica_view(&reports, 100).len(), 1);
    }
}
