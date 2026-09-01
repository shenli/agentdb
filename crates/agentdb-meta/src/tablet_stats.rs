//! Tablet stats VIEW types (metadata self-hosting stage 1).
//!
//! The `tablet_stats` SQL table is gone: storage/throughput observations
//! ride the per-node [`crate::node_operational`] summaries, and these types
//! are the reconstructed views (`node_operational::stats_view` /
//! `stats_aggregates`) that the scheduler and admin API consume.

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TabletStats {
    pub tablet_id: i64,
    pub node_id: String,
    pub key_count: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub storage_bytes: i64,
    pub applied_index: i64,
    pub sample_interval_ms: i64,
    pub write_ops_delta: i64,
    pub write_ops_per_sec_milli: i64,
    pub compaction_debt_ops: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TabletStatsAggregate {
    pub tablet_id: i64,
    pub replica_count: i64,
    pub key_count: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub storage_bytes: i64,
    pub applied_index: i64,
    pub sample_interval_ms: i64,
    pub write_ops_delta: i64,
    pub write_ops_per_sec_milli: i64,
    pub compaction_debt_ops: i64,
    pub updated_at_ms: i64,
}
