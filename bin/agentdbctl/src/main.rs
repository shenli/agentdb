//! `agentdbctl` — human and agent-friendly operations CLI.
//!
//! The server's REST API remains the stable machine contract. This binary is a
//! small operator surface on top: concise commands, table output by default,
//! JSON output for automation, and no requirement to hand-write curl payloads.

use std::{
    fmt::Write as _,
    fs::{self, OpenOptions},
    io::{self, Write as IoWrite},
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
};

use agentdb_storage::{GroupedRocksDbTabletStore, TabletStoreLifecycle};
use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use url::Url;

mod import;

const DEFAULT_URL: &str = "http://127.0.0.1:7878";
const CLICKHOUSE_SCHEMA_VERSION: &str = "2026_05_15_clickstack_views";
const CLICKHOUSE_REQUIRED_OBJECTS: &[&str] = &[
    "agentdb_schema_migrations",
    "agentdb_export_checkpoints",
    "agentdb_export_items_raw",
    "agentdb_events",
    "agentdb_otel_logs",
    "agentdb_otel_traces",
    "agentdb_clickstack_logs",
    "agentdb_clickstack_traces",
    "otel_logs",
    "otel_traces",
    "agentdb_tombstones",
];
const CLICKHOUSE_SCHEMA_DDL: &[&str] = &[
    r#"
CREATE TABLE IF NOT EXISTS agentdb_schema_migrations (
    version String,
    applied_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY version
"#,
    r#"
CREATE TABLE IF NOT EXISTS agentdb_export_checkpoints (
    consumer String,
    tenant_id String,
    cursor String,
    updated_at_ms UInt64,
    updated_at DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(toInt64(updated_at_ms))
) ENGINE = ReplacingMergeTree(updated_at_ms)
ORDER BY (consumer, tenant_id)
"#,
    r#"
CREATE TABLE IF NOT EXISTS agentdb_export_items_raw (
    export_version LowCardinality(String),
    cursor String,
    op LowCardinality(String),
    object_type LowCardinality(String),
    tenant_id String,
    agent_id Nullable(String),
    session_id Nullable(String),
    object_id String,
    seq Nullable(UInt64),
    created_at_ms UInt64,
    created_at DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(toInt64(created_at_ms)),
    observed_at_ms UInt64,
    observed_at DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(toInt64(observed_at_ms)),
    cell_id Nullable(String),
    placement_scope_id Nullable(String),
    tablet_id Nullable(Int64),
    tablet_epoch Nullable(Int64),
    applied_index Nullable(UInt64),
    payload_json String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant_id, created_at, object_type, object_id, cursor)
"#,
    r#"
CREATE TABLE IF NOT EXISTS agentdb_events (
    tenant_id String,
    agent_id String,
    session_id String,
    event_id String,
    seq UInt64,
    kind LowCardinality(String),
    created_at_ms UInt64,
    created_at DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(toInt64(created_at_ms)),
    observed_at_ms UInt64,
    observed_at DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(toInt64(observed_at_ms)),
    cell_id Nullable(String),
    placement_scope_id Nullable(String),
    tablet_id Nullable(Int64),
    tablet_epoch Nullable(Int64),
    applied_index Nullable(UInt64),
    parent_event_id Nullable(String),
    status Nullable(String),
    model Nullable(String),
    provider Nullable(String),
    finish_reason Nullable(String),
    tool_name Nullable(String),
    tool_call_id Nullable(String),
    duration_ms Nullable(UInt64),
    timeout_ms Nullable(UInt64),
    input_tokens Nullable(UInt64),
    output_tokens Nullable(UInt64),
    total_tokens Nullable(UInt64),
    cost_micros Nullable(UInt64),
    error_kind Nullable(String),
    error_message Nullable(String),
    retryable Nullable(Bool),
    exit_code Nullable(Int64),
    approval_id Nullable(String),
    approval_decision Nullable(String),
    context_pack_id Nullable(String),
    source_refs_json String,
    artifact_refs_json String,
    tags_json String,
    metadata_json String,
    payload_json String,
    envelope_json String
) ENGINE = ReplacingMergeTree(observed_at_ms)
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant_id, session_id, seq, event_id)
"#,
    r#"
CREATE TABLE IF NOT EXISTS agentdb_otel_logs (
    trace_id String,
    span_id String,
    timestamp_unix_nano UInt64,
    timestamp DateTime64(9) MATERIALIZED fromUnixTimestamp64Nano(toInt64(timestamp_unix_nano)),
    severity_text LowCardinality(String),
    body String,
    resource_attributes Map(String, String),
    log_attributes Map(String, String),
    tenant_id String,
    session_id String,
    event_id String,
    envelope_json String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tenant_id, session_id, timestamp, event_id)
"#,
    r#"
CREATE TABLE IF NOT EXISTS agentdb_otel_traces (
    trace_id String,
    span_id String,
    parent_span_id Nullable(String),
    span_name String,
    span_kind LowCardinality(String),
    start_time_unix_nano UInt64,
    end_time_unix_nano UInt64,
    start_time DateTime64(9) MATERIALIZED fromUnixTimestamp64Nano(toInt64(start_time_unix_nano)),
    end_time DateTime64(9) MATERIALIZED fromUnixTimestamp64Nano(toInt64(end_time_unix_nano)),
    duration_ms Nullable(UInt64),
    status_code LowCardinality(String),
    resource_attributes Map(String, String),
    span_attributes Map(String, String),
    links_json String,
    tenant_id String,
    session_id String,
    event_id String,
    envelope_json String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(start_time)
ORDER BY (tenant_id, session_id, start_time, span_id)
"#,
    r#"
CREATE VIEW IF NOT EXISTS agentdb_clickstack_logs AS
SELECT
    timestamp AS Timestamp,
    timestamp AS TimestampTime,
    trace_id AS TraceId,
    span_id AS SpanId,
    severity_text AS SeverityText,
    body AS Body,
    ifNull(resource_attributes['service.name'], 'agentdb') AS ServiceName,
    resource_attributes AS ResourceAttributes,
    log_attributes AS LogAttributes,
    tenant_id AS AgentDBTenantId,
    session_id AS AgentDBSessionId,
    event_id AS AgentDBEventId,
    envelope_json AS AgentDBEnvelope
FROM agentdb_otel_logs
"#,
    r#"
CREATE VIEW IF NOT EXISTS agentdb_clickstack_traces AS
SELECT
    start_time AS Timestamp,
    start_time AS TimestampTime,
    trace_id AS TraceId,
    span_id AS SpanId,
    parent_span_id AS ParentSpanId,
    span_name AS SpanName,
    span_kind AS SpanKind,
    status_code AS StatusCode,
    '' AS StatusMessage,
    toUInt64(ifNull(duration_ms, 0)) * 1000000 AS Duration,
    ifNull(resource_attributes['service.name'], 'agentdb') AS ServiceName,
    resource_attributes AS ResourceAttributes,
    span_attributes AS SpanAttributes,
    links_json AS Links,
    tenant_id AS AgentDBTenantId,
    session_id AS AgentDBSessionId,
    event_id AS AgentDBEventId,
    envelope_json AS AgentDBEnvelope
FROM agentdb_otel_traces
"#,
    r#"
CREATE VIEW IF NOT EXISTS otel_logs AS
SELECT * FROM agentdb_clickstack_logs
"#,
    r#"
CREATE VIEW IF NOT EXISTS otel_traces AS
SELECT * FROM agentdb_clickstack_traces
"#,
    r#"
CREATE TABLE IF NOT EXISTS agentdb_tombstones (
    tenant_id String,
    object_type LowCardinality(String),
    object_id String,
    reason LowCardinality(String),
    deleted_at_ms UInt64,
    deleted_at DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(toInt64(deleted_at_ms)),
    envelope_json String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(deleted_at)
ORDER BY (tenant_id, object_type, object_id, deleted_at)
"#,
];

const CLICKHOUSE_SAMPLE_QUERIES: &[(&str, &str, &str)] = &[
    (
        "recent_failures",
        "Recent event failures and tool errors",
        r#"
SELECT created_at, session_id, kind, error_kind, error_message
FROM agentdb_events
WHERE tenant_id = {tenant_id:String}
  AND created_at >= now() - INTERVAL 1 HOUR
  AND (kind = 'tool_error' OR error_kind != '')
ORDER BY created_at DESC
LIMIT 100
"#,
    ),
    (
        "model_cost_by_model",
        "Model token and cost totals by model",
        r#"
SELECT model, sum(total_tokens) AS tokens, sum(cost_micros) / 1000000 AS cost
FROM agentdb_events
WHERE tenant_id = {tenant_id:String}
  AND created_at >= now() - INTERVAL 1 DAY
GROUP BY model
ORDER BY cost DESC
"#,
    ),
    (
        "slow_tools",
        "Tool p95 latency and volume",
        r#"
SELECT tool_name, quantile(0.95)(duration_ms) AS p95_ms, count() AS n
FROM agentdb_events
WHERE tenant_id = {tenant_id:String}
  AND kind IN ('tool_result', 'tool_error')
  AND created_at >= now() - INTERVAL 1 DAY
GROUP BY tool_name
ORDER BY p95_ms DESC
"#,
    ),
    (
        "clickstack_logs",
        "ClickStack-compatible wide log rows",
        r#"
SELECT Timestamp, ServiceName, SeverityText, Body, TraceId, SpanId
FROM agentdb_clickstack_logs
WHERE AgentDBTenantId = {tenant_id:String}
ORDER BY Timestamp DESC
LIMIT 100
"#,
    ),
    (
        "clickstack_traces",
        "ClickStack-compatible trace spans",
        r#"
SELECT Timestamp, ServiceName, StatusCode, round(Duration / 1000000), SpanName, TraceId, SpanId
FROM agentdb_clickstack_traces
WHERE AgentDBTenantId = {tenant_id:String}
ORDER BY Timestamp DESC
LIMIT 100
"#,
    ),
];

#[derive(Parser, Debug)]
#[command(
    name = "agentdbctl",
    version,
    about = "Operate AgentDB over its admin API"
)]
struct Cli {
    /// AgentDB base URL.
    #[arg(long, global = true, env = "AGENTDB_URL", default_value = DEFAULT_URL)]
    url: String,

    /// Bearer token for deployments with admin auth.
    #[arg(long, global = true, env = "AGENTDB_TOKEN")]
    token: Option<String>,

    /// Output format. Use `json` for scripts and AI agents.
    ///
    /// The long flag is `--format` (with `-o` retained) so it does not collide
    /// with the `export jsonl run --output <file>` sink path, which owns the
    /// `--output` long name.
    #[arg(short = 'o', long = "format", global = true, default_value = "table")]
    output: Output,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Output {
    Table,
    Json,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
enum Command {
    /// Show cluster health, readiness, scheduler, and drain state.
    Status {
        #[arg(long)]
        stale_threshold_ms: Option<i64>,
    },
    /// Node membership operations.
    Nodes {
        #[command(subcommand)]
        command: NodesCommand,
    },
    /// Logical cell / isolation-domain operations.
    Cells {
        #[command(subcommand)]
        command: CellsCommand,
    },
    /// Tenant placement policy operations.
    TenantPolicies {
        #[command(subcommand)]
        command: TenantPoliciesCommand,
    },
    /// Tablet placement and movement operations.
    Tablets {
        #[command(subcommand)]
        command: TabletsCommand,
    },
    /// Raft replica visibility and catch-up operations.
    Raft {
        #[command(subcommand)]
        command: RaftCommand,
    },
    /// Local drain operations.
    Drain {
        #[command(subcommand)]
        command: DrainCommand,
    },
    /// Backup operations.
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    /// Restore operations.
    Restore {
        #[command(subcommand)]
        command: RestoreCommand,
    },
    /// Local tablet-store maintenance operations.
    TabletStore {
        #[command(subcommand)]
        command: TabletStoreCommand,
    },
    /// Artifact inspection and lifecycle verification operations.
    Artifacts {
        #[command(subcommand)]
        command: ArtifactsCommand,
    },
    /// Observability export operations.
    Export {
        #[command(subcommand)]
        command: ExportCommand,
    },
    /// Tenant data-encryption-key (DEK) lifecycle operations.
    Encryption {
        #[command(subcommand)]
        command: EncryptionCommand,
    },
    /// Import coding-agent session logs (one JSONL file = one session) into
    /// AgentDB over the API. Re-running is safe (idempotent per line).
    Import {
        #[command(subcommand)]
        command: ImportCommand,
    },
    /// Low-level escape hatch for endpoints not yet modeled by the CLI.
    Raw {
        #[command(subcommand)]
        command: RawCommand,
    },
}

#[derive(Subcommand, Debug)]
enum ImportCommand {
    /// Import Claude Code sessions from ~/.claude/projects.
    ClaudeCode(ImportArgs),
    /// Import Codex sessions from ~/.codex/sessions.
    Codex(ImportArgs),
}

#[derive(Args, Debug)]
pub(crate) struct ImportArgs {
    /// Directory to scan for `*.jsonl` session files. Defaults to the source's
    /// standard location (`~/.claude/projects` or `~/.codex/sessions`).
    #[arg(long)]
    pub(crate) dir: Option<std::path::PathBuf>,
    /// Keep running, importing newly appended lines as they arrive.
    #[arg(long)]
    pub(crate) watch: bool,
    /// Poll interval for `--watch`, in milliseconds.
    #[arg(long, default_value_t = 2000)]
    pub(crate) poll_ms: u64,
}

#[derive(Subcommand, Debug)]
enum EncryptionCommand {
    /// Rotate a tenant's DEKs: mint a new active version, demote the old to
    /// read-only. Existing data stays readable; new writes use the new key.
    Rotate {
        #[arg(long)]
        tenant_id: String,
    },
    /// Revoke — or crypto-shred (`--destroy`) — a tenant's DEKs. A soft revoke is
    /// reversible; `--destroy` deletes the wrapped key material irreversibly.
    Revoke {
        #[arg(long)]
        tenant_id: String,
        /// Crypto-shred: destroy the wrapped key material (irreversible).
        #[arg(long)]
        destroy: bool,
    },
    /// Re-encrypt a tenant's data (metadata + content) under the active DEK
    /// version, so a rotated-away version can be retired. Idempotent.
    Reencrypt {
        #[arg(long)]
        tenant_id: String,
    },
    /// Retire a tenant's drained DEK versions (safe completion of a rotation):
    /// re-encrypts, then marks old versions no longer in use as retired.
    Retire {
        #[arg(long)]
        tenant_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum NodesCommand {
    /// List nodes.
    List {
        #[arg(long)]
        stale_threshold_ms: Option<i64>,
    },
    /// Register or upsert a node.
    Register(RegisterNodeArgs),
    /// Delete a node metadata row.
    Delete { node_id: String },
    /// Mark a node alive, draining, or maintenance.
    Maintenance {
        node_id: String,
        #[arg(value_enum)]
        state: NodeState,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
enum CellsCommand {
    /// List cells.
    List,
    /// Create or update a cell.
    Upsert(UpsertCellArgs),
}

#[derive(Subcommand, Debug)]
enum TenantPoliciesCommand {
    /// List tenant placement policies.
    List,
    /// Create or update a tenant placement policy.
    Upsert(UpsertTenantPolicyArgs),
    /// Delete a tenant placement policy.
    Delete { tenant_id: String },
}

#[derive(Args, Debug)]
struct UpsertCellArgs {
    #[arg(long)]
    cell_id: String,
    #[arg(long)]
    region: Option<String>,
    #[arg(long, default_value = "standard")]
    domain_class: String,
    #[arg(long, default_value = "single_az_cluster")]
    deployment_profile: String,
    #[arg(long, default_value = "stable")]
    capacity_class: String,
    #[arg(long, default_value = "shared_logical")]
    metadata_isolation_level: String,
    #[arg(long, default_value = "serving")]
    state: String,
    #[arg(long)]
    metadata_namespace: Option<String>,
    #[arg(long)]
    object_store_namespace: Option<String>,
    #[arg(long, default_value_t = 0)]
    rollout_version: i64,
}

#[derive(Args, Debug)]
struct UpsertTenantPolicyArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long, default_value = "shared")]
    placement_class: String,
    #[arg(long, default_value = "default")]
    placement_scope_id: String,
    #[arg(long, default_value = "default")]
    cell_id: String,
    #[arg(long, default_value = "free")]
    tier: String,
    #[arg(long, default_value_t = 0)]
    priority: i64,
    #[arg(long, default_value = "pool")]
    encryption_class: String,
    #[arg(long)]
    key_policy_id: Option<String>,
    #[arg(long, default_value_t = 0)]
    encryption_policy_version: i64,
    #[arg(long, default_value_t = 0)]
    policy_version: i64,
}

#[derive(Args, Debug)]
struct RegisterNodeArgs {
    #[arg(long)]
    node_id: String,
    #[arg(long)]
    address: String,
    #[arg(long)]
    mongo_address: Option<String>,
    #[arg(long, default_value = "alive")]
    state: String,
    #[arg(long)]
    availability_zone: Option<String>,
    #[arg(long)]
    region: Option<String>,
    #[arg(long, default_value = "default")]
    cell_id: String,
    #[arg(long, default_value = "stable")]
    capacity_class: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum NodeState {
    Alive,
    Draining,
    Maintenance,
}

impl NodeState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Draining => "draining",
            Self::Maintenance => "maintenance",
        }
    }
}

#[derive(Subcommand, Debug)]
enum TabletsCommand {
    /// List tablets.
    List,
    /// List published per-node tablet storage stats.
    Stats {
        #[arg(long)]
        tablet_id: Option<i64>,
    },
    /// Create tablet metadata idempotently.
    Create { tablet_id: i64 },
    /// Assign an unowned tablet to a fresh alive node.
    Assign { tablet_id: i64, node_id: String },
    /// Relinquish current ownership and fence the old epoch.
    Relinquish { tablet_id: i64 },
    /// Mark a tablet for cooperative handoff.
    Handoff {
        tablet_id: i64,
        target_node_id: String,
    },
    /// Complete cooperative handoff from the expected source.
    CompleteHandoff {
        tablet_id: i64,
        expected_source_node_id: String,
    },
    /// Cancel a pending handoff signal.
    CancelHandoff { tablet_id: i64 },
    /// Prepare a replacement owner as a learner and handoff target.
    PrepareReplacement {
        tablet_id: i64,
        target_node_id: String,
        #[arg(long)]
        source_node_id: Option<String>,
        #[arg(long)]
        stale_threshold_ms: Option<i64>,
    },
    /// Promote a caught-up replacement target to owner.
    CompleteReplacement {
        tablet_id: i64,
        target_node_id: String,
        #[arg(long)]
        source_node_id: Option<String>,
        #[arg(long)]
        min_applied_index: Option<i64>,
        #[arg(long)]
        allow_lagging: bool,
        #[arg(long)]
        retire_source: bool,
    },
    /// Prepare a fenced tablet split workflow.
    PrepareSplit {
        tablet_id: i64,
        #[arg(long = "child-range", required = true)]
        child_ranges: Vec<String>,
        #[arg(long)]
        placement_scope_id: Option<String>,
    },
    /// Complete a prepared tablet split and publish range cutover.
    CompleteSplit {
        tablet_id: i64,
        #[arg(long = "child-range", required = true)]
        child_ranges: Vec<String>,
        #[arg(long)]
        placement_scope_id: Option<String>,
        #[arg(long)]
        map_epoch: i64,
        #[arg(long)]
        min_applied_index: Option<i64>,
        #[arg(long)]
        allow_unready: bool,
    },
    /// Prepare a fenced tablet merge workflow.
    PrepareMerge {
        #[arg(long = "source-tablet-id", required = true)]
        source_tablet_ids: Vec<i64>,
        destination_tablet_id: i64,
        #[arg(long)]
        placement_scope_id: Option<String>,
    },
    /// Complete a prepared tablet merge and publish range cutover.
    CompleteMerge {
        #[arg(long = "source-tablet-id", required = true)]
        source_tablet_ids: Vec<i64>,
        destination_tablet_id: i64,
        #[arg(long)]
        placement_scope_id: Option<String>,
        #[arg(long)]
        map_epoch: i64,
        #[arg(long)]
        min_applied_index: Option<i64>,
        #[arg(long)]
        allow_unready: bool,
    },
    /// List split/merge state movement work.
    Movements {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
    },
    /// Run the rebalance policy.
    Rebalance {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        stale_threshold_ms: Option<i64>,
        #[arg(long)]
        max_movements: Option<usize>,
    },
}

#[derive(Subcommand, Debug)]
enum RaftCommand {
    /// Show local Raft groups and configured replica progress.
    Status,
    /// List retained Raft snapshot metadata.
    Snapshots {
        #[arg(long)]
        tablet_id: Option<i64>,
        #[arg(long)]
        include_deleted: bool,
        #[arg(long)]
        limit: Option<i64>,
    },
    /// Mark expired retained snapshot metadata deleted.
    PruneSnapshots {
        #[arg(long)]
        dry_run: bool,
    },
    /// Trigger snapshot catch-up for lagging replicas.
    CatchUp {
        #[arg(long)]
        tablet_id: Option<i64>,
        #[arg(long)]
        target_node_id: Option<String>,
        #[arg(long, default_value_t = 1)]
        min_lag: u64,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
enum DrainCommand {
    /// Show local drain state.
    Status,
    /// Mark the local node draining.
    Start,
}

#[derive(Subcommand, Debug)]
enum BackupCommand {
    /// Create an online backup manifest and optional metadata snapshot.
    Create {
        #[arg(long)]
        backup_id: Option<String>,
        #[arg(long)]
        directory: Option<String>,
        /// Do not snapshot the metadata store.
        #[arg(long)]
        skip_meta: bool,
        /// Do not include runtime storage summary in the manifest.
        #[arg(long)]
        skip_runtime_summary: bool,
        /// Include physical tablet-store checkpoints when the node uses single-tablet domains.
        #[arg(long)]
        include_tablet_checkpoints: bool,
    },
}

#[derive(Subcommand, Debug)]
enum RestoreCommand {
    /// Validate a backup manifest. Destructive restore apply is not exposed.
    Validate { manifest_path: String },
    /// Offline: copy a verified data snapshot and optional config into target paths.
    ApplyLocal {
        #[arg(long)]
        manifest_path: PathBuf,
        #[arg(long)]
        data_backup_dir: PathBuf,
        #[arg(long)]
        target_data_dir: PathBuf,
        #[arg(long)]
        config_backup: Option<PathBuf>,
        #[arg(long)]
        target_config: Option<PathBuf>,
        #[arg(long)]
        allow_overwrite: bool,
    },
    /// Offline: install one tablet checkpoint into a local single-tablet store root.
    InstallTabletCheckpoint {
        #[arg(long)]
        tablet_id: i64,
        #[arg(long)]
        checkpoint_dir: PathBuf,
        #[arg(long)]
        tablet_store_root: PathBuf,
        #[arg(long, default_value_t = 1)]
        tablets_per_domain: i64,
        #[arg(long, default_value_t = 1)]
        max_open_domains: usize,
    },
}

#[derive(Subcommand, Debug)]
enum TabletStoreCommand {
    /// Trigger manual compaction for one local tablet-store domain/range.
    Compact {
        #[arg(long)]
        tablet_id: i64,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ArtifactsCommand {
    /// Verify artifact metadata/raw-byte lifecycle consistency for one tenant.
    VerifyLifecycle {
        #[arg(long)]
        tenant_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum ExportCommand {
    /// ClickHouse observability export golden path.
    Clickhouse {
        #[command(subcommand)]
        command: ClickHouseCommand,
    },
    /// Export durable envelopes as newline-delimited JSON.
    Jsonl {
        #[command(subcommand)]
        command: JsonlCommand,
    },
    /// Convert JSONL export archives into Parquet files.
    Parquet {
        #[command(subcommand)]
        command: ParquetCommand,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
enum ClickHouseCommand {
    /// Check AgentDB export and optional ClickHouse reachability.
    Doctor(ClickHouseDoctorArgs),
    /// Print or apply the first-party ClickHouse schema.
    Init(ClickHouseInitArgs),
    /// Apply versioned first-party ClickHouse schema migrations.
    Migrate(ClickHouseMigrateArgs),
    /// Roll back the first-party ClickHouse schema.
    Rollback(ClickHouseRollbackArgs),
    /// Show AgentDB export status for one tenant.
    Status(ClickHouseStatusArgs),
    /// Print first-party dashboard and ClickStack smoke queries.
    Queries,
    /// Print ClickStack source/dashboard configuration templates.
    ClickstackConfig(ClickStackConfigArgs),
    /// Copy one or more export-watch batches into ClickHouse.
    Run(ClickHouseRunArgs),
}

#[derive(Args, Debug)]
struct ClickHouseDoctorArgs {
    #[arg(long)]
    tenant_id: Option<String>,
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,
    #[arg(long)]
    skip_write_smoke: bool,
}

#[derive(Args, Debug)]
struct ClickHouseInitArgs {
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct ClickHouseMigrateArgs {
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,
    #[arg(long, default_value = "latest")]
    to: String,
    #[arg(long)]
    apply: bool,
    #[arg(long)]
    allow_destructive: bool,
}

#[derive(Args, Debug)]
struct ClickHouseRollbackArgs {
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,
    #[arg(long)]
    apply: bool,
    #[arg(long)]
    allow_destructive: bool,
}

#[derive(Args, Debug)]
struct ClickHouseStatusArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    consumer: Option<String>,
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,
}

#[derive(Args, Debug)]
struct ClickStackConfigArgs {
    #[arg(long, default_value = "AgentDB")]
    service_name: String,
    #[arg(long, default_value = "agentdb_clickstack_logs")]
    logs_source: String,
    #[arg(long, default_value = "agentdb_clickstack_traces")]
    traces_source: String,
}

#[derive(Args, Debug)]
struct ClickHouseRunArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: String,
    #[arg(long, default_value = "tenant")]
    scope: String,
    #[arg(long)]
    agent_id: Option<String>,
    #[arg(long)]
    session_id: Option<String>,
    /// Client-side checkpoint key. Names the local resume checkpoint row
    /// (in the `agentdb_export_checkpoints` ClickHouse table), not a server
    /// consumer.
    #[arg(long)]
    consumer: Option<String>,
    #[arg(long)]
    from_cursor: Option<String>,
    /// First-page snapshot boundary required when resuming a metadata backfill.
    #[arg(long, requires = "backfill")]
    snapshot_high_watermark: Option<String>,
    #[arg(long, default_value_t = 1000)]
    limit: usize,
    #[arg(long = "include", value_delimiter = ',')]
    include: Vec<String>,
    #[arg(long = "mode", value_delimiter = ',', default_value = "native")]
    mode: Vec<String>,
    #[arg(long)]
    backfill: bool,
    #[arg(long)]
    tail: bool,
    #[arg(long)]
    max_batches: Option<u64>,
    #[arg(long, default_value_t = 1000)]
    poll_interval_ms: u64,
    #[arg(long, default_value_t = 3)]
    max_retries: u32,
    #[arg(long, default_value_t = 250)]
    retry_initial_backoff_ms: u64,
    #[command(flatten)]
    privacy: ExportPrivacyArgs,
    /// Optional local file path to refresh with exporter status JSON.
    #[arg(long)]
    metrics_output: Option<PathBuf>,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Args, Debug, Clone, Default)]
struct ExportPrivacyArgs {
    /// Comma-separated object types to export. Empty allows all object types.
    #[arg(long = "allow-object-type", value_delimiter = ',')]
    allow_object_types: Vec<String>,
    /// Comma-separated object types to suppress.
    #[arg(long = "deny-object-type", value_delimiter = ',')]
    deny_object_types: Vec<String>,
    /// Comma-separated event kinds to export. Empty allows all event kinds.
    #[arg(long = "allow-event-kind", value_delimiter = ',')]
    allow_event_kinds: Vec<String>,
    /// Comma-separated event kinds to suppress.
    #[arg(long = "deny-event-kind", value_delimiter = ',')]
    deny_event_kinds: Vec<String>,
    /// Dot-separated JSON paths to redact before writing a sink row.
    #[arg(long = "redact-field")]
    redact_fields: Vec<String>,
    /// Payload handling policy for exported `data.payload` values.
    #[arg(long, value_enum, default_value = "full")]
    payload_mode: ExportPayloadMode,
}

#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
enum ExportPayloadMode {
    #[default]
    Full,
    Hash,
    Drop,
}

#[derive(Subcommand, Debug)]
enum JsonlCommand {
    /// Copy one or more export-watch batches into a JSONL file or object.
    Run(JsonlRunArgs),
}

#[derive(Args, Debug)]
struct JsonlRunArgs {
    #[arg(long)]
    tenant_id: String,
    /// Local path, "-", file:// URI, or object-store URI such as s3://bucket/path.jsonl.
    // Distinct clap id so this sink path does not collide with the global
    // `-o/--format` output-format arg (both fields are named `output`).
    #[arg(long = "output", id = "jsonl_output")]
    output: String,
    #[arg(long, default_value = "tenant")]
    scope: String,
    #[arg(long)]
    agent_id: Option<String>,
    #[arg(long)]
    session_id: Option<String>,
    /// Client-side checkpoint key. Names the local resume checkpoint (a small
    /// JSON file beside the output), not a server consumer.
    #[arg(long)]
    consumer: Option<String>,
    /// Local checkpoint file for resume-cursor persistence. Defaults to
    /// `<output>.checkpoint`; skipped for "-"/stdout and object-store URIs.
    #[arg(long)]
    checkpoint: Option<PathBuf>,
    #[arg(long)]
    from_cursor: Option<String>,
    /// First-page snapshot boundary required when resuming a metadata backfill.
    #[arg(long, requires = "backfill")]
    snapshot_high_watermark: Option<String>,
    #[arg(long, default_value_t = 1000)]
    limit: usize,
    #[arg(long = "include", value_delimiter = ',')]
    include: Vec<String>,
    #[arg(long)]
    backfill: bool,
    #[arg(long)]
    tail: bool,
    #[arg(long)]
    max_batches: Option<u64>,
    #[arg(long, default_value_t = 1000)]
    poll_interval_ms: u64,
    #[command(flatten)]
    privacy: ExportPrivacyArgs,
    /// Optional local file path to refresh with exporter status JSON.
    #[arg(long)]
    metrics_output: Option<PathBuf>,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Subcommand, Debug)]
enum ParquetCommand {
    /// Convert a JSONL export archive to Parquet using DuckDB.
    FromJsonl(ParquetFromJsonlArgs),
}

#[derive(Args, Debug)]
struct ParquetFromJsonlArgs {
    /// Local path or object-store URI for the JSONL input.
    #[arg(long)]
    input: String,
    /// Local path or object-store URI for the Parquet output.
    // Distinct clap id so it does not collide with the global `-o/--format`
    // output-format arg (both fields are named `output`).
    #[arg(long = "output", id = "parquet_output")]
    output: String,
    /// DuckDB executable to invoke.
    #[arg(long, default_value = "duckdb")]
    duckdb_bin: String,
    /// Optional S3 region for DuckDB httpfs.
    #[arg(long)]
    s3_region: Option<String>,
    #[arg(long, default_value = "zstd")]
    compression: String,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Subcommand, Debug)]
enum RawCommand {
    /// GET a raw API path.
    Get { path: String },
    /// POST a raw API path with an optional JSON body.
    Post {
        path: String,
        #[arg(long)]
        body: Option<String>,
    },
}

pub(crate) struct Api {
    base: String,
    token: Option<String>,
    client: Client,
}

impl Api {
    fn new(base: String, token: Option<String>) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            token,
            client: Client::new(),
        }
    }

    pub(crate) async fn get(&self, path: &str) -> Result<Value> {
        self.request(Method::GET, path, None, &[]).await
    }

    pub(crate) async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::POST, path, Some(body), &[]).await
    }

    async fn delete(&self, path: &str) -> Result<Value> {
        self.request(Method::DELETE, path, None, &[]).await
    }

    /// POST with an `Idempotency-Key` header, so a re-sent request is deduplicated
    /// server-side.
    pub(crate) async fn post_idempotent(
        &self,
        path: &str,
        body: Value,
        idempotency_key: &str,
    ) -> Result<Value> {
        self.request(
            Method::POST,
            path,
            Some(body),
            &[("Idempotency-Key", idempotency_key)],
        )
        .await
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        headers: &[(&str, &str)],
    ) -> Result<Value> {
        let url = join_url(&self.base, path);
        let mut request = self.client.request(method, url);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.context("request failed")?;
        let status = response.status();
        let text = response.text().await.context("read response failed")?;
        let value = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).with_context(|| format!("response was not JSON: {text}"))?
        };
        if !status.is_success() {
            return Err(api_error(status, value));
        }
        Ok(value)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let api = Api::new(cli.url, cli.token);
    let value = run(&api, cli.command).await?;
    print_value(cli.output, &value);
    Ok(())
}

async fn run(api: &Api, command: Command) -> Result<Value> {
    match command {
        Command::Status { stale_threshold_ms } => {
            let suffix = query_opt("stale_threshold_ms", stale_threshold_ms);
            let health = api
                .get(&format!("/api/admin/cluster/health{suffix}"))
                .await?;
            let readiness = api
                .get(&format!("/api/admin/cluster/readiness{suffix}"))
                .await
                .unwrap_or_else(|error| json!({ "error": error.to_string() }));
            let scheduler = api.get("/api/admin/scheduler").await?;
            let drain = api.get("/api/admin/drain").await?;
            Ok(json!({
                "health": health,
                "readiness": readiness,
                "scheduler": scheduler,
                "drain": drain
            }))
        }
        Command::Nodes { command } => nodes(api, command).await,
        Command::Cells { command } => cells(api, command).await,
        Command::TenantPolicies { command } => tenant_policies(api, command).await,
        Command::Tablets { command } => tablets(api, command).await,
        Command::Raft { command } => raft(api, command).await,
        Command::Drain { command } => match command {
            DrainCommand::Status => api.get("/api/admin/drain").await,
            DrainCommand::Start => api.post("/api/admin/drain", json!({})).await,
        },
        Command::Backup { command } => match command {
            BackupCommand::Create {
                backup_id,
                directory,
                skip_meta,
                skip_runtime_summary,
                include_tablet_checkpoints,
            } => {
                api.post(
                    "/api/admin/backups",
                    json!({
                        "backup_id": backup_id,
                        "directory": directory,
                        "include_meta": !skip_meta,
                        "include_runtime_summary": !skip_runtime_summary,
                        "include_tablet_checkpoints": include_tablet_checkpoints
                    }),
                )
                .await
            }
        },
        Command::Restore { command } => match command {
            RestoreCommand::Validate { manifest_path } => {
                api.post(
                    "/api/admin/restores",
                    json!({
                        "manifest_path": manifest_path,
                        "dry_run": true
                    }),
                )
                .await
            }
            RestoreCommand::ApplyLocal {
                manifest_path,
                data_backup_dir,
                target_data_dir,
                config_backup,
                target_config,
                allow_overwrite,
            } => apply_local_restore(
                manifest_path,
                data_backup_dir,
                target_data_dir,
                config_backup,
                target_config,
                allow_overwrite,
            ),
            RestoreCommand::InstallTabletCheckpoint {
                tablet_id,
                checkpoint_dir,
                tablet_store_root,
                tablets_per_domain,
                max_open_domains,
            } => install_tablet_checkpoint(
                tablet_id,
                checkpoint_dir,
                tablet_store_root,
                tablets_per_domain,
                max_open_domains,
            ),
        },
        Command::TabletStore { command } => match command {
            TabletStoreCommand::Compact { tablet_id, dry_run } => {
                api.post(
                    "/api/admin/tablet-store/compact",
                    json!({
                        "tablet_id": tablet_id,
                        "dry_run": dry_run
                    }),
                )
                .await
            }
        },
        Command::Artifacts { command } => match command {
            ArtifactsCommand::VerifyLifecycle { tenant_id } => {
                let tenant_id = require_non_empty_cli_arg("--tenant-id", tenant_id)?;
                api.get(&format!(
                    "/api/tenants/{tenant_id}/artifacts/verify-lifecycle"
                ))
                .await
            }
        },
        Command::Export { command } => export(api, command).await,
        Command::Encryption { command } => match command {
            EncryptionCommand::Rotate { tenant_id } => {
                let tenant_id = require_non_empty_cli_arg("--tenant-id", tenant_id)?;
                api.post(
                    &format!("/api/admin/tenants/{tenant_id}/rotate-dek"),
                    json!({}),
                )
                .await
            }
            EncryptionCommand::Revoke { tenant_id, destroy } => {
                let tenant_id = require_non_empty_cli_arg("--tenant-id", tenant_id)?;
                api.post(
                    &format!("/api/admin/tenants/{tenant_id}/revoke-dek?destroy={destroy}"),
                    json!({}),
                )
                .await
            }
            EncryptionCommand::Reencrypt { tenant_id } => {
                let tenant_id = require_non_empty_cli_arg("--tenant-id", tenant_id)?;
                api.post(
                    &format!("/api/admin/tenants/{tenant_id}/reencrypt-dek"),
                    json!({}),
                )
                .await
            }
            EncryptionCommand::Retire { tenant_id } => {
                let tenant_id = require_non_empty_cli_arg("--tenant-id", tenant_id)?;
                api.post(
                    &format!("/api/admin/tenants/{tenant_id}/retire-dek-versions"),
                    json!({}),
                )
                .await
            }
        },
        Command::Import { command } => match command {
            ImportCommand::ClaudeCode(args) => {
                import::run(api, import::Source::ClaudeCode, args).await
            }
            ImportCommand::Codex(args) => import::run(api, import::Source::Codex, args).await,
        },
        Command::Raw { command } => match command {
            RawCommand::Get { path } => api.get(&path).await,
            RawCommand::Post { path, body } => {
                let body = body
                    .map(|raw| serde_json::from_str(&raw))
                    .transpose()
                    .context("--body must be valid JSON")?
                    .unwrap_or_else(|| json!({}));
                api.post(&path, body).await
            }
        },
    }
}

fn require_non_empty_cli_arg(name: &str, value: String) -> Result<String> {
    if value.trim().is_empty() {
        return Err(anyhow!(
            "{name} cannot be empty; set shell variables before invoking agentdbctl, for example: TENANT_ID=ten_...; agentdbctl artifacts verify-lifecycle --tenant-id \"$TENANT_ID\""
        ));
    }
    Ok(value)
}

fn apply_local_restore(
    manifest_path: PathBuf,
    data_backup_dir: PathBuf,
    target_data_dir: PathBuf,
    config_backup: Option<PathBuf>,
    target_config: Option<PathBuf>,
    allow_overwrite: bool,
) -> Result<Value> {
    let manifest_raw = fs::read_to_string(&manifest_path)
        .with_context(|| format!("read manifest {}", manifest_path.display()))?;
    let manifest: Value = serde_json::from_str(&manifest_raw)
        .with_context(|| format!("parse manifest {}", manifest_path.display()))?;
    if manifest["schema_version"].as_i64() != Some(1) {
        return Err(anyhow!(
            "unsupported backup manifest schema_version; expected 1"
        ));
    }
    if !data_backup_dir.is_dir() {
        return Err(anyhow!(
            "data backup directory is missing or not a directory: {}",
            data_backup_dir.display()
        ));
    }
    if target_data_dir.exists() && !allow_overwrite {
        return Err(anyhow!(
            "target data dir exists: {}; rerun with --allow-overwrite after stopping AgentDB and preserving failed state",
            target_data_dir.display()
        ));
    }

    let config_pair = match (config_backup, target_config) {
        (Some(src), Some(dst)) => {
            if !src.is_file() {
                return Err(anyhow!("config backup is missing: {}", src.display()));
            }
            if dst.exists() && !allow_overwrite {
                return Err(anyhow!(
                    "target config exists: {}; rerun with --allow-overwrite after preserving it",
                    dst.display()
                ));
            }
            Some((src, dst))
        }
        (None, None) => None,
        _ => {
            return Err(anyhow!(
                "--config-backup and --target-config must be provided together"
            ));
        }
    };

    if target_data_dir.exists() {
        fs::remove_dir_all(&target_data_dir)
            .with_context(|| format!("remove target data dir {}", target_data_dir.display()))?;
    }
    copy_dir_recursive(&data_backup_dir, &target_data_dir)?;

    let mut copied_config = None;
    if let Some((src, dst)) = config_pair {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create config parent {}", parent.display()))?;
        }
        fs::copy(&src, &dst)
            .with_context(|| format!("copy config {} -> {}", src.display(), dst.display()))?;
        copied_config = Some(dst.display().to_string());
    }

    Ok(json!({
        "status": "applied_local_restore",
        "destructive": allow_overwrite,
        "manifest_path": manifest_path.display().to_string(),
        "backup_id": manifest["backup_id"].as_str(),
        "data_backup_dir": data_backup_dir.display().to_string(),
        "target_data_dir": target_data_dir.display().to_string(),
        "target_config": copied_config,
        "next_steps": [
            "run agentdb doctor --config <target-config>",
            "start AgentDB with the restored config",
            "run agentdbctl status"
        ]
    }))
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst).with_context(|| format!("create {}", dst.display()))?;
    for entry in fs::read_dir(src).with_context(|| format!("read {}", src.display()))? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else if file_type.is_file() {
            fs::copy(&src_path, &dst_path).with_context(|| {
                format!("copy {} -> {}", src_path.display(), dst_path.display())
            })?;
        } else if file_type.is_symlink() {
            return Err(anyhow!(
                "refusing to copy symlink in restore source: {}",
                src_path.display()
            ));
        }
    }
    Ok(())
}

async fn raft(api: &Api, command: RaftCommand) -> Result<Value> {
    match command {
        RaftCommand::Status => api.get("/api/admin/raft").await,
        RaftCommand::Snapshots {
            tablet_id,
            include_deleted,
            limit,
        } => {
            let mut path = "/api/admin/raft/snapshots".to_string();
            let mut params = Vec::new();
            if let Some(tablet_id) = tablet_id {
                params.push(format!("tablet_id={tablet_id}"));
            }
            if include_deleted {
                params.push("include_deleted=true".to_string());
            }
            if let Some(limit) = limit {
                params.push(format!("limit={limit}"));
            }
            if !params.is_empty() {
                path.push('?');
                path.push_str(&params.join("&"));
            }
            api.get(&path).await
        }
        RaftCommand::PruneSnapshots { dry_run } => {
            api.post(
                "/api/admin/raft/snapshots/prune",
                json!({ "dry_run": dry_run }),
            )
            .await
        }
        RaftCommand::CatchUp {
            tablet_id,
            target_node_id,
            min_lag,
            dry_run,
        } => {
            api.post(
                "/api/admin/raft/catch-up",
                json!({
                    "tablet_id": tablet_id,
                    "target_node_id": target_node_id,
                    "min_lag": min_lag,
                    "dry_run": dry_run
                }),
            )
            .await
        }
    }
}

async fn export(api: &Api, command: ExportCommand) -> Result<Value> {
    match command {
        ExportCommand::Clickhouse { command } => clickhouse(api, command).await,
        ExportCommand::Jsonl { command } => jsonl(api, command).await,
        ExportCommand::Parquet { command } => parquet(command).await,
    }
}

async fn clickhouse(api: &Api, command: ClickHouseCommand) -> Result<Value> {
    match command {
        ClickHouseCommand::Doctor(args) => clickhouse_doctor(api, args).await,
        ClickHouseCommand::Init(args) => clickhouse_init(args).await,
        ClickHouseCommand::Migrate(args) => clickhouse_migrate(args).await,
        ClickHouseCommand::Rollback(args) => clickhouse_rollback(args).await,
        ClickHouseCommand::Status(args) => clickhouse_status(api, args).await,
        ClickHouseCommand::Queries => Ok(clickhouse_sample_queries()),
        ClickHouseCommand::ClickstackConfig(args) => Ok(clickstack_config(args)),
        ClickHouseCommand::Run(args) => clickhouse_run(api, args).await,
    }
}

fn clickstack_config(args: ClickStackConfigArgs) -> Value {
    json!({
        "sources": [
            {
                "name": format!("{} Logs", args.service_name),
                "table": args.logs_source,
                "timestamp_field": "TimestampTime",
                "display_timestamp_field": "Timestamp",
                "service_name_field": "ServiceName",
                "body_field": "Body",
                "trace_id_field": "TraceId",
                "span_id_field": "SpanId",
                "attributes_field": "LogAttributes",
                "resource_attributes_field": "ResourceAttributes"
            },
            {
                "name": format!("{} Traces", args.service_name),
                "table": args.traces_source,
                "timestamp_field": "TimestampTime",
                "display_timestamp_field": "Timestamp",
                "service_name_field": "ServiceName",
                "trace_id_field": "TraceId",
                "span_id_field": "SpanId",
                "parent_span_id_field": "ParentSpanId",
                "span_name_field": "SpanName",
                "duration_field": "Duration",
                "attributes_field": "SpanAttributes",
                "resource_attributes_field": "ResourceAttributes"
            }
        ],
        "clickhouse_cloud_api": {
            "base_url": "https://api.clickhouse.cloud/v1/organizations/{organizationId}/services/{serviceId}/clickstack",
            "list_dashboards": "GET /dashboards",
            "create_dashboard": "POST /dashboards",
            "authentication": "ClickHouse Cloud API basic auth: <keyId>:<keySecret>"
        },
        "dashboard": {
            "name": format!("{} Runtime Observability", args.service_name),
            "tags": ["agentdb", "observability"],
            "tiles": [
                {
                    "x": 0,
                    "y": 0,
                    "w": 12,
                    "h": 8,
                    "name": "Recent errors",
                    "config": {
                        "displayType": "table",
                        "sourceId": "<logsSourceId>",
                        "select": [{
                            "aggFn": "count",
                            "where": "SeverityText:ERROR OR LogAttributes.agentdb.error_kind:*",
                            "whereLanguage": "lucene"
                        }]
                    }
                },
                {
                    "x": 12,
                    "y": 0,
                    "w": 12,
                    "h": 8,
                    "name": "Slow spans",
                    "config": {
                        "displayType": "table",
                        "sourceId": "<tracesSourceId>",
                        "select": [{
                            "aggFn": "count",
                            "where": "Duration:>1000000000",
                            "whereLanguage": "lucene"
                        }]
                    }
                }
            ]
        }
    })
}

fn clickhouse_sample_queries() -> Value {
    json!({
        "queries": CLICKHOUSE_SAMPLE_QUERIES
            .iter()
            .map(|(name, description, sql)| {
                json!({
                    "name": name,
                    "description": description,
                    "sql": sql.trim()
                })
            })
            .collect::<Vec<_>>()
    })
}

async fn clickhouse_doctor(api: &Api, args: ClickHouseDoctorArgs) -> Result<Value> {
    let agentdb_ready = match api.get("/health/ready").await {
        Ok(_) => json!({
            "check_id": "agentdb_reachable",
            "status": "ok",
            "remediation": null
        }),
        Err(error) => json!({
            "check_id": "agentdb_reachable",
            "status": "error",
            "message": error.to_string(),
            "remediation": "start AgentDB or pass --url/AGENTDB_URL for a reachable server"
        }),
    };
    let export_status = if let Some(tenant_id) = args.tenant_id {
        match api
            .get(&format!(
                "/api/tenants/{}/exports/status",
                query_component(&tenant_id)
            ))
            .await
        {
            Ok(value) => json!({
                "check_id": "agentdb_export_status",
                "status": "ok",
                "details": value,
                "remediation": null
            }),
            Err(error) => json!({
                "check_id": "agentdb_export_status",
                "status": "error",
                "message": error.to_string(),
                "remediation": "verify the tenant id and AgentDB export API health"
            }),
        }
    } else {
        json!({
            "check_id": "agentdb_export_status",
            "status": "skipped",
            "message": "--tenant-id not supplied",
            "remediation": "pass --tenant-id to validate tenant export status and lag"
        })
    };
    let mut checks = vec![agentdb_ready, export_status];
    if let Some(url) = args.clickhouse_url {
        checks.push(match clickhouse_query(&url, "SELECT 1").await {
            Ok(text) => json!({
                "check_id": "clickhouse_reachable",
                "status": "ok",
                "response": text.trim(),
                "remediation": null
            }),
            Err(error) => json!({
                "check_id": "clickhouse_reachable",
                "status": "error",
                "message": error.to_string(),
                "remediation": "start ClickHouse or pass --clickhouse-url/CLICKHOUSE_URL with valid credentials"
            }),
        });
        checks.push(match clickhouse_schema_status(&url).await {
            Ok(schema) if schema.current => json!({
                "check_id": "schema_current",
                "status": "ok",
                "details": schema.to_json(),
                "remediation": null
            }),
            Ok(schema) => json!({
                "check_id": "schema_current",
                "status": "error",
                "details": schema.to_json(),
                "remediation": "run `agentdbctl export clickhouse migrate --apply`"
            }),
            Err(error) => json!({
                "check_id": "schema_current",
                "status": "error",
                "message": error.to_string(),
                "remediation": "run `agentdbctl export clickhouse migrate --apply` after ClickHouse is reachable"
            }),
        });
        checks.push(
            match clickhouse_query(&url, "SELECT count() FROM agentdb_events").await {
                Ok(text) => json!({
                    "check_id": "can_query",
                    "status": "ok",
                    "response": text.trim(),
                    "remediation": null
                }),
                Err(error) => json!({
                    "check_id": "can_query",
                    "status": "error",
                    "message": error.to_string(),
                    "remediation": "grant SELECT on the AgentDB observability tables/views"
                }),
            },
        );
        if args.skip_write_smoke {
            checks.push(json!({
                "check_id": "can_insert",
                "status": "skipped",
                "message": "--skip-write-smoke supplied",
                "remediation": "run without --skip-write-smoke before production cutover"
            }));
        } else {
            checks.push(match clickhouse_write_smoke(&url).await {
                Ok(value) => json!({
                    "check_id": "can_insert",
                    "status": "ok",
                    "details": value,
                    "remediation": null
                }),
                Err(error) => json!({
                    "check_id": "can_insert",
                    "status": "error",
                    "message": error.to_string(),
                    "remediation": "grant CREATE/INSERT/SELECT/DROP for migration validation or run connector with an insert-capable role after schema is initialized"
                }),
            });
        }
    } else {
        checks.push(json!({
            "check_id": "clickhouse_reachable",
            "status": "skipped",
            "message": "--clickhouse-url or CLICKHOUSE_URL not supplied",
            "remediation": "pass --clickhouse-url to validate schema, grants, and write path"
        }));
    }
    let ok = checks
        .iter()
        .all(|check| check.get("status").and_then(Value::as_str) != Some("error"));
    Ok(json!({
        "status": if ok { "ok" } else { "error" },
        "checks": checks
    }))
}

async fn clickhouse_write_smoke(url: &str) -> Result<Value> {
    let table = format!("agentdb_export_doctor_smoke_{}", std::process::id());
    let create =
        format!("CREATE TABLE IF NOT EXISTS {table} (id UInt64, value String) ENGINE = Memory");
    let insert = format!("INSERT INTO {table} FORMAT JSONEachRow\n{{\"id\":1,\"value\":\"ok\"}}\n");
    let select = format!("SELECT count() FROM {table}");
    let drop = format!("DROP TABLE IF EXISTS {table}");
    clickhouse_query(url, &create).await?;
    let result = async {
        clickhouse_query(url, &insert).await?;
        clickhouse_scalar_u64(url, &select).await
    }
    .await;
    let _ = clickhouse_query(url, &drop).await;
    let rows = result?;
    Ok(json!({
        "table": table,
        "rows": rows
    }))
}

async fn clickhouse_status(api: &Api, args: ClickHouseStatusArgs) -> Result<Value> {
    // Health / high-watermark comes from the export status route (not deleted).
    let agentdb = api
        .get(&format!(
            "/api/tenants/{}/exports/status",
            query_component(&args.tenant_id)
        ))
        .await?;
    // The durable resume cursor now lives client-side in the checkpoint table.
    let checkpoint = match (&args.consumer, args.clickhouse_url.as_deref()) {
        (Some(consumer), Some(url)) => {
            let cursor = clickhouse_read_checkpoint(url, consumer, &args.tenant_id).await?;
            json!({ "consumer": consumer, "cursor": cursor })
        }
        (Some(consumer), None) => json!({
            "consumer": consumer,
            "cursor": Value::Null,
            "message": "--clickhouse-url or CLICKHOUSE_URL not supplied; cannot read checkpoint"
        }),
        (None, _) => Value::Null,
    };
    let clickhouse = if let Some(url) = args.clickhouse_url.as_deref() {
        let schema = clickhouse_schema_status(url).await?;
        let native_rows = clickhouse_scalar_u64(
            url,
            &format!(
                "SELECT count() FROM agentdb_events WHERE tenant_id = {}",
                sql_string(&args.tenant_id)
            ),
        )
        .await
        .unwrap_or(0);
        let otel_log_rows = clickhouse_scalar_u64(
            url,
            &format!(
                "SELECT count() FROM agentdb_otel_logs WHERE tenant_id = {}",
                sql_string(&args.tenant_id)
            ),
        )
        .await
        .unwrap_or(0);
        let otel_trace_rows = clickhouse_scalar_u64(
            url,
            &format!(
                "SELECT count() FROM agentdb_otel_traces WHERE tenant_id = {}",
                sql_string(&args.tenant_id)
            ),
        )
        .await
        .unwrap_or(0);
        json!({
            "schema": schema.to_json(),
            "rows": {
                "agentdb_events": native_rows,
                "agentdb_otel_logs": otel_log_rows,
                "agentdb_otel_traces": otel_trace_rows
            }
        })
    } else {
        json!({
            "schema": null,
            "rows": null,
            "message": "--clickhouse-url or CLICKHOUSE_URL not supplied"
        })
    };
    Ok(json!({
        "tenant_id": args.tenant_id,
        "consumer": args.consumer,
        "agentdb": agentdb,
        "checkpoint": checkpoint,
        "clickhouse": clickhouse
    }))
}

async fn clickhouse_init(args: ClickHouseInitArgs) -> Result<Value> {
    if !args.apply {
        return Ok(json!({
            "status": "dry_run",
            "latest_version": CLICKHOUSE_SCHEMA_VERSION,
            "statements": CLICKHOUSE_SCHEMA_DDL
        }));
    }
    let url = args
        .clickhouse_url
        .ok_or_else(|| anyhow!("--clickhouse-url or CLICKHOUSE_URL is required with --apply"))?;
    apply_clickhouse_schema(&url).await
}

async fn clickhouse_migrate(args: ClickHouseMigrateArgs) -> Result<Value> {
    if args.to != "latest" && args.to != CLICKHOUSE_SCHEMA_VERSION {
        return Err(anyhow!(
            "unsupported ClickHouse migration target `{}`; use latest or {}",
            args.to,
            CLICKHOUSE_SCHEMA_VERSION
        ));
    }
    if !args.apply {
        return Ok(json!({
            "status": "dry_run",
            "target": args.to,
            "latest_version": CLICKHOUSE_SCHEMA_VERSION,
            "destructive": false,
            "allow_destructive": args.allow_destructive,
            "statements": CLICKHOUSE_SCHEMA_DDL
        }));
    }
    let url = args
        .clickhouse_url
        .ok_or_else(|| anyhow!("--clickhouse-url or CLICKHOUSE_URL is required with --apply"))?;
    apply_clickhouse_schema(&url).await
}

async fn clickhouse_rollback(args: ClickHouseRollbackArgs) -> Result<Value> {
    let statements = clickhouse_rollback_statements();
    if !args.apply {
        return Ok(json!({
            "status": "dry_run",
            "latest_version": CLICKHOUSE_SCHEMA_VERSION,
            "destructive": true,
            "allow_destructive": args.allow_destructive,
            "statements": statements
        }));
    }
    if !args.allow_destructive {
        return Err(anyhow!(
            "ClickHouse rollback drops first-party AgentDB export tables/views; rerun with --allow-destructive to apply"
        ));
    }
    let url = args
        .clickhouse_url
        .ok_or_else(|| anyhow!("--clickhouse-url or CLICKHOUSE_URL is required with --apply"))?;
    let mut applied = Vec::new();
    for statement in &statements {
        clickhouse_query(&url, statement).await?;
        applied.push(json!({
            "statement": statement,
            "status": "ok"
        }));
    }
    let schema = clickhouse_schema_status(&url)
        .await
        .unwrap_or_else(|_| ClickHouseSchemaStatus {
            current: false,
            latest_version: CLICKHOUSE_SCHEMA_VERSION,
            latest_applied: false,
            existing_objects: Vec::new(),
            missing_objects: CLICKHOUSE_REQUIRED_OBJECTS
                .iter()
                .map(|name| name.to_string())
                .collect(),
        });
    Ok(json!({
        "status": "rolled_back",
        "latest_version": CLICKHOUSE_SCHEMA_VERSION,
        "destructive": true,
        "applied": applied,
        "schema": schema.to_json()
    }))
}

fn clickhouse_rollback_statements() -> Vec<String> {
    CLICKHOUSE_REQUIRED_OBJECTS
        .iter()
        .rev()
        .map(|object| clickhouse_drop_statement(object))
        .collect()
}

fn clickhouse_drop_statement(object: &str) -> String {
    let kind = match object {
        "agentdb_clickstack_logs" | "agentdb_clickstack_traces" | "otel_logs" | "otel_traces" => {
            "VIEW"
        }
        _ => "TABLE",
    };
    format!("DROP {kind} IF EXISTS {object}")
}

async fn apply_clickhouse_schema(url: &str) -> Result<Value> {
    let mut applied = Vec::new();
    for statement in CLICKHOUSE_SCHEMA_DDL {
        clickhouse_query(url, statement).await?;
        applied.push(json!({
            "statement": statement.lines().next().unwrap_or(statement).trim(),
            "status": "ok"
        }));
    }
    let version_insert = format!(
        "INSERT INTO agentdb_schema_migrations (version) SELECT {} WHERE NOT EXISTS (SELECT 1 FROM agentdb_schema_migrations WHERE version = {})",
        sql_string(CLICKHOUSE_SCHEMA_VERSION),
        sql_string(CLICKHOUSE_SCHEMA_VERSION)
    );
    clickhouse_query(url, &version_insert).await?;
    let schema = clickhouse_schema_status(url).await?;
    Ok(json!({
        "status": if schema.current { "ok" } else { "error" },
        "latest_version": CLICKHOUSE_SCHEMA_VERSION,
        "applied": applied,
        "schema": schema.to_json()
    }))
}

struct ClickHouseSchemaStatus {
    current: bool,
    latest_version: &'static str,
    latest_applied: bool,
    existing_objects: Vec<String>,
    missing_objects: Vec<String>,
}

impl ClickHouseSchemaStatus {
    fn to_json(&self) -> Value {
        json!({
            "current": self.current,
            "latest_version": self.latest_version,
            "latest_applied": self.latest_applied,
            "existing_objects": self.existing_objects,
            "missing_objects": self.missing_objects
        })
    }
}

async fn clickhouse_schema_status(url: &str) -> Result<ClickHouseSchemaStatus> {
    let names = CLICKHOUSE_REQUIRED_OBJECTS
        .iter()
        .map(|name| sql_string(name))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ({names}) FORMAT TabSeparated"
    );
    let response = clickhouse_query(url, &sql).await?;
    let mut existing_objects = response
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    existing_objects.sort();
    let mut missing_objects = CLICKHOUSE_REQUIRED_OBJECTS
        .iter()
        .filter(|required| !existing_objects.iter().any(|name| name == **required))
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    missing_objects.sort();
    let latest_applied = clickhouse_schema_version_applied(url)
        .await
        .unwrap_or(false);
    Ok(ClickHouseSchemaStatus {
        current: missing_objects.is_empty() && latest_applied,
        latest_version: CLICKHOUSE_SCHEMA_VERSION,
        latest_applied,
        existing_objects,
        missing_objects,
    })
}

async fn clickhouse_schema_version_applied(url: &str) -> Result<bool> {
    let exists_sql = "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'agentdb_schema_migrations'";
    if clickhouse_scalar_u64(url, exists_sql).await? == 0 {
        return Ok(false);
    }
    let sql = format!(
        "SELECT count() FROM agentdb_schema_migrations WHERE version = {}",
        sql_string(CLICKHOUSE_SCHEMA_VERSION)
    );
    Ok(clickhouse_scalar_u64(url, &sql).await? > 0)
}

async fn clickhouse_scalar_u64(url: &str, sql: &str) -> Result<u64> {
    let response = clickhouse_query(url, sql).await?;
    response
        .trim()
        .parse::<u64>()
        .with_context(|| format!("ClickHouse scalar response was not u64: {response:?}"))
}

/// Read the durable resume cursor for a client-side checkpoint from the
/// `agentdb_export_checkpoints` ClickHouse table. Returns `None` when no
/// checkpoint row exists yet.
async fn clickhouse_read_checkpoint(
    url: &str,
    consumer: &str,
    tenant_id: &str,
) -> Result<Option<String>> {
    let sql = format!(
        "SELECT cursor FROM agentdb_export_checkpoints WHERE consumer = {} AND tenant_id = {} ORDER BY updated_at_ms DESC LIMIT 1",
        sql_string(consumer),
        sql_string(tenant_id)
    );
    let text = clickhouse_query(url, &sql).await?;
    let trimmed = text.trim();
    Ok(if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    })
}

/// Persist the latest resume cursor for a client-side checkpoint into the
/// `agentdb_export_checkpoints` ClickHouse table (ReplacingMergeTree keyed by
/// `(consumer, tenant_id)`, newest `updated_at_ms` wins).
async fn clickhouse_write_checkpoint(
    url: &str,
    consumer: &str,
    tenant_id: &str,
    cursor: &str,
) -> Result<()> {
    let row = json!({
        "consumer": consumer,
        "tenant_id": tenant_id,
        "cursor": cursor,
        "updated_at_ms": now_ms()
    });
    let body = format!(
        "INSERT INTO agentdb_export_checkpoints (consumer, tenant_id, cursor, updated_at_ms) FORMAT JSONEachRow\n{}\n",
        serde_json::to_string(&row)?
    );
    clickhouse_query(url, &body).await?;
    Ok(())
}

/// Resolve the local checkpoint file for a JSONL export run. Explicit
/// `--checkpoint` wins; otherwise it is derived as `<output>.checkpoint` for
/// local paths, and skipped for `-`/stdout and object-store/`scheme://` URIs.
fn jsonl_checkpoint_path(args: &JsonlRunArgs) -> Option<PathBuf> {
    if let Some(path) = &args.checkpoint {
        return Some(path.clone());
    }
    if args.output == "-" || args.output.contains("://") {
        return None;
    }
    Some(PathBuf::from(format!("{}.checkpoint", args.output)))
}

/// Read the durable resume cursor from a local JSONL checkpoint file.
fn read_jsonl_checkpoint(path: &std::path::Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let value: Value = serde_json::from_str(&text)
                .with_context(|| format!("parse checkpoint file {}", path.display()))?;
            Ok(value
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_string))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(anyhow::Error::from(error).context(format!("read checkpoint file {}", path.display())))
        }
    }
}

/// Atomically persist the latest resume cursor to a local JSONL checkpoint file.
fn write_jsonl_checkpoint(path: &std::path::Path, cursor: &str) -> Result<()> {
    let body = serde_json::to_vec_pretty(&json!({
        "cursor": cursor,
        "updated_at_ms": now_ms()
    }))?;
    let tmp = path.with_extension("checkpoint.tmp");
    std::fs::write(&tmp, &body)
        .with_context(|| format!("write checkpoint file {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("commit checkpoint file {}", path.display()))?;
    Ok(())
}

async fn clickhouse_run(api: &Api, args: ClickHouseRunArgs) -> Result<Value> {
    let modes = export_modes(&args.mode)?;
    let metadata_backfill = args.backfill
        && !args.include.is_empty()
        && !args.include.iter().any(|item| item == "events");
    let mut cursor = args.from_cursor.clone();
    let mut backfill_high_watermark = args.snapshot_high_watermark.clone();
    let started_at_ms = now_ms();
    let mut retry_attempts = 0_u64;
    let mut filtered_items = 0_u64;
    let mut inserted_rows = 0_u64;
    if let Some(consumer) = &args.consumer {
        if cursor.is_none() && !metadata_backfill && !args.dry_run {
            cursor =
                clickhouse_read_checkpoint(&args.clickhouse_url, consumer, &args.tenant_id).await?;
        }
    }
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    let max_batches = args.max_batches.unwrap_or(if args.tail || args.backfill {
        u64::MAX
    } else {
        1
    });
    let mut phase = if args.backfill {
        ExportRunPhase::Backfill
    } else {
        ExportRunPhase::Watch
    };
    loop {
        if batches >= max_batches {
            break;
        }
        let response = match phase {
            ExportRunPhase::Backfill => {
                let include = if args.include.is_empty() {
                    vec!["events".to_string()]
                } else {
                    args.include.clone()
                };
                if metadata_backfill {
                    // Metadata-object snapshots still use the server's snapshot
                    // branch (cursors + snapshot_high_watermark); `changes` does
                    // not implement it.
                    api.post(
                        &format!(
                            "/api/tenants/{}/exports/backfill",
                            query_component(&args.tenant_id)
                        ),
                        json!({
                            "scope": args.scope.clone(),
                            "agent_id": args.agent_id.clone(),
                            "session_id": args.session_id.clone(),
                            "include": include.clone(),
                            "from_cursor": cursor.clone(),
                            "snapshot_high_watermark": backfill_high_watermark.clone(),
                            "limit": args.limit
                        }),
                    )
                    .await?
                } else {
                    api.get(&changes_backfill_path(
                        &args.tenant_id,
                        &args.scope,
                        args.agent_id.as_deref(),
                        args.session_id.as_deref(),
                        &include.join(","),
                        cursor.as_deref(),
                        backfill_high_watermark.as_deref(),
                        args.limit,
                    ))
                    .await?
                }
            }
            ExportRunPhase::Watch => {
                api.get(&changes_tail_path(
                    &args.tenant_id,
                    &args.scope,
                    args.agent_id.as_deref(),
                    args.session_id.as_deref(),
                    &changes_include(&args.include),
                    cursor.as_deref(),
                    args.limit,
                    args.poll_interval_ms,
                ))
                .await?
            }
        };
        if phase == ExportRunPhase::Backfill {
            let response_high_watermark = response
                .get("snapshot_high_watermark")
                .and_then(Value::as_str);
            match (backfill_high_watermark.as_deref(), response_high_watermark) {
                (None, Some(high_watermark)) => {
                    backfill_high_watermark = Some(high_watermark.to_string());
                }
                (Some(expected), Some(actual)) if expected != actual => {
                    return Err(anyhow!(
                        "export backfill changed snapshot_high_watermark between pages"
                    ));
                }
                (Some(_), None) => {
                    return Err(anyhow!(
                        "export backfill omitted snapshot_high_watermark on a continued page"
                    ));
                }
                _ => {}
            }
        }
        let raw_items = response
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let raw_item_count = raw_items.len();
        let items = apply_export_privacy(raw_items, &args.privacy)?;
        filtered_items =
            filtered_items.saturating_add(raw_item_count.saturating_sub(items.len()) as u64);
        if raw_item_count == 0 {
            if phase == ExportRunPhase::Backfill {
                cursor = backfill_high_watermark.clone();
                if args.tail {
                    phase = ExportRunPhase::Watch;
                    continue;
                }
                break;
            }
            if args.tail {
                tokio::time::sleep(std::time::Duration::from_millis(args.poll_interval_ms)).await;
                continue;
            }
            break;
        }
        batches = batches.saturating_add(1);
        rows = rows.saturating_add(items.len() as u64);
        if !args.dry_run {
            if modes.native {
                let raw_rows = clickhouse_raw_rows(&items)?;
                retry_attempts = retry_attempts.saturating_add(
                    clickhouse_insert_json_each_row(
                        &args.clickhouse_url,
                        "agentdb_export_items_raw",
                        &raw_rows,
                        &export_batch_id("agentdb_export_items_raw", &items),
                        args.max_retries,
                        args.retry_initial_backoff_ms,
                    )
                    .await? as u64,
                );
                inserted_rows = inserted_rows.saturating_add(raw_rows.len() as u64);
                let event_rows = clickhouse_event_rows(&items)?;
                retry_attempts = retry_attempts.saturating_add(
                    clickhouse_insert_json_each_row(
                        &args.clickhouse_url,
                        "agentdb_events",
                        &event_rows,
                        &export_batch_id("agentdb_events", &items),
                        args.max_retries,
                        args.retry_initial_backoff_ms,
                    )
                    .await? as u64,
                );
                inserted_rows = inserted_rows.saturating_add(event_rows.len() as u64);
            }
            if modes.otel {
                let otel_log_rows = clickhouse_otel_log_rows(&items)?;
                retry_attempts = retry_attempts.saturating_add(
                    clickhouse_insert_json_each_row(
                        &args.clickhouse_url,
                        "agentdb_otel_logs",
                        &otel_log_rows,
                        &export_batch_id("agentdb_otel_logs", &items),
                        args.max_retries,
                        args.retry_initial_backoff_ms,
                    )
                    .await? as u64,
                );
                inserted_rows = inserted_rows.saturating_add(otel_log_rows.len() as u64);
                let otel_trace_rows = clickhouse_otel_trace_rows(&items)?;
                retry_attempts = retry_attempts.saturating_add(
                    clickhouse_insert_json_each_row(
                        &args.clickhouse_url,
                        "agentdb_otel_traces",
                        &otel_trace_rows,
                        &export_batch_id("agentdb_otel_traces", &items),
                        args.max_retries,
                        args.retry_initial_backoff_ms,
                    )
                    .await? as u64,
                );
                inserted_rows = inserted_rows.saturating_add(otel_trace_rows.len() as u64);
            }
        }
        cursor = response
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        let backfill_complete = phase == ExportRunPhase::Backfill
            && response
                .get("complete")
                .and_then(Value::as_bool)
                .unwrap_or(true);
        // Persist the client-side resume checkpoint for the events path. Metadata
        // snapshots re-run from scratch each time, so they carry no checkpoint.
        if let (Some(consumer), Some(checkpoint_cursor)) = (&args.consumer, cursor.as_deref()) {
            if !args.dry_run && !metadata_backfill {
                clickhouse_write_checkpoint(
                    &args.clickhouse_url,
                    consumer,
                    &args.tenant_id,
                    checkpoint_cursor,
                )
                .await?;
            }
        }
        write_metrics_output(
            args.metrics_output.as_ref(),
            &clickhouse_run_result(
                &args,
                started_at_ms,
                batches,
                rows,
                filtered_items,
                inserted_rows,
                retry_attempts,
                cursor.as_deref(),
            ),
        )?;
        if phase == ExportRunPhase::Backfill {
            if backfill_complete {
                if args.tail {
                    if metadata_backfill {
                        cursor = backfill_high_watermark.clone();
                    }
                    phase = ExportRunPhase::Watch;
                    continue;
                }
                break;
            }
            continue;
        }
        if !args.tail {
            break;
        }
    }
    let result = clickhouse_run_result(
        &args,
        started_at_ms,
        batches,
        rows,
        filtered_items,
        inserted_rows,
        retry_attempts,
        cursor.as_deref(),
    );
    write_metrics_output(args.metrics_output.as_ref(), &result)?;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn clickhouse_run_result(
    args: &ClickHouseRunArgs,
    started_at_ms: u64,
    batches: u64,
    rows: u64,
    filtered_items: u64,
    inserted_rows: u64,
    retry_attempts: u64,
    cursor: Option<&str>,
) -> Value {
    let metrics = export_metrics(
        started_at_ms,
        batches,
        rows,
        filtered_items,
        inserted_rows,
        retry_attempts,
    );
    json!({
        "status": "ok",
        "tenant_id": &args.tenant_id,
        "consumer": &args.consumer,
        "mode": &args.mode,
        "backfill": args.backfill,
        "tail": args.tail,
        "batches": batches,
        "rows": rows,
        "last_cursor": cursor,
        "metrics": metrics,
        "dry_run": args.dry_run
    })
}

async fn jsonl(api: &Api, command: JsonlCommand) -> Result<Value> {
    match command {
        JsonlCommand::Run(args) => jsonl_run(api, args).await,
    }
}

async fn jsonl_run(api: &Api, args: JsonlRunArgs) -> Result<Value> {
    let mut output = JsonlOutput::new(
        &args.output,
        args.tail && args.max_batches.is_none(),
        args.dry_run,
    )
    .await?;
    let started_at_ms = now_ms();
    let mut filtered_items = 0_u64;
    let include = if args.include.is_empty() {
        vec!["events".to_string()]
    } else {
        args.include.clone()
    };
    let metadata_backfill =
        args.backfill && !include.is_empty() && !include.iter().any(|item| item == "events");
    let mut cursor = args.from_cursor.clone();
    let mut backfill_high_watermark = args.snapshot_high_watermark.clone();
    // Client-side resume checkpoint. `--consumer` names the checkpoint; the
    // cursor persists to a local file derived from the output (or --checkpoint).
    let checkpoint_path = if args.consumer.is_some() {
        jsonl_checkpoint_path(&args)
    } else {
        None
    };
    if let Some(path) = &checkpoint_path {
        if cursor.is_none() && !metadata_backfill && !args.dry_run {
            cursor = read_jsonl_checkpoint(path)?;
        }
    }
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    let max_batches = args.max_batches.unwrap_or(if args.tail || args.backfill {
        u64::MAX
    } else {
        1
    });
    let mut phase = if args.backfill {
        ExportRunPhase::Backfill
    } else {
        ExportRunPhase::Watch
    };
    loop {
        if batches >= max_batches {
            break;
        }
        let response = match phase {
            ExportRunPhase::Backfill => {
                if metadata_backfill {
                    // Metadata-object snapshots still use the server's snapshot
                    // branch (cursors + snapshot_high_watermark); `changes` does
                    // not implement it.
                    api.post(
                        &format!(
                            "/api/tenants/{}/exports/backfill",
                            query_component(&args.tenant_id)
                        ),
                        json!({
                            "scope": args.scope.clone(),
                            "agent_id": args.agent_id.clone(),
                            "session_id": args.session_id.clone(),
                            "include": include.clone(),
                            "from_cursor": cursor.clone(),
                            "snapshot_high_watermark": backfill_high_watermark.clone(),
                            "limit": args.limit
                        }),
                    )
                    .await?
                } else {
                    api.get(&changes_backfill_path(
                        &args.tenant_id,
                        &args.scope,
                        args.agent_id.as_deref(),
                        args.session_id.as_deref(),
                        &include.join(","),
                        cursor.as_deref(),
                        backfill_high_watermark.as_deref(),
                        args.limit,
                    ))
                    .await?
                }
            }
            ExportRunPhase::Watch => {
                api.get(&changes_tail_path(
                    &args.tenant_id,
                    &args.scope,
                    args.agent_id.as_deref(),
                    args.session_id.as_deref(),
                    &include.join(","),
                    cursor.as_deref(),
                    args.limit,
                    args.poll_interval_ms,
                ))
                .await?
            }
        };
        if phase == ExportRunPhase::Backfill {
            let response_high_watermark = response
                .get("snapshot_high_watermark")
                .and_then(Value::as_str);
            match (backfill_high_watermark.as_deref(), response_high_watermark) {
                (None, Some(high_watermark)) => {
                    backfill_high_watermark = Some(high_watermark.to_string());
                }
                (Some(expected), Some(actual)) if expected != actual => {
                    return Err(anyhow!(
                        "export backfill changed snapshot_high_watermark between pages"
                    ));
                }
                (Some(_), None) => {
                    return Err(anyhow!(
                        "export backfill omitted snapshot_high_watermark on a continued page"
                    ));
                }
                _ => {}
            }
        }
        let raw_items = response
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let raw_item_count = raw_items.len();
        let items = apply_export_privacy(raw_items, &args.privacy)?;
        filtered_items =
            filtered_items.saturating_add(raw_item_count.saturating_sub(items.len()) as u64);
        if raw_item_count == 0 {
            if phase == ExportRunPhase::Backfill {
                cursor = backfill_high_watermark.clone();
                if args.tail {
                    phase = ExportRunPhase::Watch;
                    continue;
                }
                break;
            }
            if args.tail {
                tokio::time::sleep(std::time::Duration::from_millis(args.poll_interval_ms)).await;
                continue;
            }
            break;
        }
        batches = batches.saturating_add(1);
        rows = rows.saturating_add(items.len() as u64);
        if !items.is_empty() {
            output.write_items(&items).await?;
        }
        cursor = response
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        let backfill_complete = phase == ExportRunPhase::Backfill
            && response
                .get("complete")
                .and_then(Value::as_bool)
                .unwrap_or(true);
        // Persist the client-side resume checkpoint for the events path. Metadata
        // snapshots re-run from scratch each time, so they carry no checkpoint.
        if let (Some(path), Some(checkpoint_cursor)) = (&checkpoint_path, cursor.as_deref()) {
            if !args.dry_run && !metadata_backfill {
                write_jsonl_checkpoint(path, checkpoint_cursor)?;
            }
        }
        write_metrics_output(
            args.metrics_output.as_ref(),
            &jsonl_run_result(
                &args,
                None,
                started_at_ms,
                batches,
                rows,
                filtered_items,
                cursor.as_deref(),
            ),
        )?;
        if phase == ExportRunPhase::Backfill {
            if backfill_complete {
                if args.tail {
                    if metadata_backfill {
                        cursor = backfill_high_watermark.clone();
                    }
                    phase = ExportRunPhase::Watch;
                    continue;
                }
                break;
            }
            continue;
        }
        if !args.tail {
            break;
        }
    }
    let output = output.finish().await?;
    let result = jsonl_run_result(
        &args,
        Some(output),
        started_at_ms,
        batches,
        rows,
        filtered_items,
        cursor.as_deref(),
    );
    write_metrics_output(args.metrics_output.as_ref(), &result)?;
    Ok(result)
}

fn jsonl_run_result(
    args: &JsonlRunArgs,
    output: Option<Value>,
    started_at_ms: u64,
    batches: u64,
    rows: u64,
    filtered_items: u64,
    cursor: Option<&str>,
) -> Value {
    let metrics = export_metrics(started_at_ms, batches, rows, filtered_items, rows, 0);
    json!({
        "status": "ok",
        "tenant_id": &args.tenant_id,
        "consumer": &args.consumer,
        "output": output,
        "backfill": args.backfill,
        "tail": args.tail,
        "batches": batches,
        "rows": rows,
        "last_cursor": cursor,
        "metrics": metrics,
        "dry_run": args.dry_run
    })
}

async fn parquet(command: ParquetCommand) -> Result<Value> {
    match command {
        ParquetCommand::FromJsonl(args) => parquet_from_jsonl(args).await,
    }
}

async fn parquet_from_jsonl(args: ParquetFromJsonlArgs) -> Result<Value> {
    let sql = parquet_from_jsonl_sql(
        &args.input,
        &args.output,
        args.s3_region.as_deref(),
        &args.compression,
    )?;
    if args.dry_run {
        return Ok(json!({
            "status": "dry_run",
            "input": args.input,
            "output": args.output,
            "sql": sql
        }));
    }
    let status = ProcessCommand::new(&args.duckdb_bin)
        .arg("-c")
        .arg(&sql)
        .status()
        .with_context(|| format!("run DuckDB binary `{}`", args.duckdb_bin))?;
    if !status.success() {
        return Err(anyhow!("DuckDB parquet conversion failed with {status}"));
    }
    Ok(json!({
        "status": "ok",
        "input": args.input,
        "output": args.output,
        "compression": args.compression
    }))
}

fn parquet_from_jsonl_sql(
    input: &str,
    output: &str,
    s3_region: Option<&str>,
    compression: &str,
) -> Result<String> {
    let compression = compression.trim().to_ascii_uppercase();
    if !matches!(
        compression.as_str(),
        "UNCOMPRESSED" | "SNAPPY" | "GZIP" | "ZSTD"
    ) {
        return Err(anyhow!(
            "unsupported parquet compression `{compression}`; use uncompressed, snappy, gzip, or zstd"
        ));
    }
    let mut sql = String::new();
    if uses_object_store_uri(input) || uses_object_store_uri(output) || s3_region.is_some() {
        sql.push_str("INSTALL httpfs; LOAD httpfs;\n");
    }
    if let Some(region) = s3_region {
        writeln!(sql, "SET s3_region={};", sql_string(region))?;
    }
    writeln!(
        sql,
        "COPY (SELECT * FROM read_json_auto({}, format='newline_delimited')) TO {} (FORMAT PARQUET, COMPRESSION {});",
        sql_string(input),
        sql_string(output),
        compression
    )?;
    Ok(sql)
}

fn uses_object_store_uri(value: &str) -> bool {
    Url::parse(value)
        .map(|url| url.scheme() != "file")
        .unwrap_or(false)
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExportRunPhase {
    Backfill,
    Watch,
}

fn apply_export_privacy(items: Vec<Value>, privacy: &ExportPrivacyArgs) -> Result<Vec<Value>> {
    items
        .into_iter()
        .filter(|item| export_item_allowed(item, privacy))
        .map(|mut item| {
            for field in &privacy.redact_fields {
                redact_json_path(&mut item, field);
            }
            apply_payload_mode(&mut item, privacy.payload_mode)?;
            Ok(item)
        })
        .collect()
}

fn export_item_allowed(item: &Value, privacy: &ExportPrivacyArgs) -> bool {
    let object_type = item
        .get("object_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !privacy.allow_object_types.is_empty()
        && !contains_object_type(&privacy.allow_object_types, object_type)
    {
        return false;
    }
    if contains_object_type(&privacy.deny_object_types, object_type) {
        return false;
    }
    if object_type == "event" {
        let kind = item.get("kind").and_then(Value::as_str).unwrap_or_default();
        if !privacy.allow_event_kinds.is_empty() && !contains_text(&privacy.allow_event_kinds, kind)
        {
            return false;
        }
        if contains_text(&privacy.deny_event_kinds, kind) {
            return false;
        }
    }
    true
}

fn contains_text(values: &[String], needle: &str) -> bool {
    values.iter().any(|value| value == needle)
}

fn contains_object_type(values: &[String], object_type: &str) -> bool {
    values
        .iter()
        .any(|value| object_type_filter_matches(value, object_type))
}

fn object_type_filter_matches(filter: &str, object_type: &str) -> bool {
    match filter {
        "events" => object_type == "event",
        "tenants" => object_type == "tenant",
        "agents" => object_type == "agent",
        "sessions" => object_type == "session",
        "artifacts" => object_type == "artifact",
        "records" => object_type == "record",
        other => other == object_type,
    }
}

fn redact_json_path(value: &mut Value, path: &str) {
    let mut current = value;
    let mut parts = path.split('.').filter(|part| !part.is_empty()).peekable();
    while let Some(part) = parts.next() {
        let Some(object) = current.as_object_mut() else {
            return;
        };
        if parts.peek().is_none() {
            if object.contains_key(part) {
                object.insert(part.to_string(), Value::String("[REDACTED]".to_string()));
            }
            return;
        }
        let Some(next) = object.get_mut(part) else {
            return;
        };
        current = next;
    }
}

fn apply_payload_mode(item: &mut Value, mode: ExportPayloadMode) -> Result<()> {
    if mode == ExportPayloadMode::Full {
        return Ok(());
    }
    let Some(payload) = item
        .get_mut("data")
        .and_then(|data| data.get_mut("payload"))
    else {
        return Ok(());
    };
    match mode {
        ExportPayloadMode::Full => {}
        ExportPayloadMode::Hash => {
            let payload_json = serde_json::to_string(payload)?;
            *payload = json!({
                "sha256": sha256_hex(&payload_json),
                "redacted": true
            });
        }
        ExportPayloadMode::Drop => {
            *payload = Value::Null;
        }
    }
    Ok(())
}

fn export_metrics(
    started_at_ms: u64,
    batches: u64,
    rows: u64,
    filtered_items: u64,
    inserted_rows: u64,
    retry_attempts: u64,
) -> Value {
    let completed_at_ms = now_ms();
    let duration_ms = completed_at_ms.saturating_sub(started_at_ms).max(1);
    json!({
        "started_at_ms": started_at_ms,
        "completed_at_ms": completed_at_ms,
        "duration_ms": duration_ms,
        "batches": batches,
        "rows": rows,
        "filtered_items": filtered_items,
        "inserted_rows": inserted_rows,
        "retry_attempts": retry_attempts,
        "rows_per_second": (rows as f64) * 1000.0 / (duration_ms as f64)
    })
}

fn write_metrics_output(path: Option<&PathBuf>, result: &Value) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create metrics output directory {}", parent.display()))?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(result)?)
        .with_context(|| format!("write metrics output {}", path.display()))?;
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Normalize an `include` list into the comma-separated form the `changes`
/// endpoint expects, defaulting to `events` when empty.
fn changes_include(include: &[String]) -> String {
    if include.is_empty() {
        "events".to_string()
    } else {
        include.join(",")
    }
}

/// Build the tail (non-backfill) `GET /api/tenants/{t}/changes` path. Reads
/// through the current high watermark with an optional `wait_ms` long-poll.
#[allow(clippy::too_many_arguments)]
fn changes_tail_path(
    tenant_id: &str,
    scope: &str,
    agent_id: Option<&str>,
    session_id: Option<&str>,
    include: &str,
    cursor: Option<&str>,
    limit: usize,
    wait_ms: u64,
) -> String {
    let limit = limit.to_string();
    let wait_ms = wait_ms.to_string();
    let pairs = [
        ("scope", Some(scope)),
        ("agent_id", agent_id),
        ("session_id", session_id),
        ("include", Some(include)),
        ("from_cursor", cursor),
        ("limit", Some(limit.as_str())),
        ("wait_ms", Some(wait_ms.as_str())),
    ];
    format!(
        "/api/tenants/{}/changes{}",
        query_component(tenant_id),
        query_pairs_encoded(&pairs)
    )
}

/// Build the events-backfill `GET /api/tenants/{t}/changes?backfill=true` path.
/// Pins the read to `snapshot_high_watermark` and reports `complete`.
#[allow(clippy::too_many_arguments)]
fn changes_backfill_path(
    tenant_id: &str,
    scope: &str,
    agent_id: Option<&str>,
    session_id: Option<&str>,
    include: &str,
    cursor: Option<&str>,
    snapshot_high_watermark: Option<&str>,
    limit: usize,
) -> String {
    let limit = limit.to_string();
    let pairs = [
        ("backfill", Some("true")),
        ("scope", Some(scope)),
        ("agent_id", agent_id),
        ("session_id", session_id),
        ("include", Some(include)),
        ("from_cursor", cursor),
        ("snapshot_high_watermark", snapshot_high_watermark),
        ("limit", Some(limit.as_str())),
    ];
    format!(
        "/api/tenants/{}/changes{}",
        query_component(tenant_id),
        query_pairs_encoded(&pairs)
    )
}

enum JsonlOutput {
    DryRun {
        target: String,
        rows: u64,
    },
    Stdout {
        rows: u64,
    },
    Local {
        path: PathBuf,
        rows: u64,
    },
    ObjectStore {
        uri: String,
        store: Box<dyn object_store::ObjectStore>,
        path: object_store::path::Path,
        body: Vec<u8>,
        rows: u64,
    },
}

impl JsonlOutput {
    async fn new(target: &str, infinite_tail: bool, dry_run: bool) -> Result<Self> {
        if dry_run {
            return Ok(Self::DryRun {
                target: target.to_string(),
                rows: 0,
            });
        }
        if target == "-" {
            return Ok(Self::Stdout { rows: 0 });
        }
        if let Ok(url) = Url::parse(target) {
            if url.scheme() == "file" {
                let path = url
                    .to_file_path()
                    .map_err(|_| anyhow!("file output URI must be an absolute file:// path"))?;
                return Ok(Self::Local { path, rows: 0 });
            }
            if infinite_tail {
                return Err(anyhow!(
                    "object-store JSONL output requires --max-batches when --tail is set"
                ));
            }
            let (store, path) = object_store::parse_url(&url)
                .map_err(|error| anyhow!("parse JSONL object-store output URI: {error}"))?;
            return Ok(Self::ObjectStore {
                uri: target.to_string(),
                store,
                path,
                body: Vec::new(),
                rows: 0,
            });
        }
        Ok(Self::Local {
            path: PathBuf::from(target),
            rows: 0,
        })
    }

    async fn write_items(&mut self, items: &[Value]) -> Result<()> {
        let mut batch = Vec::new();
        for item in items {
            serde_json::to_writer(&mut batch, item)?;
            batch.push(b'\n');
        }
        match self {
            Self::DryRun { rows, .. } => {
                *rows = rows.saturating_add(items.len() as u64);
            }
            Self::Stdout { rows } => {
                io::stdout().write_all(&batch)?;
                *rows = rows.saturating_add(items.len() as u64);
            }
            Self::Local { path, rows } => {
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&*path)
                    .with_context(|| format!("open JSONL output {}", path.display()))?;
                file.write_all(&batch)
                    .with_context(|| format!("write JSONL output {}", path.display()))?;
                *rows = rows.saturating_add(items.len() as u64);
            }
            Self::ObjectStore { body, rows, .. } => {
                body.extend_from_slice(&batch);
                *rows = rows.saturating_add(items.len() as u64);
            }
        }
        Ok(())
    }

    async fn finish(self) -> Result<Value> {
        match self {
            Self::DryRun { target, rows } => Ok(json!({
                "type": "dry_run",
                "target": target,
                "rows": rows
            })),
            Self::Stdout { rows } => Ok(json!({
                "type": "stdout",
                "rows": rows
            })),
            Self::Local { path, rows } => Ok(json!({
                "type": "local",
                "path": path,
                "rows": rows
            })),
            Self::ObjectStore {
                uri,
                store,
                path,
                body,
                rows,
            } => {
                store
                    .put(&path, body.into())
                    .await
                    .map_err(|error| anyhow!("write JSONL object {uri}: {error}"))?;
                Ok(json!({
                    "type": "object_store",
                    "uri": uri,
                    "path": path.as_ref(),
                    "rows": rows
                }))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExportModes {
    native: bool,
    otel: bool,
}

fn export_modes(raw_modes: &[String]) -> Result<ExportModes> {
    let mut modes = ExportModes {
        native: false,
        otel: false,
    };
    let requested = if raw_modes.is_empty() {
        vec!["native".to_string()]
    } else {
        raw_modes.to_vec()
    };
    for mode in requested {
        match mode.trim() {
            "native" => modes.native = true,
            "otel" => modes.otel = true,
            "" => {}
            other => return Err(anyhow!("unsupported ClickHouse export mode `{other}`")),
        }
    }
    if !modes.native && !modes.otel {
        return Err(anyhow!("at least one ClickHouse export mode is required"));
    }
    Ok(modes)
}

fn clickhouse_raw_rows(items: &[Value]) -> Result<Vec<Value>> {
    items
        .iter()
        .map(|item| {
            Ok(json!({
                "export_version": item.get("export_version").cloned().unwrap_or(Value::Null),
                "cursor": item.get("cursor").cloned().unwrap_or(Value::Null),
                "op": item.get("op").cloned().unwrap_or(Value::Null),
                "object_type": item.get("object_type").cloned().unwrap_or(Value::Null),
                "tenant_id": item.get("tenant_id").cloned().unwrap_or(Value::Null),
                "agent_id": item.get("agent_id").cloned().unwrap_or(Value::Null),
                "session_id": item.get("session_id").cloned().unwrap_or(Value::Null),
                "object_id": item.get("object_id").cloned().unwrap_or(Value::Null),
                "seq": item.get("seq").cloned().unwrap_or(Value::Null),
                "created_at_ms": item.get("created_at_ms").cloned().unwrap_or(Value::Null),
                "observed_at_ms": item.get("observed_at_ms").cloned().unwrap_or(Value::Null),
                "cell_id": item.get("cell_id").cloned().unwrap_or(Value::Null),
                "placement_scope_id": item.get("placement_scope_id").cloned().unwrap_or(Value::Null),
                "tablet_id": item.get("tablet_id").cloned().unwrap_or(Value::Null),
                "tablet_epoch": item.get("tablet_epoch").cloned().unwrap_or(Value::Null),
                "applied_index": item.get("applied_index").cloned().unwrap_or(Value::Null),
                "payload_json": serde_json::to_string(item)?
            }))
        })
        .collect()
}

fn clickhouse_event_rows(items: &[Value]) -> Result<Vec<Value>> {
    items
        .iter()
        .filter(|item| item.get("object_type").and_then(Value::as_str) == Some("event"))
        .map(|item| {
            let data = item.get("data").unwrap_or(&Value::Null);
            Ok(json!({
                "tenant_id": item.get("tenant_id").cloned().unwrap_or(Value::Null),
                "agent_id": item.get("agent_id").cloned().unwrap_or(Value::Null),
                "session_id": item.get("session_id").cloned().unwrap_or(Value::Null),
                "event_id": item.get("event_id").or_else(|| item.get("object_id")).cloned().unwrap_or(Value::Null),
                "seq": item.get("seq").cloned().unwrap_or(Value::Null),
                "kind": item.get("kind").cloned().unwrap_or(Value::Null),
                "created_at_ms": item.get("created_at_ms").cloned().unwrap_or(Value::Null),
                "observed_at_ms": item.get("observed_at_ms").cloned().unwrap_or(Value::Null),
                "cell_id": item.get("cell_id").cloned().unwrap_or(Value::Null),
                "placement_scope_id": item.get("placement_scope_id").cloned().unwrap_or(Value::Null),
                "tablet_id": item.get("tablet_id").cloned().unwrap_or(Value::Null),
                "tablet_epoch": item.get("tablet_epoch").cloned().unwrap_or(Value::Null),
                "applied_index": item.get("applied_index").cloned().unwrap_or(Value::Null),
                "parent_event_id": item.get("parent_event_id").cloned().unwrap_or(Value::Null),
                "status": item.get("status").cloned().unwrap_or(Value::Null),
                "model": item.get("model").cloned().unwrap_or(Value::Null),
                "provider": item.get("provider").cloned().unwrap_or(Value::Null),
                "finish_reason": item.get("finish_reason").cloned().unwrap_or(Value::Null),
                "tool_name": item.get("tool_name").cloned().unwrap_or(Value::Null),
                "tool_call_id": item.get("tool_call_id").cloned().unwrap_or(Value::Null),
                "duration_ms": item.get("duration_ms").cloned().unwrap_or(Value::Null),
                "timeout_ms": item.get("timeout_ms").cloned().unwrap_or(Value::Null),
                "input_tokens": item.get("input_tokens").cloned().unwrap_or(Value::Null),
                "output_tokens": item.get("output_tokens").cloned().unwrap_or(Value::Null),
                "total_tokens": item.get("total_tokens").cloned().unwrap_or(Value::Null),
                "cost_micros": item.get("cost_micros").cloned().unwrap_or(Value::Null),
                "error_kind": item.get("error_kind").cloned().unwrap_or(Value::Null),
                "error_message": item.get("error_message").cloned().unwrap_or(Value::Null),
                "retryable": item.get("retryable").cloned().unwrap_or(Value::Null),
                "exit_code": item.get("exit_code").cloned().unwrap_or(Value::Null),
                "approval_id": item.get("approval_id").cloned().unwrap_or(Value::Null),
                "approval_decision": item.get("approval_decision").cloned().unwrap_or(Value::Null),
                "context_pack_id": item.get("context_pack_id").cloned().unwrap_or(Value::Null),
                "source_refs_json": item.get("source_refs").map(serde_json::to_string).transpose()?.unwrap_or_default(),
                "artifact_refs_json": item.get("artifact_refs").map(serde_json::to_string).transpose()?.unwrap_or_default(),
                "tags_json": item.get("tags").map(serde_json::to_string).transpose()?.unwrap_or_default(),
                "metadata_json": item.get("metadata").map(serde_json::to_string).transpose()?.unwrap_or_default(),
                "payload_json": serde_json::to_string(data.get("payload").unwrap_or(&Value::Null))?,
                "envelope_json": serde_json::to_string(item)?
            }))
        })
        .collect()
}

fn clickhouse_otel_log_rows(items: &[Value]) -> Result<Vec<Value>> {
    items
        .iter()
        .filter(|item| item.get("object_type").and_then(Value::as_str) == Some("event"))
        .map(|item| {
            let tenant_id = item_string(item, "tenant_id");
            let session_id = item_string(item, "session_id");
            let event_id = event_id(item);
            let trace_id = otel_trace_id(&tenant_id, &session_id);
            let span_id = otel_span_id("log", &event_id);
            Ok(json!({
                "trace_id": trace_id,
                "span_id": span_id,
                "timestamp_unix_nano": timestamp_unix_nano(item),
                "severity_text": otel_severity_text(item),
                "body": otel_log_body(item)?,
                "resource_attributes": otel_resource_attributes(item),
                "log_attributes": otel_event_attributes(item),
                "tenant_id": tenant_id,
                "session_id": session_id,
                "event_id": event_id,
                "envelope_json": serde_json::to_string(item)?
            }))
        })
        .collect()
}

fn clickhouse_otel_trace_rows(items: &[Value]) -> Result<Vec<Value>> {
    items
        .iter()
        .filter(|item| item.get("object_type").and_then(Value::as_str) == Some("event"))
        .map(|item| {
            let tenant_id = item_string(item, "tenant_id");
            let session_id = item_string(item, "session_id");
            let event_id = event_id(item);
            let duration_ms = item.get("duration_ms").and_then(Value::as_u64);
            let start_time_unix_nano = timestamp_unix_nano(item);
            let end_time_unix_nano = duration_ms
                .and_then(|duration| duration.checked_mul(1_000_000))
                .and_then(|duration_ns| start_time_unix_nano.checked_add(duration_ns))
                .unwrap_or(start_time_unix_nano);
            Ok(json!({
                "trace_id": otel_trace_id(&tenant_id, &session_id),
                "span_id": otel_span_id("span", &event_id),
                "parent_span_id": item.get("parent_event_id").and_then(Value::as_str).map(|parent| otel_span_id("span", parent)),
                "span_name": item.get("kind").and_then(Value::as_str).unwrap_or("agentdb.event"),
                "span_kind": otel_span_kind(item),
                "start_time_unix_nano": start_time_unix_nano,
                "end_time_unix_nano": end_time_unix_nano,
                "duration_ms": duration_ms,
                "status_code": otel_status_code(item),
                "resource_attributes": otel_resource_attributes(item),
                "span_attributes": otel_event_attributes(item),
                "links_json": item.get("source_refs").map(serde_json::to_string).transpose()?.unwrap_or_default(),
                "tenant_id": tenant_id,
                "session_id": session_id,
                "event_id": event_id,
                "envelope_json": serde_json::to_string(item)?
            }))
        })
        .collect()
}

fn item_string(item: &Value, field: &str) -> String {
    item.get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn event_id(item: &Value) -> String {
    item.get("event_id")
        .or_else(|| item.get("object_id"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn timestamp_unix_nano(item: &Value) -> u64 {
    item.get("created_at_ms")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        .saturating_mul(1_000_000)
}

fn otel_trace_id(tenant_id: &str, session_id: &str) -> String {
    sha256_hex_128(&format!("agentdb.trace:{tenant_id}:{session_id}"))
}

fn otel_span_id(kind: &str, id: &str) -> String {
    sha256_hex_64(&format!("agentdb.{kind}:{id}"))
}

fn sha256_hex_128(input: &str) -> String {
    hex_prefix(input, 16)
}

fn sha256_hex_64(input: &str) -> String {
    hex_prefix(input, 8)
}

fn sha256_hex(input: &str) -> String {
    hex_prefix(input, 32)
}

fn hex_prefix(input: &str, bytes: usize) -> String {
    let digest = Sha256::digest(input.as_bytes());
    let mut out = String::with_capacity(bytes * 2);
    for byte in digest.iter().take(bytes) {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn otel_resource_attributes(item: &Value) -> Value {
    let mut attrs = serde_json::Map::new();
    insert_attr(&mut attrs, "service.name", "agentdb");
    for field in [
        "tenant_id",
        "agent_id",
        "session_id",
        "cell_id",
        "placement_scope_id",
    ] {
        if let Some(value) = item.get(field).and_then(Value::as_str) {
            insert_attr(&mut attrs, &format!("agentdb.{field}"), value);
        }
    }
    Value::Object(attrs)
}

fn otel_event_attributes(item: &Value) -> Value {
    let mut attrs = serde_json::Map::new();
    for field in [
        "cursor",
        "object_id",
        "event_id",
        "kind",
        "status",
        "model",
        "provider",
        "finish_reason",
        "tool_name",
        "tool_call_id",
        "error_kind",
        "approval_id",
        "approval_decision",
        "context_pack_id",
    ] {
        if let Some(value) = item.get(field).and_then(Value::as_str) {
            insert_attr(&mut attrs, &format!("agentdb.{field}"), value);
        }
    }
    for field in [
        "seq",
        "created_at_ms",
        "observed_at_ms",
        "tablet_id",
        "tablet_epoch",
        "applied_index",
        "duration_ms",
        "timeout_ms",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cost_micros",
        "exit_code",
    ] {
        if let Some(value) = item.get(field) {
            if value.is_number() || value.is_boolean() {
                insert_attr(&mut attrs, &format!("agentdb.{field}"), &value.to_string());
            }
        }
    }
    Value::Object(attrs)
}

fn insert_attr(attrs: &mut serde_json::Map<String, Value>, key: &str, value: &str) {
    attrs.insert(key.to_string(), Value::String(value.to_string()));
}

fn otel_log_body(item: &Value) -> Result<String> {
    if let Some(value) = item
        .get("data")
        .and_then(|data| data.get("payload"))
        .and_then(Value::as_str)
    {
        return Ok(value.to_string());
    }
    if let Some(value) = item.get("kind").and_then(Value::as_str) {
        return Ok(value.to_string());
    }
    Ok(serde_json::to_string(
        item.get("data").unwrap_or(&Value::Null),
    )?)
}

fn otel_severity_text(item: &Value) -> &'static str {
    match item.get("status").and_then(Value::as_str) {
        Some("error" | "failed" | "failure") => "ERROR",
        Some("warn" | "warning") => "WARN",
        _ if item.get("error_kind").is_some() || item.get("error_message").is_some() => "ERROR",
        _ => "INFO",
    }
}

fn otel_status_code(item: &Value) -> &'static str {
    match item.get("status").and_then(Value::as_str) {
        Some("error" | "failed" | "failure") => "ERROR",
        Some("ok" | "success" | "completed") => "OK",
        _ if item.get("error_kind").is_some() || item.get("error_message").is_some() => "ERROR",
        _ => "UNSET",
    }
}

fn otel_span_kind(item: &Value) -> &'static str {
    match item.get("kind").and_then(Value::as_str) {
        Some("model_call" | "tool_call" | "approval_request") => "CLIENT",
        _ => "INTERNAL",
    }
}

async fn clickhouse_insert_json_each_row(
    url: &str,
    table: &str,
    rows: &[Value],
    batch_id: &str,
    max_retries: u32,
    retry_initial_backoff_ms: u64,
) -> Result<u32> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut body = format!(
        "INSERT INTO {table} SETTINGS insert_deduplicate=1, insert_deduplication_token={} FORMAT JSONEachRow\n",
        sql_string(batch_id)
    );
    for row in rows {
        body.push_str(&serde_json::to_string(row)?);
        body.push('\n');
    }
    clickhouse_query_with_retry(url, &body, max_retries, retry_initial_backoff_ms).await
}

async fn clickhouse_query_with_retry(
    url: &str,
    sql: &str,
    max_retries: u32,
    retry_initial_backoff_ms: u64,
) -> Result<u32> {
    let mut attempt = 0_u32;
    loop {
        match clickhouse_query(url, sql).await {
            Ok(_) => return Ok(attempt),
            Err(error) if attempt < max_retries => {
                let delay = retry_initial_backoff_ms.saturating_mul(1_u64 << attempt.min(10));
                eprintln!(
                    "clickhouse insert attempt {} failed; retrying in {}ms: {error}",
                    attempt + 1,
                    delay
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

fn export_batch_id(table: &str, items: &[Value]) -> String {
    let first = items
        .first()
        .and_then(|item| item.get("cursor"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let last = items
        .last()
        .and_then(|item| item.get("cursor"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    sha256_hex_128(&format!(
        "agentdb.clickhouse.batch:{table}:{first}:{last}:{}",
        items.len()
    ))
}

async fn clickhouse_query(url: &str, sql: &str) -> Result<String> {
    let request_url = clickhouse_request_url(url)?;
    let redacted_url = redact_clickhouse_url(url);
    let response = Client::new()
        .post(request_url)
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(sql.to_string())
        .send()
        .await
        .map_err(|error| {
            anyhow!(
                "ClickHouse request to {redacted_url} failed: {}",
                sanitize_clickhouse_error(url, &error.to_string())
            )
        })?;
    let status = response.status();
    let text = response
        .text()
        .await
        .context("read ClickHouse response failed")?;
    if !status.is_success() {
        return Err(anyhow!("ClickHouse returned {status}: {text}"));
    }
    Ok(text)
}

fn clickhouse_request_url(raw: &str) -> Result<Url> {
    let mut url = Url::parse(raw).with_context(|| format!("parse ClickHouse URL `{raw}`"))?;
    let database = url.path().trim_matches('/').to_string();
    if !database.is_empty() {
        url.set_path("/");
        let has_database = url.query_pairs().any(|(key, _)| key == "database");
        if !has_database {
            url.query_pairs_mut().append_pair("database", &database);
        }
    }
    Ok(url)
}

fn sanitize_clickhouse_error(url: &str, message: &str) -> String {
    message.replace(url, &redact_clickhouse_url(url))
}

fn redact_clickhouse_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after_scheme = &url[scheme_end + 3..];
    let authority_end = after_scheme.find('/').unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_end];
    let Some(userinfo_end) = authority.rfind('@') else {
        return url.to_string();
    };
    format!(
        "{}://***{}",
        &url[..scheme_end],
        &after_scheme[userinfo_end..]
    )
}

fn install_tablet_checkpoint(
    tablet_id: i64,
    checkpoint_dir: PathBuf,
    tablet_store_root: PathBuf,
    tablets_per_domain: i64,
    max_open_domains: usize,
) -> Result<Value> {
    if tablets_per_domain != 1 {
        return Err(anyhow!(
            "offline tablet checkpoint install requires --tablets-per-domain 1"
        ));
    }
    let lifecycle =
        GroupedRocksDbTabletStore::new(tablet_store_root, tablets_per_domain, max_open_domains);
    let manifest = lifecycle
        .restore_tablet_checkpoint(tablet_id, &checkpoint_dir)
        .map_err(|error| anyhow!("install tablet checkpoint: {error}"))?;
    Ok(serde_json::to_value(manifest)?)
}

async fn nodes(api: &Api, command: NodesCommand) -> Result<Value> {
    match command {
        NodesCommand::List { stale_threshold_ms } => {
            let suffix = query_opt("stale_threshold_ms", stale_threshold_ms);
            api.get(&format!("/api/admin/nodes{suffix}")).await
        }
        NodesCommand::Register(args) => {
            api.post(
                "/api/admin/nodes",
                json!({
                    "node_id": args.node_id,
                    "address": args.address,
                    "mongo_address": args.mongo_address,
                    "state": args.state,
                    "availability_zone": args.availability_zone,
                    "region": args.region,
                    "cell_id": args.cell_id,
                    "capacity_class": args.capacity_class
                }),
            )
            .await
        }
        NodesCommand::Delete { node_id } => {
            api.delete(&format!("/api/admin/nodes/{node_id}")).await
        }
        NodesCommand::Maintenance { node_id, state } => {
            api.post(
                &format!("/api/admin/nodes/{node_id}/maintenance"),
                json!({ "state": state.as_str() }),
            )
            .await
        }
    }
}

async fn cells(api: &Api, command: CellsCommand) -> Result<Value> {
    match command {
        CellsCommand::List => api.get("/api/admin/cells").await,
        CellsCommand::Upsert(args) => {
            api.post(
                "/api/admin/cells",
                json!({
                    "cell_id": args.cell_id,
                    "region": args.region,
                    "domain_class": args.domain_class,
                    "deployment_profile": args.deployment_profile,
                    "capacity_class": args.capacity_class,
                    "metadata_isolation_level": args.metadata_isolation_level,
                    "state": args.state,
                    "metadata_namespace": args.metadata_namespace,
                    "object_store_namespace": args.object_store_namespace,
                    "rollout_version": args.rollout_version
                }),
            )
            .await
        }
    }
}

async fn tenant_policies(api: &Api, command: TenantPoliciesCommand) -> Result<Value> {
    match command {
        TenantPoliciesCommand::List => api.get("/api/admin/tenant-policies").await,
        TenantPoliciesCommand::Upsert(args) => {
            api.post(
                "/api/admin/tenant-policies",
                json!({
                    "tenant_id": args.tenant_id,
                    "placement_class": args.placement_class,
                    "placement_scope_id": args.placement_scope_id,
                    "cell_id": args.cell_id,
                    "tier": args.tier,
                    "priority": args.priority,
                    "encryption_class": args.encryption_class,
                    "key_policy_id": args.key_policy_id,
                    "encryption_policy_version": args.encryption_policy_version,
                    "policy_version": args.policy_version
                }),
            )
            .await
        }
        TenantPoliciesCommand::Delete { tenant_id } => {
            api.delete(&format!("/api/admin/tenant-policies/{tenant_id}"))
                .await
        }
    }
}

async fn tablets(api: &Api, command: TabletsCommand) -> Result<Value> {
    match command {
        TabletsCommand::List => api.get("/api/admin/tablets").await,
        TabletsCommand::Stats { tablet_id } => {
            api.get(&format!(
                "/api/admin/tablet-stats{}",
                query_opt("tablet_id", tablet_id)
            ))
            .await
        }
        TabletsCommand::Create { tablet_id } => {
            api.post(
                "/api/admin/tablets/create",
                json!({ "tablet_id": tablet_id }),
            )
            .await
        }
        TabletsCommand::Assign { tablet_id, node_id } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/assign"),
                json!({ "node_id": node_id }),
            )
            .await
        }
        TabletsCommand::Relinquish { tablet_id } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/relinquish"),
                json!({}),
            )
            .await
        }
        TabletsCommand::Handoff {
            tablet_id,
            target_node_id,
        } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/handoff"),
                json!({ "target_node_id": target_node_id }),
            )
            .await
        }
        TabletsCommand::CompleteHandoff {
            tablet_id,
            expected_source_node_id,
        } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/handoff/complete"),
                json!({ "expected_source_node_id": expected_source_node_id }),
            )
            .await
        }
        TabletsCommand::CancelHandoff { tablet_id } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/handoff/cancel"),
                json!({}),
            )
            .await
        }
        TabletsCommand::PrepareReplacement {
            tablet_id,
            target_node_id,
            source_node_id,
            stale_threshold_ms,
        } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/replacement/prepare"),
                json!({
                    "target_node_id": target_node_id,
                    "source_node_id": source_node_id,
                    "stale_threshold_ms": stale_threshold_ms
                }),
            )
            .await
        }
        TabletsCommand::CompleteReplacement {
            tablet_id,
            target_node_id,
            source_node_id,
            min_applied_index,
            allow_lagging,
            retire_source,
        } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/replacement/complete"),
                json!({
                    "target_node_id": target_node_id,
                    "source_node_id": source_node_id,
                    "min_applied_index": min_applied_index,
                    "require_healthy": !allow_lagging,
                    "retire_source": retire_source
                }),
            )
            .await
        }
        TabletsCommand::PrepareSplit {
            tablet_id,
            child_ranges,
            placement_scope_id,
        } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/split/prepare"),
                json!({
                    "placement_scope_id": placement_scope_id,
                    "child_ranges": parse_tablet_ranges(&child_ranges)?
                }),
            )
            .await
        }
        TabletsCommand::CompleteSplit {
            tablet_id,
            child_ranges,
            placement_scope_id,
            map_epoch,
            min_applied_index,
            allow_unready,
        } => {
            api.post(
                &format!("/api/admin/tablets/{tablet_id}/split/complete"),
                json!({
                    "placement_scope_id": placement_scope_id,
                    "child_ranges": parse_tablet_ranges(&child_ranges)?,
                    "map_epoch": map_epoch,
                    "min_applied_index": min_applied_index,
                    "require_ready": !allow_unready
                }),
            )
            .await
        }
        TabletsCommand::PrepareMerge {
            source_tablet_ids,
            destination_tablet_id,
            placement_scope_id,
        } => {
            api.post(
                "/api/admin/tablets/merge/prepare",
                json!({
                    "placement_scope_id": placement_scope_id,
                    "source_tablet_ids": source_tablet_ids,
                    "destination_tablet_id": destination_tablet_id
                }),
            )
            .await
        }
        TabletsCommand::CompleteMerge {
            source_tablet_ids,
            destination_tablet_id,
            placement_scope_id,
            map_epoch,
            min_applied_index,
            allow_unready,
        } => {
            api.post(
                "/api/admin/tablets/merge/complete",
                json!({
                    "placement_scope_id": placement_scope_id,
                    "source_tablet_ids": source_tablet_ids,
                    "destination_tablet_id": destination_tablet_id,
                    "map_epoch": map_epoch,
                    "min_applied_index": min_applied_index,
                    "require_ready": !allow_unready
                }),
            )
            .await
        }
        TabletsCommand::Movements { status, limit } => {
            api.get(&format!(
                "/api/admin/tablet-movements{}",
                query_pairs(&[
                    ("status", status.map(|value| value.to_string())),
                    ("limit", limit.map(|value| value.to_string()))
                ])
            ))
            .await
        }
        TabletsCommand::Rebalance {
            dry_run,
            stale_threshold_ms,
            max_movements,
        } => {
            api.post(
                "/api/admin/tablets/rebalance",
                json!({
                    "dry_run": dry_run,
                    "stale_threshold_ms": stale_threshold_ms,
                    "max_movements": max_movements
                }),
            )
            .await
        }
    }
}

fn parse_tablet_ranges(values: &[String]) -> Result<Vec<Value>> {
    values
        .iter()
        .map(|value| {
            let parts = value.split(':').collect::<Vec<_>>();
            if parts.len() != 3 {
                anyhow::bail!("tablet range must be formatted as start:end:tablet_id");
            }
            let start_shard = parts[0].parse::<i64>()?;
            let end_shard = parts[1].parse::<i64>()?;
            let tablet_id = parts[2].parse::<i64>()?;
            Ok(json!({
                "start_shard": start_shard,
                "end_shard": end_shard,
                "tablet_id": tablet_id
            }))
        })
        .collect()
}

fn query_opt<T: ToString>(name: &str, value: Option<T>) -> String {
    value
        .map(|value| format!("?{name}={}", value.to_string()))
        .unwrap_or_default()
}

fn query_pairs(pairs: &[(&str, Option<String>)]) -> String {
    let values = pairs
        .iter()
        .filter_map(|(name, value)| value.as_ref().map(|value| format!("{name}={value}")))
        .collect::<Vec<_>>();
    if values.is_empty() {
        String::new()
    } else {
        format!("?{}", values.join("&"))
    }
}

fn query_pairs_encoded(pairs: &[(&str, Option<&str>)]) -> String {
    let values = pairs
        .iter()
        .filter_map(|(name, value)| value.map(|value| format!("{name}={}", query_component(value))))
        .collect::<Vec<_>>();
    if values.is_empty() {
        String::new()
    } else {
        format!("?{}", values.join("&"))
    }
}

fn query_component(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => write!(out, "%{byte:02X}").unwrap(),
        }
    }
    out
}

fn join_url(base: &str, path: &str) -> String {
    if path.starts_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
}

fn api_error(status: StatusCode, value: Value) -> anyhow::Error {
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string());
    anyhow!("AgentDB API returned {status}: {message}")
}

fn print_value(output: Output, value: &Value) {
    match output {
        Output::Json => println!(
            "{}",
            serde_json::to_string_pretty(value).expect("valid json")
        ),
        Output::Table => println!("{}", render_table(value)),
    }
}

fn render_table(value: &Value) -> String {
    if let Some(rows) = value.as_array() {
        return render_rows(rows);
    }
    if let Some(object) = value.as_object() {
        if object.contains_key("health") && object.contains_key("readiness") {
            return render_status(value);
        }
        if let Some(queries) = object.get("queries").and_then(Value::as_array) {
            return render_queries(queries);
        }
        if let Some(statements) = object.get("statements").and_then(Value::as_array) {
            return render_statements(object, statements);
        }
        if let Some(actions) = object.get("actions").and_then(Value::as_array) {
            if !actions.is_empty() {
                return render_rows(actions);
            }
        }
        return render_object(object);
    }
    serde_json::to_string_pretty(value).expect("valid json")
}

fn render_status(value: &Value) -> String {
    let mut out = String::new();
    let health = &value["health"];
    let readiness = &value["readiness"];
    let scheduler = &value["scheduler"];
    let drain = &value["drain"];
    writeln!(
        out,
        "status={} ready={} mode={} local_node={} draining={}",
        text(&health["status"]),
        readiness
            .get("ready")
            .and_then(Value::as_bool)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        text(&health["mode"]),
        text(&health["local_node_id"]),
        drain
            .get("is_draining")
            .and_then(Value::as_bool)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    )
    .unwrap();
    writeln!(
        out,
        "nodes={} alive={} stale={} tablets={} unowned={} leader={} reassignments={} log_compactions={} log_compaction_failures={}",
        text(&health["summary"]["node_count"]),
        text(&health["summary"]["alive_node_count"]),
        text(&health["summary"]["stale_node_count"]),
        text(&health["summary"]["tablet_count"]),
        text(&health["summary"]["unowned_tablet_count"]),
        text(&scheduler["leader_node_id"]),
        text(&scheduler["tablet_reassignments_total"]),
        text(&scheduler["local_tablet_log_compactions_total"]),
        text(&scheduler["local_tablet_log_compaction_failures_total"])
    )
    .unwrap();
    if let Some(reasons) = readiness.get("reasons").and_then(Value::as_array) {
        if !reasons.is_empty() {
            writeln!(out, "reasons={}", compact(reasons)).unwrap();
        }
    }
    if let Some(tablet_store) = scheduler.get("tablet_store") {
        writeln!(
            out,
            "tablet_store={} root={} tablets_per_domain={} max_open_domains={} open_domains={} memtable_budget_mib={} background_jobs_per_domain={} pending_compaction_bytes={}",
            text(&tablet_store["layout"]),
            text(&tablet_store["root_dir"]),
            text(&tablet_store["tablets_per_domain"]),
            text(&tablet_store["max_open_domains"]),
            tablet_store
                .get("domains")
                .and_then(Value::as_array)
                .map(|domains| domains.len().to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            memtable_budget_mib(tablet_store)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            text(&tablet_store["domain_max_background_jobs"]),
            pending_compaction_bytes(tablet_store)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        )
        .unwrap();
    }
    out.trim_end().to_string()
}

fn render_queries(queries: &[Value]) -> String {
    let mut out = String::new();
    for (index, query) in queries.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let name = query.get("name").and_then(Value::as_str).unwrap_or("query");
        let description = query
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let sql = query.get("sql").and_then(Value::as_str).unwrap_or_default();
        if description.is_empty() {
            writeln!(out, "-- {name}").unwrap();
        } else {
            writeln!(out, "-- {name}: {description}").unwrap();
        }
        writeln!(out, "{sql};").unwrap();
    }
    out.trim_end().to_string()
}

fn render_statements(object: &serde_json::Map<String, Value>, statements: &[Value]) -> String {
    let mut out = String::new();
    writeln!(
        out,
        "status={} latest_version={} statements={}",
        text(object.get("status").unwrap_or(&Value::Null)),
        text(object.get("latest_version").unwrap_or(&Value::Null)),
        statements.len()
    )
    .unwrap();
    for statement in statements {
        if let Some(sql) = statement.as_str() {
            writeln!(out, "\n{};", sql.trim()).unwrap();
        }
    }
    out.trim_end().to_string()
}

fn memtable_budget_mib(tablet_store: &Value) -> Option<u64> {
    Some(
        tablet_store
            .get("max_open_domains")?
            .as_u64()?
            .saturating_mul(tablet_store.get("domain_write_buffer_size_mib")?.as_u64()?)
            .saturating_mul(tablet_store.get("domain_max_write_buffers")?.as_u64()?),
    )
}

fn pending_compaction_bytes(tablet_store: &Value) -> Option<u64> {
    Some(
        tablet_store
            .get("domains")?
            .as_array()?
            .iter()
            .filter_map(|domain| {
                domain
                    .get("metrics")?
                    .get("pending_compaction_bytes")?
                    .as_u64()
            })
            .sum(),
    )
}

fn render_object(object: &serde_json::Map<String, Value>) -> String {
    let rows = object
        .iter()
        .map(|(key, value)| json!({ "field": key, "value": text(value) }))
        .collect::<Vec<_>>();
    render_rows(&rows)
}

fn render_rows(rows: &[Value]) -> String {
    if rows.is_empty() {
        return "(empty)".to_string();
    }
    let columns = columns_for(rows);
    let mut widths = columns
        .iter()
        .map(|column| column.len())
        .collect::<Vec<_>>();
    let rendered = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let cell = text(row.get(column).unwrap_or(&Value::Null));
                    widths[index] = widths[index].max(cell.len());
                    cell
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut out = String::new();
    write_row(&mut out, &columns, &widths);
    let separators = widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>();
    write_row(&mut out, &separators, &widths);
    for row in rendered {
        write_row(&mut out, &row, &widths);
    }
    out.trim_end().to_string()
}

fn columns_for(rows: &[Value]) -> Vec<String> {
    let preferred = [
        "node_id",
        "tenant_id",
        "consumer_name",
        "tablet_id",
        "state",
        "status",
        "action",
        "changed",
        "movement_id",
        "movement_type",
        "placement_scope_id",
        "owner_node_id",
        "source_node_id",
        "target_node_id",
        "source_tablet_id",
        "destination_tablet_id",
        "source_tablet_ids",
        "destination_tablet_ids",
        "new_owner_node_id",
        "required_applied_index",
        "start_shard",
        "end_shard",
        "raft_membership_changed",
        "retired_source",
        "from_raft_node_id",
        "to_raft_node_id",
        "lag",
        "pending_items",
        "lag_ms",
        "lease_active",
        "high_watermark",
        "last_acked_cursor",
        "snapshot_index",
        "snapshot_id",
        "target_raft_node_id",
        "size_bytes",
        "age_ms",
        "expired",
        "handoff_target_node_id",
        "is_stale",
        "ready",
        "reason",
        "address",
        "backup_dir",
        "manifest_path",
        "meta_backup_path",
    ];
    let mut columns = Vec::new();
    for column in preferred {
        if rows.iter().any(|row| row.get(column).is_some()) {
            columns.push(column.to_string());
        }
    }
    if columns.is_empty() {
        if let Some(object) = rows.first().and_then(Value::as_object) {
            columns.extend(object.keys().take(8).cloned());
        }
    }
    columns
}

fn write_row(out: &mut String, values: &[String], widths: &[usize]) {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push_str("  ");
        }
        write!(out, "{value:<width$}", width = widths[index]).unwrap();
    }
    out.push('\n');
}

fn text(value: &Value) -> String {
    match value {
        Value::Null => "-".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => compact(values),
        Value::Object(_) => serde_json::to_string(value).expect("valid json"),
    }
}

fn compact(values: &[Value]) -> String {
    values.iter().map(text).collect::<Vec<_>>().join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_base_url_and_path() {
        assert_eq!(
            join_url("http://127.0.0.1:7878", "/api/admin/nodes"),
            "http://127.0.0.1:7878/api/admin/nodes"
        );
        assert_eq!(
            join_url("http://127.0.0.1:7878", "api/admin/nodes"),
            "http://127.0.0.1:7878/api/admin/nodes"
        );
    }

    #[test]
    fn require_non_empty_cli_arg_rejects_empty_shell_expansion() {
        let err = require_non_empty_cli_arg("--tenant-id", "".to_string())
            .unwrap_err()
            .to_string();
        assert!(err.contains("--tenant-id cannot be empty"));
        assert_eq!(
            require_non_empty_cli_arg("--tenant-id", "ten_1".to_string()).unwrap(),
            "ten_1"
        );
    }

    #[test]
    fn apply_local_restore_copies_data_and_requires_overwrite_flag() {
        let temp = tempfile::tempdir().unwrap();
        let manifest_path = temp.path().join("manifest.json");
        let data_backup_dir = temp.path().join("data-backup");
        let nested = data_backup_dir.join("runtime");
        let target_data_dir = temp.path().join("target-data");
        let config_backup = temp.path().join("agentdb.toml.bak");
        let target_config = temp.path().join("agentdb.toml");

        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(
            &manifest_path,
            r#"{"schema_version":1,"backup_id":"nightly-1"}"#,
        )
        .unwrap();
        std::fs::write(nested.join("state.json"), "{}").unwrap();
        std::fs::write(&config_backup, "bind = '127.0.0.1:7878'\n").unwrap();

        let result = apply_local_restore(
            manifest_path.clone(),
            data_backup_dir.clone(),
            target_data_dir.clone(),
            Some(config_backup.clone()),
            Some(target_config.clone()),
            false,
        )
        .unwrap();

        assert_eq!(result["status"], "applied_local_restore");
        assert_eq!(result["backup_id"], "nightly-1");
        assert_eq!(
            std::fs::read_to_string(target_data_dir.join("runtime/state.json")).unwrap(),
            "{}"
        );
        assert_eq!(
            std::fs::read_to_string(&target_config).unwrap(),
            "bind = '127.0.0.1:7878'\n"
        );

        let err = apply_local_restore(
            manifest_path,
            data_backup_dir,
            target_data_dir,
            Some(config_backup),
            Some(target_config),
            false,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("target data dir exists"));
    }

    #[test]
    fn apply_local_restore_validates_config_before_copying_data() {
        let temp = tempfile::tempdir().unwrap();
        let manifest_path = temp.path().join("manifest.json");
        let data_backup_dir = temp.path().join("data-backup");
        let target_data_dir = temp.path().join("target-data");
        let config_backup = temp.path().join("agentdb.toml.bak");
        let target_config = temp.path().join("agentdb.toml");

        std::fs::create_dir_all(&data_backup_dir).unwrap();
        std::fs::write(
            &manifest_path,
            r#"{"schema_version":1,"backup_id":"nightly-1"}"#,
        )
        .unwrap();
        std::fs::write(data_backup_dir.join("state.json"), "{}").unwrap();
        std::fs::write(&config_backup, "new\n").unwrap();
        std::fs::write(&target_config, "existing\n").unwrap();

        let err = apply_local_restore(
            manifest_path,
            data_backup_dir,
            target_data_dir.clone(),
            Some(config_backup),
            Some(target_config),
            false,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("target config exists"));
        assert!(!target_data_dir.exists());
    }

    #[test]
    fn renders_table_for_node_rows() {
        let value = json!([
            {
                "node_id": "node-a",
                "state": "alive",
                "is_stale": false,
                "address": "http://127.0.0.1:7878"
            }
        ]);
        let table = render_table(&value);
        assert!(table.contains("node_id"));
        assert!(table.contains("node-a"));
        assert!(table.contains("alive"));
    }

    #[test]
    fn renders_compact_status() {
        let value = json!({
            "health": {
                "status": "ok",
                "mode": "snapshot",
                "local_node_id": "node-a",
                "summary": {
                    "node_count": 2,
                    "alive_node_count": 2,
                    "stale_node_count": 0,
                    "tablet_count": 4,
                    "unowned_tablet_count": 0
                }
            },
            "readiness": { "ready": true, "reasons": [] },
            "scheduler": {
                "leader_node_id": "node-a",
                "tablet_reassignments_total": 3,
                "tablet_store": {
                    "layout": "grouped_rocksdb",
                    "root_dir": "/var/lib/agentdb/tablet-stores",
                    "tablets_per_domain": 32,
                    "max_open_domains": 16,
                    "domain_write_buffer_size_mib": 16,
                    "domain_max_write_buffers": 2,
                    "domain_max_background_jobs": 1,
                    "domains": [
                        {
                            "domain_id": 0,
                            "open": true,
                            "metrics": {
                                "pending_compaction_bytes": 1234
                            }
                        }
                    ]
                }
            },
            "drain": { "is_draining": false }
        });
        let rendered = render_status(&value);
        assert!(rendered.contains("status=ok"));
        assert!(rendered.contains("ready=true"));
        assert!(rendered.contains("leader=node-a"));
        assert!(rendered.contains("tablet_store=grouped_rocksdb"));
        assert!(rendered.contains("open_domains=1"));
        assert!(rendered.contains("memtable_budget_mib=512"));
        assert!(rendered.contains("pending_compaction_bytes=1234"));
    }

    #[test]
    fn parses_tablet_range_triples() {
        let ranges = parse_tablet_ranges(&["0:49:8".into(), "50:99:9".into()]).unwrap();
        assert_eq!(ranges[0]["start_shard"], 0);
        assert_eq!(ranges[0]["end_shard"], 49);
        assert_eq!(ranges[0]["tablet_id"], 8);
        assert!(parse_tablet_ranges(&["0:49".into()]).is_err());
    }

    #[test]
    fn builds_query_pairs() {
        assert_eq!(
            query_pairs(&[
                ("status", Some("pending".into())),
                ("limit", Some("10".into()))
            ]),
            "?status=pending&limit=10"
        );
        assert_eq!(query_pairs(&[("status", None)]), "");
    }

    #[test]
    fn builds_encoded_changes_tail_path() {
        assert_eq!(
            changes_tail_path(
                "ten_1",
                "session",
                None,
                Some("ses_1"),
                &changes_include(&[]),
                Some("v2:0001:ses_1:0002:cell=cell-a"),
                500,
                2000,
            ),
            "/api/tenants/ten_1/changes?scope=session&session_id=ses_1&include=events&from_cursor=v2%3A0001%3Ases_1%3A0002%3Acell%3Dcell-a&limit=500&wait_ms=2000"
        );
    }

    #[test]
    fn builds_encoded_changes_backfill_path() {
        assert_eq!(
            changes_backfill_path(
                "ten_1",
                "agent",
                Some("agt_1"),
                None,
                &changes_include(&["events".into(), "sessions".into()]),
                Some("ledger:ten_1/0001/event/evt_1"),
                Some("ledger:ten_1/0009"),
                250,
            ),
            "/api/tenants/ten_1/changes?backfill=true&scope=agent&agent_id=agt_1&include=events%2Csessions&from_cursor=ledger%3Aten_1%2F0001%2Fevent%2Fevt_1&snapshot_high_watermark=ledger%3Aten_1%2F0009&limit=250"
        );
    }

    #[test]
    fn clickhouse_schema_contains_event_and_raw_tables() {
        let ddl = CLICKHOUSE_SCHEMA_DDL.join("\n");
        assert!(ddl.contains("agentdb_export_items_raw"));
        assert!(ddl.contains("agentdb_events"));
        assert!(ddl.contains("agentdb_otel_logs"));
        assert!(ddl.contains("agentdb_otel_traces"));
        assert!(ddl.contains("agentdb_clickstack_logs"));
        assert!(ddl.contains("agentdb_clickstack_traces"));
        assert!(ddl.contains("CREATE VIEW IF NOT EXISTS otel_logs"));
        assert!(ddl.contains("TimestampTime"));
        assert!(ddl.contains("TraceId"));
        assert!(ddl.contains("agentdb_schema_migrations"));
        assert!(ddl.contains("ReplacingMergeTree(observed_at_ms)"));
    }

    #[test]
    fn clickhouse_required_objects_include_latest_views() {
        assert_eq!(CLICKHOUSE_SCHEMA_VERSION, "2026_05_15_clickstack_views");
        for required in [
            "agentdb_events",
            "agentdb_otel_logs",
            "agentdb_clickstack_logs",
            "otel_logs",
        ] {
            assert!(CLICKHOUSE_REQUIRED_OBJECTS.contains(&required));
        }
    }

    #[test]
    fn clickhouse_rollback_statements_drop_views_as_views() {
        let statements = clickhouse_rollback_statements();
        assert!(statements.contains(&"DROP VIEW IF EXISTS otel_logs".to_string()));
        assert!(statements.contains(&"DROP VIEW IF EXISTS agentdb_clickstack_logs".to_string()));
        assert!(statements.contains(&"DROP TABLE IF EXISTS agentdb_events".to_string()));
    }

    #[test]
    fn clickhouse_sample_queries_include_native_and_clickstack_views() {
        let queries = clickhouse_sample_queries();
        let rows = queries["queries"].as_array().unwrap();
        assert!(rows.iter().any(|row| row["name"] == "recent_failures"
            && row["sql"].as_str().unwrap().contains("agentdb_events")));
        assert!(rows.iter().any(|row| row["name"] == "clickstack_logs"
            && row["sql"]
                .as_str()
                .unwrap()
                .contains("agentdb_clickstack_logs")));
        assert!(rows.iter().any(|row| row["name"] == "clickstack_traces"
            && row["sql"]
                .as_str()
                .unwrap()
                .contains("agentdb_clickstack_traces")));
    }

    #[test]
    fn renders_clickhouse_queries_as_copyable_sql() {
        let rendered = render_table(&clickhouse_sample_queries());
        assert!(rendered.contains("-- recent_failures:"));
        assert!(rendered.contains("FROM agentdb_events"));
        assert!(rendered.contains("FROM agentdb_clickstack_logs"));
        assert!(rendered.contains("LIMIT 100;"));
    }

    #[test]
    fn clickstack_config_includes_source_fields_and_dashboard_payload() {
        let config = clickstack_config(ClickStackConfigArgs {
            service_name: "AgentDB Prod".into(),
            logs_source: "agentdb_clickstack_logs".into(),
            traces_source: "agentdb_clickstack_traces".into(),
        });
        assert_eq!(config["sources"][0]["table"], "agentdb_clickstack_logs");
        assert_eq!(config["sources"][0]["trace_id_field"], "TraceId");
        assert_eq!(config["sources"][1]["duration_field"], "Duration");
        assert_eq!(
            config["dashboard"]["name"],
            "AgentDB Prod Runtime Observability"
        );
        assert!(config["clickhouse_cloud_api"]["base_url"]
            .as_str()
            .unwrap()
            .contains("/clickstack"));
    }

    #[test]
    fn renders_clickhouse_schema_plan_as_copyable_sql() {
        let rendered = render_table(&json!({
            "status": "dry_run",
            "latest_version": CLICKHOUSE_SCHEMA_VERSION,
            "statements": CLICKHOUSE_SCHEMA_DDL
        }));
        assert!(rendered.contains("status=dry_run"));
        assert!(rendered.contains("latest_version=2026_05_15_clickstack_views"));
        assert!(rendered.contains("CREATE TABLE IF NOT EXISTS agentdb_events"));
        assert!(rendered.contains("CREATE VIEW IF NOT EXISTS agentdb_clickstack_logs"));
    }

    #[test]
    fn export_batch_id_is_stable_for_replayed_cursor_range() {
        let items = vec![
            json!({"cursor": "ledger:ten_1/0001/event/evt_1"}),
            json!({"cursor": "ledger:ten_1/0002/event/evt_2"}),
        ];
        assert_eq!(
            export_batch_id("agentdb_events", &items),
            export_batch_id("agentdb_events", &items)
        );
        assert_ne!(
            export_batch_id("agentdb_events", &items),
            export_batch_id("agentdb_otel_logs", &items)
        );
    }

    #[test]
    fn export_privacy_filters_and_redacts_items() {
        let items = vec![
            json!({
                "cursor": "ledger:1",
                "object_type": "event",
                "kind": "model_call",
                "data": {
                    "payload": {
                        "prompt": "secret",
                        "nested": {"token": "abc"}
                    }
                }
            }),
            json!({
                "cursor": "ledger:2",
                "object_type": "event",
                "kind": "debug"
            }),
            json!({
                "cursor": "ledger:3",
                "object_type": "record",
                "data": {"payload": "record-secret"}
            }),
        ];
        let privacy = ExportPrivacyArgs {
            allow_object_types: vec!["events".into()],
            deny_object_types: vec![],
            allow_event_kinds: vec![],
            deny_event_kinds: vec!["debug".into()],
            redact_fields: vec!["data.payload.nested.token".into()],
            payload_mode: ExportPayloadMode::Hash,
        };

        let filtered = apply_export_privacy(items, &privacy).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["kind"], "model_call");
        assert_eq!(filtered[0]["data"]["payload"]["redacted"], true);
        assert!(filtered[0]["data"]["payload"]["sha256"].as_str().is_some());
        assert!(!filtered[0].to_string().contains("secret"));
        assert!(!filtered[0].to_string().contains("abc"));
    }

    #[test]
    fn export_metrics_reports_rates_and_filtered_items() {
        let metrics = export_metrics(now_ms().saturating_sub(1000), 2, 10, 3, 20, 1);
        assert_eq!(metrics["batches"], 2);
        assert_eq!(metrics["rows"], 10);
        assert_eq!(metrics["filtered_items"], 3);
        assert_eq!(metrics["inserted_rows"], 20);
        assert_eq!(metrics["retry_attempts"], 1);
        assert!(metrics["rows_per_second"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn redacts_clickhouse_url_credentials() {
        let url = "https://agentdb:secret@clickhouse.example.com:8443/agentdb";
        assert_eq!(
            redact_clickhouse_url(url),
            "https://***@clickhouse.example.com:8443/agentdb"
        );
        assert_eq!(
            sanitize_clickhouse_error(url, &format!("request failed for {url}")),
            "request failed for https://***@clickhouse.example.com:8443/agentdb"
        );
    }

    #[test]
    fn clickhouse_request_url_maps_path_to_database_query() {
        assert_eq!(
            clickhouse_request_url("http://agentdb:secret@127.0.0.1:8123/agentdb")
                .unwrap()
                .as_str(),
            "http://agentdb:secret@127.0.0.1:8123/?database=agentdb"
        );
        assert_eq!(
            clickhouse_request_url("http://127.0.0.1:8123/?database=custom")
                .unwrap()
                .as_str(),
            "http://127.0.0.1:8123/?database=custom"
        );
    }

    #[test]
    fn clickhouse_rows_project_export_envelopes() {
        let items = vec![json!({
            "export_version": "2026-05-14",
            "cursor": "v2:0001:ses_1:0000",
            "op": "insert",
            "object_type": "event",
            "tenant_id": "ten_1",
            "agent_id": "agt_1",
            "session_id": "ses_1",
            "object_id": "evt_1",
            "event_id": "evt_1",
            "seq": 0,
            "kind": "model_call",
            "created_at_ms": 1000,
            "observed_at_ms": 1001,
            "cell_id": "cell-a",
            "placement_scope_id": "scope-a",
            "tablet_id": 7,
            "tablet_epoch": 2,
            "applied_index": 99,
            "model": "gpt-test",
            "provider": "openai",
            "duration_ms": 123,
            "total_tokens": 18,
            "metadata": {"trace": "abc"},
            "tags": ["golden-path"],
            "data": {
                "payload": {
                    "model": "gpt-test",
                    "metadata": {"trace": "abc"}
                }
            }
        })];

        let raw = clickhouse_raw_rows(&items).unwrap();
        assert_eq!(raw.len(), 1);
        assert_eq!(raw[0]["cursor"], "v2:0001:ses_1:0000");
        assert_eq!(raw[0]["object_type"], "event");
        assert!(raw[0]["payload_json"]
            .as_str()
            .unwrap()
            .contains("\"event_id\":\"evt_1\""));

        let events = clickhouse_event_rows(&items).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["tenant_id"], "ten_1");
        assert_eq!(events[0]["event_id"], "evt_1");
        assert_eq!(events[0]["seq"], 0);
        assert_eq!(events[0]["kind"], "model_call");
        assert_eq!(events[0]["model"], "gpt-test");
        assert_eq!(events[0]["duration_ms"], 123);
        assert_eq!(events[0]["total_tokens"], 18);
        assert_eq!(events[0]["tablet_id"], 7);
        assert_eq!(
            events[0]["metadata_json"].as_str().unwrap(),
            "{\"trace\":\"abc\"}"
        );
        assert!(events[0]["payload_json"]
            .as_str()
            .unwrap()
            .contains("\"model\":\"gpt-test\""));

        let otel_logs = clickhouse_otel_log_rows(&items).unwrap();
        assert_eq!(otel_logs.len(), 1);
        assert_eq!(otel_logs[0]["tenant_id"], "ten_1");
        assert_eq!(otel_logs[0]["session_id"], "ses_1");
        assert_eq!(otel_logs[0]["event_id"], "evt_1");
        assert_eq!(otel_logs[0]["timestamp_unix_nano"], 1_000_000_000_u64);
        assert_eq!(otel_logs[0]["severity_text"], "INFO");
        assert_eq!(
            otel_logs[0]["resource_attributes"]["agentdb.tenant_id"],
            "ten_1"
        );
        assert_eq!(otel_logs[0]["log_attributes"]["agentdb.kind"], "model_call");

        let otel_traces = clickhouse_otel_trace_rows(&items).unwrap();
        assert_eq!(otel_traces.len(), 1);
        assert_eq!(otel_traces[0]["trace_id"], otel_logs[0]["trace_id"]);
        assert_eq!(otel_traces[0]["span_name"], "model_call");
        assert_eq!(otel_traces[0]["span_kind"], "CLIENT");
        assert_eq!(otel_traces[0]["duration_ms"], 123);
        assert_eq!(otel_traces[0]["status_code"], "UNSET");
    }

    #[test]
    fn parses_clickhouse_export_modes() {
        let native = export_modes(&["native".into()]).unwrap();
        assert_eq!(
            native,
            ExportModes {
                native: true,
                otel: false
            }
        );
        let both = export_modes(&["native".into(), "otel".into()]).unwrap();
        assert_eq!(
            both,
            ExportModes {
                native: true,
                otel: true
            }
        );
        assert!(export_modes(&["unknown".into()]).is_err());
    }

    #[tokio::test]
    async fn jsonl_output_writes_local_file() {
        let path = std::env::temp_dir().join(format!(
            "agentdbctl-jsonl-{}-{}.jsonl",
            std::process::id(),
            "writer"
        ));
        let _ = std::fs::remove_file(&path);
        let mut output = JsonlOutput::new(path.to_str().unwrap(), false, false)
            .await
            .unwrap();
        output
            .write_items(&[json!({"cursor": "ledger:1", "object_type": "event"})])
            .await
            .unwrap();
        output
            .write_items(&[json!({"cursor": "ledger:2", "object_type": "session"})])
            .await
            .unwrap();
        let summary = output.finish().await.unwrap();
        assert_eq!(summary["type"], "local");
        assert_eq!(summary["rows"], 2);
        let body = std::fs::read_to_string(&path).unwrap();
        let lines = body.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"cursor\":\"ledger:1\""));
        assert!(lines[1].contains("\"object_type\":\"session\""));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn jsonl_output_dry_run_does_not_create_target() {
        let path = std::env::temp_dir().join(format!(
            "agentdbctl-jsonl-{}-{}.jsonl",
            std::process::id(),
            "dry-run"
        ));
        let _ = std::fs::remove_file(&path);
        let mut output = JsonlOutput::new(path.to_str().unwrap(), true, true)
            .await
            .unwrap();
        output
            .write_items(&[json!({"cursor": "ledger:dry", "object_type": "event"})])
            .await
            .unwrap();
        let summary = output.finish().await.unwrap();
        assert_eq!(summary["type"], "dry_run");
        assert_eq!(summary["rows"], 1);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn jsonl_output_rejects_unbounded_object_store_tail() {
        let err =
            match JsonlOutput::new("s3://agentdb-exports/path/events.jsonl", true, false).await {
                Ok(_) => panic!("expected unbounded object-store tail to be rejected"),
                Err(err) => err,
            };
        assert!(err
            .to_string()
            .contains("requires --max-batches when --tail is set"));
    }

    #[test]
    fn builds_parquet_from_jsonl_sql() {
        let sql = parquet_from_jsonl_sql(
            "s3://agentdb-exports/tenant/events.jsonl",
            "s3://agentdb-exports/tenant/events.parquet",
            Some("us-east-1"),
            "zstd",
        )
        .unwrap();
        assert!(sql.contains("INSTALL httpfs; LOAD httpfs;"));
        assert!(sql.contains("SET s3_region='us-east-1';"));
        assert!(sql.contains("read_json_auto('s3://agentdb-exports/tenant/events.jsonl'"));
        assert!(sql.contains("COMPRESSION ZSTD"));

        let escaped =
            parquet_from_jsonl_sql("agent's.jsonl", "agent's.parquet", None, "snappy").unwrap();
        assert!(escaped.contains("'agent''s.jsonl'"));
        assert!(escaped.contains("'agent''s.parquet'"));
        assert!(parquet_from_jsonl_sql("in.jsonl", "out.parquet", None, "brotli").is_err());
    }
}
