//! `agentdbctl` — human and agent-friendly operations CLI.
//!
//! The server's REST API remains the stable machine contract. This binary is a
//! small operator surface on top: concise commands, table output by default,
//! JSON output for automation, and no requirement to hand-write curl payloads.

use std::{fmt::Write as _, path::PathBuf};

use agentdb_storage::{GroupedRocksDbTabletStore, TabletStoreLifecycle};
use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};

const DEFAULT_URL: &str = "http://127.0.0.1:7878";
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
    #[arg(short = 'o', long, global = true, default_value = "table")]
    output: Output,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Output {
    Table,
    Json,
}

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
    /// Observability export operations.
    Export {
        #[command(subcommand)]
        command: ExportCommand,
    },
    /// Low-level escape hatch for endpoints not yet modeled by the CLI.
    Raw {
        #[command(subcommand)]
        command: RawCommand,
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
enum ExportCommand {
    /// ClickHouse observability export golden path.
    Clickhouse {
        #[command(subcommand)]
        command: ClickHouseCommand,
    },
}

#[derive(Subcommand, Debug)]
enum ClickHouseCommand {
    /// Check AgentDB export and optional ClickHouse reachability.
    Doctor(ClickHouseDoctorArgs),
    /// Print or apply the first-party ClickHouse schema.
    Init(ClickHouseInitArgs),
    /// Show AgentDB export status for one tenant.
    Status(ClickHouseStatusArgs),
    /// Copy one or more export-watch batches into ClickHouse.
    Run(ClickHouseRunArgs),
}

#[derive(Args, Debug)]
struct ClickHouseDoctorArgs {
    #[arg(long)]
    tenant_id: Option<String>,
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,
}

#[derive(Args, Debug)]
struct ClickHouseInitArgs {
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct ClickHouseStatusArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    consumer: Option<String>,
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
    #[arg(long)]
    consumer: Option<String>,
    #[arg(long, default_value = "agentdbctl")]
    lease_owner: String,
    #[arg(long, default_value_t = 30_000)]
    lease_ms: u64,
    #[arg(long)]
    from_cursor: Option<String>,
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

struct Api {
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

    async fn get(&self, path: &str) -> Result<Value> {
        self.request(Method::GET, path, None).await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::POST, path, Some(body)).await
    }

    async fn delete(&self, path: &str) -> Result<Value> {
        self.request(Method::DELETE, path, None).await
    }

    async fn request(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let url = join_url(&self.base, path);
        let mut request = self.client.request(method, url);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
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
        Command::Export { command } => export(api, command).await,
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
    }
}

async fn clickhouse(api: &Api, command: ClickHouseCommand) -> Result<Value> {
    match command {
        ClickHouseCommand::Doctor(args) => clickhouse_doctor(api, args).await,
        ClickHouseCommand::Init(args) => clickhouse_init(args).await,
        ClickHouseCommand::Status(args) => {
            if let Some(consumer) = args.consumer {
                api.get(&format!(
                    "/api/tenants/{}/exports/consumers/{}",
                    query_component(&args.tenant_id),
                    query_component(&consumer)
                ))
                .await
            } else {
                api.get(&format!(
                    "/api/tenants/{}/exports/status",
                    query_component(&args.tenant_id)
                ))
                .await
            }
        }
        ClickHouseCommand::Run(args) => clickhouse_run(api, args).await,
    }
}

async fn clickhouse_doctor(api: &Api, args: ClickHouseDoctorArgs) -> Result<Value> {
    let agentdb_ready = match api.get("/health/ready").await {
        Ok(_) => json!({
            "check_id": "agentdb_reachable",
            "status": "ok"
        }),
        Err(error) => json!({
            "check_id": "agentdb_reachable",
            "status": "error",
            "message": error.to_string()
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
                "details": value
            }),
            Err(error) => json!({
                "check_id": "agentdb_export_status",
                "status": "error",
                "message": error.to_string()
            }),
        }
    } else {
        json!({
            "check_id": "agentdb_export_status",
            "status": "skipped",
            "message": "--tenant-id not supplied"
        })
    };
    let clickhouse = if let Some(url) = args.clickhouse_url {
        match clickhouse_query(&url, "SELECT 1").await {
            Ok(text) => json!({
                "check_id": "clickhouse_reachable",
                "status": "ok",
                "response": text.trim()
            }),
            Err(error) => json!({
                "check_id": "clickhouse_reachable",
                "status": "error",
                "message": error.to_string()
            }),
        }
    } else {
        json!({
            "check_id": "clickhouse_reachable",
            "status": "skipped",
            "message": "--clickhouse-url or CLICKHOUSE_URL not supplied"
        })
    };
    let checks = vec![agentdb_ready, export_status, clickhouse];
    let ok = checks
        .iter()
        .all(|check| check.get("status").and_then(Value::as_str) != Some("error"));
    Ok(json!({
        "status": if ok { "ok" } else { "error" },
        "checks": checks
    }))
}

async fn clickhouse_init(args: ClickHouseInitArgs) -> Result<Value> {
    if !args.apply {
        return Ok(json!({
            "status": "dry_run",
            "statements": CLICKHOUSE_SCHEMA_DDL
        }));
    }
    let url = args
        .clickhouse_url
        .ok_or_else(|| anyhow!("--clickhouse-url or CLICKHOUSE_URL is required with --apply"))?;
    let mut applied = Vec::new();
    for statement in CLICKHOUSE_SCHEMA_DDL {
        clickhouse_query(&url, statement).await?;
        applied.push(json!({
            "statement": statement.lines().next().unwrap_or(statement).trim(),
            "status": "ok"
        }));
    }
    Ok(json!({
        "status": "ok",
        "applied": applied
    }))
}

async fn ensure_claimed_export_consumer(
    api: &Api,
    args: &ClickHouseRunArgs,
    consumer: &str,
) -> Result<Value> {
    api.post(
        &format!(
            "/api/tenants/{}/exports/consumers",
            query_component(&args.tenant_id)
        ),
        json!({
            "name": consumer,
            "scope": args.scope.clone(),
            "agent_id": args.agent_id.clone(),
            "session_id": args.session_id.clone(),
            "include": ["events"]
        }),
    )
    .await?;
    api.post(
        &format!(
            "/api/tenants/{}/exports/consumers/{}/claim",
            query_component(&args.tenant_id),
            query_component(consumer)
        ),
        json!({
            "lease_owner": args.lease_owner.clone(),
            "lease_ms": args.lease_ms
        }),
    )
    .await
}

async fn clickhouse_run(api: &Api, args: ClickHouseRunArgs) -> Result<Value> {
    let mut cursor = args.from_cursor.clone();
    if let Some(consumer) = &args.consumer {
        let claimed = ensure_claimed_export_consumer(api, &args, consumer).await?;
        if cursor.is_none() {
            cursor = claimed
                .get("last_acked_cursor")
                .and_then(Value::as_str)
                .map(str::to_string);
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
                api.post(
                    &format!(
                        "/api/tenants/{}/exports/backfill",
                        query_component(&args.tenant_id)
                    ),
                    json!({
                        "scope": args.scope.clone(),
                        "agent_id": args.agent_id.clone(),
                        "session_id": args.session_id.clone(),
                        "include": include,
                        "from_cursor": cursor.clone(),
                        "limit": args.limit
                    }),
                )
                .await?
            }
            ExportRunPhase::Watch => {
                api.get(&export_watch_path(&args, cursor.as_deref()))
                    .await?
            }
        };
        let items = response
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if items.is_empty() {
            if phase == ExportRunPhase::Backfill {
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
            clickhouse_insert_json_each_row(
                &args.clickhouse_url,
                "agentdb_export_items_raw",
                &clickhouse_raw_rows(&items)?,
            )
            .await?;
            clickhouse_insert_json_each_row(
                &args.clickhouse_url,
                "agentdb_events",
                &clickhouse_event_rows(&items)?,
            )
            .await?;
        }
        cursor = response
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        if phase == ExportRunPhase::Backfill
            && args.tail
            && !args.include.is_empty()
            && !args.include.iter().any(|item| item == "events")
        {
            cursor = None;
        }
        if let (Some(consumer), Some(cursor)) = (&args.consumer, cursor.as_deref()) {
            if !args.dry_run {
                api.post(
                    &format!(
                        "/api/tenants/{}/exports/consumers/{}/ack",
                        query_component(&args.tenant_id),
                        query_component(consumer)
                    ),
                    json!({
                        "cursor": cursor,
                        "snapshot_high_watermark": response
                            .get("snapshot_high_watermark")
                            .and_then(Value::as_str),
                        "lease_owner": args.lease_owner.clone()
                    }),
                )
                .await?;
            }
        }
        if phase == ExportRunPhase::Backfill {
            let complete = response
                .get("complete")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            if complete {
                if args.tail {
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
    Ok(json!({
        "status": "ok",
        "tenant_id": args.tenant_id,
        "consumer": args.consumer,
        "backfill": args.backfill,
        "tail": args.tail,
        "batches": batches,
        "rows": rows,
        "last_cursor": cursor,
        "dry_run": args.dry_run
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExportRunPhase {
    Backfill,
    Watch,
}

fn export_watch_path(args: &ClickHouseRunArgs, cursor: Option<&str>) -> String {
    let limit = args.limit.to_string();
    let include = if args.include.is_empty() {
        "events".to_string()
    } else {
        args.include.join(",")
    };
    let pairs = [
        ("scope", Some(args.scope.as_str())),
        ("agent_id", args.agent_id.as_deref()),
        ("session_id", args.session_id.as_deref()),
        ("include", Some(include.as_str())),
        ("from_cursor", cursor),
        ("limit", Some(limit.as_str())),
    ];
    format!(
        "/api/tenants/{}/exports/watch{}",
        query_component(&args.tenant_id),
        query_pairs_encoded(&pairs)
    )
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

async fn clickhouse_insert_json_each_row(url: &str, table: &str, rows: &[Value]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut body = format!("INSERT INTO {table} FORMAT JSONEachRow\n");
    for row in rows {
        body.push_str(&serde_json::to_string(row)?);
        body.push('\n');
    }
    clickhouse_query(url, &body).await?;
    Ok(())
}

async fn clickhouse_query(url: &str, sql: &str) -> Result<String> {
    let redacted_url = redact_clickhouse_url(url);
    let response = Client::new()
        .post(url)
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
    fn builds_encoded_export_watch_path() {
        let args = ClickHouseRunArgs {
            tenant_id: "ten_1".into(),
            clickhouse_url: "http://clickhouse:8123/agentdb".into(),
            scope: "session".into(),
            agent_id: None,
            session_id: Some("ses_1".into()),
            consumer: None,
            lease_owner: "agentdbctl".into(),
            lease_ms: 30_000,
            from_cursor: Some("v2:0001:ses_1:0002:cell=cell-a".into()),
            limit: 500,
            include: vec![],
            backfill: false,
            tail: false,
            max_batches: None,
            poll_interval_ms: 1000,
            dry_run: false,
        };
        assert_eq!(
            export_watch_path(&args, args.from_cursor.as_deref()),
            "/api/tenants/ten_1/exports/watch?scope=session&session_id=ses_1&include=events&from_cursor=v2%3A0001%3Ases_1%3A0002%3Acell%3Dcell-a&limit=500"
        );
    }

    #[test]
    fn clickhouse_schema_contains_event_and_raw_tables() {
        let ddl = CLICKHOUSE_SCHEMA_DDL.join("\n");
        assert!(ddl.contains("agentdb_export_items_raw"));
        assert!(ddl.contains("agentdb_events"));
        assert!(ddl.contains("ReplacingMergeTree(observed_at_ms)"));
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
    }
}
