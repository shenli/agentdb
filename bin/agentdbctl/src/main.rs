//! `agentdbctl` — human and agent-friendly operations CLI.
//!
//! The server's REST API remains the stable machine contract. This binary is a
//! small operator surface on top: concise commands, table output by default,
//! JSON output for automation, and no requirement to hand-write curl payloads.

use std::fmt::Write as _;

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};

const DEFAULT_URL: &str = "http://127.0.0.1:7878";

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
    },
}

#[derive(Subcommand, Debug)]
enum RestoreCommand {
    /// Validate a backup manifest. Destructive restore apply is not exposed.
    Validate { manifest_path: String },
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
            } => {
                api.post(
                    "/api/admin/backups",
                    json!({
                        "backup_id": backup_id,
                        "directory": directory,
                        "include_meta": !skip_meta,
                        "include_runtime_summary": !skip_runtime_summary
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
                    "region": args.region
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

async fn tablets(api: &Api, command: TabletsCommand) -> Result<Value> {
    match command {
        TabletsCommand::List => api.get("/api/admin/tablets").await,
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

fn query_opt<T: ToString>(name: &str, value: Option<T>) -> String {
    value
        .map(|value| format!("?{name}={}", value.to_string()))
        .unwrap_or_default()
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
        "nodes={} alive={} stale={} tablets={} unowned={} leader={} reassignments={}",
        text(&health["summary"]["node_count"]),
        text(&health["summary"]["alive_node_count"]),
        text(&health["summary"]["stale_node_count"]),
        text(&health["summary"]["tablet_count"]),
        text(&health["summary"]["unowned_tablet_count"]),
        text(&scheduler["leader_node_id"]),
        text(&scheduler["tablet_reassignments_total"])
    )
    .unwrap();
    if let Some(reasons) = readiness.get("reasons").and_then(Value::as_array) {
        if !reasons.is_empty() {
            writeln!(out, "reasons={}", compact(reasons)).unwrap();
        }
    }
    out.trim_end().to_string()
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
        "tablet_id",
        "state",
        "status",
        "action",
        "changed",
        "owner_node_id",
        "source_node_id",
        "target_node_id",
        "new_owner_node_id",
        "required_applied_index",
        "raft_membership_changed",
        "retired_source",
        "from_raft_node_id",
        "to_raft_node_id",
        "lag",
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
            "scheduler": { "leader_node_id": "node-a", "tablet_reassignments_total": 3 },
            "drain": { "is_draining": false }
        });
        let rendered = render_status(&value);
        assert!(rendered.contains("status=ok"));
        assert!(rendered.contains("ready=true"));
        assert!(rendered.contains("leader=node-a"));
    }
}
