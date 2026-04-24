//! Command-line shape for the `agentdb` binary. Parsed via clap.
//! Subcommands: `serve`, `dump-config`, plus admin HTTP-client
//! wrappers (`drain`, `show-tablets`, `show-scheduler`, `show-nodes`).
//!
//! Every env var the server previously honored still works — flags
//! are overrides, not replacements. The new contract is:
//!
//!   default < env var < explicit flag
//!
//! so operators who already script around env vars lose nothing.

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "agentdb",
    version,
    about = "Agent-native operational database",
    long_about = "Single-binary entry point for the AgentDB server and its admin client. \
                  The server process runs under `agentdb serve`; admin operations \
                  (drain, show-*) are HTTP-client wrappers that talk to a running server."
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Start the AgentDB server.
    Serve(ServeOpts),
    /// Print the effective configuration (default / env / flag) as JSON.
    DumpConfig(DumpConfigOpts),
    /// Mark this server as draining — refuses new writes, stops lease
    /// renewal. HTTP-client wrapper for `POST /api/admin/drain`.
    Drain(AdminOpts),
    /// List every tablet row in the control plane. HTTP-client
    /// wrapper for `GET /api/admin/tablets`.
    ShowTablets(AdminOpts),
    /// Show scheduler leader election + reassignment counters.
    /// HTTP-client wrapper for `GET /api/admin/scheduler`.
    ShowScheduler(AdminOpts),
    /// List registered cluster nodes. HTTP-client wrapper for
    /// `GET /api/admin/nodes`.
    ShowNodes(AdminOpts),
    /// Resolve a session id to its shard + tablet assignment.
    /// HTTP-client wrapper for `GET /api/admin/shards?session_id=...`.
    ShowShard(ShowShardOpts),
    /// Pin a shard to a specific tablet, or revert it to default
    /// routing with `--clear`. HTTP-client wrapper for
    /// `POST /api/admin/shards/:id`.
    SetShard(SetShardOpts),
    /// List shards that currently carry an explicit tablet override.
    /// HTTP-client wrapper for `GET /api/admin/shards/list`.
    ListShardOverrides(AdminOpts),
    /// Manage SCRAM-SHA-256 user accounts (M19).
    #[command(subcommand)]
    User(UserCmd),
    /// Show tablets the hot-tablet detector has flagged as
    /// saturated (M25).
    /// HTTP-client wrapper for `GET /api/admin/tablets/hot`.
    ShowHotTablets(AdminOpts),
    /// Show every tablet's current rate + hot/cool status
    /// (M25). Useful for "which tablet is warm but not yet
    /// saturated?".
    /// HTTP-client wrapper for `GET /api/admin/tablets/load`.
    ShowTabletLoad(AdminOpts),
    /// Move a fraction of one tablet's overridden shards to
    /// another tablet (M25). Manual load redistribution; pair
    /// with `show-hot-tablets` to pick a source.
    /// HTTP-client wrapper for
    /// `POST /api/admin/tablets/:id/shed`.
    ShedTablet(ShedTabletOpts),
    /// Create a new tablet on this node at runtime (M25
    /// dynamic-tablet). Grows the box pool by one; the new
    /// tablet starts empty and owned by the node that
    /// received this call.
    /// HTTP-client wrapper for
    /// `POST /api/admin/tablets/create`.
    CreateTablet(CreateTabletOpts),
    /// Manage admin bearer tokens. Required when
    /// `AGENTDB_REQUIRE_ADMIN_AUTH=true` on the server.
    #[command(subcommand)]
    AdminToken(AdminTokenCmd),
    /// Migrate a warm session from its current tablet to a
    /// new one. Use `shed-tablet` first to move the shard,
    /// then `migrate-session` to move any previously-
    /// written events so reads via the new tablet find
    /// them.
    /// HTTP-client wrapper for
    /// `POST /api/admin/sessions/:sid/migrate`.
    MigrateSession(MigrateSessionOpts),
    /// Show p50/p95/p99 latency histograms for one box
    /// (append and admission-wait).
    /// HTTP-client wrapper for
    /// `GET /api/admin/boxes/:id/latency`.
    ShowLatency(ShowLatencyOpts),
    /// List recent admin-plane requests from the audit log.
    /// Every request that hits `/api/admin/*` (including
    /// 401 attempts) is recorded.
    /// HTTP-client wrapper for `GET /api/admin/audit`.
    AuditLog(AuditLogOpts),
    /// Set (or replace) the global per-tenant rate-limit for
    /// a tenant. Subsequent writes above `max_qps` with a
    /// full `burst` deplete the bucket and are refused.
    SetTenantQuota(SetTenantQuotaOpts),
    /// Show the current quota + running deny count for a
    /// tenant, or list every configured quota when no
    /// tenant_id is given.
    ShowTenantQuota(ShowTenantQuotaOpts),
    /// Clear the global per-tenant rate-limit for a tenant.
    /// Idempotent.
    ClearTenantQuota(ClearTenantQuotaOpts),
    /// Show recent slow-query records (requests whose
    /// elapsed time exceeded `AGENTDB_SLOW_QUERY_THRESHOLD_MS`).
    /// HTTP-client wrapper for `GET /api/admin/slow-queries`.
    ShowSlowQueries(ShowSlowQueriesOpts),
    /// Take a point-in-time backup of the meta store and the
    /// live KV state into an output directory (which must not
    /// exist). Restore is offline — see
    /// `docs/ARCHITECTURE.md § Point-in-time backup`.
    /// HTTP-client wrapper for `POST /api/admin/backup`.
    Backup(BackupOpts),
}

#[derive(Args, Debug, Clone)]
pub struct BackupOpts {
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Absolute path to the backup destination. Must not exist;
    /// the server creates it and populates `kv/`, `meta.sqlite`,
    /// `manifest.json`.
    #[arg(long = "out")]
    pub out_dir: String,
}

#[derive(Args, Debug, Clone)]
pub struct ShowSlowQueriesOpts {
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Cap on returned rows. Server clamps to its MAX_LIMIT.
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
    /// Filter by kind: `append`, `find`, or `aggregate`.
    #[arg(long)]
    pub kind: Option<String>,
    /// Exact external tenant id.
    #[arg(long)]
    pub tenant: Option<String>,
    /// Return rows with id > this value. Pair with the
    /// largest id from a previous page for pagination.
    #[arg(long = "after-id")]
    pub after_id: Option<u64>,
}

#[derive(Args, Debug, Clone)]
pub struct ShowLatencyOpts {
    /// Base URL of the server.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Box id. Required positional.
    pub box_id: u16,
}

#[derive(Args, Debug, Clone)]
pub struct AuditLogOpts {
    /// Base URL of the server.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Maximum rows to return (server clamps to its MAX_LIMIT).
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
    /// Only include rows at or after this `ts_ms` (UTC ms).
    #[arg(long = "since-ms")]
    pub since_ms: Option<i64>,
    /// Only include rows strictly before this `ts_ms`. Pair
    /// with `--since-ms` for windowed queries, or with the
    /// oldest `ts_ms` from a previous page to paginate.
    #[arg(long = "until-ms")]
    pub until_ms: Option<i64>,
    /// Exact match on admin token id (e.g. `9f2a...`).
    #[arg(long = "actor")]
    pub actor_id: Option<String>,
    /// Literal URL-path prefix (LIKE wildcards are escaped).
    #[arg(long = "path-prefix")]
    pub path_prefix: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct SetTenantQuotaOpts {
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// External tenant id.
    pub tenant_id: String,
    /// Steady-state refill rate in requests/second.
    #[arg(long = "max-qps")]
    pub max_qps: u64,
    /// Bucket capacity (max burst above the steady rate).
    #[arg(long)]
    pub burst: u64,
}

#[derive(Args, Debug, Clone)]
pub struct ShowTenantQuotaOpts {
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// External tenant id. Omit to list every configured quota.
    pub tenant_id: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct ClearTenantQuotaOpts {
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// External tenant id.
    pub tenant_id: String,
}

#[derive(Args, Debug, Clone)]
pub struct MigrateSessionOpts {
    /// Base URL of the server.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Session to move. Required positional.
    pub session_id: String,
    /// Tenant that owns the session.
    #[arg(long)]
    pub tenant: String,
    /// Destination tablet id.
    #[arg(long)]
    pub to: u16,
}

#[derive(Subcommand, Debug)]
pub enum AdminTokenCmd {
    /// Mint a new token. The plaintext is printed to stdout
    /// ONCE and never again; capture it immediately.
    Add(AdminTokenAddOpts),
    /// List active (non-revoked) tokens as summaries.
    List(AdminOpts),
    /// Soft-revoke a token by id. Idempotent.
    Revoke(AdminTokenRevokeOpts),
}

#[derive(Args, Debug, Clone)]
pub struct AdminTokenAddOpts {
    /// Base URL of the server.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Free-form label for this token (shown in `list`).
    #[arg(long)]
    pub description: Option<String>,
    /// RBAC scope: `admin` (default) or `readonly`. Readonly
    /// tokens can only issue GETs against `/api/admin/*`.
    #[arg(long, default_value = "admin")]
    pub scope: String,
    /// Bearer token to authenticate this request. Not
    /// needed for the bootstrap mint (no tokens exist
    /// yet). Falls back to the AGENTDB_ADMIN_TOKEN env var.
    #[arg(long)]
    pub token: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct AdminTokenRevokeOpts {
    /// Base URL of the server.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Token id (opaque 12-char prefix) to revoke.
    pub token_id: String,
    /// Bearer token to authenticate this request. Falls
    /// back to the AGENTDB_ADMIN_TOKEN env var.
    #[arg(long)]
    pub token: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct CreateTabletOpts {
    /// Base URL of the server to talk to.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Explicit tablet id. Omit to let the server pick
    /// `max(existing) + 1`.
    #[arg(long)]
    pub id: Option<i64>,
}

#[derive(Args, Debug, Clone)]
pub struct ShedTabletOpts {
    /// Base URL of the server to talk to.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Source tablet id (the hot one). Required positional
    /// argument.
    pub tablet_id: i64,
    /// Destination tablet. If omitted, the server picks the
    /// least-loaded tablet excluding the source.
    #[arg(long)]
    pub to: Option<i64>,
    /// Fraction of the source's overridden shards to move.
    /// Defaults server-side to 0.5. Clamps to (0, 1].
    #[arg(long)]
    pub fraction: Option<f64>,
    /// When set, after moving the shards the server also
    /// rekeys every warm session on those shards to the
    /// destination tablet in an atomic write-batch per
    /// session. Cold / archive / fork-parent sessions are
    /// skipped with a reason in the response. Without
    /// this flag, shed only moves future routing —
    /// already-written events stay on source.
    #[arg(long = "migrate-data", default_value_t = false)]
    pub migrate_data: bool,
}

#[derive(Subcommand, Debug)]
pub enum UserCmd {
    /// Create a new user. Reads the password from `--password`
    /// or, if omitted, from stdin. Maps the user to a tenant;
    /// all requests from this user are clamped to that tenant.
    Add(UserAddOpts),
    /// List registered users (summaries only — credentials are
    /// never returned over the wire).
    List(AdminOpts),
    /// Delete a user. Idempotent — exits success even if the
    /// user doesn't exist.
    Delete(UserDeleteOpts),
}

#[derive(Args, Debug, Clone)]
pub struct UserAddOpts {
    /// Base URL of the server to talk to.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// SCRAM username. Required positional argument.
    pub user_id: String,
    /// External tenant id this user can access. Required.
    #[arg(long)]
    pub tenant: String,
    /// Plaintext password. If omitted, read from stdin (useful
    /// for scripting + keeping passwords out of shell history).
    #[arg(long)]
    pub password: Option<String>,
    /// PBKDF2 iterations. Defaults to the RFC 5802 floor (4096).
    /// Higher values slow down per-auth PBKDF2 work.
    #[arg(long)]
    pub iterations: Option<u32>,
}

#[derive(Args, Debug, Clone)]
pub struct UserDeleteOpts {
    /// Base URL of the server to talk to.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// SCRAM username. Required positional argument.
    pub user_id: String,
}

#[derive(Args, Debug, Clone)]
pub struct ShowShardOpts {
    /// Base URL of the server to talk to.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Session id to hash + look up. Required positional argument.
    pub session_id: String,
}

#[derive(Args, Debug, Clone)]
pub struct SetShardOpts {
    /// Base URL of the server to talk to.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Shard id to set (0..16384). Required positional argument.
    pub shard_id: u16,
    /// Tablet id to pin this shard to. Mutually exclusive with
    /// `--clear`; exactly one of the two must be set.
    #[arg(long, conflicts_with = "clear")]
    pub tablet: Option<i64>,
    /// Revert this shard to default (hash) routing. Mutually
    /// exclusive with `--tablet`.
    #[arg(long, conflicts_with = "tablet")]
    pub clear: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct ServeOpts {
    /// Dev mode: ephemeral tempdirs for KV + object store, in-memory
    /// SQLite meta, relaxed timings. Perfect for `cargo run` or a
    /// first-time-user quickstart. Overrides every storage-path env
    /// var but leaves admission / lease config alone.
    #[arg(long)]
    pub dev: bool,

    /// Override AGENTDB_ADDR. HTTP listen address.
    #[arg(long)]
    pub addr: Option<String>,

    /// Override AGENTDB_MONGO_ADDR. Mongo wire listen address.
    #[arg(long = "mongo-addr")]
    pub mongo_addr: Option<String>,

    /// Override AGENTDB_BOXES. Number of boxes per process.
    #[arg(long)]
    pub boxes: Option<u16>,

    /// Override AGENTDB_NODE_ID. This process's node id. Appears in
    /// lease rows and the nodes table.
    #[arg(long = "node-id")]
    pub node_id: Option<String>,

    /// Override AGENTDB_DATA. Local KV (RocksDB) directory.
    #[arg(long = "data-dir")]
    pub data_dir: Option<std::path::PathBuf>,

    /// Override AGENTDB_OBJECTS. Local object-store root directory.
    #[arg(long = "objects-dir")]
    pub objects_dir: Option<std::path::PathBuf>,

    /// Override AGENTDB_META_PATH. Control-plane SQLite file.
    #[arg(long = "meta-path")]
    pub meta_path: Option<String>,

    /// Path to a TLS certificate chain (PEM). When set
    /// together with `--tls-key`, the REST listener serves
    /// HTTPS via rustls. Falls back to
    /// `AGENTDB_TLS_CERT` if the flag is omitted. Mongo
    /// wire stays plaintext either way — a separate PR
    /// adds Mongo TLS.
    #[arg(long = "tls-cert")]
    pub tls_cert: Option<std::path::PathBuf>,

    /// Path to the TLS private key (PEM, PKCS#8 or RSA).
    /// Falls back to `AGENTDB_TLS_KEY`.
    #[arg(long = "tls-key")]
    pub tls_key: Option<std::path::PathBuf>,

    /// Generate an ephemeral self-signed certificate at
    /// startup so operators can smoke-test TLS without
    /// provisioning a real cert. Valid for `localhost` and
    /// `127.0.0.1`. Mutually exclusive with explicit
    /// `--tls-cert` / `--tls-key`.
    #[arg(long = "dev-tls", conflicts_with_all = ["tls_cert", "tls_key"])]
    pub dev_tls: bool,
}

#[derive(Args, Debug, Default, Clone)]
pub struct DumpConfigOpts {
    /// Alternate `--dev` preview: print what the config *would*
    /// look like with `--dev` enabled, without actually running.
    #[arg(long)]
    pub dev: bool,
}

#[derive(Args, Debug, Clone)]
pub struct AdminOpts {
    /// Base URL of the server to talk to. Defaults to the local
    /// REST listener at `http://127.0.0.1:7878`.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
}
