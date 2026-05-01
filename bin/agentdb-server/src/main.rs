//! `agentdb` binary — V2 server entrypoint.
//!
//! Mounts the V2 HTTP routes from `agentdb-api` against a `Runtime`. By default
//! the runtime is RocksDB-backed at `--data-dir/runtime`; pass `--dev` or
//! `--in-memory` to get the ephemeral backend (useful for smoke tests and
//! local dev).

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::routing::get;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "agentdb", version, about = "AgentDB runtime database (v2)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    /// Start the HTTP server with the V2 router.
    Serve(ServeOpts),
}

#[derive(Parser, Debug)]
struct ServeOpts {
    /// `host:port` to bind. Defaults to `127.0.0.1:7878`.
    ///
    /// Environment fallback order: `AGENTDB_BIND`, `AGENTDB_ADDR`,
    /// `AGENTDB_HTTP_ADDR`.
    #[arg(long, env = "AGENTDB_BIND")]
    bind: Option<SocketAddr>,

    /// Filesystem path that holds the RocksDB-backed runtime state. Created if
    /// it doesn't exist. Ignored when `--in-memory` is set.
    ///
    /// Environment fallback order: `AGENTDB_DATA_DIR`, `AGENTDB_DATA`.
    #[arg(long, env = "AGENTDB_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Filesystem path that holds artifact content (raw blobs). Created if
    /// it doesn't exist. Defaults to `<data_dir>/content`. Ignored when
    /// `--in-memory` is set.
    ///
    /// Environment fallback order: `AGENTDB_CONTENT_DIR`, `AGENTDB_OBJECTS`.
    #[arg(long, env = "AGENTDB_CONTENT_DIR")]
    content_dir: Option<PathBuf>,

    /// Use an ephemeral in-memory backend instead of RocksDB. Convenient for
    /// smoke testing; everything is lost on shutdown.
    #[arg(long, env = "AGENTDB_IN_MEMORY")]
    in_memory: bool,

    /// Alias for `--in-memory` with friendlier local-development wording.
    #[arg(long, env = "AGENTDB_DEV")]
    dev: bool,

    /// Enable metadata-backed routing for this node id. When unset, the server
    /// keeps the single-node routing-disabled behavior.
    #[arg(long)]
    route_node_id: Option<String>,

    /// Shard count used by the route planner when no session locator exists.
    #[arg(long, default_value_t = agentdb_meta::shards::DEFAULT_SHARD_COUNT)]
    route_shard_count: i64,

    /// Optional region for residency-aware routing. Compared with
    /// `nodes.region`, falling back to `availability_zone` for older rows that
    /// only populated the prototype location label.
    #[arg(long)]
    route_region: Option<String>,

    /// Backward-compatible alias for the old prototype location label. Prefer
    /// `--route-region` for new deployments.
    #[arg(long)]
    route_location: Option<String>,

    /// Route snapshot refresh interval in milliseconds. Periodic refresh is the
    /// correctness path for SQLite and session locator changes; Postgres
    /// LISTEN/NOTIFY can trigger faster refreshes for shard/node/tablet changes.
    #[arg(long, default_value_t = 5_000)]
    route_refresh_interval_ms: u64,

    /// Timeout for same-cluster routing proxy requests.
    #[arg(long, default_value_t = 5_000)]
    route_proxy_timeout_ms: u64,

    /// Maximum owner-node response body size that the routing proxy will pass
    /// through to the original caller.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    route_proxy_max_response_bytes: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();
    match args.cmd {
        Cmd::Serve(opts) => serve(opts).await,
    }
}

async fn serve(opts: ServeOpts) -> anyhow::Result<()> {
    let config = ResolvedServeConfig::from_opts(opts);
    let runtime = if config.in_memory {
        tracing::info!("using in-memory runtime (state lost on shutdown)");
        agentdb_core::Runtime::in_memory()
    } else {
        let kv_path = config.data_dir.join("runtime");
        std::fs::create_dir_all(&kv_path)?;
        let kv = Arc::new(agentdb_storage::RocksKvStore::open(&kv_path)?)
            as Arc<dyn agentdb_storage::KvStore + Send + Sync>;
        tracing::info!(path = %kv_path.display(), "opened RocksDB runtime backend");

        // Artifact content lives in a separate filesystem tree so the
        // RocksDB metadata directory stays small and operators can mount the
        // content tree on a different disk if they need to. Default is
        // `<data_dir>/content`; `--content-dir` overrides it.
        let content_dir = config.content_dir.clone();
        std::fs::create_dir_all(&content_dir)?;
        tracing::info!(path = %content_dir.display(), "opened LocalFs artifact content backend");
        let content: Arc<dyn agentdb_storage::ContentBackend> =
            Arc::new(agentdb_storage::LocalFsContentBackend::new(&content_dir));
        agentdb_core::Runtime::with_kv(kv, content)
    };

    let mut state =
        agentdb_api::AppState::new(runtime).with_proxy_policy(agentdb_api::state::ProxyPolicy {
            timeout_ms: config.route_proxy_timeout_ms,
            max_response_bytes: config.route_proxy_max_response_bytes,
        });
    let route_location = config
        .route_region
        .clone()
        .or(config.route_location.clone());
    if let Some(node_id) = config.route_node_id.as_deref() {
        let meta = agentdb_meta::MetaStore::open_from_env().await?;
        let snapshot =
            load_route_snapshot(&meta, config.route_shard_count, route_location.clone()).await?;
        let routing = agentdb_api::routing_http::RouteRuntime::snapshot(node_id, snapshot);
        spawn_route_snapshot_refresher(
            meta,
            routing.clone(),
            config.route_shard_count,
            route_location.clone(),
            config.route_refresh_interval_ms,
        );
        state = state.with_routing(routing);
        tracing::info!(
            node_id,
            shard_count = config.route_shard_count,
            location = ?route_location,
            refresh_interval_ms = config.route_refresh_interval_ms,
            "enabled metadata-backed route snapshot"
        );
    }
    let app = agentdb_api::router(state)
        .route("/dashboard/", get(agentdb_dashboard::index))
        .nest("/dashboard", agentdb_dashboard::router());
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    print_startup_banner(&config);
    tracing::info!(addr = %config.bind, "agentdb listening");
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedServeConfig {
    bind: SocketAddr,
    data_dir: PathBuf,
    content_dir: PathBuf,
    in_memory: bool,
    route_node_id: Option<String>,
    route_shard_count: i64,
    route_region: Option<String>,
    route_location: Option<String>,
    route_refresh_interval_ms: u64,
    route_proxy_timeout_ms: u64,
    route_proxy_max_response_bytes: u64,
}

impl ResolvedServeConfig {
    fn from_opts(opts: ServeOpts) -> Self {
        let bind = opts
            .bind
            .or_else(|| env_socket_addr("AGENTDB_ADDR"))
            .or_else(|| env_socket_addr("AGENTDB_HTTP_ADDR"))
            .unwrap_or_else(default_bind);
        let data_dir = opts
            .data_dir
            .or_else(|| env_path("AGENTDB_DATA"))
            .unwrap_or_else(default_data_dir);
        let content_dir = opts
            .content_dir
            .or_else(|| env_path("AGENTDB_OBJECTS"))
            .unwrap_or_else(|| data_dir.join("content"));
        Self {
            bind,
            data_dir,
            content_dir,
            in_memory: opts.in_memory || opts.dev,
            route_node_id: opts.route_node_id,
            route_shard_count: opts.route_shard_count,
            route_region: opts.route_region,
            route_location: opts.route_location,
            route_refresh_interval_ms: opts.route_refresh_interval_ms,
            route_proxy_timeout_ms: opts.route_proxy_timeout_ms,
            route_proxy_max_response_bytes: opts.route_proxy_max_response_bytes,
        }
    }
}

fn default_bind() -> SocketAddr {
    "127.0.0.1:7878"
        .parse()
        .expect("default bind address must parse")
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn env_socket_addr(name: &str) -> Option<SocketAddr> {
    let value = env::var(name).ok()?;
    if value.trim().is_empty() {
        return None;
    }
    match value.parse() {
        Ok(addr) => Some(addr),
        Err(error) => {
            tracing::warn!(%name, %value, %error, "ignoring invalid socket address environment variable");
            None
        }
    }
}

fn print_startup_banner(config: &ResolvedServeConfig) {
    let mode = if config.in_memory {
        "dev in-memory (state is lost on shutdown)".to_string()
    } else {
        format!(
            "persistent (data: {}, artifacts: {})",
            config.data_dir.display(),
            config.content_dir.display()
        )
    };
    println!("AgentDB listening on http://{}", config.bind);
    println!("  mode: {mode}");
    println!("  health:    http://{}/health/ready", config.bind);
    println!("  dashboard: http://{}/dashboard/", config.bind);
}

async fn load_route_snapshot(
    meta: &agentdb_meta::MetaStore,
    shard_count: i64,
    route_location: Option<String>,
) -> agentdb_meta::MetaResult<agentdb_api::routing_http::RouteSnapshot> {
    agentdb_api::routing_http::RouteSnapshot::load_from_meta(
        meta,
        shard_count,
        route_location,
        agentdb_meta::routing::ResidencyPolicy::default(),
    )
    .await
}

fn spawn_route_snapshot_refresher(
    meta: agentdb_meta::MetaStore,
    routing: agentdb_api::routing_http::RouteRuntime,
    shard_count: i64,
    route_location: Option<String>,
    refresh_interval_ms: u64,
) {
    let refresh_interval_ms = refresh_interval_ms.max(100);
    let mut changes = agentdb_meta::change_notifier::subscribe(
        &meta,
        &[
            agentdb_meta::change_notifier::Channel::Shards,
            agentdb_meta::change_notifier::Channel::Nodes,
            agentdb_meta::change_notifier::Channel::Tablets,
            agentdb_meta::change_notifier::Channel::SessionLocators,
        ],
    );
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(refresh_interval_ms));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    refresh_route_snapshot(&meta, &routing, shard_count, route_location.clone()).await;
                }
                change = changes.recv() => {
                    match change {
                        Some(change) => {
                            tracing::debug!(channel = ?change.channel, payload = %change.payload, "route metadata change received");
                            refresh_route_snapshot(&meta, &routing, shard_count, route_location.clone()).await;
                        }
                        None => {
                            tracing::warn!("route metadata change stream closed; continuing with periodic refresh");
                            changes = agentdb_meta::change_notifier::subscribe(
                                &meta,
                                &[
                                    agentdb_meta::change_notifier::Channel::Shards,
                                    agentdb_meta::change_notifier::Channel::Nodes,
                                    agentdb_meta::change_notifier::Channel::Tablets,
                                    agentdb_meta::change_notifier::Channel::SessionLocators,
                                ],
                            );
                        }
                    }
                }
            }
        }
    });
}

async fn refresh_route_snapshot(
    meta: &agentdb_meta::MetaStore,
    routing: &agentdb_api::routing_http::RouteRuntime,
    shard_count: i64,
    route_location: Option<String>,
) {
    match load_route_snapshot(meta, shard_count, route_location).await {
        Ok(snapshot) => {
            routing.refresh_snapshot(snapshot);
            tracing::debug!("refreshed route snapshot");
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to refresh route snapshot");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_opts() -> ServeOpts {
        ServeOpts {
            bind: None,
            data_dir: None,
            content_dir: None,
            in_memory: false,
            dev: false,
            route_node_id: None,
            route_shard_count: agentdb_meta::shards::DEFAULT_SHARD_COUNT,
            route_region: None,
            route_location: None,
            route_refresh_interval_ms: 5_000,
            route_proxy_timeout_ms: 5_000,
            route_proxy_max_response_bytes: 16 * 1024 * 1024,
        }
    }

    #[test]
    fn default_single_node_config_matches_public_quickstarts() {
        let config = ResolvedServeConfig::from_opts(base_opts());

        assert_eq!(config.bind, default_bind());
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.content_dir, PathBuf::from("./data/content"));
        assert!(!config.in_memory);
    }

    #[test]
    fn dev_alias_uses_in_memory_runtime() {
        let mut opts = base_opts();
        opts.dev = true;

        assert!(ResolvedServeConfig::from_opts(opts).in_memory);
    }

    #[test]
    fn explicit_content_dir_overrides_data_dir_default() {
        let mut opts = base_opts();
        opts.data_dir = Some(PathBuf::from("/var/lib/agentdb"));
        opts.content_dir = Some(PathBuf::from("/mnt/agentdb-content"));

        let config = ResolvedServeConfig::from_opts(opts);

        assert_eq!(config.data_dir, PathBuf::from("/var/lib/agentdb"));
        assert_eq!(config.content_dir, PathBuf::from("/mnt/agentdb-content"));
    }
}
