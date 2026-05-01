//! `agentdb` binary — server entrypoint.
//!
//! Mounts the HTTP routes from `agentdb-api` against a `Runtime`. By default
//! the runtime is RocksDB-backed at `--data-dir/runtime`; pass `--dev` or
//! `--in-memory` to get the ephemeral backend (useful for smoke tests and
//! local dev).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::routing::get;
use clap::Parser;
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "agentdb.toml";
const SAMPLE_CONFIG: &str = include_str!("../../../config/agentdb.toml");

#[derive(Parser, Debug)]
#[command(name = "agentdb", version, about = "AgentDB runtime database")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    /// Start the HTTP server with the runtime router.
    Serve(ServeOpts),
    /// Check local single-node configuration and data directories.
    Doctor(DoctorOpts),
    /// Write a starter single-node config file.
    InitConfig(InitConfigOpts),
}

#[derive(Parser, Debug)]
struct ServeOpts {
    /// Path to a TOML config file. Defaults to `agentdb.toml` when it exists.
    #[arg(long)]
    config: Option<PathBuf>,

    /// `host:port` to bind. Defaults to `127.0.0.1:7878`.
    ///
    #[arg(long)]
    bind: Option<SocketAddr>,

    /// Filesystem path that holds the RocksDB-backed runtime state. Created if
    /// it doesn't exist. Ignored when `--in-memory` is set.
    ///
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Filesystem path that holds artifact content (raw blobs). Created if
    /// it doesn't exist. Defaults to `<data_dir>/content`. Ignored when
    /// `--in-memory` is set.
    ///
    #[arg(long)]
    content_dir: Option<PathBuf>,

    /// Use an ephemeral in-memory backend instead of RocksDB. Convenient for
    /// smoke testing; everything is lost on shutdown.
    #[arg(long)]
    in_memory: bool,

    /// Alias for `--in-memory` with friendlier local-development wording.
    #[arg(long)]
    dev: bool,

    /// Enable metadata-backed routing for this node id. When unset, the server
    /// keeps the single-node routing-disabled behavior.
    #[arg(long)]
    route_node_id: Option<String>,

    /// Shard count used by the route planner when no session locator exists.
    #[arg(long)]
    route_shard_count: Option<i64>,

    /// Optional region for residency-aware routing. Compared with
    /// `nodes.region`, falling back to `availability_zone` for older rows that
    /// only populated the prototype location label.
    #[arg(long)]
    route_region: Option<String>,

    /// Older location label. Prefer `--route-region` for new deployments.
    #[arg(long)]
    route_location: Option<String>,

    /// Route snapshot refresh interval in milliseconds. Periodic refresh is the
    /// correctness path for SQLite and session locator changes; Postgres
    /// LISTEN/NOTIFY can trigger faster refreshes for shard/node/tablet changes.
    #[arg(long)]
    route_refresh_interval_ms: Option<u64>,

    /// Timeout for same-cluster routing proxy requests.
    #[arg(long)]
    route_proxy_timeout_ms: Option<u64>,

    /// Maximum owner-node response body size that the routing proxy will pass
    /// through to the original caller.
    #[arg(long)]
    route_proxy_max_response_bytes: Option<u64>,
}

#[derive(Parser, Debug)]
struct DoctorOpts {
    /// Path to a TOML config file. Defaults to `agentdb.toml` when it exists.
    #[arg(long)]
    config: Option<PathBuf>,

    /// `host:port` to bind. Overrides config file.
    #[arg(long)]
    bind: Option<SocketAddr>,

    /// Filesystem path that holds persistent runtime state.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Filesystem path that holds artifact content.
    #[arg(long)]
    content_dir: Option<PathBuf>,

    /// Check an ephemeral in-memory setup.
    #[arg(long)]
    in_memory: bool,

    /// Alias for `--in-memory`.
    #[arg(long)]
    dev: bool,
}

#[derive(Parser, Debug)]
struct InitConfigOpts {
    /// Path to write. Defaults to `agentdb.toml`.
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    output: PathBuf,

    /// Overwrite the file if it already exists.
    #[arg(long)]
    force: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();
    match args.cmd {
        Cmd::Serve(opts) => serve(opts).await,
        Cmd::Doctor(opts) => doctor(opts),
        Cmd::InitConfig(opts) => init_config(opts),
    }
}

async fn serve(opts: ServeOpts) -> anyhow::Result<()> {
    let config = ResolvedServeConfig::from_serve_opts(opts)?;
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
    fn from_serve_opts(opts: ServeOpts) -> anyhow::Result<Self> {
        let file = load_config(opts.config.as_ref())?;
        let bind = opts.bind.or(file.server.bind).unwrap_or_else(default_bind);
        let data_dir = opts
            .data_dir
            .or(file.storage.data_dir)
            .unwrap_or_else(default_data_dir);
        let content_dir = opts
            .content_dir
            .or(file.storage.content_dir)
            .unwrap_or_else(|| data_dir.join("content"));
        Ok(Self {
            bind,
            data_dir,
            content_dir,
            in_memory: opts.in_memory || opts.dev || file.server.dev.unwrap_or(false),
            route_node_id: opts.route_node_id.or(file.routing.node_id),
            route_shard_count: opts
                .route_shard_count
                .or(file.routing.shard_count)
                .unwrap_or(agentdb_meta::shards::DEFAULT_SHARD_COUNT),
            route_region: opts.route_region.or(file.routing.region),
            route_location: opts.route_location.or(file.routing.location),
            route_refresh_interval_ms: opts
                .route_refresh_interval_ms
                .or(file.routing.refresh_interval_ms)
                .unwrap_or(5_000),
            route_proxy_timeout_ms: opts
                .route_proxy_timeout_ms
                .or(file.routing.proxy_timeout_ms)
                .unwrap_or(5_000),
            route_proxy_max_response_bytes: opts
                .route_proxy_max_response_bytes
                .or(file.routing.proxy_max_response_bytes)
                .unwrap_or(16 * 1024 * 1024),
        })
    }

    fn from_doctor_opts(opts: DoctorOpts) -> anyhow::Result<Self> {
        let file = load_config(opts.config.as_ref())?;
        let bind = opts.bind.or(file.server.bind).unwrap_or_else(default_bind);
        let data_dir = opts
            .data_dir
            .or(file.storage.data_dir)
            .unwrap_or_else(default_data_dir);
        let content_dir = opts
            .content_dir
            .or(file.storage.content_dir)
            .unwrap_or_else(|| data_dir.join("content"));
        Ok(Self {
            bind,
            data_dir,
            content_dir,
            in_memory: opts.in_memory || opts.dev || file.server.dev.unwrap_or(false),
            route_node_id: file.routing.node_id,
            route_shard_count: file
                .routing
                .shard_count
                .unwrap_or(agentdb_meta::shards::DEFAULT_SHARD_COUNT),
            route_region: file.routing.region,
            route_location: file.routing.location,
            route_refresh_interval_ms: file.routing.refresh_interval_ms.unwrap_or(5_000),
            route_proxy_timeout_ms: file.routing.proxy_timeout_ms.unwrap_or(5_000),
            route_proxy_max_response_bytes: file
                .routing
                .proxy_max_response_bytes
                .unwrap_or(16 * 1024 * 1024),
        })
    }
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    server: ServerFileConfig,
    #[serde(default)]
    storage: StorageFileConfig,
    #[serde(default)]
    routing: RoutingFileConfig,
}

#[derive(Debug, Default, Deserialize)]
struct ServerFileConfig {
    bind: Option<SocketAddr>,
    dev: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct StorageFileConfig {
    data_dir: Option<PathBuf>,
    content_dir: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct RoutingFileConfig {
    node_id: Option<String>,
    shard_count: Option<i64>,
    region: Option<String>,
    location: Option<String>,
    refresh_interval_ms: Option<u64>,
    proxy_timeout_ms: Option<u64>,
    proxy_max_response_bytes: Option<u64>,
}

fn load_config(path: Option<&PathBuf>) -> anyhow::Result<FileConfig> {
    let Some(path) = resolve_config_path(path) else {
        return Ok(FileConfig::default());
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|error| anyhow::anyhow!("failed to read config {}: {error}", path.display()))?;
    toml::from_str(&text)
        .map_err(|error| anyhow::anyhow!("failed to parse config {}: {error}", path.display()))
}

fn resolve_config_path(path: Option<&PathBuf>) -> Option<PathBuf> {
    match path {
        Some(path) => Some(path.clone()),
        None => {
            let default = PathBuf::from(DEFAULT_CONFIG_PATH);
            default.exists().then_some(default)
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

fn doctor(opts: DoctorOpts) -> anyhow::Result<()> {
    let config = ResolvedServeConfig::from_doctor_opts(opts)?;
    let mut failed = false;

    println!("AgentDB doctor");
    println!("  bind: {}", config.bind);
    if config.in_memory {
        println!("  mode: dev in-memory");
        println!("  storage: skipped");
    } else {
        println!(
            "  mode: persistent (data: {}, artifacts: {})",
            config.data_dir.display(),
            config.content_dir.display()
        );
        failed |= !check_writable_dir("data dir", &config.data_dir);
        failed |= !check_writable_dir("content dir", &config.content_dir);
        failed |= !check_writable_dir("runtime dir", &config.data_dir.join("runtime"));
    }

    match std::net::TcpListener::bind(config.bind) {
        Ok(listener) => {
            drop(listener);
            println!("  ok: bind address is available");
        }
        Err(error) => {
            failed = true;
            println!("  error: cannot bind {}: {error}", config.bind);
        }
    }

    if failed {
        anyhow::bail!("doctor found configuration problems");
    }
    println!("  ok: single-node configuration looks usable");
    Ok(())
}

fn check_writable_dir(label: &str, path: &PathBuf) -> bool {
    if let Err(error) = std::fs::create_dir_all(path) {
        println!("  error: cannot create {label} {}: {error}", path.display());
        return false;
    }
    let probe = path.join(".agentdb-write-test");
    match std::fs::write(&probe, b"ok").and_then(|_| std::fs::remove_file(&probe)) {
        Ok(()) => {
            println!("  ok: {label} is writable ({})", path.display());
            true
        }
        Err(error) => {
            println!(
                "  error: {label} is not writable ({}): {error}",
                path.display()
            );
            false
        }
    }
}

fn init_config(opts: InitConfigOpts) -> anyhow::Result<()> {
    if opts.output.exists() && !opts.force {
        anyhow::bail!(
            "{} already exists; pass --force to overwrite it",
            opts.output.display()
        );
    }
    if let Some(parent) = opts
        .output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&opts.output, SAMPLE_CONFIG)?;
    println!("Wrote {}", opts.output.display());
    println!("Try:");
    println!("  agentdb doctor --config {}", opts.output.display());
    println!("  agentdb serve --config {}", opts.output.display());
    Ok(())
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
            config: None,
            bind: None,
            data_dir: None,
            content_dir: None,
            in_memory: false,
            dev: false,
            route_node_id: None,
            route_shard_count: None,
            route_region: None,
            route_location: None,
            route_refresh_interval_ms: None,
            route_proxy_timeout_ms: None,
            route_proxy_max_response_bytes: None,
        }
    }

    #[test]
    fn default_single_node_config_matches_public_quickstarts() {
        let config = ResolvedServeConfig::from_serve_opts(base_opts()).unwrap();

        assert_eq!(config.bind, default_bind());
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.content_dir, PathBuf::from("./data/content"));
        assert!(!config.in_memory);
    }

    #[test]
    fn dev_alias_uses_in_memory_runtime() {
        let mut opts = base_opts();
        opts.dev = true;

        assert!(
            ResolvedServeConfig::from_serve_opts(opts)
                .unwrap()
                .in_memory
        );
    }

    #[test]
    fn explicit_content_dir_overrides_data_dir_default() {
        let mut opts = base_opts();
        opts.data_dir = Some(PathBuf::from("/var/lib/agentdb"));
        opts.content_dir = Some(PathBuf::from("/mnt/agentdb-content"));

        let config = ResolvedServeConfig::from_serve_opts(opts).unwrap();

        assert_eq!(config.data_dir, PathBuf::from("/var/lib/agentdb"));
        assert_eq!(config.content_dir, PathBuf::from("/mnt/agentdb-content"));
    }

    #[test]
    fn config_file_sets_single_node_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[server]
bind = "127.0.0.1:8787"

[storage]
data_dir = "/tmp/agentdb-test"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);

        let config = ResolvedServeConfig::from_serve_opts(opts).unwrap();

        assert_eq!(config.bind, "127.0.0.1:8787".parse().unwrap());
        assert_eq!(config.data_dir, PathBuf::from("/tmp/agentdb-test"));
        assert_eq!(
            config.content_dir,
            PathBuf::from("/tmp/agentdb-test/content")
        );
    }

    #[test]
    fn cli_values_override_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[server]
bind = "127.0.0.1:8787"
dev = false

[storage]
data_dir = "/tmp/from-config"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);
        opts.bind = Some("127.0.0.1:8989".parse().unwrap());
        opts.data_dir = Some(PathBuf::from("/tmp/from-cli"));
        opts.dev = true;

        let config = ResolvedServeConfig::from_serve_opts(opts).unwrap();

        assert_eq!(config.bind, "127.0.0.1:8989".parse().unwrap());
        assert_eq!(config.data_dir, PathBuf::from("/tmp/from-cli"));
        assert!(config.in_memory);
    }
}
