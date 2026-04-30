//! `agentdb` binary — V2 server entrypoint.
//!
//! Mounts the V2 HTTP routes from `agentdb-api` against a `Runtime`. By default
//! the runtime is RocksDB-backed at `--data-dir/runtime`; pass `--in-memory` to
//! get the ephemeral backend (useful for smoke tests and local dev).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "agentdb", about = "AgentDB runtime database (v2)")]
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
    /// `host:port` to bind. Defaults to `127.0.0.1:8080`.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,

    /// Filesystem path that holds the RocksDB-backed runtime state. Created if
    /// it doesn't exist. Ignored when `--in-memory` is set.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Filesystem path that holds artifact content (raw blobs). Created if
    /// it doesn't exist. Defaults to `<data_dir>/content`. Ignored when
    /// `--in-memory` is set.
    #[arg(long)]
    content_dir: Option<PathBuf>,

    /// Use an ephemeral in-memory backend instead of RocksDB. Convenient for
    /// smoke testing; everything is lost on shutdown.
    #[arg(long)]
    in_memory: bool,

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
    let runtime = if opts.in_memory {
        tracing::info!("using in-memory runtime (state lost on shutdown)");
        agentdb_core::Runtime::in_memory()
    } else {
        let kv_path = opts.data_dir.join("runtime");
        std::fs::create_dir_all(&kv_path)?;
        let kv = Arc::new(agentdb_storage::RocksKvStore::open(&kv_path)?)
            as Arc<dyn agentdb_storage::KvStore + Send + Sync>;
        tracing::info!(path = %kv_path.display(), "opened RocksDB runtime backend");

        // Artifact content lives in a separate filesystem tree so the
        // RocksDB metadata directory stays small and operators can mount the
        // content tree on a different disk if they need to. Default is
        // `<data_dir>/content`; `--content-dir` overrides it.
        let content_dir = opts
            .content_dir
            .clone()
            .unwrap_or_else(|| opts.data_dir.join("content"));
        std::fs::create_dir_all(&content_dir)?;
        tracing::info!(path = %content_dir.display(), "opened LocalFs artifact content backend");
        let content: Arc<dyn agentdb_storage::ContentBackend> =
            Arc::new(agentdb_storage::LocalFsContentBackend::new(&content_dir));
        agentdb_core::Runtime::with_kv(kv, content)
    };

    let mut state =
        agentdb_api::AppState::new(runtime).with_proxy_policy(agentdb_api::state::ProxyPolicy {
            timeout_ms: opts.route_proxy_timeout_ms,
            max_response_bytes: opts.route_proxy_max_response_bytes,
        });
    let route_location = opts.route_region.clone().or(opts.route_location.clone());
    if let Some(node_id) = opts.route_node_id.as_deref() {
        let meta = agentdb_meta::MetaStore::open_from_env().await?;
        let snapshot =
            load_route_snapshot(&meta, opts.route_shard_count, route_location.clone()).await?;
        let routing = agentdb_api::routing_http::RouteRuntime::snapshot(node_id, snapshot);
        spawn_route_snapshot_refresher(
            meta,
            routing.clone(),
            opts.route_shard_count,
            route_location.clone(),
            opts.route_refresh_interval_ms,
        );
        state = state.with_routing(routing);
        tracing::info!(
            node_id,
            shard_count = opts.route_shard_count,
            location = ?route_location,
            refresh_interval_ms = opts.route_refresh_interval_ms,
            "enabled metadata-backed route snapshot"
        );
    }
    let app = agentdb_api::router(state).nest("/dashboard", agentdb_dashboard::router());
    let listener = tokio::net::TcpListener::bind(opts.bind).await?;
    tracing::info!(addr = %opts.bind, "agentdb listening");
    axum::serve(listener, app).await?;
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
