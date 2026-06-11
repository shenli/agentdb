use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agentdb_meta::{
    cells, nodes, placement_scopes, tablet_ranges, tablet_replicas, tablets, tenant_policies,
    MetaStore,
};
use agentdb_storage::{GroupedRocksDbTabletStore, TabletStoreLifecycle};
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};

const DEFAULT_INTERNAL_TOKEN: &str = "agentdb-cluster-smoke-token";
const DEFAULT_SHARD_COUNT: i64 = agentdb_meta::shards::DEFAULT_SHARD_COUNT;
const PRIMARY_TABLET_ID: i64 = 1;
const SECONDARY_TABLET_ID: i64 = 2;
const SECONDARY_CELL_ID: &str = "cell-b";
const SECONDARY_PLACEMENT_SCOPE_ID: &str = "cell-b";
const SMOKE_STALE_NODE_THRESHOLD_MS: u64 = 180_000;
const APPEND_RECOVERY_ATTEMPTS: usize = 16;
const DEFAULT_APPEND_VISIBILITY_ATTEMPTS: usize = 50;

#[derive(Debug, Parser)]
#[command(about = "Run a throwaway multi-process AgentDB cluster smoke test")]
struct Args {
    /// Throwaway Postgres URL for AgentDB metadata.
    #[arg(long, env = "AGENTDB_META_URL")]
    meta_url: String,

    /// Path to the agentdb-server binary.
    #[arg(
        long,
        env = "AGENTDB_SERVER_BIN",
        default_value = "target/debug/agentdb-server"
    )]
    server_bin: PathBuf,

    /// Work directory for generated configs, data dirs, and logs.
    #[arg(
        long,
        env = "AGENTDB_CLUSTER_SMOKE_WORK_DIR",
        default_value = "/tmp/agentdb-cluster-smoke"
    )]
    work_dir: PathBuf,

    /// Number of local server processes to launch.
    #[arg(long, default_value_t = 3)]
    nodes: usize,

    /// First HTTP port. Node i listens on base_port + i.
    #[arg(long, default_value_t = 30100)]
    base_port: u16,

    /// S3-compatible endpoint, for example MinIO.
    #[arg(
        long,
        env = "AGENTDB_S3_ENDPOINT",
        default_value = "http://127.0.0.1:9000"
    )]
    s3_endpoint: String,

    /// S3 bucket used by SlateDB/content/snapshot storage.
    #[arg(long, env = "AGENTDB_S3_BUCKET", default_value = "agentdb-smoke")]
    s3_bucket: String,

    /// S3 region used by the object_store client.
    #[arg(long, env = "AGENTDB_S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    /// Shared token for internal Raft and movement RPCs.
    #[arg(long, env = "AGENTDB_INTERNAL_AUTH_TOKEN", default_value = DEFAULT_INTERNAL_TOKEN)]
    internal_auth_token: String,

    /// Delete cluster-control rows from the metadata database before seeding.
    #[arg(long)]
    reset_meta: bool,

    /// Keep server processes alive after the smoke test completes.
    #[arg(long)]
    keep_running: bool,

    /// Keep generated work directory on success.
    #[arg(long)]
    keep_work_dir: bool,

    /// Also kill the initial owner and verify the surviving nodes stay observable.
    #[arg(long)]
    failover: bool,

    /// Run bounded random node kill/restart iterations while verifying writes.
    #[arg(long)]
    chaos: bool,

    /// Run all deterministic hardening probes except random chaos.
    #[arg(long)]
    full_hardening: bool,

    /// Verify idempotent append, gap-free session order, and replica convergence.
    #[arg(long)]
    consistency: bool,

    /// Verify record write/search visibility and record-index watermarks.
    #[arg(long)]
    record_index: bool,

    /// Verify tenant/session watch and durable export changefeed cursor replay.
    #[arg(long)]
    changefeed: bool,

    /// Kill and restart the current owner with the same data directories.
    #[arg(long)]
    restart_persistence: bool,

    /// Exercise explicit tablet replacement and handoff.
    #[arg(long)]
    replacement: bool,

    /// Exercise backup checkpoint creation and offline checkpoint install.
    #[arg(long)]
    checkpoint_replacement: bool,

    /// Exercise split/merge metadata cutover while writes are active before cutover.
    #[arg(long)]
    split_merge: bool,

    /// Kill one AZ worth of nodes, verify quorum writes continue, then restart them.
    #[arg(long)]
    az_failure: bool,

    /// Restart every node one at a time and verify writes after each restart.
    #[arg(long)]
    rolling_restart: bool,

    /// Seed a second cell/tablet and verify independent writes there.
    #[arg(long)]
    two_cell: bool,

    /// Number of chaos kill/restart iterations.
    #[arg(long, default_value_t = 12)]
    chaos_iterations: usize,

    /// Seed for deterministic chaos victim selection. Defaults to current time.
    #[arg(long)]
    chaos_seed: Option<u64>,

    /// Maximum delay inserted between chaos actions.
    #[arg(long, default_value_t = 1500)]
    chaos_max_sleep_ms: u64,
}

#[derive(Debug, Clone)]
struct NodeSpec {
    id: String,
    port: u16,
    url: String,
    cell_id: String,
    availability_zone: String,
    config_path: PathBuf,
    data_dir: PathBuf,
    tablet_store_dir: PathBuf,
    content_dir: PathBuf,
    log_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.nodes < 3 {
        bail!("--nodes must be at least 3 for a quorum smoke test");
    }
    if args.two_cell && args.nodes < 6 {
        bail!("--two-cell requires --nodes 6 or more so each cell has quorum");
    }
    if args.internal_auth_token.len() < 16 {
        bail!("internal auth token must be at least 16 characters");
    }
    if args.full_hardening && args.chaos {
        bail!("--full-hardening and --chaos are separate scenarios; run chaos separately");
    }
    if args.failover && args.chaos {
        bail!(
            "--failover and --chaos are separate scenarios; run them as separate smoke invocations"
        );
    }
    if !args.server_bin.exists() {
        bail!(
            "agentdb-server binary not found at {}; run `cargo build -p agentdb-server` or pass --server-bin",
            args.server_bin.display()
        );
    }

    prepare_work_dir(&args.work_dir, args.reset_meta)?;
    let nodes = node_specs(&args);
    for node in &nodes {
        write_node_config(&args, node)?;
    }

    let meta = MetaStore::open_postgres(&args.meta_url)
        .await
        .context("open metadata store")?;
    if args.reset_meta {
        reset_cluster_meta(&meta).await?;
    }
    ensure_cluster_meta_empty(&meta).await?;
    seed_cluster_meta(&meta, &nodes, args.two_cell).await?;

    let mut children = spawn_nodes(&args, &nodes)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(75))
        .build()
        .context("build HTTP client")?;
    let result = run_smoke(&args, &client, &nodes, &mut children).await;

    if !args.keep_running {
        stop_children(&mut children);
    } else {
        eprintln!(
            "cluster left running; logs are under {}",
            args.work_dir.display()
        );
    }

    result?;
    if !args.keep_work_dir && !args.keep_running {
        let _ = fs::remove_dir_all(&args.work_dir);
    }
    Ok(())
}

fn prepare_work_dir(work_dir: &Path, reset: bool) -> Result<()> {
    if reset && work_dir.exists() {
        fs::remove_dir_all(work_dir).with_context(|| format!("reset {}", work_dir.display()))?;
    }
    fs::create_dir_all(work_dir).with_context(|| format!("create {}", work_dir.display()))?;
    fs::create_dir_all(work_dir.join("logs"))
        .with_context(|| format!("create {}", work_dir.join("logs").display()))?;
    Ok(())
}

fn node_specs(args: &Args) -> Vec<NodeSpec> {
    (0..args.nodes)
        .map(|index| {
            let id = format!("node-{}", (b'a' + u8::try_from(index).unwrap()) as char);
            let port = args.base_port + u16::try_from(index).unwrap();
            NodeSpec {
                config_path: args.work_dir.join(format!("{id}.toml")),
                data_dir: args.work_dir.join(&id).join("runtime"),
                tablet_store_dir: args.work_dir.join(&id).join("tablet-store"),
                content_dir: args.work_dir.join(&id).join("content"),
                log_path: args.work_dir.join("logs").join(format!("{id}.log")),
                url: format!("http://127.0.0.1:{port}"),
                cell_id: if args.two_cell && index >= 3 {
                    SECONDARY_CELL_ID.into()
                } else {
                    cells::DEFAULT_CELL_ID.into()
                },
                availability_zone: local_availability_zone(index),
                id,
                port,
            }
        })
        .collect()
}

fn local_availability_zone(index: usize) -> String {
    format!(
        "local-{}",
        (b'a' + u8::try_from(index % 3).unwrap()) as char
    )
}

fn write_node_config(args: &Args, node: &NodeSpec) -> Result<()> {
    fs::create_dir_all(&node.data_dir)?;
    fs::create_dir_all(&node.tablet_store_dir)?;
    fs::create_dir_all(&node.content_dir)?;
    let text = format!(
        r#"[deployment]
mode = "cluster"

[server]
bind = "127.0.0.1:{port}"
internal_auth_token = "{internal_auth_token}"

[storage]
data_dir = "{data_dir}"
content_dir = "{content_dir}"

[storage.kv]
backend = "slatedb"

[storage.tablet]
layout = "grouped_rocksdb"
root_dir = "{tablet_store_dir}"
tablets_per_domain = 1
max_open_domains = 4
domain_write_buffer_size_mib = 4
domain_max_write_buffers = 2
domain_parallelism = 1
domain_max_background_jobs = 1
domain_max_open_files = 64

[storage.content]
backend = "object_store"

[storage.object_store]
backend = "s3"
bucket = "{bucket}"
region = "{region}"
endpoint = "{endpoint}"
allow_http = true

[routing]
node_id = "{node_id}"
shard_count = {shard_count}
default_tablet_id = {default_tablet_id}
cell_id = "{cell_id}"
region = "local"
availability_zone = "{availability_zone}"
capacity_class = "stable"
refresh_interval_ms = 500
proxy_timeout_ms = 70000
"#,
        port = node.port,
        internal_auth_token = args.internal_auth_token,
        data_dir = node.data_dir.display(),
        tablet_store_dir = node.tablet_store_dir.display(),
        content_dir = node.content_dir.display(),
        bucket = args.s3_bucket,
        region = args.s3_region,
        endpoint = args.s3_endpoint,
        node_id = node.id,
        default_tablet_id = if node.cell_id == SECONDARY_CELL_ID {
            SECONDARY_TABLET_ID
        } else {
            PRIMARY_TABLET_ID
        },
        cell_id = node.cell_id,
        availability_zone = node.availability_zone,
        shard_count = DEFAULT_SHARD_COUNT,
    );
    fs::write(&node.config_path, text)?;
    Ok(())
}

async fn reset_cluster_meta(meta: &MetaStore) -> Result<()> {
    let pool = meta.pool();
    for table in [
        "tablet_snapshots",
        "tablet_stats",
        "tablet_movements",
        "tablet_replicas",
        "tablet_ranges",
        "session_overrides",
        "shards",
        "tablets",
        "nodes",
        "scheduler_leader",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(pool)
            .await
            .with_context(|| format!("clear {table}"))?;
    }
    Ok(())
}

async fn ensure_cluster_meta_empty(meta: &MetaStore) -> Result<()> {
    let pool = meta.pool();
    let (node_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM nodes")
        .fetch_one(pool)
        .await?;
    let (tablet_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tablets")
        .fetch_one(pool)
        .await?;
    let (replica_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tablet_replicas")
        .fetch_one(pool)
        .await?;
    if node_count != 0 || tablet_count != 0 || replica_count != 0 {
        bail!(
            "metadata store is not empty (nodes={node_count}, tablets={tablet_count}, replicas={replica_count}); use --reset-meta only with a throwaway database"
        );
    }
    Ok(())
}

async fn seed_cluster_meta(
    meta: &MetaStore,
    node_specs: &[NodeSpec],
    two_cell: bool,
) -> Result<()> {
    let pool = meta.pool();
    let now_ms = now_ms();
    cells::upsert(
        pool,
        cells::DEFAULT_CELL_ID,
        Some("local"),
        cells::DEFAULT_DOMAIN_CLASS,
        cells::DEFAULT_DEPLOYMENT_PROFILE,
        cells::DEFAULT_CAPACITY_CLASS,
        cells::DEFAULT_METADATA_ISOLATION_LEVEL,
        "serving",
        Some("default"),
        Some("default"),
        0,
        now_ms,
    )
    .await?;
    let default_nodes: Vec<_> = node_specs
        .iter()
        .filter(|node| node.cell_id == cells::DEFAULT_CELL_ID)
        .collect();
    seed_tablet_meta(
        pool,
        PRIMARY_TABLET_ID,
        placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID,
        &default_nodes,
        now_ms,
    )
    .await?;
    if two_cell {
        cells::upsert(
            pool,
            SECONDARY_CELL_ID,
            Some("local"),
            cells::DEFAULT_DOMAIN_CLASS,
            cells::DEFAULT_DEPLOYMENT_PROFILE,
            cells::DEFAULT_CAPACITY_CLASS,
            cells::DEFAULT_METADATA_ISOLATION_LEVEL,
            "serving",
            Some("secondary"),
            Some("secondary"),
            0,
            now_ms,
        )
        .await?;
        placement_scopes::upsert_with_cell(
            pool,
            SECONDARY_PLACEMENT_SCOPE_ID,
            "shared_pool",
            DEFAULT_SHARD_COUNT,
            None,
            "serving",
            SECONDARY_CELL_ID,
            now_ms,
        )
        .await?;
        let secondary_nodes: Vec<_> = node_specs
            .iter()
            .filter(|node| node.cell_id == SECONDARY_CELL_ID)
            .collect();
        seed_tablet_meta(
            pool,
            SECONDARY_TABLET_ID,
            SECONDARY_PLACEMENT_SCOPE_ID,
            &secondary_nodes,
            now_ms,
        )
        .await?;
    }
    for node in node_specs {
        nodes::upsert_with_cell(
            pool,
            &node.id,
            &node.url,
            None,
            "alive",
            now_ms,
            Some(&node.availability_zone),
            Some("local"),
            &node.cell_id,
            cells::DEFAULT_CAPACITY_CLASS,
        )
        .await?;
    }
    Ok(())
}

async fn seed_tablet_meta(
    pool: &sqlx::AnyPool,
    tablet_id: i64,
    placement_scope_id: &str,
    node_specs: &[&NodeSpec],
    now_ms: i64,
) -> Result<()> {
    if node_specs.len() < 3 {
        bail!("tablet {tablet_id} requires at least three nodes for smoke quorum");
    }
    tablets::create_with_placement(
        pool,
        tablet_id,
        placement_scope_id,
        "serving",
        Some(&format!("tablet-{tablet_id}")),
        0,
        now_ms,
    )
    .await?;
    tablets::assign_to(pool, tablet_id, &node_specs[0].id).await?;
    let tablet = tablets::get(pool, tablet_id)
        .await?
        .ok_or_else(|| anyhow!("seeded tablet missing"))?;
    tablets::assume_ownership(
        pool,
        tablet_id,
        tablet.epoch,
        &node_specs[0].id,
        now_ms + 300_000,
    )
    .await?;
    tablet_ranges::upsert(
        pool,
        placement_scope_id,
        0,
        DEFAULT_SHARD_COUNT - 1,
        tablet_id,
        1,
        now_ms,
    )
    .await?;
    for node in node_specs {
        tablet_replicas::upsert(pool, tablet_id, &node.id, "voter", "healthy", 0, 0, now_ms)
            .await?;
    }
    Ok(())
}

fn spawn_nodes(args: &Args, nodes: &[NodeSpec]) -> Result<Vec<Child>> {
    let mut children = Vec::new();
    for node in nodes {
        children.push(spawn_node(args, node)?);
    }
    Ok(children)
}

fn spawn_node(args: &Args, node: &NodeSpec) -> Result<Child> {
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&node.log_path)?;
    let err = log.try_clone()?;
    Command::new(&args.server_bin)
        .arg("serve")
        .arg("--config")
        .arg(&node.config_path)
        .env("AGENTDB_META_BACKEND", "postgres")
        .env("AGENTDB_META_URL", &args.meta_url)
        .env("AGENTDB_META_ANY_POOL_MAX_CONNECTIONS", "64")
        .env("AGENTDB_META_PG_POOL_MAX_CONNECTIONS", "32")
        .env("AGENTDB_NODE_HEARTBEAT_MS", "500")
        .env("AGENTDB_SCHEDULER_RENEW_INTERVAL_MS", "500")
        // Lease + reaper margin raised to absorb the 60s meta-query stalls
        // observed under SQL contention on compose Postgres (see
        // docs/stability-findings-2026-05.md F_postgres_pool_starvation /
        // F_LEUSS). Renewal cadence stays at 500ms so genuine ownership
        // changes still propagate quickly; only the lease/expiry window
        // grows to tolerate transient meta slowness.
        .env("AGENTDB_SCHEDULER_LEASE_MS", "120000")
        .env(
            "AGENTDB_STALE_NODE_THRESHOLD_MS",
            SMOKE_STALE_NODE_THRESHOLD_MS.to_string(),
        )
        .env("AGENTDB_REAPER_TICK_MS", "500")
        .env("AGENTDB_REAPER_SAFETY_MARGIN_MS", "60000")
        .env("AGENTDB_REBALANCE_MAX_MOVEMENTS", "0")
        .env("AGENTDB_TABLET_MOVEMENT_BATCH_SIZE", "0")
        .env("AGENTDB_TABLET_LIFECYCLE_MAX_ACTIONS", "0")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env(
            "RUST_LOG",
            env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        )
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .with_context(|| format!("spawn {}", node.id))
}

async fn run_smoke(
    args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    children: &mut [Child],
) -> Result<()> {
    eprintln!("smoke: waiting for node health");
    for node in nodes {
        wait_json(
            client,
            &node.url,
            "/api/admin/cluster/health",
            Duration::from_secs(90),
        )
        .await?;
    }
    assert_cell_and_node_labels(client, nodes).await?;
    eprintln!("smoke: waiting for raft readiness");
    let raft_ready_timeout = if args.two_cell {
        Duration::from_secs(180)
    } else {
        Duration::from_secs(90)
    };
    wait_raft_ready(client, nodes, raft_ready_timeout).await?;

    eprintln!("smoke: resolving initial tablet writer");
    let owner = wait_tablet_owner(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        None,
        Duration::from_secs(90),
    )
    .await?;
    eprintln!("smoke: initial tablet writer {}", owner.id);
    eprintln!("smoke: creating tenant/agent/session");
    let (tenant_id, agent_id, session_id) =
        create_session_smoke(client, nodes, PRIMARY_TABLET_ID, None).await?;
    eprintln!("smoke: appending event 0");
    append_and_verify(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        &tenant_id,
        &agent_id,
        &session_id,
        0,
    )
    .await?;
    eprintln!("smoke: appending event 1");
    append_and_verify(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        &tenant_id,
        &agent_id,
        &session_id,
        1,
    )
    .await?;
    let mut next_seq = 2;
    let mut secondary = if args.two_cell {
        eprintln!("smoke: creating tenant/agent/session in {SECONDARY_CELL_ID}");
        let session = create_session_smoke(
            client,
            nodes,
            SECONDARY_TABLET_ID,
            Some(TenantPlacement {
                cell_id: SECONDARY_CELL_ID,
                placement_scope_id: SECONDARY_PLACEMENT_SCOPE_ID,
            }),
        )
        .await?;
        eprintln!("smoke: appending secondary cell event 0");
        append_and_verify(
            client,
            nodes,
            SECONDARY_TABLET_ID,
            &session.0,
            &session.1,
            &session.2,
            0,
        )
        .await?;
        eprintln!("smoke: appending secondary cell event 1");
        append_and_verify(
            client,
            nodes,
            SECONDARY_TABLET_ID,
            &session.0,
            &session.1,
            &session.2,
            1,
        )
        .await?;
        Some((session.0, session.1, session.2, 2_u64))
    } else {
        None
    };

    if args.full_hardening || args.consistency {
        run_data_consistency_probe(
            client,
            nodes,
            PRIMARY_TABLET_ID,
            &tenant_id,
            &agent_id,
            &session_id,
            &mut next_seq,
        )
        .await?;
        if let Some((tenant_id, agent_id, session_id, next_secondary_seq)) = secondary.as_mut() {
            run_data_consistency_probe(
                client,
                nodes,
                SECONDARY_TABLET_ID,
                tenant_id,
                agent_id,
                session_id,
                next_secondary_seq,
            )
            .await?;
        }
    }

    if args.full_hardening || args.record_index {
        run_record_index_probe(client, nodes, PRIMARY_TABLET_ID, &tenant_id, &session_id).await?;
        if let Some((tenant_id, _agent_id, session_id, _next_secondary_seq)) = secondary.as_ref() {
            run_record_index_probe(client, nodes, SECONDARY_TABLET_ID, tenant_id, session_id)
                .await?;
        }
    }

    if args.full_hardening || args.changefeed {
        run_changefeed_probe(
            client,
            nodes,
            PRIMARY_TABLET_ID,
            &tenant_id,
            &agent_id,
            &session_id,
            &mut next_seq,
        )
        .await?;
        if let Some((tenant_id, agent_id, session_id, next_secondary_seq)) = secondary.as_mut() {
            run_changefeed_probe(
                client,
                nodes,
                SECONDARY_TABLET_ID,
                tenant_id,
                agent_id,
                session_id,
                next_secondary_seq,
            )
            .await?;
        }
    }

    eprintln!("smoke: forcing raft catch-up snapshot after setup writes");
    let catch_up_started = Instant::now();
    let catch_up = force_raft_catch_up(client, nodes, PRIMARY_TABLET_ID).await?;
    let secondary_catch_up = if args.two_cell {
        Some(force_raft_catch_up(client, nodes, SECONDARY_TABLET_ID).await?)
    } else {
        None
    };
    eprintln!(
        "smoke: raft catch-up request completed in {:.3}s attempted={} sent={} skipped={}",
        catch_up_started.elapsed().as_secs_f64(),
        text_u64(&catch_up["attempted"]),
        text_u64(&catch_up["sent"]),
        text_u64(&catch_up["skipped"])
    );
    if let Some(catch_up) = secondary_catch_up {
        eprintln!(
            "smoke: secondary raft catch-up attempted={} sent={} skipped={}",
            text_u64(&catch_up["attempted"]),
            text_u64(&catch_up["sent"]),
            text_u64(&catch_up["skipped"])
        );
    }
    eprintln!("smoke: waiting for raft convergence after setup writes");
    let convergence_started = Instant::now();
    wait_raft_ready(client, nodes, Duration::from_secs(90)).await?;
    eprintln!(
        "smoke: raft convergence after catch-up completed in {:.3}s",
        convergence_started.elapsed().as_secs_f64()
    );
    print_diagnostics(client, nodes).await?;

    if args.full_hardening || args.restart_persistence {
        run_restart_persistence(
            args,
            client,
            nodes,
            children,
            &tenant_id,
            &agent_id,
            &session_id,
            &mut next_seq,
        )
        .await?;
    }

    if args.full_hardening || args.checkpoint_replacement {
        run_checkpoint_install(args, client, nodes, PRIMARY_TABLET_ID).await?;
    }

    if args.full_hardening || args.az_failure {
        run_az_failure(
            args,
            client,
            nodes,
            children,
            &tenant_id,
            &agent_id,
            &session_id,
            &mut next_seq,
            secondary.as_mut(),
        )
        .await?;
    }

    if args.full_hardening || args.rolling_restart {
        run_rolling_restart(
            args,
            client,
            nodes,
            children,
            &tenant_id,
            &agent_id,
            &session_id,
            &mut next_seq,
        )
        .await?;
    }

    if args.failover {
        eprintln!("smoke: killing initial writer {}", owner.id);
        let owner_index = nodes
            .iter()
            .position(|node| node.id == owner.id)
            .ok_or_else(|| anyhow!("owner {} is not in smoke node list", owner.id))?;
        children[owner_index].kill().ok();
        children[owner_index].wait().ok();
        let survivor = nodes
            .iter()
            .find(|node| node.id != owner.id)
            .ok_or_else(|| anyhow!("no surviving node after killing owner {}", owner.id))?;
        eprintln!("smoke: waiting for tablet owner change");
        wait_tablet_owner_changed(
            client,
            &survivor.url,
            PRIMARY_TABLET_ID,
            &owner.id,
            Duration::from_secs(45),
        )
        .await?;
        eprintln!("smoke: waiting for raft observability after failover");
        wait_raft_observable(client, &survivor.url, Duration::from_secs(20)).await?;
        eprintln!("smoke: waiting for new tablet writer");
        wait_tablet_owner(
            client,
            nodes,
            PRIMARY_TABLET_ID,
            Some(&owner.id),
            Duration::from_secs(90),
        )
        .await?;
        eprintln!("smoke: appending event {next_seq} after failover");
        append_and_verify(
            client,
            nodes,
            PRIMARY_TABLET_ID,
            &tenant_id,
            &agent_id,
            &session_id,
            next_seq,
        )
        .await?;
        next_seq += 1;
        if let Some((tenant_id, agent_id, session_id, next_secondary_seq)) = secondary.as_mut() {
            eprintln!(
                "smoke: appending secondary cell event {next_secondary_seq} after primary failover"
            );
            append_and_verify(
                client,
                nodes,
                SECONDARY_TABLET_ID,
                tenant_id,
                agent_id,
                session_id,
                *next_secondary_seq,
            )
            .await?;
            *next_secondary_seq += 1;
        }
        eprintln!("smoke: waiting for {} to be marked dead", owner.id);
        wait_node_marked_dead(client, &survivor.url, &owner.id, stale_node_wait_timeout()).await?;
        let survivors: Vec<_> = nodes
            .iter()
            .filter(|node| node.id != owner.id)
            .cloned()
            .collect();
        print_diagnostics(client, &survivors).await?;
    }

    if args.chaos {
        eprintln!("chaos: waiting for post-setup raft convergence");
        wait_raft_ready(client, nodes, Duration::from_secs(90)).await?;
        run_chaos(
            args,
            client,
            nodes,
            children,
            ChaosSession {
                tenant_id: &tenant_id,
                agent_id: &agent_id,
                session_id: &session_id,
                next_seq,
            },
        )
        .await?;
    }

    let mut active_nodes = nodes.to_vec();
    if args.full_hardening || args.replacement {
        active_nodes = run_explicit_replacement(
            args,
            client,
            nodes,
            children,
            &tenant_id,
            &agent_id,
            &session_id,
            &mut next_seq,
        )
        .await?;
    }

    if args.full_hardening || args.split_merge {
        run_split_merge_lifecycle(
            client,
            &active_nodes,
            &tenant_id,
            &agent_id,
            &session_id,
            &mut next_seq,
        )
        .await?;
    }

    Ok(())
}

async fn run_data_consistency_probe(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    next_seq: &mut u64,
) -> Result<()> {
    eprintln!("consistency: verifying idempotent append and gap-free session order");
    let expected_seq = *next_seq;
    let idempotency_key = format!("cluster-smoke-consistency-{session_id}-seq-{expected_seq}");
    let path = format!("/api/tenants/{tenant_id}/sessions/{session_id}/events");
    let body = json!({
        "agent_id": agent_id,
        "kind": "message",
        "payload": {"seq": expected_seq, "source": "cluster-smoke-consistency"},
        "artifact_refs": [],
        "source_refs": []
    });
    let first = post_json_with_idempotency_key_retry(
        client,
        nodes,
        tablet_id,
        &path,
        body.clone(),
        &idempotency_key,
        expected_seq,
    )
    .await?;
    let replay = post_json_with_idempotency_key_retry(
        client,
        nodes,
        tablet_id,
        &path,
        body.clone(),
        &idempotency_key,
        expected_seq,
    )
    .await?;
    let first_seq = first
        .get("seq")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("idempotent append response missing seq: {first}"))?;
    let replay_seq = replay
        .get("seq")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("idempotent append replay missing seq: {replay}"))?;
    if first_seq != expected_seq || replay_seq != expected_seq {
        bail!(
            "idempotent append expected seq {expected_seq}, got first={first_seq} replay={replay_seq}"
        );
    }
    if first["event_id"] != replay["event_id"] {
        bail!(
            "idempotent append replay returned a different event id: first={first} replay={replay}"
        );
    }
    expect_idempotency_conflict_retry(
        client,
        nodes,
        tablet_id,
        &path,
        json!({
            "agent_id": agent_id,
            "kind": "message",
            "payload": {"seq": expected_seq + 10_000, "source": "cluster-smoke-consistency-conflict"},
            "artifact_refs": [],
            "source_refs": []
        }),
        &idempotency_key,
    )
    .await?;
    *next_seq += 1;

    let _ = force_raft_catch_up(client, nodes, tablet_id).await?;
    wait_raft_ready(client, nodes, Duration::from_secs(120)).await?;
    assert_session_converged_on_all_nodes(client, nodes, tenant_id, session_id, *next_seq).await?;
    Ok(())
}

async fn run_record_index_probe(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    session_id: &str,
) -> Result<()> {
    eprintln!("record-index: writing records and verifying local/global search visibility");
    let token = format!("stability-index-{}", now_ms());
    let (target, commit_seq) = create_record_with_commit_retry(
        client,
        nodes,
        tablet_id,
        tenant_id,
        json!({
            "namespace": ["cluster-smoke", "stability"],
            "text": format!("{token} primary searchable text"),
            "tags": ["stability", "primary"],
            "metadata": {
                "title": token,
                "probe": "record-index",
                "component": "runtime",
                "status": "passed",
                "confidence": 0.92,
                "source": {"commit": "cluster-smoke"}
            },
            "policy": {"mode": "selected_fields", "fields": ["title"], "vector_allowed": false}
        }),
    )
    .await?;
    let target_id = string_field(&target, "record_id")?;
    let (control, control_commit_seq) = create_record_with_commit_retry(
        client,
        nodes,
        tablet_id,
        tenant_id,
        json!({
            "namespace": ["cluster-smoke", "stability"],
            "text": format!("{token} control searchable text"),
            "tags": ["stability", "control"],
            "metadata": {
                "title": format!("{token} control"),
                "probe": "record-index",
                "component": "runtime",
                "status": "failed",
                "confidence": 0.10,
                "source": {"commit": "cluster-smoke"}
            },
            "policy": {"mode": "selected_fields", "fields": ["title"], "vector_allowed": false}
        }),
    )
    .await?;
    let control_id = string_field(&control, "record_id")?;
    let (source_child, source_commit_seq) = create_record_with_commit_retry(
        client,
        nodes,
        tablet_id,
        tenant_id,
        json!({
            "namespace": ["cluster-smoke", "source"],
            "text": format!("{token} child source searchable text"),
            "tags": ["stability", "source-child"],
            "source": {"record_id": target_id.clone()},
            "metadata": {"title": format!("{token} child"), "probe": "record-index-source"},
            "policy": {"mode": "selected_fields", "fields": ["title"], "vector_allowed": false}
        }),
    )
    .await?;
    let source_child_id = string_field(&source_child, "record_id")?;
    let (vector_east, vector_east_commit_seq) = create_record_with_commit_retry(
        client,
        nodes,
        tablet_id,
        tenant_id,
        json!({
            "namespace": ["cluster-smoke", "vectors"],
            "text": "east-vector",
            "embedding": [1.0, 0.0, 0.0],
            "tags": ["stability", "vector"],
            "metadata": {"title": format!("{token} east vector")},
            "policy": {"mode": "metadata_only", "fields": [], "vector_allowed": true}
        }),
    )
    .await?;
    let (vector_north, vector_north_commit_seq) = create_record_with_commit_retry(
        client,
        nodes,
        tablet_id,
        tenant_id,
        json!({
            "namespace": ["cluster-smoke", "vectors"],
            "text": "north-vector",
            "embedding": [0.0, 1.0, 0.0],
            "tags": ["stability", "vector"],
            "metadata": {"title": format!("{token} north vector")},
            "policy": {"mode": "metadata_only", "fields": [], "vector_allowed": true}
        }),
    )
    .await?;
    let _vector_east_id = string_field(&vector_east, "record_id")?;
    let _vector_north_id = string_field(&vector_north, "record_id")?;
    let max_commit_seq = [
        commit_seq,
        control_commit_seq,
        source_commit_seq,
        vector_east_commit_seq,
        vector_north_commit_seq,
    ]
    .into_iter()
    .max()
    .unwrap_or(commit_seq);

    let owner = wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
    let global_path = format!(
        "/api/tenants/{tenant_id}/records/search?text={}&namespace=cluster-smoke&namespace=stability&tag=primary&min_visible_record_commit_seq={commit_seq}",
        query_encode(&token)
    );
    wait_search_finds_record(
        client,
        nodes,
        tablet_id,
        &global_path,
        &target_id,
        Duration::from_secs(30),
    )
    .await?;

    let metadata_filter_response = post_json(
        client,
        &owner.url,
        &format!("/api/tenants/{tenant_id}/records/search"),
        json!({
            "namespace": ["cluster-smoke", "stability"],
            "text": token,
            "metadata_filter": {
                "all": [
                    {"path": "probe", "op": "eq", "value": "record-index"},
                    {"path": "component", "op": "in", "value": ["runtime", "wire"]},
                    {"path": "confidence", "op": "gte", "value": 0.9},
                    {"path": "source.commit", "op": "exists", "value": true}
                ],
                "none": [
                    {"path": "status", "op": "eq", "value": "failed"}
                ]
            },
            "limit": 5,
            "min_visible_record_commit_seq": commit_seq
        }),
    )
    .await?;
    assert_record_search_match(
        &metadata_filter_response,
        &target_id,
        "metadata-filter search",
    )?;
    assert_record_search_missing(
        &metadata_filter_response,
        &control_id,
        "metadata-filter search",
    )?;
    assert_ranked_scores(&metadata_filter_response, "metadata-filter search")?;

    let local_path = format!(
        "/api/tenants/{tenant_id}/records/search?text={}&namespace=cluster-smoke&namespace=stability&tag=primary&sharding_key={}&min_visible_record_commit_seq={commit_seq}",
        query_encode(&token),
        query_encode(session_id)
    );
    wait_search_finds_record(
        client,
        nodes,
        tablet_id,
        &local_path,
        &target_id,
        Duration::from_secs(30),
    )
    .await?;

    let control_path = format!(
        "/api/tenants/{tenant_id}/records/search?text={}&namespace=cluster-smoke&namespace=stability&tag=control&min_visible_record_commit_seq={commit_seq}",
        query_encode(&token)
    );
    wait_search_finds_record(
        client,
        nodes,
        tablet_id,
        &control_path,
        &control_id,
        Duration::from_secs(30),
    )
    .await?;

    let source_path = format!(
        "/api/tenants/{tenant_id}/records/search?text={}&namespace=cluster-smoke&namespace=source&source_record={}&min_visible_record_commit_seq={source_commit_seq}",
        query_encode(&token),
        query_encode(&target_id)
    );
    wait_search_finds_record(
        client,
        nodes,
        tablet_id,
        &source_path,
        &source_child_id,
        Duration::from_secs(30),
    )
    .await?;

    let vector_response = post_json(
        client,
        &owner.url,
        &format!("/api/tenants/{tenant_id}/records/search"),
        json!({
            "namespace": ["cluster-smoke", "vectors"],
            "embedding": [0.0, 1.0, 0.0],
            "limit": 5,
            "min_visible_record_commit_seq": max_commit_seq
        }),
    )
    .await?;
    let vector_hits = record_search_matches(&vector_response, "vector search")?;
    if vector_hits.len() < 2
        || vector_hits[0]["record"]["text"].as_str() != Some("north-vector")
        || !vector_hits
            .iter()
            .any(|matched| matched["record"]["text"].as_str() == Some("east-vector"))
    {
        bail!("vector search expected north first and east in top results: {vector_response}");
    }
    assert_ranked_scores(&vector_response, "vector search")?;
    if vector_hits[0]["components"]["vector"].as_f64().is_none() {
        bail!("vector search first match missing vector score component: {vector_response}");
    }

    delete_record_retry(client, nodes, tablet_id, tenant_id, &control_id).await?;
    wait_search_missing_record(
        client,
        nodes,
        tablet_id,
        &control_path,
        &control_id,
        Duration::from_secs(30),
    )
    .await?;
    wait_record_index_watermark(client, &owner.url, max_commit_seq, Duration::from_secs(60))
        .await?;
    Ok(())
}

async fn run_changefeed_probe(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    next_seq: &mut u64,
) -> Result<()> {
    eprintln!("changefeed: verifying watch and export cursor replay");
    let owner = wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
    let before_watch = watch_session(client, &owner.url, tenant_id, session_id).await?;
    let watch_cursor = before_watch
        .as_array()
        .and_then(|items| items.last())
        .and_then(|item| item["cursor"].as_str())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("changefeed setup watch response missing cursor: {before_watch}"))?;
    let before_tenant_watch = watch_tenant(client, &owner.url, tenant_id).await?;
    let tenant_watch_cursor = before_tenant_watch
        .as_array()
        .and_then(|items| items.last())
        .and_then(|item| item["cursor"].as_str())
        .map(ToString::to_string)
        .ok_or_else(|| {
            anyhow!("changefeed setup tenant watch response missing cursor: {before_tenant_watch}")
        })?;
    let before_export =
        export_watch_session(client, &owner.url, tenant_id, session_id, None, 100).await?;
    let export_cursor = before_export["items"]
        .as_array()
        .and_then(|items| items.last())
        .and_then(|item| item["cursor"].as_str())
        .map(ToString::to_string);

    let first_seq = *next_seq;
    append_and_verify(
        client, nodes, tablet_id, tenant_id, agent_id, session_id, first_seq,
    )
    .await?;
    *next_seq += 1;
    let second_seq = *next_seq;
    append_and_verify(
        client, nodes, tablet_id, tenant_id, agent_id, session_id, second_seq,
    )
    .await?;
    *next_seq += 1;

    let second_session_id = create_session_for_agent(client, nodes, tablet_id, tenant_id, agent_id)
        .await
        .context("create second session for tenant changefeed fanout")?;
    append_and_verify(
        client,
        nodes,
        tablet_id,
        tenant_id,
        agent_id,
        &second_session_id,
        0,
    )
    .await?;

    let first_page = watch_session_from_cursor_limit(
        client,
        &owner.url,
        tenant_id,
        session_id,
        &watch_cursor,
        1,
    )
    .await?;
    let first_items = first_page
        .as_array()
        .ok_or_else(|| anyhow!("watch page was not an array: {first_page}"))?;
    if first_items.len() != 1 || watch_item_seq(&first_items[0]) != Some(first_seq) {
        bail!("watch first page expected seq {first_seq}: {first_page}");
    }
    let next_watch_cursor = first_items[0]["cursor"]
        .as_str()
        .ok_or_else(|| anyhow!("watch first page missing cursor: {first_page}"))?;
    let second_page = watch_session_from_cursor_limit(
        client,
        &owner.url,
        tenant_id,
        session_id,
        next_watch_cursor,
        10,
    )
    .await?;
    let second_items = second_page
        .as_array()
        .ok_or_else(|| anyhow!("watch second page was not an array: {second_page}"))?;
    if !second_items
        .iter()
        .any(|item| watch_item_seq(item) == Some(second_seq))
    {
        bail!("watch replay did not include seq {second_seq}: {second_page}");
    }

    let tenant_page =
        watch_tenant_from_cursor_limit(client, &owner.url, tenant_id, &tenant_watch_cursor, 10)
            .await?;
    let tenant_items = tenant_page
        .as_array()
        .ok_or_else(|| anyhow!("tenant watch page was not an array: {tenant_page}"))?;
    for expected_session in [session_id, second_session_id.as_str()] {
        if !tenant_items
            .iter()
            .any(|item| watch_item_session_id(item) == Some(expected_session))
        {
            bail!("tenant watch did not include session {expected_session}: {tenant_page}");
        }
    }

    let export_page = export_watch_session(
        client,
        &owner.url,
        tenant_id,
        session_id,
        export_cursor.as_deref(),
        10,
    )
    .await?;
    let export_items = export_page["items"]
        .as_array()
        .ok_or_else(|| anyhow!("export watch response missing items: {export_page}"))?;
    for expected in [first_seq, second_seq] {
        let Some(item) = export_items
            .iter()
            .find(|item| item["seq"].as_u64() == Some(expected))
        else {
            bail!("export watch did not include seq {expected}: {export_page}");
        };
        if item["object_type"] != "event" || item["op"] != "insert" {
            bail!("export watch item for seq {expected} had unexpected shape: {item}");
        }
        let cursor = item["cursor"]
            .as_str()
            .ok_or_else(|| anyhow!("export watch item missing cursor: {item}"))?;
        if !cursor.starts_with("ledger:") {
            bail!("export watch cursor should use durable ledger format: {cursor}");
        }
    }
    Ok(())
}

async fn run_restart_persistence(
    args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    children: &mut [Child],
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    next_seq: &mut u64,
) -> Result<()> {
    let _ = force_raft_catch_up(client, nodes, PRIMARY_TABLET_ID).await?;
    wait_raft_ready(client, nodes, Duration::from_secs(120)).await?;
    let owner = wait_tablet_owner(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        None,
        Duration::from_secs(90),
    )
    .await?;
    let owner_index = node_index(nodes, &owner.id)?;
    eprintln!(
        "restart-persistence: restarting owner {} with existing data dirs",
        owner.id
    );
    restart_node(args, client, nodes, children, owner_index).await?;
    wait_raft_ready(client, nodes, Duration::from_secs(120)).await?;
    append_and_verify(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        tenant_id,
        agent_id,
        session_id,
        *next_seq,
    )
    .await?;
    *next_seq += 1;
    let _ = force_raft_catch_up(client, nodes, PRIMARY_TABLET_ID).await?;
    wait_raft_ready(client, nodes, Duration::from_secs(120)).await?;
    Ok(())
}

async fn run_checkpoint_install(
    args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
) -> Result<()> {
    let owner = wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
    eprintln!(
        "checkpoint-replacement: creating tablet-store checkpoint via {}",
        owner.id
    );
    let backup_id = format!("cluster-smoke-checkpoint-{}", now_ms());
    let response = post_json(
        client,
        &owner.url,
        "/api/admin/backups",
        json!({
            "backup_id": backup_id,
            "directory": args.work_dir.join("backups").display().to_string(),
            "include_meta": false,
            "include_runtime_summary": false,
            "include_tablet_checkpoints": true
        }),
    )
    .await?;
    let manifest_path = string_field(&response, "manifest_path")?;
    let manifest_text = fs::read_to_string(&manifest_path)
        .with_context(|| format!("read backup manifest {manifest_path}"))?;
    let manifest: Value = serde_json::from_str(&manifest_text)?;
    let checkpoint = manifest["tablet_store_checkpoints"]
        .as_array()
        .and_then(|checkpoints| {
            checkpoints
                .iter()
                .find(|checkpoint| checkpoint["tablet_id"].as_i64() == Some(tablet_id))
        })
        .ok_or_else(|| anyhow!("backup manifest did not include tablet {tablet_id}: {manifest}"))?;
    let checkpoint_dir = checkpoint["path"]
        .as_str()
        .ok_or_else(|| anyhow!("tablet checkpoint is missing path: {checkpoint}"))?;
    let restore_root = args
        .work_dir
        .join("checkpoint-install")
        .join(format!("tablet-{tablet_id}-{}", now_ms()));
    let lifecycle = GroupedRocksDbTabletStore::new(restore_root.clone(), 1, 1);
    let restored = lifecycle
        .restore_tablet_checkpoint(tablet_id, Path::new(checkpoint_dir))
        .with_context(|| {
            format!(
                "install checkpoint {checkpoint_dir} into {}",
                restore_root.display()
            )
        })?;
    if restored.tablet_id != tablet_id {
        bail!(
            "checkpoint install restored tablet {}, expected {tablet_id}",
            restored.tablet_id
        );
    }
    eprintln!(
        "checkpoint-replacement: installed tablet {} checkpoint size={} checksum={}",
        restored.tablet_id,
        restored.size_bytes.unwrap_or_default(),
        restored.checksum_sha256.as_deref().unwrap_or("<none>")
    );
    Ok(())
}

async fn run_explicit_replacement(
    _args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    children: &mut [Child],
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    next_seq: &mut u64,
) -> Result<Vec<NodeSpec>> {
    let owner = wait_tablet_owner(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        None,
        Duration::from_secs(90),
    )
    .await?;
    let target = nodes
        .iter()
        .find(|node| node.cell_id == owner.cell_id && node.id != owner.id)
        .ok_or_else(|| anyhow!("no same-cell replacement target for owner {}", owner.id))?;
    eprintln!(
        "replacement: preparing tablet {} handoff from {} to {}",
        PRIMARY_TABLET_ID, owner.id, target.id
    );
    let prepare = post_json(
        client,
        &owner.url,
        &format!("/api/admin/tablets/{PRIMARY_TABLET_ID}/replacement/prepare"),
        json!({
            "target_node_id": target.id,
            "source_node_id": owner.id,
            "stale_threshold_ms": SMOKE_STALE_NODE_THRESHOLD_MS
        }),
    )
    .await?;
    if prepare["action"] != "prepare_replacement" {
        bail!("unexpected replacement prepare response: {prepare}");
    }
    let required_applied_index =
        raft_observed_applied_index(client, &owner.url, PRIMARY_TABLET_ID, &owner.id).await?;
    eprintln!(
        "replacement: completing tablet {} handoff to {} at applied_index {}",
        PRIMARY_TABLET_ID, target.id, required_applied_index
    );
    let mut complete_error = None;
    let mut complete = None;
    for attempt in 0..APPEND_RECOVERY_ATTEMPTS {
        match post_json(
            client,
            &owner.url,
            &format!("/api/admin/tablets/{PRIMARY_TABLET_ID}/replacement/complete"),
            json!({
                "target_node_id": target.id,
                "source_node_id": owner.id,
                "require_healthy": true,
                "retire_source": true,
                "min_applied_index": required_applied_index,
                "catch_up": true,
                "catch_up_attempts": 5,
                "catch_up_wait_ms": 1000
            }),
        )
        .await
        {
            Ok(value) => {
                complete = Some(value);
                break;
            }
            Err(error)
                if is_retryable_cluster_convergence_error(&error)
                    && attempt + 1 < APPEND_RECOVERY_ATTEMPTS =>
            {
                eprintln!(
                    "replacement: complete retry {} after convergence error: {error:?}",
                    attempt + 1
                );
                complete_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    let complete = complete.ok_or_else(|| {
        complete_error
            .unwrap_or_else(|| anyhow!("replacement complete failed without a captured error"))
    })?;
    if complete["action"] != "complete_replacement"
        || complete["new_owner_node_id"].as_str() != Some(target.id.as_str())
    {
        bail!("unexpected replacement complete response: {complete}");
    }
    let owner_index = node_index(nodes, &owner.id)?;
    eprintln!(
        "replacement: stopping source {} so target {} can take ownership",
        owner.id, target.id
    );
    children[owner_index].kill().ok();
    children[owner_index].wait().ok();
    let survivors: Vec<_> = nodes
        .iter()
        .filter(|node| node.id != owner.id)
        .cloned()
        .collect();
    wait_tablet_owner(
        client,
        &survivors,
        PRIMARY_TABLET_ID,
        Some(&owner.id),
        Duration::from_secs(90),
    )
    .await?;
    append_and_verify(
        client,
        &survivors,
        PRIMARY_TABLET_ID,
        tenant_id,
        agent_id,
        session_id,
        *next_seq,
    )
    .await?;
    *next_seq += 1;
    let _ = force_raft_catch_up(client, &survivors, PRIMARY_TABLET_ID).await?;
    Ok(survivors)
}

async fn run_az_failure(
    args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    children: &mut [Child],
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    next_seq: &mut u64,
    secondary: Option<&mut (String, String, String, u64)>,
) -> Result<()> {
    let has_secondary = secondary.is_some();
    let failed_az = nodes
        .first()
        .ok_or_else(|| anyhow!("AZ failure requires nodes"))?
        .availability_zone
        .clone();
    let failed_indexes: Vec<_> = nodes
        .iter()
        .enumerate()
        .filter_map(|(index, node)| (node.availability_zone == failed_az).then_some(index))
        .collect();
    eprintln!(
        "az-failure: stopping AZ {failed_az} nodes {:?}",
        failed_indexes
            .iter()
            .map(|index| nodes[*index].id.as_str())
            .collect::<Vec<_>>()
    );
    for index in &failed_indexes {
        children[*index].kill().ok();
    }
    for index in &failed_indexes {
        children[*index].wait().ok();
    }
    let survivors: Vec<_> = nodes
        .iter()
        .enumerate()
        .filter_map(|(index, node)| (!failed_indexes.contains(&index)).then_some(node.clone()))
        .collect();
    let survivor = survivors
        .first()
        .ok_or_else(|| anyhow!("AZ failure left no survivors"))?;
    wait_raft_observable(client, &survivor.url, Duration::from_secs(45)).await?;
    wait_tablet_owner(
        client,
        &survivors,
        PRIMARY_TABLET_ID,
        None,
        stale_node_wait_timeout(),
    )
    .await?;
    append_and_verify(
        client,
        &survivors,
        PRIMARY_TABLET_ID,
        tenant_id,
        agent_id,
        session_id,
        *next_seq,
    )
    .await?;
    *next_seq += 1;
    let _ = force_raft_catch_up(client, &survivors, PRIMARY_TABLET_ID).await?;
    wait_raft_survivor_ready(
        client,
        &survivors,
        failed_indexes.len(),
        Duration::from_secs(120),
    )
    .await?;
    if let Some((tenant_id, agent_id, session_id, next_secondary_seq)) = secondary {
        wait_tablet_owner(
            client,
            &survivors,
            SECONDARY_TABLET_ID,
            None,
            stale_node_wait_timeout(),
        )
        .await?;
        append_and_verify(
            client,
            &survivors,
            SECONDARY_TABLET_ID,
            tenant_id,
            agent_id,
            session_id,
            *next_secondary_seq,
        )
        .await?;
        *next_secondary_seq += 1;
        let _ = force_raft_catch_up(client, &survivors, SECONDARY_TABLET_ID).await?;
        wait_raft_survivor_ready(
            client,
            &survivors,
            failed_indexes.len(),
            Duration::from_secs(120),
        )
        .await?;
    }
    eprintln!("az-failure: restarting AZ {failed_az}");
    for index in failed_indexes {
        children[index] = spawn_node(args, &nodes[index])?;
        wait_json(
            client,
            &nodes[index].url,
            "/api/admin/cluster/health",
            Duration::from_secs(90),
        )
        .await?;
    }
    let _ = force_raft_catch_up(client, nodes, PRIMARY_TABLET_ID).await?;
    if has_secondary {
        let _ = force_raft_catch_up(client, nodes, SECONDARY_TABLET_ID).await?;
    }
    wait_raft_ready(client, nodes, Duration::from_secs(120)).await?;
    Ok(())
}

async fn run_rolling_restart(
    args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    children: &mut [Child],
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    next_seq: &mut u64,
) -> Result<()> {
    for index in 0..nodes.len() {
        eprintln!("rolling-restart: restarting {}", nodes[index].id);
        restart_node(args, client, nodes, children, index).await?;
        wait_raft_ready(client, nodes, Duration::from_secs(120)).await?;
        append_and_verify(
            client,
            nodes,
            PRIMARY_TABLET_ID,
            tenant_id,
            agent_id,
            session_id,
            *next_seq,
        )
        .await?;
        *next_seq += 1;
        let _ = force_raft_catch_up(client, nodes, PRIMARY_TABLET_ID).await?;
        wait_raft_ready(client, nodes, Duration::from_secs(120)).await?;
    }
    Ok(())
}

async fn run_split_merge_lifecycle(
    client: &Client,
    nodes: &[NodeSpec],
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    next_seq: &mut u64,
) -> Result<()> {
    let owner = wait_tablet_owner(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        None,
        Duration::from_secs(90),
    )
    .await?;
    eprintln!("split-merge: appending event {next_seq} before split prepare");
    append_and_verify(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        tenant_id,
        agent_id,
        session_id,
        *next_seq,
    )
    .await?;
    *next_seq += 1;
    let before_watch = watch_session(client, &owner.url, tenant_id, session_id).await?;
    let last_cursor = before_watch
        .as_array()
        .and_then(|items| items.last())
        .and_then(|item| item["cursor"].as_str())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("watch response did not include a cursor: {before_watch}"))?;
    let midpoint = (DEFAULT_SHARD_COUNT - 1) / 2;
    let split_body = json!({
        "placement_scope_id": placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID,
        "child_ranges": [
            {"start_shard": 0, "end_shard": midpoint, "tablet_id": 11},
            {"start_shard": midpoint + 1, "end_shard": DEFAULT_SHARD_COUNT - 1, "tablet_id": 12}
        ],
        "require_ready": false
    });
    let prepare = post_json(
        client,
        &owner.url,
        &format!("/api/admin/tablets/{PRIMARY_TABLET_ID}/split/prepare"),
        split_body.clone(),
    )
    .await?;
    if prepare["action"] != "prepare_split" {
        bail!("unexpected split prepare response: {prepare}");
    }
    eprintln!("split-merge: appending event {next_seq} while split is prepared");
    append_and_verify(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        tenant_id,
        agent_id,
        session_id,
        *next_seq,
    )
    .await?;
    *next_seq += 1;
    let after_prepare =
        watch_session_from_cursor(client, &owner.url, tenant_id, session_id, &last_cursor).await?;
    if after_prepare.as_array().map_or(0, Vec::len) == 0 {
        bail!("watch did not return events written during split prepare");
    }
    let complete_split_body = json!({
        "placement_scope_id": placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID,
        "child_ranges": [
            {"start_shard": 0, "end_shard": midpoint, "tablet_id": 11},
            {"start_shard": midpoint + 1, "end_shard": DEFAULT_SHARD_COUNT - 1, "tablet_id": 12}
        ],
        "map_epoch": 2,
        "min_applied_index": 0,
        "require_ready": false
    });
    let complete = post_json(
        client,
        &owner.url,
        &format!("/api/admin/tablets/{PRIMARY_TABLET_ID}/split/complete"),
        complete_split_body,
    )
    .await?;
    if complete["action"] != "complete_split" {
        bail!("unexpected split complete response: {complete}");
    }
    let merge_body = json!({
        "placement_scope_id": placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID,
        "source_tablet_ids": [11, 12],
        "destination_tablet_id": 13,
        "require_ready": false
    });
    let merge_prepare = post_json(
        client,
        &owner.url,
        "/api/admin/tablets/merge/prepare",
        merge_body.clone(),
    )
    .await?;
    if merge_prepare["action"] != "prepare_merge" {
        bail!("unexpected merge prepare response: {merge_prepare}");
    }
    let merge_complete = post_json(
        client,
        &owner.url,
        "/api/admin/tablets/merge/complete",
        json!({
            "placement_scope_id": placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID,
            "source_tablet_ids": [11, 12],
            "destination_tablet_id": 13,
            "map_epoch": 3,
            "min_applied_index": 0,
            "require_ready": false
        }),
    )
    .await?;
    if merge_complete["action"] != "complete_merge" {
        bail!("unexpected merge complete response: {merge_complete}");
    }
    eprintln!("split-merge: completed metadata split and merge cutover");
    Ok(())
}

async fn restart_node(
    args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    children: &mut [Child],
    index: usize,
) -> Result<()> {
    children[index].kill().ok();
    children[index].wait().ok();
    children[index] = spawn_node(args, &nodes[index])?;
    wait_json(
        client,
        &nodes[index].url,
        "/api/admin/cluster/health",
        Duration::from_secs(90),
    )
    .await?;
    Ok(())
}

fn node_index(nodes: &[NodeSpec], node_id: &str) -> Result<usize> {
    nodes
        .iter()
        .position(|node| node.id == node_id)
        .ok_or_else(|| anyhow!("node {node_id} is not in smoke node list"))
}

async fn assert_cell_and_node_labels(client: &Client, nodes: &[NodeSpec]) -> Result<()> {
    let first = nodes
        .first()
        .ok_or_else(|| anyhow!("cluster smoke requires at least one node"))?;
    let cells = get_json(client, &first.url, "/api/admin/cells").await?;
    let default_cell = cells
        .as_array()
        .and_then(|rows| {
            rows.iter()
                .find(|cell| cell["cell_id"] == cells::DEFAULT_CELL_ID)
        })
        .ok_or_else(|| anyhow!("default cell was not exposed by /api/admin/cells: {cells}"))?;
    if default_cell["deployment_profile"] != cells::DEFAULT_DEPLOYMENT_PROFILE
        || default_cell["capacity_class"] != cells::DEFAULT_CAPACITY_CLASS
    {
        bail!("default cell has unexpected placement metadata: {default_cell}");
    }
    if nodes.iter().any(|node| node.cell_id == SECONDARY_CELL_ID) {
        let secondary = cells
            .as_array()
            .and_then(|rows| {
                rows.iter()
                    .find(|cell| cell["cell_id"] == SECONDARY_CELL_ID)
            })
            .ok_or_else(|| {
                anyhow!("secondary cell was not exposed by /api/admin/cells: {cells}")
            })?;
        if secondary["capacity_class"] != cells::DEFAULT_CAPACITY_CLASS {
            bail!("secondary cell has unexpected placement metadata: {secondary}");
        }
    }

    let node_rows = get_json(client, &first.url, "/api/admin/nodes").await?;
    let mut zones = BTreeSet::new();
    for node in nodes {
        let row = node_rows
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["node_id"] == node.id))
            .ok_or_else(|| {
                anyhow!(
                    "node {} missing from /api/admin/nodes: {node_rows}",
                    node.id
                )
            })?;
        if row["cell_id"] != node.cell_id
            || row["availability_zone"] != node.availability_zone
            || row["capacity_class"] != cells::DEFAULT_CAPACITY_CLASS
        {
            bail!("node {} has unexpected placement labels: {row}", node.id);
        }
        zones.insert(node.availability_zone.as_str());
    }
    let expected_zones = nodes.len().min(3);
    if zones.len() < expected_zones {
        bail!("cluster smoke expected at least {expected_zones} AZ labels, saw {zones:?}");
    }
    Ok(())
}

struct ChaosSession<'a> {
    tenant_id: &'a str,
    agent_id: &'a str,
    session_id: &'a str,
    next_seq: u64,
}

#[derive(Debug, Clone, Copy)]
struct TenantPlacement<'a> {
    cell_id: &'a str,
    placement_scope_id: &'a str,
}

async fn run_chaos(
    args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    children: &mut [Child],
    session: ChaosSession<'_>,
) -> Result<()> {
    let tenant_id = session.tenant_id;
    let agent_id = session.agent_id;
    let session_id = session.session_id;
    let mut next_seq = session.next_seq;
    let seed = args.chaos_seed.unwrap_or_else(|| now_ms() as u64);
    let mut rng = SimpleRng::new(seed);
    eprintln!(
        "chaos: starting {} iterations with seed {seed}",
        args.chaos_iterations
    );
    for iteration in 0..args.chaos_iterations {
        eprintln!("chaos: waiting for raft convergence before next fault");
        wait_raft_ready(client, nodes, Duration::from_secs(90)).await?;
        let victim_index = rng.next_usize(nodes.len());
        let victim = &nodes[victim_index];
        let owner_before = wait_tablet_owner(
            client,
            nodes,
            PRIMARY_TABLET_ID,
            None,
            Duration::from_secs(90),
        )
        .await?;
        eprintln!(
            "chaos: iteration {} killing {} (owner_before={})",
            iteration + 1,
            victim.id,
            owner_before.id
        );
        children[victim_index].kill().ok();
        children[victim_index].wait().ok();

        let survivors: Vec<_> = nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| (index != victim_index).then_some(node.clone()))
            .collect();
        if owner_before.id == victim.id {
            let survivor = survivors
                .first()
                .ok_or_else(|| anyhow!("chaos requires at least one surviving node"))?;
            wait_node_marked_dead(client, &survivor.url, &victim.id, stale_node_wait_timeout())
                .await?;
            wait_tablet_owner_changed(
                client,
                &survivor.url,
                PRIMARY_TABLET_ID,
                &victim.id,
                stale_node_wait_timeout(),
            )
            .await?;
        } else {
            sleep_random(&mut rng, args.chaos_max_sleep_ms).await;
        }

        eprintln!(
            "chaos: appending event {next_seq} while {} is down",
            victim.id
        );
        append_and_verify(
            client,
            &survivors,
            PRIMARY_TABLET_ID,
            tenant_id,
            agent_id,
            session_id,
            next_seq,
        )
        .await?;
        next_seq += 1;

        eprintln!("chaos: restarting {}", victim.id);
        children[victim_index] = spawn_node(args, victim)?;
        wait_json(
            client,
            &victim.url,
            "/api/admin/cluster/health",
            Duration::from_secs(90),
        )
        .await?;
        let _ = force_raft_catch_up(client, nodes, PRIMARY_TABLET_ID).await?;
        wait_raft_ready(client, nodes, Duration::from_secs(90)).await?;
        sleep_random(&mut rng, args.chaos_max_sleep_ms).await;

        eprintln!(
            "chaos: appending event {next_seq} after {} restart",
            victim.id
        );
        append_and_verify(
            client,
            nodes,
            PRIMARY_TABLET_ID,
            tenant_id,
            agent_id,
            session_id,
            next_seq,
        )
        .await?;
        next_seq += 1;
    }

    eprintln!("chaos: waiting for all nodes to converge");
    wait_raft_ready(client, nodes, Duration::from_secs(90)).await?;
    let len = event_count_with_convergence_retry(
        client,
        nodes,
        PRIMARY_TABLET_ID,
        tenant_id,
        session_id,
        10,
    )
    .await?;
    if len < usize::try_from(next_seq).unwrap() {
        bail!("chaos expected at least {next_seq} events, got {len}");
    }
    print_diagnostics(client, nodes).await?;
    Ok(())
}

async fn sleep_random(rng: &mut SimpleRng, max_ms: u64) {
    if max_ms == 0 {
        return;
    }
    let delay_ms = 100 + (rng.next_u64() % max_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}

fn stale_node_wait_timeout() -> Duration {
    Duration::from_millis(SMOKE_STALE_NODE_THRESHOLD_MS + 30_000)
}

fn append_visibility_attempts() -> usize {
    env::var("AGENTDB_CLUSTER_SMOKE_APPEND_VISIBILITY_ATTEMPTS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|attempts| *attempts > 0)
        .unwrap_or(DEFAULT_APPEND_VISIBILITY_ATTEMPTS)
}

struct SimpleRng(u64);

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        (self.next_u64() as usize) % upper
    }
}

async fn create_session_smoke(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    placement: Option<TenantPlacement<'_>>,
) -> Result<(String, String, String)> {
    let mut last_error = None;
    for attempt in 0..10 {
        eprintln!("smoke: create session attempt {}", attempt + 1);
        let suffix = format!("{}-{attempt}", now_ms());
        match create_session_smoke_once(client, nodes, tablet_id, placement, &suffix).await {
            Ok(session) => return Ok(session),
            Err(error) if is_retryable_cluster_convergence_error(&error) => {
                eprintln!(
                    "smoke: create session retry {} after convergence error: {error:?}",
                    attempt + 1
                );
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("session smoke failed without a captured error")))
}

async fn create_session_smoke_once(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    placement: Option<TenantPlacement<'_>>,
    suffix: &str,
) -> Result<(String, String, String)> {
    let owner = wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
    eprintln!("smoke: creating tenant via {}", owner.id);
    let tenant_body = match placement {
        Some(placement) => json!({
            "external_key": format!("cluster-smoke-{suffix}"),
            "policy": {},
            "placement": {
                "placement_scope_id": placement.placement_scope_id,
                "cell_id": placement.cell_id,
                "placement_class": "shared",
                "tier": tenant_policies::DEFAULT_TIER,
                "priority": tenant_policies::DEFAULT_PRIORITY,
                "encryption_class": tenant_policies::DEFAULT_ENCRYPTION_CLASS
            }
        }),
        None => json!({"external_key": format!("cluster-smoke-{suffix}"), "policy": {}}),
    };
    let tenant = if let Some(placement) = placement {
        post_json_with_route_headers(
            client,
            &owner.url,
            "/api/tenants",
            tenant_body,
            tablet_id,
            placement.cell_id,
            placement.placement_scope_id,
        )
        .await?
    } else {
        post_json(client, &owner.url, "/api/tenants", tenant_body).await?
    };
    let tenant_id = string_field(&tenant, "tenant_id")?;
    if let Some(placement) = placement {
        wait_route_decision(
            client,
            nodes,
            &tenant_id,
            tablet_id,
            placement.cell_id,
            placement.placement_scope_id,
            Duration::from_secs(30),
        )
        .await?;
    }
    eprintln!("smoke: creating agent for tenant {tenant_id}");
    let agent_alias = format!("cluster-smoke-agent-{suffix}");
    let agent =
        create_agent_with_convergence_retry(client, nodes, tablet_id, &tenant_id, &agent_alias, 10)
            .await?;
    let agent_id = string_field(&agent, "agent_id")?;
    eprintln!("smoke: creating session for agent {agent_id}");
    let session = post_json_with_convergence_retry(
        client,
        nodes,
        tablet_id,
        &format!("/api/tenants/{tenant_id}/agents/{agent_id}/sessions"),
        json!({"alias": null}),
        10,
    )
    .await?;
    let session_id = string_field(&session, "session_id")?;
    Ok((tenant_id, agent_id, session_id))
}

async fn create_agent_with_convergence_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    alias: &str,
    attempts: usize,
) -> Result<Value> {
    let path = format!("/api/tenants/{tenant_id}/agents");
    let body = json!({"alias": alias, "config": {}});
    let mut last_error = None;
    for attempt in 0..attempts.max(1) {
        let owner =
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
        eprintln!(
            "smoke: POST {path} attempt {} via {}",
            attempt + 1,
            owner.id
        );
        match post_json(client, &owner.url, &path, body.clone()).await {
            Ok(value) => return Ok(value),
            Err(error) if is_alias_conflict(&error) => {
                return find_agent_by_alias(client, nodes, tablet_id, tenant_id, alias).await;
            }
            Err(error) if is_retryable_cluster_convergence_error(&error) => {
                eprintln!(
                    "smoke: POST {path} retry {} after convergence error: {error:?}",
                    attempt + 1
                );
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow!("create agent {alias} failed without a captured error")))
}

async fn create_session_for_agent(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    agent_id: &str,
) -> Result<String> {
    let session = post_json_with_convergence_retry(
        client,
        nodes,
        tablet_id,
        &format!("/api/tenants/{tenant_id}/agents/{agent_id}/sessions"),
        json!({"alias": null}),
        10,
    )
    .await?;
    string_field(&session, "session_id")
}

async fn find_agent_by_alias(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    alias: &str,
) -> Result<Value> {
    let path = format!("/api/tenants/{tenant_id}/agents");
    let owner = wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
    let response = get_json(client, &owner.url, &path).await?;
    if let Some(agent) = response["agents"].as_array().and_then(|agents| {
        agents
            .iter()
            .find(|agent| agent["alias"].as_str() == Some(alias))
    }) {
        return Ok(agent.clone());
    }
    bail!("agent alias {alias} conflicted but existing agent was not observable")
}

async fn post_json_with_convergence_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    path: &str,
    body: Value,
    attempts: usize,
) -> Result<Value> {
    let mut last_error = None;
    let mut owner_hint = None;
    for attempt in 0..attempts.max(1) {
        let owner = if let Some(owner) = owner_hint.take() {
            owner
        } else {
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?
        };
        eprintln!(
            "smoke: POST {path} attempt {} via {}",
            attempt + 1,
            owner.id
        );
        match post_json(client, &owner.url, path, body.clone()).await {
            Ok(value) => return Ok(value),
            Err(error) if is_retryable_cluster_convergence_error(&error) => {
                eprintln!(
                    "smoke: POST {path} retry {} after convergence error: {error:?}",
                    attempt + 1
                );
                owner_hint = retry_owner_hint_from_error(&error, nodes);
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("POST {path} failed without a captured error")))
}

async fn append_and_verify(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    expected_seq: u64,
) -> Result<()> {
    let idempotency_key =
        format!("cluster-smoke-tablet-{tablet_id}-{session_id}-seq-{expected_seq}");
    let visibility_attempts = append_visibility_attempts();
    let mut owner_hint = None;
    for attempt in 0..APPEND_RECOVERY_ATTEMPTS {
        let owner = if let Some(owner) = owner_hint.take() {
            owner
        } else {
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?
        };
        eprintln!(
            "smoke: append seq {expected_seq} attempt {} via {}",
            attempt + 1,
            owner.id
        );
        let append_result = post_json_with_idempotency_key(
            client,
            &owner.url,
            &format!("/api/tenants/{tenant_id}/sessions/{session_id}/events"),
            json!({
                "agent_id": agent_id,
                "kind": "message",
                "payload": {"seq": expected_seq, "source": "cluster-smoke"},
                "artifact_refs": [],
                "source_refs": []
            }),
            &idempotency_key,
        )
        .await;
        match append_result {
            Ok(event) => {
                let seq = event
                    .get("seq")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| anyhow!("append response missing seq: {event}"))?;
                if seq == expected_seq {
                    let exact = session_has_exact_event_seq_from_base(
                        client,
                        &owner.url,
                        tenant_id,
                        session_id,
                        expected_seq,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "read back appended seq {expected_seq} from owner {}",
                            owner.id
                        )
                    })?;
                    if exact {
                        return Ok(());
                    }
                    if session_has_exact_event_seq_with_convergence_retry(
                        client,
                        nodes,
                        tablet_id,
                        tenant_id,
                        session_id,
                        expected_seq,
                        visibility_attempts,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                    let _ = force_raft_catch_up(client, nodes, tablet_id).await;
                    if session_has_exact_event_seq_with_convergence_retry(
                        client,
                        nodes,
                        tablet_id,
                        tenant_id,
                        session_id,
                        expected_seq,
                        visibility_attempts,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                    if attempt + 1 < APPEND_RECOVERY_ATTEMPTS {
                        eprintln!(
                            "smoke: append seq {expected_seq} returned success before read visibility; retrying with same idempotency key"
                        );
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    let observed = session_events_from_base(
                        client,
                        &owner.url,
                        tenant_id,
                        session_id,
                        expected_seq.saturating_add(10),
                    )
                    .await
                    .unwrap_or_default();
                    bail!(
                        "expected appended seq {expected_seq} to become visible; observed={observed:?}"
                    );
                }
                bail!("expected appended seq {expected_seq}, got {seq}");
            }
            Err(error)
                if is_retryable_cluster_convergence_error(&error)
                    && attempt + 1 < APPEND_RECOVERY_ATTEMPTS =>
            {
                eprintln!(
                    "smoke: append seq {expected_seq} retry {} after convergence error: {error:?}",
                    attempt + 1
                );
                owner_hint = retry_owner_hint_from_error(&error, nodes);
                tokio::time::sleep(Duration::from_secs(2)).await;
                if session_has_exact_event_seq_from_base_allow_convergence_error(
                    client,
                    &owner.url,
                    tenant_id,
                    session_id,
                    expected_seq,
                )
                .await?
                {
                    let _ = force_raft_catch_up(client, nodes, tablet_id).await?;
                    if session_has_exact_event_seq_from_base_allow_convergence_error(
                        client,
                        &owner.url,
                        tenant_id,
                        session_id,
                        expected_seq,
                    )
                    .await?
                    {
                        break;
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }
    if !session_has_exact_event_seq_with_convergence_retry(
        client,
        nodes,
        tablet_id,
        tenant_id,
        session_id,
        expected_seq,
        visibility_attempts,
    )
    .await?
    {
        bail!("expected appended seq {expected_seq} to become visible");
    }
    Ok(())
}

async fn event_count_from_base(
    client: &Client,
    base: &str,
    tenant_id: &str,
    session_id: &str,
) -> Result<usize> {
    let events = get_json(
        client,
        base,
        &format!("/api/tenants/{tenant_id}/sessions/{session_id}/events?from_seq=0&limit=100"),
    )
    .await?;
    Ok(events
        .as_array()
        .ok_or_else(|| anyhow!("events response is not an array: {events}"))?
        .len())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventObservation {
    seq: u64,
    payload_seq: Option<u64>,
    event_id: String,
}

async fn session_events_from_base(
    client: &Client,
    base: &str,
    tenant_id: &str,
    session_id: &str,
    limit: u64,
) -> Result<Vec<EventObservation>> {
    let events = get_json(
        client,
        base,
        &format!("/api/tenants/{tenant_id}/sessions/{session_id}/events?from_seq=0&limit={limit}"),
    )
    .await?;
    events
        .as_array()
        .ok_or_else(|| anyhow!("events response is not an array: {events}"))?
        .iter()
        .map(|event| {
            let seq = event
                .get("seq")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("event missing seq: {event}"))?;
            let event_id = string_field(event, "event_id")?;
            let payload_seq = event
                .get("payload")
                .and_then(|payload| payload.get("seq"))
                .and_then(Value::as_u64);
            Ok(EventObservation {
                seq,
                payload_seq,
                event_id,
            })
        })
        .collect()
}

async fn assert_session_converged_on_all_nodes(
    client: &Client,
    nodes: &[NodeSpec],
    tenant_id: &str,
    session_id: &str,
    expected_next_seq: u64,
) -> Result<()> {
    let limit = expected_next_seq.saturating_add(10);
    let expected: Vec<u64> = (0..expected_next_seq).collect();
    let mut baseline = None;
    for node in nodes {
        let observed =
            session_events_from_base(client, &node.url, tenant_id, session_id, limit).await?;
        let seqs = observed.iter().map(|event| event.seq).collect::<Vec<_>>();
        if seqs != expected {
            bail!(
                "node {} observed non-gap-free session seqs; expected {:?}, got {:?}",
                node.id,
                expected,
                seqs
            );
        }
        for event in &observed {
            if let Some(payload_seq) = event.payload_seq {
                if payload_seq != event.seq {
                    bail!(
                        "node {} event {} has seq {} but payload.seq {}",
                        node.id,
                        event.event_id,
                        event.seq,
                        payload_seq
                    );
                }
            }
        }
        if let Some(baseline) = &baseline {
            if baseline != &observed {
                bail!(
                    "node {} session events differ from baseline; baseline={:?} observed={:?}",
                    node.id,
                    baseline,
                    observed
                );
            }
        } else {
            baseline = Some(observed);
        }
    }
    Ok(())
}

async fn session_has_exact_event_seq_from_base(
    client: &Client,
    base: &str,
    tenant_id: &str,
    session_id: &str,
    expected_seq: u64,
) -> Result<bool> {
    let events = get_json(
        client,
        base,
        &format!("/api/tenants/{tenant_id}/sessions/{session_id}/events?from_seq=0&limit=100"),
    )
    .await?;
    let events = events
        .as_array()
        .ok_or_else(|| anyhow!("events response is not an array: {events}"))?;
    Ok(events.iter().any(|event| {
        event.get("seq").and_then(Value::as_u64) == Some(expected_seq)
            && event
                .get("payload")
                .and_then(|payload| payload.get("seq"))
                .and_then(Value::as_u64)
                == Some(expected_seq)
    }))
}

async fn session_has_exact_event_seq_from_base_allow_convergence_error(
    client: &Client,
    base: &str,
    tenant_id: &str,
    session_id: &str,
    expected_seq: u64,
) -> Result<bool> {
    match session_has_exact_event_seq_from_base(client, base, tenant_id, session_id, expected_seq)
        .await
    {
        Ok(visible) => Ok(visible),
        Err(error) if is_retryable_cluster_convergence_error(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

async fn session_has_exact_event_seq(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    session_id: &str,
    expected_seq: u64,
) -> Result<bool> {
    let owner = wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
    session_has_exact_event_seq_from_base(client, &owner.url, tenant_id, session_id, expected_seq)
        .await
}

async fn session_has_exact_event_seq_with_convergence_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    session_id: &str,
    expected_seq: u64,
    attempts: usize,
) -> Result<bool> {
    let mut last_error = None;
    for attempt in 0..attempts.max(1) {
        match session_has_exact_event_seq(
            client,
            nodes,
            tablet_id,
            tenant_id,
            session_id,
            expected_seq,
        )
        .await
        {
            Ok(true) => return Ok(true),
            Ok(false) => {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) if is_retryable_cluster_convergence_error(&error) => {
                eprintln!(
                    "smoke: exact event seq retry {} after convergence error: {error}",
                    attempt + 1
                );
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    if let Some(error) = last_error {
        if is_retryable_cluster_convergence_error(&error) {
            return Ok(false);
        }
    }
    Ok(false)
}

async fn watch_session(
    client: &Client,
    base: &str,
    tenant_id: &str,
    session_id: &str,
) -> Result<Value> {
    get_json(
        client,
        base,
        &format!("/api/tenants/{tenant_id}/watch?scope=session&session_id={session_id}&limit=100"),
    )
    .await
}

async fn watch_tenant(client: &Client, base: &str, tenant_id: &str) -> Result<Value> {
    get_json(
        client,
        base,
        &format!("/api/tenants/{tenant_id}/watch?scope=tenant&limit=100"),
    )
    .await
}

async fn watch_session_from_cursor(
    client: &Client,
    base: &str,
    tenant_id: &str,
    session_id: &str,
    cursor: &str,
) -> Result<Value> {
    get_json(
        client,
        base,
        &format!(
            "/api/tenants/{tenant_id}/watch?scope=session&session_id={session_id}&from_cursor={}&limit=100",
            query_encode(cursor)
        ),
    )
    .await
}

async fn watch_tenant_from_cursor_limit(
    client: &Client,
    base: &str,
    tenant_id: &str,
    cursor: &str,
    limit: usize,
) -> Result<Value> {
    get_json(
        client,
        base,
        &format!(
            "/api/tenants/{tenant_id}/watch?scope=tenant&from_cursor={}&limit={limit}",
            query_encode(cursor)
        ),
    )
    .await
}

async fn watch_session_from_cursor_limit(
    client: &Client,
    base: &str,
    tenant_id: &str,
    session_id: &str,
    cursor: &str,
    limit: usize,
) -> Result<Value> {
    get_json(
        client,
        base,
        &format!(
            "/api/tenants/{tenant_id}/watch?scope=session&session_id={session_id}&from_cursor={}&limit={limit}",
            query_encode(cursor)
        ),
    )
    .await
}

fn watch_item_seq(item: &Value) -> Option<u64> {
    item.get("seq")
        .and_then(Value::as_u64)
        .or_else(|| item.get("event")?.get("seq")?.as_u64())
}

fn watch_item_session_id(item: &Value) -> Option<&str> {
    item.get("session_id")
        .and_then(Value::as_str)
        .or_else(|| item.get("event")?.get("session_id")?.as_str())
}

async fn export_watch_session(
    client: &Client,
    base: &str,
    tenant_id: &str,
    session_id: &str,
    from_cursor: Option<&str>,
    limit: usize,
) -> Result<Value> {
    let mut path =
        format!("/api/tenants/{tenant_id}/exports/watch?scope=session&session_id={session_id}&include=events&limit={limit}");
    if let Some(cursor) = from_cursor {
        path.push_str("&from_cursor=");
        path.push_str(&query_encode(cursor));
    }
    get_json(client, base, &path).await
}

fn query_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

async fn event_count(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    session_id: &str,
) -> Result<u64> {
    let owner = wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
    let count = event_count_from_base(client, &owner.url, tenant_id, session_id).await?;
    Ok(count as u64)
}

async fn event_count_with_convergence_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    session_id: &str,
    attempts: usize,
) -> Result<usize> {
    let mut last_error = None;
    for attempt in 0..attempts.max(1) {
        match event_count(client, nodes, tablet_id, tenant_id, session_id).await {
            Ok(count) => return usize::try_from(count).map_err(Into::into),
            Err(error) if is_retryable_cluster_convergence_error(&error) => {
                eprintln!(
                    "smoke: event count retry {} after convergence error: {error}",
                    attempt + 1
                );
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("event count failed without a captured error")))
}

async fn force_raft_catch_up(client: &Client, nodes: &[NodeSpec], tablet_id: i64) -> Result<Value> {
    let mut last_error = None;
    for attempt in 0..5 {
        let expected_node_ids = current_reachable_node_ids(client, nodes).await;
        let expected_node_ids = if expected_node_ids.is_empty() {
            nodes.iter().map(|node| node.id.clone()).collect()
        } else {
            expected_node_ids
        };
        let owner =
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
        let response = post_json(
            client,
            &owner.url,
            "/api/admin/raft/catch-up",
            json!({"tablet_id": tablet_id, "min_lag": 0}),
        )
        .await;
        match response {
            Ok(response) if catch_up_failures_are_outside_scope(&response, &expected_node_ids) => {
                return Ok(response);
            }
            Ok(response) => {
                last_error = Some(anyhow!("raft catch-up reported failures: {response}"));
            }
            Err(error) if is_retryable_cluster_convergence_error(&error) => {
                last_error = Some(error);
            }
            Err(error) => return Err(error),
        }
        eprintln!(
            "smoke: raft catch-up retry {} for tablet {tablet_id}: {}",
            attempt + 1,
            last_error
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "unknown error".into())
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Err(last_error.unwrap_or_else(|| anyhow!("raft catch-up failed without a captured error")))
}

fn catch_up_failures_are_outside_scope(
    response: &Value,
    expected_node_ids: &BTreeSet<String>,
) -> bool {
    response["failures"].as_array().is_none_or(|failures| {
        failures.iter().all(|failure| {
            failure["node_id"]
                .as_str()
                .is_some_and(|node_id| !expected_node_ids.contains(node_id))
        })
    })
}

fn text_u64(value: &Value) -> u64 {
    value.as_u64().unwrap_or_default()
}

fn is_retryable_raft_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("\"retryable\":true")
        || message.contains("raft-rs command did not commit")
        || message.contains("raft-rs local node")
        || message.contains("is not leader for tablet")
}

fn is_retryable_cluster_convergence_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:?}");
    is_retryable_raft_error(error)
        || message.contains("\"code\":\"RAFT_NOT_LEADER\"")
        || message.contains("\"code\":\"TENANT_NOT_FOUND\"")
        || message.contains("\"code\":\"AGENT_NOT_FOUND\"")
        || message.contains("\"code\":\"SESSION_NOT_FOUND\"")
        || message.contains("\"code\":\"ROUTE_UNASSIGNED\"")
        || message.contains("503 Service Unavailable")
        || message.contains("operation timed out")
        || message.contains("connection closed before message completed")
        || message.contains("raft leader was not observable")
        || message.contains("idempotency replay") && message.contains("is not yet visible")
}

fn retry_owner_hint_from_error(error: &anyhow::Error, nodes: &[NodeSpec]) -> Option<NodeSpec> {
    let message = error.to_string();
    let json_start = message.find('{')?;
    let value: Value = serde_json::from_str(&message[json_start..]).ok()?;
    let body = value.get("error").unwrap_or(&value);
    if body.get("leader_raft_node_id").is_some_and(Value::is_null) {
        eprintln!("smoke: raft leader hint unavailable; rediscovering tablet owner");
        return None;
    }
    let owner_node_id = body.get("owner_node_id")?.as_str()?;
    let owner = nodes.iter().find(|node| node.id == owner_node_id)?.clone();
    eprintln!(
        "smoke: retrying with raft leader hint owner_node_id={}",
        owner.id
    );
    Some(owner)
}

fn is_alias_conflict(error: &anyhow::Error) -> bool {
    format!("{error:?}").contains("\"code\":\"ALIAS_CONFLICT\"")
}

async fn raft_observed_applied_index(
    client: &Client,
    base: &str,
    tablet_id: i64,
    source_node_id: &str,
) -> Result<i64> {
    let raft = get_json(client, base, "/api/admin/raft").await?;
    let tablet = raft["tablets"]
        .as_array()
        .and_then(|tablets| {
            tablets
                .iter()
                .find(|tablet| tablet["tablet_id"].as_i64() == Some(tablet_id))
        })
        .ok_or_else(|| anyhow!("raft status did not include tablet {tablet_id}: {raft}"))?;
    let runtime_max = tablet["replicas"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|replica| replica["applied_index"].as_u64())
        .max();
    let configured_source = tablet["configured_replicas"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|replica| replica["node_id"].as_str() == Some(source_node_id))
        .and_then(|replica| replica["applied_index"].as_i64())
        .and_then(|applied| u64::try_from(applied).ok());
    let max_applied = runtime_max.or(configured_source).ok_or_else(|| {
        anyhow!(
            "raft status has no observed applied index for tablet {tablet_id} source {source_node_id}: {tablet}"
        )
    })?;
    i64::try_from(max_applied)
        .with_context(|| format!("tablet {tablet_id} applied_index {max_applied} exceeds i64"))
}

async fn wait_raft_ready(client: &Client, nodes: &[NodeSpec], timeout: Duration) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut last_observed = Vec::new();
    loop {
        let mut ready = true;
        last_observed.clear();
        for node in nodes {
            let raft = match get_json(client, &node.url, "/api/admin/raft").await {
                Ok(raft) => raft,
                Err(error) => {
                    ready = false;
                    last_observed.push(format!("{}: raft_status_error={error}", node.id));
                    continue;
                }
            };
            last_observed.push(format!(
                "{}: tablets={} unhealthy={} outbound={} summary={}",
                node.id,
                raft["tablet_count"].as_u64().unwrap_or(0),
                raft["unhealthy_replica_count"].as_u64().unwrap_or(0),
                raft["outbound_message_count"].as_u64().unwrap_or(0),
                raft["summary"]
            ));
            ready &= raft["enabled"] == true
                && raft["tablet_count"].as_u64().unwrap_or(0) >= 1
                && raft["unhealthy_replica_count"].as_u64().unwrap_or(1) == 0
                && raft["outbound_message_count"].as_u64().unwrap_or(1) == 0
                && raft["summary"]["leaderless_tablet_count"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0
                && raft["summary"]["multi_leader_tablet_count"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0
                && raft["summary"]["commit_index_drift_tablet_count"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0
                && raft["summary"]["applied_index_drift_tablet_count"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0;
        }
        if ready {
            return Ok(());
        }
        if start.elapsed() > timeout {
            bail!(
                "raft groups did not become ready within {timeout:?}; last observed: {}",
                last_observed.join(" | ")
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_raft_survivor_ready(
    client: &Client,
    nodes: &[NodeSpec],
    allowed_unreachable: usize,
    timeout: Duration,
) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut last_observed = Vec::new();
    loop {
        let mut ready = true;
        last_observed.clear();
        for node in nodes {
            let raft = match get_json(client, &node.url, "/api/admin/raft").await {
                Ok(raft) => raft,
                Err(error) => {
                    ready = false;
                    last_observed.push(format!("{}: raft_status_error={error}", node.id));
                    continue;
                }
            };
            let unreachable = raft["summary"]["unreachable_replica_count"]
                .as_u64()
                .unwrap_or(u64::MAX);
            let lagging = raft["summary"]["lagging_replica_count"]
                .as_u64()
                .unwrap_or(u64::MAX);
            let catch_up_required = raft["summary"]["catch_up_required_replica_count"]
                .as_u64()
                .unwrap_or(u64::MAX);
            let snapshot_absent = raft["summary"]["snapshot_absent_for_catch_up_count"]
                .as_u64()
                .unwrap_or(u64::MAX);
            last_observed.push(format!(
                "{}: tablets={} unhealthy={} outbound={} summary={}",
                node.id,
                raft["tablet_count"].as_u64().unwrap_or(0),
                raft["unhealthy_replica_count"].as_u64().unwrap_or(0),
                raft["outbound_message_count"].as_u64().unwrap_or(0),
                raft["summary"]
            ));
            let allowed_unreachable = allowed_unreachable as u64;
            ready &= raft["enabled"] == true
                && raft["tablet_count"].as_u64().unwrap_or(0) >= 1
                && raft["summary"]["leaderless_tablet_count"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0
                && raft["summary"]["multi_leader_tablet_count"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0
                && raft["summary"]["commit_index_drift_tablet_count"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0
                && raft["summary"]["applied_index_drift_tablet_count"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0
                && unreachable <= allowed_unreachable
                && lagging <= allowed_unreachable
                && catch_up_required <= allowed_unreachable
                && snapshot_absent <= allowed_unreachable;
        }
        if ready {
            return Ok(());
        }
        if start.elapsed() > timeout {
            bail!(
                "surviving raft groups did not become AZ-ready within {timeout:?}; last observed: {}",
                last_observed.join(" | ")
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_raft_observable(client: &Client, base: &str, timeout: Duration) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut last_error = None;
    loop {
        match get_json(client, base, "/api/admin/raft").await {
            Ok(raft) => {
                if raft["enabled"] == true
                    && raft["tablet_count"].as_u64().unwrap_or(0) >= 1
                    && raft["summary"]["leaderless_tablet_count"]
                        .as_u64()
                        .unwrap_or(1)
                        == 0
                {
                    return Ok(());
                }
            }
            Err(error) => last_error = Some(error),
        }
        if start.elapsed() > timeout {
            if let Some(error) = last_error {
                return Err(error).with_context(|| {
                    format!("raft status did not become observable within {timeout:?}")
                });
            }
            bail!("raft status did not become observable within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_node_marked_dead(
    client: &Client,
    base: &str,
    node_id: &str,
    timeout: Duration,
) -> Result<()> {
    let start = tokio::time::Instant::now();
    loop {
        let nodes = get_json(client, base, "/api/admin/nodes").await?;
        let dead = nodes
            .as_array()
            .map(|nodes| {
                nodes
                    .iter()
                    .any(|node| node["node_id"] == node_id && node["state"] == "dead")
            })
            .unwrap_or(false);
        if dead {
            return Ok(());
        }
        if start.elapsed() > timeout {
            bail!("node {node_id} was not marked dead within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_tablet_owner_changed(
    client: &Client,
    base: &str,
    tablet_id: i64,
    old_owner: &str,
    timeout: Duration,
) -> Result<()> {
    let start = tokio::time::Instant::now();
    loop {
        let tablets = get_json(client, base, "/api/admin/tablets").await?;
        let moved = tablets
            .as_array()
            .map(|tablets| {
                tablets.iter().any(|tablet| {
                    tablet["tablet_id"] == tablet_id
                        && tablet["owner_node_id"].as_str().is_some()
                        && tablet["owner_node_id"] != old_owner
                })
            })
            .unwrap_or(false);
        if moved {
            return Ok(());
        }
        if start.elapsed() > timeout {
            bail!("tablet {tablet_id} did not move away from {old_owner} within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_tablet_owner(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    previous_owner: Option<&str>,
    timeout: Duration,
) -> Result<NodeSpec> {
    let start = tokio::time::Instant::now();
    loop {
        let live_node_ids = current_live_node_ids(client, nodes).await;
        for node in nodes {
            let Ok(tablets) = get_json(client, &node.url, "/api/admin/tablets").await else {
                continue;
            };
            let Some(tablet) = tablets.as_array().and_then(|tablets| {
                tablets
                    .iter()
                    .find(|tablet| tablet["tablet_id"] == tablet_id)
            }) else {
                continue;
            };
            let Some(owner_id) = tablet["owner_node_id"].as_str() else {
                continue;
            };
            if previous_owner.is_some_and(|previous| previous == owner_id) {
                continue;
            }
            if !live_node_ids.is_empty() && !live_node_ids.contains(owner_id) {
                continue;
            }
            if let Some(owner) = nodes.iter().find(|candidate| candidate.id == owner_id) {
                return Ok(owner.clone());
            }
            continue;
        }
        if start.elapsed() > timeout {
            bail!("tablet {tablet_id} metadata owner was not observable within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn current_live_node_ids(client: &Client, nodes: &[NodeSpec]) -> BTreeSet<String> {
    for node in nodes {
        let Ok(response) = get_json(
            client,
            &node.url,
            &format!("/api/admin/nodes?stale_threshold_ms={SMOKE_STALE_NODE_THRESHOLD_MS}"),
        )
        .await
        else {
            continue;
        };
        let Some(items) = response.as_array() else {
            continue;
        };
        return items
            .iter()
            .filter_map(|node| {
                let node_id = node["node_id"].as_str()?;
                let live = node["state"] == "alive" && node["is_stale"] == false;
                live.then_some(node_id.to_string())
            })
            .collect();
    }
    BTreeSet::new()
}

async fn current_reachable_node_ids(client: &Client, nodes: &[NodeSpec]) -> BTreeSet<String> {
    let mut reachable = BTreeSet::new();
    for node in nodes {
        let Ok(response) = client
            .get(format!("{}/health/ready", node.url))
            .send()
            .await
        else {
            continue;
        };
        if response.status().is_success() {
            reachable.insert(node.id.clone());
        }
    }
    reachable
}

async fn print_diagnostics(client: &Client, nodes: &[NodeSpec]) -> Result<()> {
    for node in nodes {
        let status = get_json(client, &node.url, "/api/admin/cluster/health").await?;
        let raft = get_json(client, &node.url, "/api/admin/raft").await?;
        let scheduler = get_json(client, &node.url, "/api/admin/scheduler").await?;
        println!(
            "{} health={} raft_summary={} scheduler={}",
            node.id, status["status"], raft["summary"], scheduler
        );
    }
    Ok(())
}

async fn wait_json(client: &Client, base: &str, path: &str, timeout: Duration) -> Result<Value> {
    let start = tokio::time::Instant::now();
    loop {
        match get_json(client, base, path).await {
            Ok(value) => return Ok(value),
            Err(error) if start.elapsed() <= timeout => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => return Err(error).with_context(|| format!("wait for {base}{path}")),
        }
    }
}

async fn wait_route_decision(
    client: &Client,
    nodes: &[NodeSpec],
    tenant_id: &str,
    tablet_id: i64,
    cell_id: &str,
    placement_scope_id: &str,
    timeout: Duration,
) -> Result<()> {
    let start = tokio::time::Instant::now();
    loop {
        for node in nodes {
            let Ok(decision) = get_json(
                client,
                &node.url,
                &format!("/api/admin/routing/decision?tenant_id={tenant_id}"),
            )
            .await
            else {
                continue;
            };
            if decision["cell_id"] == cell_id
                && decision["placement_scope_id"] == placement_scope_id
                && decision["tablet_id"].as_i64() == Some(tablet_id)
            {
                return Ok(());
            }
        }
        if start.elapsed() > timeout {
            bail!(
                "tenant {tenant_id} route did not converge to {cell_id}/{placement_scope_id}/tablet {tablet_id} within {timeout:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn get_json(client: &Client, base: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("HTTP {status} for {base}{path}: {text}");
    }
    Ok(serde_json::from_str(&text)?)
}

async fn create_record_with_commit(
    client: &Client,
    base: &str,
    tenant_id: &str,
    body: Value,
) -> Result<(Value, u64)> {
    let response = client
        .post(format!("{base}/api/tenants/{tenant_id}/records"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let commit_seq = response
        .headers()
        .get("X-AgentDB-Record-Commit-Seq")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let text = response.text().await?;
    if !status.is_success() {
        bail!("HTTP {status} for {base}/api/tenants/{tenant_id}/records: {text}");
    }
    let record = serde_json::from_str(&text)?;
    let commit_seq = commit_seq.ok_or_else(|| {
        anyhow!("record create response missing X-AgentDB-Record-Commit-Seq header: {record}")
    })?;
    Ok((record, commit_seq))
}

async fn create_record_with_commit_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    body: Value,
) -> Result<(Value, u64)> {
    let mut last_error = None;
    for attempt in 0..APPEND_RECOVERY_ATTEMPTS {
        let owner =
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
        eprintln!(
            "record-index: create record attempt {} via {}",
            attempt + 1,
            owner.id
        );
        match create_record_with_commit(client, &owner.url, tenant_id, body.clone()).await {
            Ok(value) => return Ok(value),
            Err(error)
                if is_retryable_cluster_convergence_error(&error)
                    && attempt + 1 < APPEND_RECOVERY_ATTEMPTS =>
            {
                eprintln!(
                    "record-index: create record retry {} after convergence error: {error:?}",
                    attempt + 1
                );
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("create record failed without a captured error")))
}

async fn assert_search_finds_record(
    client: &Client,
    base: &str,
    path: &str,
    expected_record_id: &str,
) -> Result<()> {
    let response = get_json(client, base, path).await?;
    assert_record_search_match(&response, expected_record_id, "record search")
}

async fn assert_search_missing_record(
    client: &Client,
    base: &str,
    path: &str,
    record_id: &str,
) -> Result<()> {
    let response = get_json(client, base, path).await?;
    assert_record_search_missing(&response, record_id, "record search")
}

fn record_search_matches<'a>(response: &'a Value, context: &str) -> Result<&'a Vec<Value>> {
    response["matches"]
        .as_array()
        .ok_or_else(|| anyhow!("{context} response missing matches array: {response}"))
}

fn assert_record_search_match(
    response: &Value,
    expected_record_id: &str,
    context: &str,
) -> Result<()> {
    let matches = record_search_matches(response, context)?;
    if !matches
        .iter()
        .any(|matched| matched["record"]["record_id"].as_str() == Some(expected_record_id))
    {
        bail!("{context} did not find {expected_record_id}: {response}");
    }
    Ok(())
}

fn assert_record_search_missing(response: &Value, record_id: &str, context: &str) -> Result<()> {
    let matches = record_search_matches(response, context)?;
    if matches
        .iter()
        .any(|matched| matched["record"]["record_id"].as_str() == Some(record_id))
    {
        bail!("{context} still found deleted or filtered record {record_id}: {response}");
    }
    Ok(())
}

fn assert_ranked_scores(response: &Value, context: &str) -> Result<()> {
    let matches = record_search_matches(response, context)?;
    let mut previous_score = f64::INFINITY;
    for (idx, matched) in matches.iter().enumerate() {
        let rank = matched["rank"]
            .as_u64()
            .ok_or_else(|| anyhow!("{context} match missing rank: {matched}"))?;
        let expected_rank = (idx + 1) as u64;
        if rank != expected_rank {
            bail!("{context} expected rank {expected_rank}, got {rank}: {response}");
        }
        let score = matched["score"]
            .as_f64()
            .ok_or_else(|| anyhow!("{context} match missing score: {matched}"))?;
        if score > previous_score {
            bail!("{context} scores are not descending: {response}");
        }
        previous_score = score;
        if matched["components"].as_object().is_none() {
            bail!("{context} match missing score components: {matched}");
        }
    }
    Ok(())
}

async fn wait_search_finds_record(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    path: &str,
    record_id: &str,
    timeout: Duration,
) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut last = anyhow!("record {record_id} did not become visible");
    loop {
        if start.elapsed() > timeout {
            return Err(last);
        }
        let owner =
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
        match assert_search_finds_record(client, &owner.url, path, record_id).await {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_search_missing_record(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    path: &str,
    record_id: &str,
    timeout: Duration,
) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut last = anyhow!("record {record_id} remained visible after delete");
    loop {
        if start.elapsed() > timeout {
            return Err(last);
        }
        let owner =
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
        match assert_search_missing_record(client, &owner.url, path, record_id).await {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn delete_record(
    client: &Client,
    base: &str,
    tenant_id: &str,
    record_id: &str,
) -> Result<()> {
    let response = client
        .delete(format!(
            "{base}/api/tenants/{tenant_id}/records/{record_id}"
        ))
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("HTTP {status} deleting record {record_id}: {text}");
    }
    Ok(())
}

async fn delete_record_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    tenant_id: &str,
    record_id: &str,
) -> Result<()> {
    let mut last_error = None;
    for attempt in 0..APPEND_RECOVERY_ATTEMPTS {
        let owner =
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
        eprintln!(
            "record-index: delete record attempt {} via {}",
            attempt + 1,
            owner.id
        );
        match delete_record(client, &owner.url, tenant_id, record_id).await {
            Ok(()) => return Ok(()),
            Err(error)
                if is_retryable_cluster_convergence_error(&error)
                    && attempt + 1 < APPEND_RECOVERY_ATTEMPTS =>
            {
                eprintln!(
                    "record-index: delete record retry {} after convergence error: {error:?}",
                    attempt + 1
                );
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("delete record failed without a captured error")))
}

async fn wait_record_index_watermark(
    client: &Client,
    base: &str,
    commit_seq: u64,
    timeout: Duration,
) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut last = None;
    loop {
        if start.elapsed() > timeout {
            bail!(
                "record indexes did not reach commit_seq {commit_seq} within {timeout:?}; last={}",
                last.unwrap_or_else(|| json!(null))
            );
        }
        match get_json(client, base, "/api/admin/indexes/records").await {
            Ok(status) => {
                let local = status["local_index_watermark"].as_u64().unwrap_or(0);
                let global = status["global_index_watermark"].as_u64().unwrap_or(0);
                let local_lag = status["local_index_lag_tokens"]
                    .as_u64()
                    .unwrap_or(u64::MAX);
                let global_lag = status["global_index_lag_tokens"]
                    .as_u64()
                    .unwrap_or(u64::MAX);
                let worker_failures = status["record_index_worker_failures_total"]
                    .as_u64()
                    .unwrap_or(u64::MAX);
                let apply_failures = status["global_index_apply_failures_total"]
                    .as_u64()
                    .unwrap_or(u64::MAX);
                if local >= commit_seq
                    && global >= commit_seq
                    && local_lag == 0
                    && global_lag == 0
                    && worker_failures == 0
                    && apply_failures == 0
                {
                    return Ok(());
                }
                last = Some(status);
            }
            Err(error) => last = Some(json!({"error": error.to_string()})),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn post_json_with_route_headers(
    client: &Client,
    base: &str,
    path: &str,
    body: Value,
    tablet_id: i64,
    cell_id: &str,
    placement_scope_id: &str,
) -> Result<Value> {
    let response = client
        .post(format!("{base}{path}"))
        .header("x-agentdb-tablet-id", tablet_id.to_string())
        .header("x-agentdb-cell-id", cell_id)
        .header("x-agentdb-placement-scope-id", placement_scope_id)
        .header("x-agentdb-tablet-epoch", "0")
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("HTTP {status} for {base}{path}: {text}");
    }
    Ok(serde_json::from_str(&text)?)
}

async fn post_json(client: &Client, base: &str, path: &str, body: Value) -> Result<Value> {
    let response = client
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("HTTP {status} for {base}{path}: {text}");
    }
    Ok(serde_json::from_str(&text)?)
}

async fn post_json_with_idempotency_key(
    client: &Client,
    base: &str,
    path: &str,
    body: Value,
    idempotency_key: &str,
) -> Result<Value> {
    let response = client
        .post(format!("{base}{path}"))
        .header("Idempotency-Key", idempotency_key)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("HTTP {status} for {base}{path}: {text}");
    }
    Ok(serde_json::from_str(&text)?)
}

async fn post_json_with_idempotency_key_expect_error(
    client: &Client,
    base: &str,
    path: &str,
    body: Value,
    idempotency_key: &str,
) -> Result<String> {
    let response = client
        .post(format!("{base}{path}"))
        .header("Idempotency-Key", idempotency_key)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if status.is_success() {
        bail!("expected idempotency conflict for {base}{path}, got HTTP {status}: {text}");
    }
    Ok(format!("HTTP {status} for {base}{path}: {text}"))
}

async fn post_json_with_idempotency_key_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    path: &str,
    body: Value,
    idempotency_key: &str,
    expected_seq: u64,
) -> Result<Value> {
    let mut last_error = None;
    for attempt in 0..APPEND_RECOVERY_ATTEMPTS {
        let owner =
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
        eprintln!(
            "smoke: idempotent POST {path} seq {expected_seq} attempt {} via {}",
            attempt + 1,
            owner.id
        );
        match post_json_with_idempotency_key(
            client,
            &owner.url,
            path,
            body.clone(),
            idempotency_key,
        )
        .await
        {
            Ok(value) => return Ok(value),
            Err(error)
                if is_retryable_cluster_convergence_error(&error)
                    && attempt + 1 < APPEND_RECOVERY_ATTEMPTS =>
            {
                eprintln!(
                    "smoke: idempotent POST {path} seq {expected_seq} retry {} after convergence error: {error:?}",
                    attempt + 1
                );
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow!("idempotent POST {path} failed without a captured error")))
}

async fn expect_idempotency_conflict_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tablet_id: i64,
    path: &str,
    body: Value,
    idempotency_key: &str,
) -> Result<()> {
    let mut last_error = None;
    for attempt in 0..APPEND_RECOVERY_ATTEMPTS {
        let owner =
            wait_tablet_owner(client, nodes, tablet_id, None, Duration::from_secs(90)).await?;
        eprintln!(
            "smoke: idempotency conflict check {path} attempt {} via {}",
            attempt + 1,
            owner.id
        );
        match post_json_with_idempotency_key_expect_error(
            client,
            &owner.url,
            path,
            body.clone(),
            idempotency_key,
        )
        .await
        {
            Ok(message)
                if message.contains("\"code\":\"IDEMPOTENCY_CONFLICT\"")
                    || message.contains("IdempotencyConflict")
                    || message.contains("IDEMPOTENCY_CONFLICT") =>
            {
                return Ok(());
            }
            Ok(message)
                if is_retryable_cluster_convergence_error(&anyhow!(message.clone()))
                    && attempt + 1 < APPEND_RECOVERY_ATTEMPTS =>
            {
                eprintln!(
                    "smoke: idempotency conflict check retry {} after convergence error: {message}",
                    attempt + 1
                );
                last_error = Some(anyhow!(message));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Ok(message) => bail!("expected idempotency conflict, got {message}"),
            Err(error)
                if is_retryable_cluster_convergence_error(&error)
                    && attempt + 1 < APPEND_RECOVERY_ATTEMPTS =>
            {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow!("idempotency conflict check failed without a captured error")))
}

fn string_field(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("missing string field {field}: {value}"))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn stop_children(children: &mut [Child]) {
    for child in children.iter_mut() {
        let _ = child.kill();
    }
    for child in children.iter_mut() {
        let _ = child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_classifier_treats_leader_discovery_gap_as_transient() {
        let error = anyhow!("tablet 1 raft leader was not observable within 90s");

        assert!(is_retryable_cluster_convergence_error(&error));
    }

    #[test]
    fn retry_classifier_treats_session_not_found_as_transient() {
        let error = anyhow!(r#"HTTP 404: {{"code":"SESSION_NOT_FOUND"}}"#);

        assert!(is_retryable_cluster_convergence_error(&error));
    }

    #[test]
    fn retry_classifier_treats_connection_close_as_transient() {
        let error = anyhow!("connection closed before message completed");

        assert!(is_retryable_cluster_convergence_error(&error));
    }

    #[test]
    fn retry_classifier_rejects_unrelated_errors() {
        let error = anyhow!("schema validation failed");

        assert!(!is_retryable_cluster_convergence_error(&error));
    }

    #[test]
    fn alias_conflict_classifier_detects_committed_retry() {
        let error = anyhow!(r#"HTTP 409: {{"code":"ALIAS_CONFLICT"}}"#);

        assert!(is_alias_conflict(&error));
    }
}
