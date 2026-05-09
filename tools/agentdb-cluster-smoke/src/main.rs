use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentdb_meta::{nodes, tablet_ranges, tablet_replicas, tablets, MetaStore};
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};

const DEFAULT_INTERNAL_TOKEN: &str = "agentdb-cluster-smoke-token";
const DEFAULT_SHARD_COUNT: i64 = agentdb_meta::shards::DEFAULT_SHARD_COUNT;
const PRIMARY_TABLET_ID: i64 = 1;

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
    config_path: PathBuf,
    data_dir: PathBuf,
    content_dir: PathBuf,
    log_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.nodes < 3 {
        bail!("--nodes must be at least 3 for a quorum smoke test");
    }
    if args.internal_auth_token.len() < 16 {
        bail!("internal auth token must be at least 16 characters");
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
    seed_cluster_meta(&meta, &nodes).await?;

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
                content_dir: args.work_dir.join(&id).join("content"),
                log_path: args.work_dir.join("logs").join(format!("{id}.log")),
                url: format!("http://127.0.0.1:{port}"),
                id,
                port,
            }
        })
        .collect()
}

fn write_node_config(args: &Args, node: &NodeSpec) -> Result<()> {
    fs::create_dir_all(&node.data_dir)?;
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
region = "local"
refresh_interval_ms = 500
proxy_timeout_ms = 70000
"#,
        port = node.port,
        internal_auth_token = args.internal_auth_token,
        data_dir = node.data_dir.display(),
        content_dir = node.content_dir.display(),
        bucket = args.s3_bucket,
        region = args.s3_region,
        endpoint = args.s3_endpoint,
        node_id = node.id,
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

async fn seed_cluster_meta(meta: &MetaStore, node_specs: &[NodeSpec]) -> Result<()> {
    let pool = meta.pool();
    let now_ms = now_ms();
    tablets::create_with_placement(
        pool,
        PRIMARY_TABLET_ID,
        "default",
        "serving",
        Some("tablet-1"),
        0,
        now_ms,
    )
    .await?;
    tablets::assign_to(pool, PRIMARY_TABLET_ID, &node_specs[0].id).await?;
    let tablet = tablets::get(pool, PRIMARY_TABLET_ID)
        .await?
        .ok_or_else(|| anyhow!("seeded tablet missing"))?;
    tablets::assume_ownership(
        pool,
        PRIMARY_TABLET_ID,
        tablet.epoch,
        &node_specs[0].id,
        now_ms + 300_000,
    )
    .await?;
    tablet_ranges::upsert(
        pool,
        "default",
        0,
        DEFAULT_SHARD_COUNT - 1,
        PRIMARY_TABLET_ID,
        1,
        now_ms,
    )
    .await?;
    for node in node_specs {
        nodes::upsert(
            pool,
            &node.id,
            &node.url,
            None,
            "alive",
            now_ms,
            Some("local-a"),
            Some("local"),
        )
        .await?;
        tablet_replicas::upsert(
            pool,
            PRIMARY_TABLET_ID,
            &node.id,
            "voter",
            "healthy",
            0,
            0,
            now_ms,
        )
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
        .env("AGENTDB_NODE_HEARTBEAT_MS", "500")
        .env("AGENTDB_SCHEDULER_RENEW_INTERVAL_MS", "500")
        .env("AGENTDB_SCHEDULER_LEASE_MS", "1500")
        .env("AGENTDB_STALE_NODE_THRESHOLD_MS", "10000")
        .env("AGENTDB_REAPER_TICK_MS", "500")
        .env("AGENTDB_REBALANCE_MAX_MOVEMENTS", "0")
        .env("AGENTDB_TABLET_MOVEMENT_BATCH_SIZE", "0")
        .env("AGENTDB_TABLET_LIFECYCLE_MAX_ACTIONS", "0")
        .env("AWS_EC2_METADATA_DISABLED", "true")
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
    eprintln!("smoke: waiting for raft readiness");
    wait_raft_ready(client, nodes, Duration::from_secs(45)).await?;

    eprintln!("smoke: resolving initial tablet writer");
    let owner = wait_tablet_owner(client, nodes, None, Duration::from_secs(90)).await?;
    eprintln!("smoke: initial tablet writer {}", owner.id);
    eprintln!("smoke: creating tenant/agent/session");
    let (tenant_id, agent_id, session_id) = create_session_smoke(client, nodes).await?;
    eprintln!("smoke: appending event 0");
    append_and_verify(client, nodes, &tenant_id, &agent_id, &session_id, 0).await?;
    eprintln!("smoke: appending event 1");
    append_and_verify(client, nodes, &tenant_id, &agent_id, &session_id, 1).await?;
    let mut next_seq = 2;

    print_diagnostics(client, nodes).await?;

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
        eprintln!("smoke: waiting for {} to be marked dead", owner.id);
        wait_node_marked_dead(client, &survivor.url, &owner.id, Duration::from_secs(20)).await?;
        eprintln!("smoke: waiting for tablet owner change");
        wait_tablet_owner_changed(client, &survivor.url, &owner.id, Duration::from_secs(90))
            .await?;
        eprintln!("smoke: waiting for raft observability after failover");
        wait_raft_observable(client, &survivor.url, Duration::from_secs(20)).await?;
        eprintln!("smoke: waiting for new tablet writer");
        wait_tablet_owner(client, nodes, Some(&owner.id), Duration::from_secs(90)).await?;
        eprintln!("smoke: appending event {next_seq} after failover");
        append_and_verify(client, nodes, &tenant_id, &agent_id, &session_id, next_seq).await?;
        next_seq += 1;
        let survivors: Vec<_> = nodes
            .iter()
            .filter(|node| node.id != owner.id)
            .cloned()
            .collect();
        print_diagnostics(client, &survivors).await?;
    }

    if args.chaos {
        run_chaos(
            args,
            client,
            nodes,
            children,
            &tenant_id,
            &agent_id,
            &session_id,
            next_seq,
        )
        .await?;
    }

    Ok(())
}

async fn run_chaos(
    args: &Args,
    client: &Client,
    nodes: &[NodeSpec],
    children: &mut [Child],
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    mut next_seq: u64,
) -> Result<()> {
    let seed = args.chaos_seed.unwrap_or_else(|| now_ms() as u64);
    let mut rng = SimpleRng::new(seed);
    eprintln!(
        "chaos: starting {} iterations with seed {seed}",
        args.chaos_iterations
    );
    for iteration in 0..args.chaos_iterations {
        let victim_index = rng.next_usize(nodes.len());
        let victim = &nodes[victim_index];
        let owner_before = wait_tablet_owner(client, nodes, None, Duration::from_secs(90)).await?;
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
            wait_node_marked_dead(client, &survivor.url, &victim.id, Duration::from_secs(20))
                .await?;
            wait_tablet_owner_changed(client, &survivor.url, &victim.id, Duration::from_secs(90))
                .await?;
        } else {
            sleep_random(&mut rng, args.chaos_max_sleep_ms).await;
        }

        eprintln!(
            "chaos: appending event {next_seq} while {} is down",
            victim.id
        );
        append_and_verify(
            client, &survivors, tenant_id, agent_id, session_id, next_seq,
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
        sleep_random(&mut rng, args.chaos_max_sleep_ms).await;

        eprintln!(
            "chaos: appending event {next_seq} after {} restart",
            victim.id
        );
        append_and_verify(client, nodes, tenant_id, agent_id, session_id, next_seq).await?;
        next_seq += 1;
    }

    eprintln!("chaos: waiting for all nodes to converge");
    wait_raft_ready(client, nodes, Duration::from_secs(90)).await?;
    let len = event_count_with_convergence_retry(client, nodes, tenant_id, session_id, 10).await?;
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
) -> Result<(String, String, String)> {
    let mut last_error = None;
    for attempt in 0..10 {
        eprintln!("smoke: create session attempt {}", attempt + 1);
        let suffix = format!("{}-{attempt}", now_ms());
        match create_session_smoke_once(client, nodes, &suffix).await {
            Ok(session) => return Ok(session),
            Err(error) if is_retryable_cluster_convergence_error(&error) => {
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
    suffix: &str,
) -> Result<(String, String, String)> {
    let owner = wait_tablet_owner(client, nodes, None, Duration::from_secs(90)).await?;
    eprintln!("smoke: creating tenant via {}", owner.id);
    let tenant = post_json(
        client,
        &owner.url,
        "/api/tenants",
        json!({"external_key": format!("cluster-smoke-{suffix}"), "policy": {}}),
    )
    .await?;
    let tenant_id = string_field(&tenant, "tenant_id")?;
    eprintln!("smoke: creating agent for tenant {tenant_id}");
    let agent = post_json_with_convergence_retry(
        client,
        nodes,
        &format!("/api/tenants/{tenant_id}/agents"),
        json!({"alias": "cluster-smoke-agent", "config": {}}),
        10,
    )
    .await?;
    let agent_id = string_field(&agent, "agent_id")?;
    eprintln!("smoke: creating session for agent {agent_id}");
    let session = post_json_with_convergence_retry(
        client,
        nodes,
        &format!("/api/tenants/{tenant_id}/agents/{agent_id}/sessions"),
        json!({"alias": null}),
        10,
    )
    .await?;
    let session_id = string_field(&session, "session_id")?;
    Ok((tenant_id, agent_id, session_id))
}

async fn post_json_with_convergence_retry(
    client: &Client,
    nodes: &[NodeSpec],
    path: &str,
    body: Value,
    attempts: usize,
) -> Result<Value> {
    let mut last_error = None;
    for attempt in 0..attempts.max(1) {
        let owner = wait_tablet_owner(client, nodes, None, Duration::from_secs(90)).await?;
        eprintln!(
            "smoke: POST {path} attempt {} via {}",
            attempt + 1,
            owner.id
        );
        match post_json(client, &owner.url, path, body.clone()).await {
            Ok(value) => return Ok(value),
            Err(error) if is_retryable_cluster_convergence_error(&error) => {
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
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    expected_seq: u64,
) -> Result<()> {
    for attempt in 0..3 {
        let owner = wait_tablet_owner(client, nodes, None, Duration::from_secs(90)).await?;
        eprintln!(
            "smoke: append seq {expected_seq} attempt {} via {}",
            attempt + 1,
            owner.id
        );
        let append_result = post_json(
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
        )
        .await;
        match append_result {
            Ok(event) => {
                let seq = event
                    .get("seq")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| anyhow!("append response missing seq: {event}"))?;
                if seq == expected_seq
                    || event_count(client, nodes, tenant_id, session_id).await? > expected_seq
                {
                    break;
                }
                bail!("expected appended seq {expected_seq}, got {seq}");
            }
            Err(error) if is_retryable_cluster_convergence_error(&error) && attempt < 2 => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if event_count(client, nodes, tenant_id, session_id).await? > expected_seq {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }
    let len = event_count_with_convergence_retry(client, nodes, tenant_id, session_id, 10).await?;
    if len < usize::try_from(expected_seq + 1).unwrap() {
        bail!("expected at least {} events, got {len}", expected_seq + 1);
    }
    Ok(())
}

async fn event_count(
    client: &Client,
    nodes: &[NodeSpec],
    tenant_id: &str,
    session_id: &str,
) -> Result<u64> {
    let owner = wait_tablet_owner(client, nodes, None, Duration::from_secs(90)).await?;
    let events = get_json(
        client,
        &owner.url,
        &format!("/api/tenants/{tenant_id}/sessions/{session_id}/events?from_seq=0&limit=10"),
    )
    .await?;
    Ok(events
        .as_array()
        .ok_or_else(|| anyhow!("events response is not an array: {events}"))?
        .len() as u64)
}

async fn event_count_with_convergence_retry(
    client: &Client,
    nodes: &[NodeSpec],
    tenant_id: &str,
    session_id: &str,
    attempts: usize,
) -> Result<usize> {
    let mut last_error = None;
    for attempt in 0..attempts.max(1) {
        match event_count(client, nodes, tenant_id, session_id).await {
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
        || message.contains("\"code\":\"TENANT_NOT_FOUND\"")
        || message.contains("\"code\":\"AGENT_NOT_FOUND\"")
        || message.contains("\"code\":\"SESSION_NOT_FOUND\"")
        || message.contains("503 Service Unavailable")
        || message.contains("operation timed out")
}

async fn wait_raft_ready(client: &Client, nodes: &[NodeSpec], timeout: Duration) -> Result<()> {
    let start = tokio::time::Instant::now();
    loop {
        let mut ready = true;
        for node in nodes {
            let raft = get_json(client, &node.url, "/api/admin/raft").await?;
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
            bail!("raft groups did not become ready within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_raft_observable(client: &Client, base: &str, timeout: Duration) -> Result<()> {
    let start = tokio::time::Instant::now();
    loop {
        let raft = get_json(client, base, "/api/admin/raft").await?;
        if raft["enabled"] == true
            && raft["tablet_count"].as_u64().unwrap_or(0) >= 1
            && raft["summary"]["leaderless_tablet_count"]
                .as_u64()
                .unwrap_or(1)
                == 0
        {
            return Ok(());
        }
        if start.elapsed() > timeout {
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
                    tablet["tablet_id"] == PRIMARY_TABLET_ID
                        && tablet["owner_node_id"].as_str().is_some()
                        && tablet["owner_node_id"] != old_owner
                })
            })
            .unwrap_or(false);
        if moved {
            return Ok(());
        }
        if start.elapsed() > timeout {
            bail!(
                "tablet {PRIMARY_TABLET_ID} did not move away from {old_owner} within {timeout:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_tablet_owner(
    client: &Client,
    nodes: &[NodeSpec],
    previous_owner: Option<&str>,
    timeout: Duration,
) -> Result<NodeSpec> {
    let start = tokio::time::Instant::now();
    loop {
        let leader_id = current_raft_leader_node_id(client, nodes).await?;
        for node in nodes {
            let Ok(tablets) = get_json(client, &node.url, "/api/admin/tablets").await else {
                continue;
            };
            let tablet_exists = tablets.as_array().is_some_and(|tablets| {
                tablets
                    .iter()
                    .any(|tablet| tablet["tablet_id"] == PRIMARY_TABLET_ID)
            });
            if !tablet_exists {
                continue;
            }
            let Some(leader_id) = leader_id.as_deref() else {
                continue;
            };
            if previous_owner.is_some_and(|previous| previous == leader_id) {
                continue;
            }
            if let Some(leader) = nodes.iter().find(|candidate| candidate.id == leader_id) {
                return Ok(leader.clone());
            }
            bail!("tablet {PRIMARY_TABLET_ID} raft leader {leader_id} is not in smoke node list");
        }
        if start.elapsed() > timeout {
            bail!("tablet {PRIMARY_TABLET_ID} raft leader was not observable within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn current_raft_leader_node_id(
    client: &Client,
    nodes: &[NodeSpec],
) -> Result<Option<String>> {
    for node in nodes {
        let Ok(raft) = get_json(client, &node.url, "/api/admin/raft").await else {
            continue;
        };
        let Some(tablet) = raft["tablets"].as_array().and_then(|tablets| {
            tablets
                .iter()
                .find(|tablet| tablet["tablet_id"] == PRIMARY_TABLET_ID)
        }) else {
            continue;
        };
        let Some(leader_raft_node_id) = tablet["replicas"].as_array().and_then(|replicas| {
            replicas.iter().find_map(|replica| {
                (replica["role"] == "Leader")
                    .then(|| replica["node_id"].as_u64())
                    .flatten()
            })
        }) else {
            continue;
        };
        let Some(configured) = tablet["configured_replicas"].as_array() else {
            continue;
        };
        for replica in configured {
            if replica["raft_node_id"].as_u64() == Some(leader_raft_node_id) {
                if let Some(node_id) = replica["node_id"].as_str() {
                    return Ok(Some(node_id.to_string()));
                }
            }
        }
    }
    Ok(None)
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

async fn get_json(client: &Client, base: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
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
