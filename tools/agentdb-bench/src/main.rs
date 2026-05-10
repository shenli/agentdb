use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::Mutex;

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:7878";
const DEFAULT_OUT_DIR: &str = "bench-out";

#[derive(Parser, Debug)]
#[command(
    name = "agentdb-bench",
    version,
    about = "AgentDB benchmark harness for agent-runtime workloads"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Prepare a synthetic agent-runtime dataset.
    Prepare(PrepareArgs),
    /// Run a workload against a previously prepared dataset.
    Run(RunArgs),
}

#[derive(Args, Debug)]
struct PrepareArgs {
    #[arg(long, default_value = "agentdb-native-rocksdb")]
    backend: String,
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value = "postgres://bench:bench@127.0.0.1:5433/bench")]
    pg_dsn: String,
    #[arg(long, default_value = DEFAULT_OUT_DIR)]
    out: PathBuf,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long, value_enum, default_value_t = Profile::Smoke)]
    profile: Profile,
    #[arg(long)]
    tenants: Option<usize>,
    #[arg(long)]
    agents_per_tenant: Option<usize>,
    #[arg(long)]
    sessions_per_agent: Option<usize>,
    #[arg(long)]
    events_per_session: Option<usize>,
    #[arg(long)]
    records_per_session: Option<usize>,
    #[arg(long)]
    payload_bytes: Option<usize>,
    #[arg(long)]
    artifact_rate: Option<f64>,
    #[arg(long)]
    artifact_small_bytes: Option<usize>,
    #[arg(long)]
    artifact_large_bytes: Option<usize>,
    #[arg(long, value_enum)]
    artifact_mode: Option<ArtifactMode>,
    #[arg(long)]
    forks_per_parent: Option<usize>,
    #[arg(long, default_value = "rocksdb")]
    storage_engine: String,
    #[arg(long, default_value = "localfs")]
    content_engine: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    content_dir: Option<PathBuf>,
    #[arg(long, default_value = "durable-single")]
    durability_profile: String,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[derive(Args, Debug)]
struct RunArgs {
    #[arg(long, default_value = "agentdb-native-rocksdb")]
    backend: String,
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value = "postgres://bench:bench@127.0.0.1:5433/bench")]
    pg_dsn: String,
    #[arg(long, default_value = DEFAULT_OUT_DIR)]
    out: PathBuf,
    #[arg(long)]
    run_id: String,
    #[arg(long, value_enum, default_value_t = Workload::MixedAgentLoop)]
    workload: Workload,
    #[arg(long, default_value = "1,4")]
    concurrency: String,
    #[arg(long, default_value = "5s")]
    warmup: String,
    #[arg(long, default_value = "15s")]
    duration: String,
    #[arg(long, default_value_t = 20)]
    fetch_last_n: usize,
    #[arg(long)]
    no_raw_jsonl: bool,
    #[arg(long, default_value_t = 43)]
    seed: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Profile {
    Smoke,
    DefaultAgentRuntime,
    LongContext,
    ArtifactHeavy,
    MemoryHeavy,
    ForkHeavy,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum ArtifactMode {
    Reference,
    ContentBackend,
    Mixed,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum Workload {
    AppendFetchHot,
    LongContextResume,
    ForkBranch,
    MemoryWriteSearch,
    ArtifactPointer,
    MixedAgentLoop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DatasetConfig {
    profile: String,
    tenants: usize,
    agents_per_tenant: usize,
    sessions_per_agent: usize,
    events_per_session: usize,
    records_per_session: usize,
    payload_bytes: usize,
    artifact_rate: f64,
    artifact_small_bytes: usize,
    artifact_large_bytes: usize,
    artifact_mode: ArtifactMode,
    forks_per_parent: usize,
    seed: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ArtifactStats {
    content_backend_artifacts: u64,
    content_backend_bytes: u64,
    external_reference_artifacts: u64,
    external_reference_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DatabaseConfig {
    backend: String,
    storage_engine: String,
    content_engine: String,
    data_dir: Option<PathBuf>,
    content_dir: Option<PathBuf>,
    durability_profile: String,
    note: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DatasetManifest {
    schema_version: u32,
    run_id: String,
    backend: String,
    base_url: String,
    created_at_ms: u64,
    config: DatasetConfig,
    database_config: DatabaseConfig,
    artifact_stats: ArtifactStats,
    tenants: Vec<TenantFixture>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TenantFixture {
    tenant_id: String,
    external_key: String,
    agents: Vec<AgentFixture>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AgentFixture {
    agent_id: String,
    alias: String,
    sessions: Vec<SessionFixture>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionFixture {
    session_id: String,
    alias: String,
    events_written: usize,
    child_sessions: Vec<String>,
}

#[derive(Debug, Serialize)]
struct BenchReport {
    schema_version: u32,
    run_id: String,
    backend: String,
    workload: Workload,
    generated_at_ms: u64,
    host_manifest: Value,
    software_manifest: Value,
    database_config: Value,
    config: Value,
    cells: Vec<CellReport>,
}

#[derive(Debug, Serialize)]
struct CellReport {
    concurrency: usize,
    warmup_secs: f64,
    duration_secs: f64,
    successful_ops: u64,
    errors: u64,
    ops_per_sec: f64,
    latency: LatencyReport,
    operations: Vec<OperationReport>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct LatencyReport {
    count: usize,
    p50_ms: f64,
    p90_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
struct OperationReport {
    operation: String,
    successful_ops: u64,
    errors: u64,
    first_error: Option<String>,
    latency: LatencyReport,
}

#[derive(Clone, Debug)]
struct OperationSample {
    operation: Operation,
    latency_ms: f64,
    success: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct OperationRecord {
    schema_version: u32,
    run_id: String,
    backend: String,
    workload: Workload,
    concurrency: usize,
    worker_id: usize,
    operation: String,
    started_at_ms: u64,
    latency_ms: f64,
    success: bool,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Prepare(args) => prepare(args).await,
        Commands::Run(args) => run(args).await,
    }
}

async fn prepare(args: PrepareArgs) -> Result<()> {
    if args.backend == "postgres-event-log" {
        return prepare_postgres_event_log(args).await;
    }
    ensure_agentdb_backend(&args.backend)?;
    let run_id = args.run_id.clone().unwrap_or_else(default_run_id);
    let run_dir = args.out.join("runs").join(&run_id);
    create_new_run_dir(&run_dir)?;

    let config = dataset_config(&args);
    let database_config = DatabaseConfig {
        backend: args.backend.clone(),
        storage_engine: args.storage_engine.clone(),
        content_engine: args.content_engine.clone(),
        data_dir: args.data_dir.clone(),
        content_dir: args.content_dir.clone(),
        durability_profile: args.durability_profile.clone(),
        note: "For agentdb-native-rocksdb, RocksDB stores metadata/indexes; artifact bytes are stored by the content engine."
            .into(),
    };
    write_json(run_dir.join("config.json"), &config)?;
    write_json(run_dir.join("database-config.json"), &database_config)?;
    write_json(run_dir.join("host-manifest.json"), &host_manifest())?;
    write_json(run_dir.join("software-manifest.json"), &software_manifest())?;

    let client = Client::new();
    wait_ready(&client, &args.base_url).await?;
    let mut rng = StdRng::seed_from_u64(config.seed);

    let mut tenants = Vec::new();
    let mut artifact_stats = ArtifactStats::default();
    for tenant_idx in 0..config.tenants {
        let external_key = format!("bench-{run_id}-tenant-{tenant_idx}");
        let tenant = post_json(
            &client,
            &args.base_url,
            "/api/tenants",
            &json!({
                "external_key": external_key,
                "policy": {"bench_run_id": run_id}
            }),
        )
        .await?;
        let tenant_id = required_str(&tenant, "tenant_id")?.to_string();
        let mut agents = Vec::new();

        for agent_idx in 0..config.agents_per_tenant {
            let alias = format!("bench-agent-{tenant_idx}-{agent_idx}");
            let agent = post_json(
                &client,
                &args.base_url,
                &format!("/api/tenants/{tenant_id}/agents"),
                &json!({
                    "alias": alias,
                    "config": {"bench_run_id": run_id, "agent_index": agent_idx}
                }),
            )
            .await?;
            let agent_id = required_str(&agent, "agent_id")?.to_string();
            let mut sessions = Vec::new();

            for session_idx in 0..config.sessions_per_agent {
                let alias = format!("bench-session-{tenant_idx}-{agent_idx}-{session_idx}");
                let session = post_json(
                    &client,
                    &args.base_url,
                    &format!("/api/tenants/{tenant_id}/agents/{agent_id}/sessions"),
                    &json!({ "alias": alias }),
                )
                .await?;
                let session_id = required_str(&session, "session_id")?.to_string();
                let mut artifacts = Vec::new();

                for seq in 0..config.events_per_session {
                    let prepared_artifact = maybe_create_artifact(
                        &client,
                        &args.base_url,
                        &tenant_id,
                        &config,
                        &mut rng,
                    )
                    .await?;
                    artifacts.extend(prepared_artifact.artifact_refs.iter().cloned());
                    artifact_stats.content_backend_artifacts +=
                        prepared_artifact.content_backend_artifacts;
                    artifact_stats.content_backend_bytes += prepared_artifact.content_backend_bytes;
                    artifact_stats.external_reference_artifacts +=
                        prepared_artifact.external_reference_artifacts;
                    artifact_stats.external_reference_bytes +=
                        prepared_artifact.external_reference_bytes;
                    let mut payload =
                        event_payload(&config, &mut rng, tenant_idx, agent_idx, session_idx, seq);
                    if !prepared_artifact.external_refs.is_empty() {
                        payload["external_artifact_refs"] =
                            Value::Array(prepared_artifact.external_refs);
                    }
                    append_event(
                        &client,
                        &args.base_url,
                        &tenant_id,
                        &agent_id,
                        &session_id,
                        "message",
                        payload,
                        prepared_artifact.artifact_refs,
                        Vec::new(),
                    )
                    .await?;
                }

                for record_idx in 0..config.records_per_session {
                    create_record(
                        &client,
                        &args.base_url,
                        &tenant_id,
                        &session_id,
                        record_idx,
                        &config,
                    )
                    .await?;
                }

                let mut child_sessions = Vec::new();
                for fork_idx in 0..config.forks_per_parent {
                    let child = post_json(
                        &client,
                        &args.base_url,
                        &format!("/api/tenants/{tenant_id}/sessions/{session_id}/fork"),
                        &json!({
                            "at_seq": config.events_per_session.saturating_sub(1) as u64,
                            "alias": format!("{alias}-fork-{fork_idx}")
                        }),
                    )
                    .await?;
                    child_sessions.push(required_str(&child, "session_id")?.to_string());
                }

                if !artifacts.is_empty() {
                    let artifact_summary_event = !artifacts.is_empty();
                    let payload = json!({
                        "role": "system",
                        "content": "artifact summary",
                        "artifact_count": artifacts.len()
                    });
                    append_event(
                        &client,
                        &args.base_url,
                        &tenant_id,
                        &agent_id,
                        &session_id,
                        "summary",
                        payload,
                        artifacts,
                        Vec::new(),
                    )
                    .await?;
                    sessions.push(SessionFixture {
                        session_id,
                        alias,
                        events_written: config.events_per_session
                            + if artifact_summary_event { 1 } else { 0 },
                        child_sessions,
                    });
                } else {
                    sessions.push(SessionFixture {
                        session_id,
                        alias,
                        events_written: config.events_per_session,
                        child_sessions,
                    });
                }
            }

            agents.push(AgentFixture {
                agent_id,
                alias,
                sessions,
            });
        }

        tenants.push(TenantFixture {
            tenant_id,
            external_key,
            agents,
        });
    }

    let manifest = DatasetManifest {
        schema_version: 1,
        run_id: run_id.clone(),
        backend: args.backend,
        base_url: args.base_url,
        created_at_ms: now_ms(),
        config,
        database_config,
        artifact_stats,
        tenants,
    };
    write_json(run_dir.join("dataset.json"), &manifest)?;
    println!("prepared dataset {run_id} at {}", run_dir.display());
    Ok(())
}

async fn prepare_postgres_event_log(args: PrepareArgs) -> Result<()> {
    let run_id = args.run_id.clone().unwrap_or_else(default_run_id);
    let run_dir = args.out.join("runs").join(&run_id);
    create_new_run_dir(&run_dir)?;

    let config = dataset_config(&args);
    let database_config = DatabaseConfig {
        backend: args.backend.clone(),
        storage_engine: "postgres-event-log".into(),
        content_engine: "metadata-reference".into(),
        data_dir: None,
        content_dir: None,
        durability_profile: args.durability_profile.clone(),
        note: "Postgres event-log baseline stores normalized session/event rows and artifact metadata references; large blob bytes are not stored inline by default."
            .into(),
    };
    write_json(run_dir.join("config.json"), &config)?;
    write_json(run_dir.join("database-config.json"), &database_config)?;
    write_json(run_dir.join("host-manifest.json"), &host_manifest())?;
    write_json(run_dir.join("software-manifest.json"), &software_manifest())?;

    let pool = connect_postgres(&args.pg_dsn).await?;
    init_postgres_event_log_schema(&pool).await?;
    reset_postgres_run(&pool, &run_id).await?;

    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut tenants = Vec::new();
    let mut artifact_stats = ArtifactStats::default();

    for tenant_idx in 0..config.tenants {
        let external_key = format!("bench-{run_id}-tenant-{tenant_idx}");
        let tenant_id = format!("ten-pg-{tenant_idx:06}");
        pg_create_tenant(&pool, &run_id, &tenant_id, &external_key).await?;
        let mut agents = Vec::new();

        for agent_idx in 0..config.agents_per_tenant {
            let alias = format!("bench-agent-{tenant_idx}-{agent_idx}");
            let agent_id = format!("agt-pg-{tenant_idx:06}-{agent_idx:06}");
            pg_create_agent(&pool, &run_id, &tenant_id, &agent_id, &alias).await?;
            let mut sessions = Vec::new();

            for session_idx in 0..config.sessions_per_agent {
                let alias = format!("bench-session-{tenant_idx}-{agent_idx}-{session_idx}");
                let session_id = format!("ses-pg-{tenant_idx:06}-{agent_idx:06}-{session_idx:06}");
                pg_create_session(
                    &pool,
                    &run_id,
                    &tenant_id,
                    &agent_id,
                    &session_id,
                    &alias,
                    None,
                    None,
                )
                .await?;

                for seq in 0..config.events_per_session {
                    let prepared_artifact = pg_maybe_create_artifact(
                        &pool,
                        &run_id,
                        &tenant_id,
                        &session_id,
                        &config,
                        &mut rng,
                    )
                    .await?;
                    artifact_stats.content_backend_artifacts +=
                        prepared_artifact.content_backend_artifacts;
                    artifact_stats.content_backend_bytes += prepared_artifact.content_backend_bytes;
                    artifact_stats.external_reference_artifacts +=
                        prepared_artifact.external_reference_artifacts;
                    artifact_stats.external_reference_bytes +=
                        prepared_artifact.external_reference_bytes;
                    let mut payload =
                        event_payload(&config, &mut rng, tenant_idx, agent_idx, session_idx, seq);
                    if !prepared_artifact.external_refs.is_empty() {
                        payload["external_artifact_refs"] =
                            Value::Array(prepared_artifact.external_refs.clone());
                    }
                    pg_insert_event(
                        &pool,
                        &run_id,
                        &tenant_id,
                        &agent_id,
                        &session_id,
                        seq as i64,
                        "message",
                        &payload,
                        &prepared_artifact.artifact_refs,
                    )
                    .await?;
                }
                pg_set_next_seq(
                    &pool,
                    &run_id,
                    &tenant_id,
                    &session_id,
                    config.events_per_session as i64,
                )
                .await?;

                for record_idx in 0..config.records_per_session {
                    pg_create_record(&pool, &run_id, &tenant_id, &session_id, record_idx, &config)
                        .await?;
                }

                let mut child_sessions = Vec::new();
                for fork_idx in 0..config.forks_per_parent {
                    let child_id = format!("{session_id}-fork-{fork_idx}");
                    pg_create_session(
                        &pool,
                        &run_id,
                        &tenant_id,
                        &agent_id,
                        &child_id,
                        &format!("{alias}-fork-{fork_idx}"),
                        Some(&session_id),
                        Some(config.events_per_session.saturating_sub(1) as i64),
                    )
                    .await?;
                    child_sessions.push(child_id);
                }

                sessions.push(SessionFixture {
                    session_id,
                    alias,
                    events_written: config.events_per_session,
                    child_sessions,
                });
            }

            agents.push(AgentFixture {
                agent_id,
                alias,
                sessions,
            });
        }

        tenants.push(TenantFixture {
            tenant_id,
            external_key,
            agents,
        });
    }

    let manifest = DatasetManifest {
        schema_version: 1,
        run_id: run_id.clone(),
        backend: args.backend,
        base_url: args.pg_dsn,
        created_at_ms: now_ms(),
        config,
        database_config,
        artifact_stats,
        tenants,
    };
    write_json(run_dir.join("dataset.json"), &manifest)?;
    println!(
        "prepared postgres dataset {run_id} at {}",
        run_dir.display()
    );
    Ok(())
}

async fn run(args: RunArgs) -> Result<()> {
    ensure_benchmark_backend(&args.backend)?;
    let run_dir = args.out.join("runs").join(&args.run_id);
    let manifest: DatasetManifest = read_json(run_dir.join("dataset.json"))?;
    let concurrencies = parse_concurrency(&args.concurrency)?;
    let warmup = parse_duration(&args.warmup)?;
    let duration = parse_duration(&args.duration)?;
    let client = Client::new();
    let pg_pool = if args.backend == "postgres-event-log" {
        Some(Arc::new(connect_postgres(&args.pg_dsn).await?))
    } else {
        wait_ready(&client, &args.base_url).await?;
        None
    };
    let database_config = read_json(run_dir.join("database-config.json")).unwrap_or_else(|_| {
        json!({
            "backend": args.backend.clone(),
            "note": "database-config.json was not found for this run"
        })
    });
    let raw_writer = if !args.no_raw_jsonl {
        let raw_path = run_dir.join("raw.jsonl");
        let file =
            File::create(&raw_path).with_context(|| format!("create {}", raw_path.display()))?;
        Some(Arc::new(Mutex::new(BufWriter::new(file))))
    } else {
        None
    };

    let mut cells = Vec::new();
    for concurrency in concurrencies {
        run_cell_warmup(&client, &args, &manifest, concurrency, warmup, args.seed).await?;
        let cell = run_cell(
            &client,
            &args,
            &manifest,
            concurrency,
            warmup.as_secs_f64(),
            duration,
            args.seed + concurrency as u64,
            raw_writer.clone(),
            pg_pool.clone(),
        )
        .await?;
        print_cell(&cell);
        cells.push(cell);
    }
    if let Some(writer) = &raw_writer {
        writer.lock().await.flush()?;
    }

    let report = BenchReport {
        schema_version: 1,
        run_id: args.run_id.clone(),
        backend: args.backend.clone(),
        workload: args.workload,
        generated_at_ms: now_ms(),
        host_manifest: host_manifest(),
        software_manifest: software_manifest(),
        database_config,
        config: json!({
            "base_url": args.base_url,
            "concurrency": args.concurrency,
            "warmup": args.warmup,
            "duration": args.duration,
            "fetch_last_n": args.fetch_last_n,
            "raw_jsonl": !args.no_raw_jsonl,
            "seed": args.seed
        }),
        cells,
    };

    let report_path = run_dir.join(format!("report-{:?}.json", args.workload).to_lowercase());
    write_json(&report_path, &report)?;
    write_markdown_report(run_dir.join("report.md"), &manifest, &report)?;
    println!("wrote {}", report_path.display());
    Ok(())
}

async fn run_cell_warmup(
    client: &Client,
    args: &RunArgs,
    manifest: &DatasetManifest,
    concurrency: usize,
    duration: Duration,
    seed: u64,
) -> Result<()> {
    if duration.is_zero() {
        return Ok(());
    }
    let _ = run_cell(
        client,
        args,
        manifest,
        concurrency,
        0.0,
        duration,
        seed,
        None,
        None,
    )
    .await?;
    Ok(())
}

async fn run_cell(
    client: &Client,
    args: &RunArgs,
    manifest: &DatasetManifest,
    concurrency: usize,
    warmup_secs: f64,
    duration: Duration,
    seed: u64,
    raw_writer: Option<Arc<Mutex<BufWriter<File>>>>,
    pg_pool: Option<Arc<PgPool>>,
) -> Result<CellReport> {
    let samples = Arc::new(Mutex::new(Vec::<OperationSample>::new()));
    let deadline = Instant::now() + duration;
    let fixtures = Arc::new(flatten_fixtures(manifest)?);
    let config = Arc::new(manifest.config.clone());
    let mut handles = Vec::new();

    for worker_id in 0..concurrency {
        let client = client.clone();
        let args = args.clone_for_worker();
        let samples = samples.clone();
        let fixtures = fixtures.clone();
        let config = config.clone();
        let raw_writer = raw_writer.clone();
        let pg_pool = pg_pool.clone();
        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed + worker_id as u64);
            while Instant::now() < deadline {
                let operation = choose_operation(args.workload, &mut rng);
                let started_at_ms = now_ms();
                let started = Instant::now();
                let result = run_one_operation(
                    &client,
                    pg_pool.as_deref(),
                    &args,
                    &config,
                    &fixtures,
                    &mut rng,
                    operation,
                )
                .await;
                let elapsed = started.elapsed().as_secs_f64() * 1000.0;
                let error = result.err().map(|err| err.to_string());
                let success = error.is_none();
                let sample = OperationSample {
                    operation,
                    latency_ms: elapsed,
                    success,
                    error: error.clone(),
                };
                samples.lock().await.push(sample);

                if let Some(writer) = &raw_writer {
                    let record = OperationRecord {
                        schema_version: 1,
                        run_id: args.run_id.clone(),
                        backend: args.backend.clone(),
                        workload: args.workload,
                        concurrency,
                        worker_id,
                        operation: operation.as_str().into(),
                        started_at_ms,
                        latency_ms: elapsed,
                        success,
                        error,
                    };
                    let mut writer = writer.lock().await;
                    serde_json::to_writer(&mut *writer, &record)?;
                    writer.write_all(b"\n")?;
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let samples = samples.lock().await.clone();
    let successful_ops = samples.iter().filter(|sample| sample.success).count() as u64;
    let errors = samples.len() as u64 - successful_ops;
    let successful_latencies = samples
        .iter()
        .filter(|sample| sample.success)
        .map(|sample| sample.latency_ms)
        .collect();
    Ok(CellReport {
        concurrency,
        warmup_secs,
        duration_secs: duration.as_secs_f64(),
        successful_ops,
        errors,
        ops_per_sec: successful_ops as f64 / duration.as_secs_f64(),
        latency: summarize_latency(successful_latencies),
        operations: summarize_operations(&samples),
    })
}

async fn run_one_operation(
    client: &Client,
    pg_pool: Option<&PgPool>,
    args: &RunArgs,
    config: &DatasetConfig,
    fixtures: &[FlatSession],
    rng: &mut StdRng,
    operation: Operation,
) -> Result<()> {
    if fixtures.is_empty() {
        return Err(anyhow!("dataset has no sessions"));
    }
    let fixture = &fixtures[rng.gen_range(0..fixtures.len())];
    if args.backend == "postgres-event-log" {
        let pool = pg_pool.ok_or_else(|| anyhow!("postgres backend missing pool"))?;
        return run_one_postgres_operation(pool, args, config, fixture, rng, operation).await;
    }
    match operation {
        Operation::AppendFetch => {
            append_and_fetch(
                client,
                &args.base_url,
                &fixture.tenant_id,
                &fixture.agent_id,
                &fixture.session_id,
                args.fetch_last_n,
                json!({
                    "role": "assistant",
                    "content": deterministic_string(128, rng),
                    "bench_op": "append_fetch"
                }),
            )
            .await
        }
        Operation::FetchRecent => {
            let from_seq = fixture.events_written.saturating_sub(args.fetch_last_n) as u64;
            get_ok(
                client,
                &args.base_url,
                &format!(
                    "/api/tenants/{}/sessions/{}/events?from_seq={from_seq}&limit={}",
                    fixture.tenant_id, fixture.session_id, args.fetch_last_n
                ),
            )
            .await
            .map(|_| ())
        }
        Operation::RecordWrite => {
            create_record(
                client,
                &args.base_url,
                &fixture.tenant_id,
                &fixture.session_id,
                rng.gen_range(0..1_000_000),
                &DatasetConfig::small_runtime(args.seed),
            )
            .await
        }
        Operation::RecordSearch => post_json(
            client,
            &args.base_url,
            &format!("/api/tenants/{}/records/search", fixture.tenant_id),
            &json!({
                "namespace": ["bench", "session"],
                "tags": ["bench"],
                "limit": 10
            }),
        )
        .await
        .map(|_| ()),
        Operation::Fork => {
            let alias = format!("bench-runtime-fork-{}", rng.gen::<u64>());
            post_json(
                client,
                &args.base_url,
                &format!(
                    "/api/tenants/{}/sessions/{}/fork",
                    fixture.tenant_id, fixture.session_id
                ),
                &json!({
                    "at_seq": fixture.events_written.saturating_sub(1) as u64,
                    "alias": alias
                }),
            )
            .await
            .map(|_| ())
        }
        Operation::Artifact => {
            run_one_agentdb_artifact_operation(client, args, config, fixture, rng).await
        }
    }
}

#[derive(Clone)]
struct FlatSession {
    tenant_id: String,
    agent_id: String,
    session_id: String,
    events_written: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Operation {
    AppendFetch,
    FetchRecent,
    RecordWrite,
    RecordSearch,
    Fork,
    Artifact,
}

impl Operation {
    fn as_str(self) -> &'static str {
        match self {
            Operation::AppendFetch => "append_fetch",
            Operation::FetchRecent => "fetch_recent",
            Operation::RecordWrite => "record_write",
            Operation::RecordSearch => "record_search",
            Operation::Fork => "fork",
            Operation::Artifact => "artifact",
        }
    }
}

impl RunArgs {
    fn clone_for_worker(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            base_url: self.base_url.clone(),
            out: self.out.clone(),
            run_id: self.run_id.clone(),
            pg_dsn: self.pg_dsn.clone(),
            workload: self.workload,
            concurrency: self.concurrency.clone(),
            warmup: self.warmup.clone(),
            duration: self.duration.clone(),
            fetch_last_n: self.fetch_last_n,
            no_raw_jsonl: self.no_raw_jsonl,
            seed: self.seed,
        }
    }
}

impl DatasetConfig {
    fn small_runtime(seed: u64) -> Self {
        Self {
            profile: "runtime".into(),
            tenants: 1,
            agents_per_tenant: 1,
            sessions_per_agent: 1,
            events_per_session: 1,
            records_per_session: 1,
            payload_bytes: 256,
            artifact_rate: 0.0,
            artifact_small_bytes: 1024,
            artifact_large_bytes: 1024 * 1024,
            artifact_mode: ArtifactMode::Reference,
            forks_per_parent: 0,
            seed,
        }
    }
}

fn choose_operation(workload: Workload, rng: &mut StdRng) -> Operation {
    match workload {
        Workload::AppendFetchHot => Operation::AppendFetch,
        Workload::LongContextResume => Operation::FetchRecent,
        Workload::ForkBranch => Operation::Fork,
        Workload::MemoryWriteSearch => {
            if rng.gen_bool(0.5) {
                Operation::RecordWrite
            } else {
                Operation::RecordSearch
            }
        }
        Workload::ArtifactPointer => Operation::Artifact,
        Workload::MixedAgentLoop => {
            let roll = rng.gen_range(0..100);
            match roll {
                0..=49 => Operation::AppendFetch,
                50..=74 => Operation::FetchRecent,
                75..=82 => Operation::RecordWrite,
                83..=89 => Operation::RecordSearch,
                90..=96 => Operation::Fork,
                _ => Operation::Artifact,
            }
        }
    }
}

fn flatten_fixtures(manifest: &DatasetManifest) -> Result<Vec<FlatSession>> {
    let mut out = Vec::new();
    for tenant in &manifest.tenants {
        for agent in &tenant.agents {
            for session in &agent.sessions {
                out.push(FlatSession {
                    tenant_id: tenant.tenant_id.clone(),
                    agent_id: agent.agent_id.clone(),
                    session_id: session.session_id.clone(),
                    events_written: session.events_written,
                });
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Default)]
struct PreparedArtifact {
    artifact_refs: Vec<Value>,
    external_refs: Vec<Value>,
    content_backend_artifacts: u64,
    content_backend_bytes: u64,
    external_reference_artifacts: u64,
    external_reference_bytes: u64,
}

async fn maybe_create_artifact(
    client: &Client,
    base_url: &str,
    tenant_id: &str,
    config: &DatasetConfig,
    rng: &mut StdRng,
) -> Result<PreparedArtifact> {
    if config.artifact_rate <= 0.0 || !rng.gen_bool(config.artifact_rate.clamp(0.0, 1.0)) {
        return Ok(PreparedArtifact::default());
    }

    match config.artifact_mode {
        // AgentDB's current native artifact API creates metadata and blob
        // together. Metadata-only external artifact references are represented
        // in event payloads by later workload slices, not in `artifact_refs`.
        ArtifactMode::Reference => Ok(PreparedArtifact {
            artifact_refs: Vec::new(),
            external_refs: vec![json!({
                "content_uri": format!("bench://artifact/{}", rng.gen::<u64>()),
                "content_type": "application/octet-stream",
                "bytes": config.artifact_large_bytes,
                "sha256": format!("{:016x}", rng.gen::<u64>())
            })],
            content_backend_artifacts: 0,
            content_backend_bytes: 0,
            external_reference_artifacts: 1,
            external_reference_bytes: config.artifact_large_bytes as u64,
        }),
        ArtifactMode::ContentBackend => {
            create_content_backend_artifact(client, base_url, tenant_id, config, rng).await
        }
        ArtifactMode::Mixed => {
            if rng.gen_bool(0.5) {
                Ok(PreparedArtifact {
                    artifact_refs: Vec::new(),
                    external_refs: vec![json!({
                        "content_uri": format!("bench://artifact/{}", rng.gen::<u64>()),
                        "content_type": "application/octet-stream",
                        "bytes": config.artifact_large_bytes,
                        "sha256": format!("{:016x}", rng.gen::<u64>())
                    })],
                    content_backend_artifacts: 0,
                    content_backend_bytes: 0,
                    external_reference_artifacts: 1,
                    external_reference_bytes: config.artifact_large_bytes as u64,
                })
            } else {
                create_content_backend_artifact(client, base_url, tenant_id, config, rng).await
            }
        }
    }
}

async fn create_content_backend_artifact(
    client: &Client,
    base_url: &str,
    tenant_id: &str,
    config: &DatasetConfig,
    rng: &mut StdRng,
) -> Result<PreparedArtifact> {
    let size = if rng.gen_bool(0.2) {
        config.artifact_large_bytes
    } else {
        config.artifact_small_bytes
    };
    let bytes = deterministic_bytes(size, rng);
    let artifact = post_bytes(
        client,
        base_url,
        &format!("/api/tenants/{tenant_id}/artifacts"),
        bytes,
        "application/octet-stream",
    )
    .await?;
    Ok(PreparedArtifact {
        artifact_refs: vec![json!({
            "artifact_id": required_str(&artifact, "artifact_id")?,
            "content_type": artifact.get("content_type").and_then(Value::as_str),
            "bytes": artifact.get("bytes").and_then(Value::as_u64),
            "sha256": artifact.get("sha256").and_then(Value::as_str)
        })],
        external_refs: Vec::new(),
        content_backend_artifacts: 1,
        content_backend_bytes: size as u64,
        external_reference_artifacts: 0,
        external_reference_bytes: 0,
    })
}

async fn append_event(
    client: &Client,
    base_url: &str,
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    kind: &str,
    payload: Value,
    artifact_refs: Vec<Value>,
    source_refs: Vec<Value>,
) -> Result<Value> {
    post_json(
        client,
        base_url,
        &format!("/api/tenants/{tenant_id}/sessions/{session_id}/events"),
        &json!({
            "agent_id": agent_id,
            "kind": kind,
            "payload": payload,
            "artifact_refs": artifact_refs,
            "source_refs": source_refs
        }),
    )
    .await
}

async fn append_and_fetch(
    client: &Client,
    base_url: &str,
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    last: usize,
    payload: Value,
) -> Result<()> {
    post_json(
        client,
        base_url,
        &format!("/api/tenants/{tenant_id}/sessions/{session_id}/append-and-fetch"),
        &json!({
            "agent_id": agent_id,
            "kind": "message",
            "payload": payload,
            "last": last,
            "max_bytes": 1_000_000
        }),
    )
    .await?;
    Ok(())
}

async fn run_one_agentdb_artifact_operation(
    client: &Client,
    args: &RunArgs,
    config: &DatasetConfig,
    fixture: &FlatSession,
    rng: &mut StdRng,
) -> Result<()> {
    match config.artifact_mode {
        ArtifactMode::Reference => append_event(
            client,
            &args.base_url,
            &fixture.tenant_id,
            &fixture.agent_id,
            &fixture.session_id,
            "artifact",
            json!({
                "role": "tool",
                "content": "external artifact reference",
                "external_artifact_refs": [{
                    "content_uri": format!("bench://runtime-artifact/{}", rng.gen::<u64>()),
                    "content_type": "application/octet-stream",
                    "bytes": config.artifact_large_bytes,
                    "sha256": format!("{:016x}", rng.gen::<u64>())
                }]
            }),
            Vec::new(),
            Vec::new(),
        )
        .await
        .map(|_| ()),
        ArtifactMode::ContentBackend => {
            let size = if rng.gen_bool(0.2) {
                config.artifact_large_bytes
            } else {
                config.artifact_small_bytes
            };
            let bytes = deterministic_bytes(size, rng);
            post_bytes(
                client,
                &args.base_url,
                &format!("/api/tenants/{}/artifacts", fixture.tenant_id),
                bytes,
                "application/octet-stream",
            )
            .await
            .map(|_| ())
        }
        ArtifactMode::Mixed => {
            if rng.gen_bool(0.5) {
                append_event(
                    client,
                    &args.base_url,
                    &fixture.tenant_id,
                    &fixture.agent_id,
                    &fixture.session_id,
                    "artifact",
                    json!({
                        "role": "tool",
                        "content": "external artifact reference",
                        "external_artifact_refs": [{
                            "content_uri": format!("bench://runtime-artifact/{}", rng.gen::<u64>()),
                            "content_type": "application/octet-stream",
                            "bytes": config.artifact_large_bytes,
                            "sha256": format!("{:016x}", rng.gen::<u64>())
                        }]
                    }),
                    Vec::new(),
                    Vec::new(),
                )
                .await
                .map(|_| ())
            } else {
                let size = if rng.gen_bool(0.2) {
                    config.artifact_large_bytes
                } else {
                    config.artifact_small_bytes
                };
                let bytes = deterministic_bytes(size, rng);
                post_bytes(
                    client,
                    &args.base_url,
                    &format!("/api/tenants/{}/artifacts", fixture.tenant_id),
                    bytes,
                    "application/octet-stream",
                )
                .await
                .map(|_| ())
            }
        }
    }
}

async fn create_record(
    client: &Client,
    base_url: &str,
    tenant_id: &str,
    session_id: &str,
    record_idx: usize,
    config: &DatasetConfig,
) -> Result<()> {
    post_json(
        client,
        base_url,
        &format!("/api/tenants/{tenant_id}/records"),
        &json!({
            "namespace": ["bench", "session"],
            "scope": {"session_id": session_id},
            "text": format!("bench memory record {record_idx} for {session_id}"),
            "metadata": {
                "bench": true,
                "record_index": record_idx,
                "payload": "x".repeat(config.payload_bytes.min(4096))
            },
            "tags": ["bench", "memory"],
            "source": {"session_id": session_id},
            "policy": {"mode": "metadata_only", "vector_allowed": false}
        }),
    )
    .await?;
    Ok(())
}

async fn run_one_postgres_operation(
    pool: &PgPool,
    args: &RunArgs,
    config: &DatasetConfig,
    fixture: &FlatSession,
    rng: &mut StdRng,
    operation: Operation,
) -> Result<()> {
    match operation {
        Operation::AppendFetch => {
            pg_append_fetch(
                pool,
                &args.run_id,
                &fixture.tenant_id,
                &fixture.agent_id,
                &fixture.session_id,
                args.fetch_last_n,
                &json!({
                    "role": "assistant",
                    "content": deterministic_string(128, rng),
                    "bench_op": "append_fetch"
                }),
            )
            .await
        }
        Operation::FetchRecent => {
            pg_fetch_recent(
                pool,
                &args.run_id,
                &fixture.tenant_id,
                &fixture.session_id,
                args.fetch_last_n,
            )
            .await
        }
        Operation::RecordWrite => {
            pg_create_record(
                pool,
                &args.run_id,
                &fixture.tenant_id,
                &fixture.session_id,
                rng.gen_range(0..1_000_000),
                &DatasetConfig::small_runtime(args.seed),
            )
            .await
        }
        Operation::RecordSearch => {
            let _rows = sqlx::query(
                "SELECT record_id FROM agentdb_bench.records
                 WHERE run_id = $1 AND tenant_id = $2 AND tags @> ARRAY['bench']::text[]
                 ORDER BY created_at DESC
                 LIMIT 10",
            )
            .bind(&args.run_id)
            .bind(&fixture.tenant_id)
            .fetch_all(pool)
            .await?;
            Ok(())
        }
        Operation::Fork => {
            let child_id = format!("{}-runtime-fork-{}", fixture.session_id, rng.gen::<u64>());
            pg_create_session(
                pool,
                &args.run_id,
                &fixture.tenant_id,
                &fixture.agent_id,
                &child_id,
                &child_id,
                Some(&fixture.session_id),
                Some(fixture.events_written.saturating_sub(1) as i64),
            )
            .await
        }
        Operation::Artifact => {
            let bytes = match config.artifact_mode {
                ArtifactMode::ContentBackend | ArtifactMode::Mixed if rng.gen_bool(0.2) => {
                    config.artifact_large_bytes
                }
                ArtifactMode::ContentBackend | ArtifactMode::Mixed => config.artifact_small_bytes,
                ArtifactMode::Reference => config.artifact_large_bytes,
            };
            pg_create_artifact_reference(
                pool,
                &args.run_id,
                &fixture.tenant_id,
                &fixture.session_id,
                bytes,
                rng,
            )
            .await
            .map(|_| ())
        }
    }
}

async fn connect_postgres(dsn: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(32)
        .connect(dsn)
        .await
        .with_context(|| format!("connect postgres {dsn}"))
}

async fn init_postgres_event_log_schema(pool: &PgPool) -> Result<()> {
    let schema = include_str!("../../../benchmarks/postgres/event_log.sql");
    for statement in schema.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(pool).await?;
        }
    }
    Ok(())
}

async fn reset_postgres_run(pool: &PgPool, run_id: &str) -> Result<()> {
    for table in [
        "artifacts",
        "records",
        "events",
        "sessions",
        "agents",
        "tenants",
    ] {
        sqlx::query(&format!(
            "DELETE FROM agentdb_bench.{table} WHERE run_id = $1"
        ))
        .bind(run_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn pg_create_tenant(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    external_key: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agentdb_bench.tenants (run_id, tenant_id, external_key)
         VALUES ($1, $2, $3)",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(external_key)
    .execute(pool)
    .await?;
    Ok(())
}

async fn pg_create_agent(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    agent_id: &str,
    alias: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agentdb_bench.agents (run_id, tenant_id, agent_id, alias, config)
         VALUES ($1, $2, $3, $4, $5::jsonb)",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(agent_id)
    .bind(alias)
    .bind(json!({"bench": true}).to_string())
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn pg_create_session(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    alias: &str,
    fork_parent_id: Option<&str>,
    fork_at_seq: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agentdb_bench.sessions
         (run_id, tenant_id, agent_id, session_id, alias, fork_parent_id, fork_at_seq)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(agent_id)
    .bind(session_id)
    .bind(alias)
    .bind(fork_parent_id)
    .bind(fork_at_seq)
    .execute(pool)
    .await?;
    Ok(())
}

async fn pg_set_next_seq(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    session_id: &str,
    next_seq: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE agentdb_bench.sessions
         SET next_seq = $4, updated_at = now()
         WHERE run_id = $1 AND tenant_id = $2 AND session_id = $3",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(session_id)
    .bind(next_seq)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn pg_insert_event(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    seq: i64,
    kind: &str,
    payload: &Value,
    artifact_refs: &[Value],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agentdb_bench.events
         (run_id, tenant_id, agent_id, session_id, seq, kind, payload, artifact_refs)
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(agent_id)
    .bind(session_id)
    .bind(seq)
    .bind(kind)
    .bind(payload.to_string())
    .bind(Value::Array(artifact_refs.to_vec()).to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn pg_append_fetch(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    agent_id: &str,
    session_id: &str,
    last: usize,
    payload: &Value,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let seq: i64 = sqlx::query_scalar(
        "UPDATE agentdb_bench.sessions
         SET next_seq = next_seq + 1, updated_at = now()
         WHERE run_id = $1 AND tenant_id = $2 AND session_id = $3
         RETURNING next_seq - 1",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query(
        "INSERT INTO agentdb_bench.events
         (run_id, tenant_id, agent_id, session_id, seq, kind, payload)
         VALUES ($1, $2, $3, $4, $5, 'message', $6::jsonb)",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(agent_id)
    .bind(session_id)
    .bind(seq)
    .bind(payload.to_string())
    .execute(&mut *tx)
    .await?;

    let _rows = sqlx::query(
        "SELECT seq, payload FROM agentdb_bench.events
         WHERE run_id = $1 AND tenant_id = $2 AND session_id = $3
         ORDER BY seq DESC
         LIMIT $4",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(session_id)
    .bind(last as i64)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn pg_fetch_recent(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    session_id: &str,
    last: usize,
) -> Result<()> {
    let _rows = sqlx::query(
        "SELECT seq, payload FROM agentdb_bench.events
         WHERE run_id = $1 AND tenant_id = $2 AND session_id = $3
         ORDER BY seq DESC
         LIMIT $4",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(session_id)
    .bind(last as i64)
    .fetch_all(pool)
    .await?;
    Ok(())
}

async fn pg_create_record(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    session_id: &str,
    record_idx: usize,
    config: &DatasetConfig,
) -> Result<()> {
    let record_id = format!("rec-pg-{session_id}-{record_idx}");
    sqlx::query(
        "INSERT INTO agentdb_bench.records
         (run_id, tenant_id, record_id, session_id, namespace, tags, text, metadata)
         VALUES ($1, $2, $3, $4, ARRAY['bench','session'], ARRAY['bench','memory'], $5, $6::jsonb)
         ON CONFLICT (run_id, tenant_id, record_id) DO NOTHING",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(record_id)
    .bind(session_id)
    .bind(format!("bench memory record {record_idx} for {session_id}"))
    .bind(
        json!({
            "bench": true,
            "record_index": record_idx,
            "payload": "x".repeat(config.payload_bytes.min(4096))
        })
        .to_string(),
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn pg_maybe_create_artifact(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    session_id: &str,
    config: &DatasetConfig,
    rng: &mut StdRng,
) -> Result<PreparedArtifact> {
    if config.artifact_rate <= 0.0 || !rng.gen_bool(config.artifact_rate.clamp(0.0, 1.0)) {
        return Ok(PreparedArtifact::default());
    }
    pg_create_artifact_reference(
        pool,
        run_id,
        tenant_id,
        session_id,
        config.artifact_large_bytes,
        rng,
    )
    .await
}

async fn pg_create_artifact_reference(
    pool: &PgPool,
    run_id: &str,
    tenant_id: &str,
    session_id: &str,
    bytes: usize,
    rng: &mut StdRng,
) -> Result<PreparedArtifact> {
    let artifact_id = format!("art-pg-{}", rng.gen::<u64>());
    let content_uri = format!("bench://postgres-artifact/{artifact_id}");
    let sha256 = format!("{:016x}", rng.gen::<u64>());
    sqlx::query(
        "INSERT INTO agentdb_bench.artifacts
         (run_id, tenant_id, artifact_id, session_id, content_uri, content_type, bytes, sha256)
         VALUES ($1, $2, $3, $4, $5, 'application/octet-stream', $6, $7)",
    )
    .bind(run_id)
    .bind(tenant_id)
    .bind(&artifact_id)
    .bind(session_id)
    .bind(&content_uri)
    .bind(bytes as i64)
    .bind(&sha256)
    .execute(pool)
    .await?;
    Ok(PreparedArtifact {
        artifact_refs: Vec::new(),
        external_refs: vec![json!({
            "artifact_id": artifact_id,
            "content_uri": content_uri,
            "content_type": "application/octet-stream",
            "bytes": bytes,
            "sha256": sha256
        })],
        content_backend_artifacts: 0,
        content_backend_bytes: 0,
        external_reference_artifacts: 1,
        external_reference_bytes: bytes as u64,
    })
}

async fn post_json<T: Serialize + ?Sized>(
    client: &Client,
    base_url: &str,
    path: &str,
    body: &T,
) -> Result<Value> {
    let resp = client
        .post(format!("{}{}", base_url.trim_end_matches('/'), path))
        .json(body)
        .send()
        .await?;
    parse_json_response(resp).await
}

async fn post_bytes(
    client: &Client,
    base_url: &str,
    path: &str,
    body: Vec<u8>,
    content_type: &str,
) -> Result<Value> {
    let resp = client
        .post(format!("{}{}", base_url.trim_end_matches('/'), path))
        .header("content-type", content_type)
        .body(body)
        .send()
        .await?;
    parse_json_response(resp).await
}

async fn get_ok(client: &Client, base_url: &str, path: &str) -> Result<Value> {
    let resp = client
        .get(format!("{}{}", base_url.trim_end_matches('/'), path))
        .send()
        .await?;
    parse_json_response(resp).await
}

async fn parse_json_response(resp: reqwest::Response) -> Result<Value> {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("HTTP {status}: {text}"));
    }
    if status == StatusCode::NO_CONTENT || text.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text).with_context(|| format!("invalid JSON response: {text}"))
}

async fn wait_ready(client: &Client, base_url: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let url = format!("{}/health/ready", base_url.trim_end_matches('/'));
    loop {
        if Instant::now() > deadline {
            return Err(anyhow!("AgentDB did not become ready at {url}"));
        }
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

fn dataset_config(args: &PrepareArgs) -> DatasetConfig {
    let mut cfg = match args.profile {
        Profile::Smoke => DatasetConfig {
            profile: "smoke".into(),
            tenants: 1,
            agents_per_tenant: 1,
            sessions_per_agent: 2,
            events_per_session: 5,
            records_per_session: 2,
            payload_bytes: 256,
            artifact_rate: 0.2,
            artifact_small_bytes: 1024,
            artifact_large_bytes: 1024 * 1024,
            artifact_mode: ArtifactMode::Reference,
            forks_per_parent: 1,
            seed: args.seed,
        },
        Profile::DefaultAgentRuntime => DatasetConfig {
            profile: "default-agent-runtime".into(),
            tenants: 100,
            agents_per_tenant: 10,
            sessions_per_agent: 20,
            events_per_session: 1000,
            records_per_session: 50,
            payload_bytes: 1024,
            artifact_rate: 0.05,
            artifact_small_bytes: 4096,
            artifact_large_bytes: 1024 * 1024,
            artifact_mode: ArtifactMode::Reference,
            forks_per_parent: 1,
            seed: args.seed,
        },
        Profile::LongContext => DatasetConfig {
            profile: "long-context".into(),
            tenants: 4,
            agents_per_tenant: 2,
            sessions_per_agent: 4,
            events_per_session: 10_000,
            records_per_session: 20,
            payload_bytes: 1024,
            artifact_rate: 0.01,
            artifact_small_bytes: 4096,
            artifact_large_bytes: 1024 * 1024,
            artifact_mode: ArtifactMode::Reference,
            forks_per_parent: 0,
            seed: args.seed,
        },
        Profile::ArtifactHeavy => DatasetConfig {
            profile: "artifact-heavy".into(),
            tenants: 10,
            agents_per_tenant: 4,
            sessions_per_agent: 8,
            events_per_session: 500,
            records_per_session: 10,
            payload_bytes: 512,
            artifact_rate: 0.25,
            artifact_small_bytes: 8192,
            artifact_large_bytes: 2 * 1024 * 1024,
            artifact_mode: ArtifactMode::Mixed,
            forks_per_parent: 0,
            seed: args.seed,
        },
        Profile::MemoryHeavy => DatasetConfig {
            profile: "memory-heavy".into(),
            tenants: 20,
            agents_per_tenant: 5,
            sessions_per_agent: 10,
            events_per_session: 500,
            records_per_session: 200,
            payload_bytes: 512,
            artifact_rate: 0.01,
            artifact_small_bytes: 1024,
            artifact_large_bytes: 1024 * 1024,
            artifact_mode: ArtifactMode::Reference,
            forks_per_parent: 0,
            seed: args.seed,
        },
        Profile::ForkHeavy => DatasetConfig {
            profile: "fork-heavy".into(),
            tenants: 10,
            agents_per_tenant: 5,
            sessions_per_agent: 10,
            events_per_session: 1000,
            records_per_session: 10,
            payload_bytes: 512,
            artifact_rate: 0.01,
            artifact_small_bytes: 1024,
            artifact_large_bytes: 1024 * 1024,
            artifact_mode: ArtifactMode::Reference,
            forks_per_parent: 10,
            seed: args.seed,
        },
    };

    if let Some(v) = args.tenants {
        cfg.tenants = v;
    }
    if let Some(v) = args.agents_per_tenant {
        cfg.agents_per_tenant = v;
    }
    if let Some(v) = args.sessions_per_agent {
        cfg.sessions_per_agent = v;
    }
    if let Some(v) = args.events_per_session {
        cfg.events_per_session = v;
    }
    if let Some(v) = args.records_per_session {
        cfg.records_per_session = v;
    }
    if let Some(v) = args.payload_bytes {
        cfg.payload_bytes = v;
    }
    if let Some(v) = args.artifact_rate {
        cfg.artifact_rate = v;
    }
    if let Some(v) = args.artifact_small_bytes {
        cfg.artifact_small_bytes = v;
    }
    if let Some(v) = args.artifact_large_bytes {
        cfg.artifact_large_bytes = v;
    }
    if let Some(v) = args.artifact_mode {
        cfg.artifact_mode = v;
    }
    if let Some(v) = args.forks_per_parent {
        cfg.forks_per_parent = v;
    }
    cfg.seed = args.seed;
    cfg
}

fn ensure_agentdb_backend(backend: &str) -> Result<()> {
    if backend == "agentdb-native-rocksdb" {
        return Ok(());
    }
    Err(anyhow!(
        "backend {backend:?} is not implemented yet; first slice supports agentdb-native-rocksdb"
    ))
}

fn ensure_benchmark_backend(backend: &str) -> Result<()> {
    if backend == "agentdb-native-rocksdb" || backend == "postgres-event-log" {
        return Ok(());
    }
    Err(anyhow!(
        "backend {backend:?} is not implemented yet; supported backends: agentdb-native-rocksdb, postgres-event-log"
    ))
}

fn create_new_run_dir(path: &Path) -> Result<()> {
    if path.exists() {
        return Err(anyhow!(
            "{} already exists; use a new --run-id or remove the old run directory",
            path.display()
        ));
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: impl AsRef<Path>) -> Result<T> {
    let path = path.as_ref();
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn write_json(path: impl AsRef<Path>, value: &impl Serialize) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}

fn write_markdown_report(
    path: impl AsRef<Path>,
    manifest: &DatasetManifest,
    report: &BenchReport,
) -> Result<()> {
    let mut out = String::new();
    out.push_str("# AgentDB Benchmark Report\n\n");
    out.push_str("## Run Configuration\n\n");
    out.push_str(&format!("- Run ID: `{}`\n", report.run_id));
    out.push_str(&format!("- Backend: `{}`\n", report.backend));
    out.push_str(&format!("- Workload: `{:?}`\n", report.workload));
    out.push_str(&format!(
        "- Storage engine: `{}`\n",
        manifest.database_config.storage_engine
    ));
    out.push_str(&format!(
        "- Content engine: `{}`\n",
        manifest.database_config.content_engine
    ));
    if let Some(path) = &manifest.database_config.data_dir {
        out.push_str(&format!("- RocksDB data dir: `{}`\n", path.display()));
    }
    if let Some(path) = &manifest.database_config.content_dir {
        out.push_str(&format!("- Content dir: `{}`\n", path.display()));
    }
    out.push_str(&format!(
        "- Dataset profile: `{}`\n",
        manifest.config.profile
    ));
    out.push_str(&format!("- Tenants: `{}`\n", manifest.config.tenants));
    out.push_str(&format!(
        "- Agents per tenant: `{}`\n",
        manifest.config.agents_per_tenant
    ));
    out.push_str(&format!(
        "- Sessions per agent: `{}`\n",
        manifest.config.sessions_per_agent
    ));
    out.push_str(&format!(
        "- Events per session: `{}`\n",
        manifest.config.events_per_session
    ));
    out.push_str(&format!(
        "- Artifact mode: `{:?}`\n",
        manifest.config.artifact_mode
    ));
    out.push_str(&format!(
        "- Content-engine artifacts: `{}` artifacts, `{}` bytes\n",
        manifest.artifact_stats.content_backend_artifacts,
        manifest.artifact_stats.content_backend_bytes
    ));
    out.push_str(&format!(
        "- External artifact references: `{}` references, `{}` bytes\n",
        manifest.artifact_stats.external_reference_artifacts,
        manifest.artifact_stats.external_reference_bytes
    ));
    out.push_str(&format!(
        "- Host: `{}`\n",
        report
            .host_manifest
            .get("hostname")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
    ));
    out.push_str(&format!(
        "- Kernel: `{}`\n",
        report
            .host_manifest
            .get("kernel")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
    ));
    out.push_str("\n## Results\n\n");
    out.push_str("| concurrency | ops/s | ops | errors | p50 ms | p95 ms | p99 ms | max ms |\n");
    out.push_str("|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for cell in &report.cells {
        out.push_str(&format!(
            "| {} | {:.2} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} |\n",
            cell.concurrency,
            cell.ops_per_sec,
            cell.successful_ops,
            cell.errors,
            cell.latency.p50_ms,
            cell.latency.p95_ms,
            cell.latency.p99_ms,
            cell.latency.max_ms
        ));
    }
    out.push_str("\n## Operation Breakdown\n\n");
    out.push_str(
        "| concurrency | operation | ops | errors | p50 ms | p95 ms | p99 ms | max ms |\n",
    );
    out.push_str("|---:|---|---:|---:|---:|---:|---:|---:|\n");
    for cell in &report.cells {
        for op in &cell.operations {
            out.push_str(&format!(
                "| {} | `{}` | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} |\n",
                cell.concurrency,
                op.operation,
                op.successful_ops,
                op.errors,
                op.latency.p50_ms,
                op.latency.p95_ms,
                op.latency.p99_ms,
                op.latency.max_ms
            ));
        }
    }
    fs::write(path, out)?;
    Ok(())
}

fn event_payload(
    config: &DatasetConfig,
    rng: &mut StdRng,
    tenant_idx: usize,
    agent_idx: usize,
    session_idx: usize,
    seq: usize,
) -> Value {
    json!({
        "role": if seq % 3 == 0 { "user" } else { "assistant" },
        "content": deterministic_string(config.payload_bytes, rng),
        "bench": {
            "tenant_index": tenant_idx,
            "agent_index": agent_idx,
            "session_index": session_idx,
            "seq": seq
        }
    })
}

fn deterministic_string(bytes: usize, rng: &mut StdRng) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";
    (0..bytes)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect()
}

fn deterministic_bytes(bytes: usize, rng: &mut StdRng) -> Vec<u8> {
    let mut out = vec![0; bytes];
    rng.fill(&mut out[..]);
    out
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing string field {key} in {value}"))
}

fn summarize_latency(mut samples: Vec<f64>) -> LatencyReport {
    if samples.is_empty() {
        return LatencyReport::default();
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    LatencyReport {
        count: samples.len(),
        p50_ms: percentile(&samples, 0.50),
        p90_ms: percentile(&samples, 0.90),
        p95_ms: percentile(&samples, 0.95),
        p99_ms: percentile(&samples, 0.99),
        max_ms: *samples.last().unwrap_or(&0.0),
    }
}

fn percentile(samples: &[f64], q: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let idx = ((samples.len() - 1) as f64 * q).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

fn summarize_operations(samples: &[OperationSample]) -> Vec<OperationReport> {
    let mut grouped: BTreeMap<Operation, Vec<&OperationSample>> = BTreeMap::new();
    for sample in samples {
        grouped.entry(sample.operation).or_default().push(sample);
    }

    grouped
        .into_iter()
        .map(|(operation, samples)| {
            let successful_ops = samples.iter().filter(|sample| sample.success).count() as u64;
            let errors = samples.len() as u64 - successful_ops;
            let latencies = samples
                .iter()
                .filter(|sample| sample.success)
                .map(|sample| sample.latency_ms)
                .collect();
            OperationReport {
                operation: operation.as_str().into(),
                successful_ops,
                errors,
                first_error: samples.iter().find_map(|sample| sample.error.clone()),
                latency: summarize_latency(latencies),
            }
        })
        .collect()
}

fn print_cell(cell: &CellReport) {
    println!(
        "c={} ops/s={:.2} ops={} errors={} p50={:.3}ms p95={:.3}ms p99={:.3}ms",
        cell.concurrency,
        cell.ops_per_sec,
        cell.successful_ops,
        cell.errors,
        cell.latency.p50_ms,
        cell.latency.p95_ms,
        cell.latency.p99_ms
    );
}

fn parse_concurrency(raw: &str) -> Result<Vec<usize>> {
    raw.split(',')
        .map(|part| {
            part.trim()
                .parse::<usize>()
                .with_context(|| format!("invalid concurrency value {part:?}"))
        })
        .collect()
}

fn parse_duration(raw: &str) -> Result<Duration> {
    if let Some(ms) = raw.strip_suffix("ms") {
        return Ok(Duration::from_millis(ms.parse()?));
    }
    if let Some(s) = raw.strip_suffix('s') {
        return Ok(Duration::from_secs_f64(s.parse()?));
    }
    if let Some(m) = raw.strip_suffix('m') {
        let minutes: f64 = m.parse()?;
        return Ok(Duration::from_secs_f64(minutes * 60.0));
    }
    Ok(Duration::from_secs_f64(raw.parse()?))
}

fn default_run_id() -> String {
    format!("run-{}", now_ms())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn host_manifest() -> Value {
    json!({
        "captured_at_ms": now_ms(),
        "hostname": command_output("hostname", &[]),
        "kernel": command_output("uname", &["-a"]),
        "os_release": command_output("sh", &["-c", "cat /etc/os-release 2>/dev/null || true"]),
        "cpu": command_output("sh", &["-c", "lscpu 2>/dev/null || sysctl -a 2>/dev/null | grep -E 'machdep.cpu|hw.mem|hw.ncpu' || true"]),
        "memory": command_output("sh", &["-c", "free -h 2>/dev/null || vm_stat 2>/dev/null || true"]),
        "disk": command_output("sh", &["-c", "df -h / . 2>/dev/null || true"]),
    })
}

fn software_manifest() -> Value {
    json!({
        "captured_at_ms": now_ms(),
        "agentdb_bench_version": env!("CARGO_PKG_VERSION"),
        "git_sha": command_output("git", &["rev-parse", "HEAD"]),
        "rustc": command_output("rustc", &["--version"]),
        "cargo": command_output("cargo", &["--version"]),
        "docker": command_output("docker", &["--version"]),
        "docker_compose": command_output("docker", &["compose", "version"]),
    })
}

fn command_output(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_default()
}
