# M11: SlateDB + S3 — Design

## Goal

Add a second `KvStore` implementation backed by SlateDB-on-object-storage,
so AgentDB can run its operational state on S3 (or any `object_store`
target) instead of a local RocksDB. RocksDB stays the local-default. The
result is the first concrete expression of the "operational DB on object
storage" thesis from `docs/agent-db.md` §3.

## Motivation

The original prototype design (`docs/superpowers/specs/2026-04-17-agentdb-prototype-design.md`)
named `SlateKvStore` as a "post-prototype stretch" that "proves 'LSM on
object storage' thesis from source doc §3.1." Today the KV layer is
RocksDB on local SSD, while cold segments already live behind
`object_store`. With M9 (MVCC snapshot read) landed, the `KvStore` +
`KvSnapshot` trait surface has been battle-tested enough to add a
second implementation without further trait changes.

Adding SlateDB closes two gaps:

1. **Narrative gap.** Today's prototype cannot run end-to-end against
   S3 because the warm KV is local-disk-only. M11 makes the S3-backed
   deployment a single config change.
2. **API-hardening gap.** Having two `KvStore` implementations keeps
   the trait honest. Any implicit RocksDB assumption (transactional
   semantics, cheap snapshots, compaction behavior) surfaces as a
   real portability constraint rather than a latent bug.

## Scope

**In:**

- New `SlateKvStore` in `crates/agentdb-storage/src/slate.rs`
  implementing the existing `KvStore` + `KvSnapshot` traits unchanged.
- `slatedb` + `object_store` become regular deps of
  `crates/agentdb-storage/Cargo.toml`. `object_store` is already a
  transitive dep through `agentdb-core`; here it becomes direct.
- Server-binary wiring: `AGENTDB_KV_BACKEND=rocksdb|slatedb` at
  startup picks the `KvStore` impl. Default is `rocksdb`.
- Object-store selection: `AGENTDB_OBJECT_STORE=local|s3` (existing
  `LocalFileSystem` path stays the default). When
  `AGENTDB_KV_BACKEND=slatedb` and `AGENTDB_OBJECT_STORE` is unset, a
  `tracing::warn!` line nudges toward `s3` but doesn't force it.
- Env vars: `AGENTDB_SLATEDB_PATH` (default `agentdb/slate`),
  `AGENTDB_S3_BUCKET`, `AGENTDB_S3_REGION`, `AGENTDB_S3_ENDPOINT`.
  Credentials via the standard AWS env chain
  (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).
- One-time S3 liveness probe on startup (`list` with `max_results=1`
  under the configured prefix). Fail fast with a clear error.
- Integration tests: `crates/agentdb-storage/tests/slate_integration.rs`
  runs the existing KV contract (get/put/scan/write_batch/snapshot/
  snapshot_owned) against SlateDB-on-LocalFileSystem in a tempdir. If
  the two integration-test files diverge, extract a shared
  `tests/kv_contract.rs` used by both.
- One cross-backend test in `crates/agentdb-core/tests/mvcc_isolation.rs`
  that exercises `append_and_fetch` over `SlateKvStore`. Whether the
  snapshot-isolation assertion holds on SlateDB depends on the
  implementation spike (see Risks §2).
- Opt-in smoke script `scripts/smoke-s3.sh` that boots MinIO via
  docker, runs the existing smoke scenarios against SlateDB+S3, tears
  MinIO down.
- Criterion benches: extend `crates/agentdb-core/benches/mvcc_bench.rs`
  with a SlateDB-on-LocalFileSystem variant alongside the existing
  RocksDB variant.
- Docs: new `docs/S3.md` deployment guide; `ARCHITECTURE.md` gets a
  paragraph about the pluggable backend and links to `S3.md`;
  `PERFORMANCE.md` gets a "Storage backend comparison" table.

**Explicitly out of scope:**

- Running against real AWS S3 in CI. AWS is a config-only example in
  `S3.md`. CI stays on RocksDB; the MinIO smoke is opt-in.
- Migrating existing on-disk data from RocksDB to SlateDB. The two
  backends point at independent storage roots (different meaning of
  `AGENTDB_DATA`). No import tool. No dual-read.
- Changing the `KvStore` trait surface. If SlateDB lacks a primitive
  we need, document the limitation; do not expand the trait.
- SlateDB tuning (WAL batching, flush thresholds, compaction knobs).
  Use crate defaults.
- Read-path caching / hot-key acceleration. Out of scope per Q1.
- Admission control's interaction with SlateDB. M10 lands
  independently.
- Multi-node / distributed topology. SlateDB on shared S3 enables
  it, but coordination, leader election, and write fencing are
  future milestones.

## Design

### SlateKvStore

```rust
// crates/agentdb-storage/src/slate.rs (new)

use std::sync::Arc;
use object_store::ObjectStore;
use slatedb::Db as SlateDb;

use crate::error::{Error, Result};
use crate::kv::{KvIter, KvSnapshot, KvStore, WriteOp};

pub struct SlateKvStore {
    inner: Arc<SlateDb>,
    /// Dedicated single-thread runtime. NOT a handle to the main runtime —
    /// sync trait methods use `runtime.block_on(...)`, which deadlocks if
    /// called from inside an active tokio task on the same runtime. Owning
    /// a separate runtime sidesteps that hazard at the cost of one extra
    /// OS thread per box.
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SlateKvStore {
    /// Open (or create) a SlateDB database at the given `path` prefix inside
    /// `object_store`. Constructs a dedicated tokio runtime for bridging
    /// the sync trait surface to SlateDB's async API.
    pub async fn open(
        object_store: Arc<dyn ObjectStore>,
        path: &str,
    ) -> Result<Self> { /* Db::open(path, object_store, opts).await */ }
}

impl KvStore for SlateKvStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.runtime
            .block_on(async { self.inner.get(key).await })
            .map_err(Error::from)
    }
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> { /* .. */ }
    fn delete(&self, key: &[u8]) -> Result<()> { /* .. */ }
    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> { /* .. */ }
    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()> { /* native batch */ }
    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> { /* .. */ }
    fn snapshot_owned(self: Arc<Self>) -> Result<Arc<dyn KvSnapshot>> { /* .. */ }
}

impl Drop for SlateKvStore {
    fn drop(&mut self) {
        // Since we own a dedicated runtime, `block_on` from Drop is safe:
        // Drop is never called from inside our own runtime's tasks (the
        // Arc<Runtime> would need to be live, but Drop means no more
        // strong refs). Best-effort close; SlateDB's WAL+SSTs on object
        // storage already preserve committed writes, so a failed close
        // just means the final in-memory batch isn't flushed.
        let db = self.inner.clone();
        let rt = self.runtime.clone();
        if let Err(err) = rt.block_on(async move { db.close().await }) {
            tracing::warn!(?err, "slatedb close failed during Drop");
        }
    }
}
```

**Key properties:**

- **Sync trait over async crate.** `KvStore` is sync; SlateDB is async.
  `SlateKvStore` owns a **dedicated** `tokio::runtime::Runtime` (not a
  handle to the main server runtime) and uses `runtime.block_on(...)`
  inside each trait method. This sidesteps the deadlock that happens
  when `block_on` is called on a handle whose task is the current
  executor. Cost: one extra OS thread per box. Matches how
  `RocksKvStore` keeps a sync front to its sync DB.
- **Native atomic batch.** `write_batch` uses SlateDB's atomic-batch
  API. If the pinned SlateDB version does not expose atomic batches,
  this is a blocker to flag and not a silent fallback (see Risks §1).
- **Drop close is synchronous.** Because the runtime is dedicated and
  owned by `SlateKvStore`, calling `runtime.block_on(db.close())` from
  `Drop` is safe — nothing else is executing on that runtime once
  the last `Arc<SlateKvStore>` is dropped. Best-effort: a failed close
  is logged and swallowed, since committed writes are already durable
  on object storage.
- **Key shape compatibility.** The existing `crate::key` helpers
  produce byte-vector keys with length-prefixed big-endian ordering.
  SlateDB sorts keys lexicographically. Prefix scans use the same
  `[low, high)` byte range under both backends. No changes to
  `key.rs`.

### Snapshot strategy — three tiers

M9's `ReadCtx` requires `snapshot_owned()` to return a view that
doesn't change under concurrent writes. SlateDB's exposed snapshot
primitive is unknown at spec time; the implementation spike picks
the first tier that works at the pinned version:

1. **Native snapshot.** If SlateDB exposes point-in-time snapshots or
   a sequence-number-pinned read, wrap it. `ReadCtx` gets the same
   guarantee as against RocksDB.
2. **Immutable-SST + memtable-freeze emulation.** If SlateDB doesn't
   snapshot natively but exposes the underlying immutable SSTs, copy
   out the current SST manifest + freeze the memtable contents at
   `snapshot` time. Implementable but intrusive.
3. **Read-committed fallback.** If neither is feasible, the SlateDB
   `KvSnapshot` impl reads live at each `get`/`scan_prefix` call.
   `ReadCtx` structurally works — it just loses snapshot isolation
   on this backend. Document in `S3.md` and
   `docs/ARCHITECTURE.md`. The M9 `mvcc_isolation.rs` tests gain a
   SlateDB-backend variant that asserts the relaxed contract.

The spike is the first implementation task. The spec doesn't commit
to a tier until the spike picks one.

### Server wiring

```rust
// bin/agentdb-server/src/main.rs (modified)

let object_store: Arc<dyn ObjectStore> = build_object_store(&objects_dir)?;

let kv: Arc<dyn KvStore> = match std::env::var("AGENTDB_KV_BACKEND").as_deref() {
    Ok("slatedb") => {
        if std::env::var("AGENTDB_OBJECT_STORE").is_err() {
            tracing::warn!(
                "AGENTDB_KV_BACKEND=slatedb but AGENTDB_OBJECT_STORE is unset; \
                 defaulting to local filesystem. Set AGENTDB_OBJECT_STORE=s3 \
                 for the production shape."
            );
        }
        let slate_path = std::env::var("AGENTDB_SLATEDB_PATH")
            .unwrap_or_else(|_| "agentdb/slate".to_string());
        Arc::new(
            SlateKvStore::open(object_store.clone(), &slate_path)
                .await
                .context("open slatedb")?,
        )
    }
    Ok("rocksdb") | Err(_) => {
        Arc::new(RocksKvStore::open(&data_dir).context("open rocksdb")?)
    }
    Ok(other) => anyhow::bail!(
        "unknown AGENTDB_KV_BACKEND={other}; expected rocksdb|slatedb"
    ),
};
```

`build_object_store` is a new helper that returns a shared
`Arc<dyn ObjectStore>` for both the cold-segment path (passed to
`BoxRegistry::new`) and — when `AGENTDB_KV_BACKEND=slatedb` — the
SlateDB backing store. One bucket, two prefixes:
`agentdb/slate/...` for the LSM, `boxes/.../cold/...` for segments.

`build_object_store` honors `AGENTDB_OBJECT_STORE`:

- `local` (default): `LocalFileSystem::new_with_prefix(objects_dir)`.
- `s3`: `AmazonS3Builder` populated from the AWS env chain +
  `AGENTDB_S3_BUCKET` / `AGENTDB_S3_REGION` / `AGENTDB_S3_ENDPOINT`.

### Startup liveness probe

Inside `build_object_store`, after constructing the backend, do one
`list(Some(&ObjPath::from("agentdb/")))` call with a small limit.
Fail fast if the probe errors. A server that starts but can't reach
its storage is worse than one that refuses to start. Error message
includes the endpoint + prefix so operators can diagnose config
quickly.

### Testing

**Unit tests** in `crates/agentdb-storage/src/slate.rs` tests module:

- Round-trip a single key.
- `scan_prefix` returns keys in ascending order; bounded to the
  prefix.
- `write_batch` is atomic — exact failure semantics depend on
  SlateDB's batch API; the test asserts whichever contract the crate
  provides and documents it.
- `snapshot()` / `snapshot_owned()` return a view invariant under
  concurrent writes (contract depends on snapshot tier).

**Integration tests** in `crates/agentdb-storage/tests/slate_integration.rs`:

- Mirror the existing `rocks_integration.rs` coverage.
- If the two diverge substantially, extract shared contract logic
  into a `tests/kv_contract.rs` module that both call.

**Cross-backend test** in `crates/agentdb-core/tests/mvcc_isolation.rs`:

- One new `#[tokio::test]` that constructs an `append_and_fetch`
  scenario over `SlateKvStore` and asserts the same invariants as
  the RocksDB path — or a relaxed variant if the SlateDB snapshot
  tier is #3.

**Smoke script** `scripts/smoke-s3.sh`:

- Starts MinIO via `docker run --rm -p 9000:9000 minio/minio server /data`.
- Creates a bucket via `aws --endpoint-url=http://localhost:9000 s3 mb`.
- Exports `AGENTDB_KV_BACKEND=slatedb`, `AGENTDB_OBJECT_STORE=s3`,
  `AGENTDB_S3_ENDPOINT=http://localhost:9000`, `AGENTDB_S3_BUCKET=agentdb-test`,
  `AGENTDB_S3_REGION=us-east-1`, and test creds.
- Runs the existing `scripts/smoke.sh` scenario body.
- `trap` tears MinIO down on exit.
- Exits non-zero on any assertion failure. Not part of CI.

**Benches** in `crates/agentdb-core/benches/mvcc_bench.rs`:

- Existing `scan_1000_events_via_ctx` stays (RocksDB).
- New `scan_1000_events_via_ctx_slatedb` runs the same workload
  against SlateDB-on-LocalFileSystem. `bench_scan_1000_events_via_ctx_s3`
  is *not* added — criterion over a network roundtrip has too much
  variance to be a useful CI bench.

### Docs

- **`docs/S3.md`** (new): env var reference, MinIO quick-start,
  AWS S3 walkthrough (bucket creation + IAM policy example),
  observed latencies, snapshot-tier note, troubleshooting.
- **`docs/ARCHITECTURE.md`**: short paragraph in the "Storage layout"
  section noting the pluggable backend and linking to `S3.md`. No
  changes to the "cold tier" section — tiering is backend-agnostic.
- **`docs/PERFORMANCE.md`**: new "Storage backend comparison" table:
  append p50/p99, fetch-last-N p50/p99, segment-write latency for
  RocksDB-local vs SlateDB-LocalFileSystem. AWS S3 called out as
  "not measured in CI; expect +~30ms per object-store roundtrip."

## Risks / open questions

1. **SlateDB atomic-batch support.** Per user direction, batched
   `write_batch` is required. If the pinned latest-stable SlateDB
   does not expose atomic batches, M11 pauses pending either a
   SlateDB upstream fix or an explicit scope revision (e.g. downgrade
   the `write_batch` contract and document). This is a scope
   decision, not something the spec can paper over.
2. **SlateDB snapshot support.** Unknown at spec time. First task of
   M11 is a one-day implementation spike that picks among the three
   tiers in the Snapshot Strategy section. Relaxed tier #3 documents
   a real capability downgrade on this backend.
3. **SlateDB compaction threads.** SlateDB runs compaction in a
   background tokio task. May require extra worker threads beyond
   what `#[tokio::main]` currently allocates. If bench results show
   compaction starvation, bump the server's default worker thread
   count (currently platform default).
4. **Prototype-scale AWS S3 cost.** Sustained QPS against real S3
   produces non-trivial GET/PUT bills. `S3.md` includes a "disable
   SlateDB when idle" note and a latency-vs-cost table.
5. **M10 (admission control) already landed.** Merged in main at
   `88062e0`. M10 did NOT touch `bin/agentdb-server/src/main.rs`
   near KvStore construction, and `BoxRegistry::new`'s three-argument
   signature is unchanged. M11's proposed `match` block for backend
   selection lands on the existing `let kv: Arc<dyn KvStore> = ...`
   line at `main.rs:78` without modification. No conflict
   materialized during rebase.
6. **Drop close blocks the caller thread.** `SlateKvStore::drop` runs
   `runtime.block_on(db.close())` synchronously. On a server shutting
   down cleanly, this is what we want — the close finishes before
   the process exits. On a long-running flush (large memtable + slow
   object storage), Drop will visibly hang for seconds. Documented in
   `S3.md`; could be hardened later with a timeout + force-kill.

## Success criteria

- All existing `agentdb-core`, `agentdb-wire`, `agentdb-api`, and
  `agentdb-storage` tests pass unchanged under both
  `AGENTDB_KV_BACKEND=rocksdb` (default) and
  `AGENTDB_KV_BACKEND=slatedb` — where snapshot-isolation semantics
  differ, tests split into two variants that assert each backend's
  actual contract.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `scripts/smoke.sh` passes on the RocksDB path.
- `scripts/smoke-s3.sh` passes on the SlateDB+MinIO path when run
  manually.
- `docs/S3.md` exists and documents MinIO local setup end-to-end.
- `docs/PERFORMANCE.md` has the storage-backend comparison section
  with measured numbers from the mvcc_bench output.
- `docs/ARCHITECTURE.md` references the pluggable backend and points
  at `S3.md`.
- `AGENTDB_KV_BACKEND=slatedb AGENTDB_OBJECT_STORE=s3 AGENTDB_S3_*=...
  cargo run --bin agentdb-server` starts, serves an
  `append_and_fetch` request, and the cold-tier sweep writes
  segments to the S3 bucket.
