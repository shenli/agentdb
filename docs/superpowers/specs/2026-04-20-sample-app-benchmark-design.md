# Sample App + Benchmark Harness — Design

_Date: 2026-04-20_
_Status: design, implementation underway._

> **Scope change 2026-04-20 (during implementation):** Bench 4 is
> **vector-only retrieval with a tenant filter**, not hybrid BM25+vector.
> AgentDB has no single-RPC hybrid endpoint — the `HybridPlanner` exists
> in core but is not exposed over REST, and the Mongo wire protocol
> handles text (`$search`) and vector (`$vectorSearch`) as separate
> pipeline stages. Follow-up: add `/api/query/hybrid` and re-enable
> hybrid as a second retrieval bench. Throughout the rest of this doc,
> read "hybrid retrieval" as "vector retrieval with tenant filter."

## Purpose

AgentDB's prototype currently proves its primitives with synthetic traffic
(a customer-support workload generator + a single-op perf probe). Nothing
in the repo tells a viewer **why** an agent would want these primitives or
**where** a Postgres-based PoC would fall over on the same workload.

This spec defines two companion deliverables that close that gap:

1. A **scripted research-agent demo** (Python) that exercises every AgentDB
   primitive — `append_and_fetch`, session fork, hybrid retrieval, per-box
   isolation, warm→cold tiering (as observed via the dashboard) — in a
   narrative a Python-native agent builder can skim.
2. A **benchmark harness** (Go) that runs four comparison benchmarks
   against AgentDB and two Postgres schemas (naive jsonb-blob +
   best-effort-tuned with pgvector HNSW and tsvector GIN), publishing
   percentile latencies, throughput, and — for the retrieval bench —
   recall@10 vs. a committed ground-truth.

The two audiences (blog/README skimmer + technical evaluator) are served
by one repo, two entry points.

## Non-goals

- **No LLM in the loop.** The demo is deterministic and scripted; tool
  calls come from a fixed catalog keyed by seed. A live-LLM variant is a
  later-milestone concern.
- **No live-updating benchmark UI.** The `/benchmarks` page is a static
  report of the last completed run. A live side-by-side dashboard is a
  possible v2.
- **No absolute "which DB is faster" claim.** The benchmark is
  characterization of a specific workload mix; hardware and tuning move
  numbers.
- **No warm/cold tier benchmark comparison.** Postgres tiering = table
  partitioning or external archive; a fair head-to-head isn't possible.
  Tiering stays a narrative element visible in the existing dashboard.
- **No new AgentDB primitives.** This work only consumes the existing
  REST/primitive surface.

## Scope summary (from brainstorming)

| Decision | Choice |
|---|---|
| Audience | A+C — blog/README reader + technical evaluator |
| Agent storyline | Branching research agent (question → 3 plans → fork per plan → 5–10 tool calls per branch → hybrid retrieval → winner) |
| Postgres baselines | Two schemas: naive (`sessions(jsonb messages)`) + tuned (`events` + pgvector HNSW + tsvector GIN) |
| Benchmark set | Four benches — hot-session append+fetch; fork-cost scaling; append-latency vs. payload size; hybrid retrieval |
| Presentation | Markdown + static HTML + Chart.js page served at `/benchmarks` |
| Dataset | Committed fixture — ~10k short real docs + precomputed 384-dim embeddings |
| Demo language | Python (sync, `httpx`) — matches the Python-native agent-builder audience |
| Bench language | Go — extends existing `workload/` module, uses pgx |

## Architecture

### Repo layout

```
workload/                            existing Go module
├── cmd/
│   ├── agentdb-workload              (unchanged)
│   ├── perf-probe                    (unchanged)
│   └── agentdb-bench                 NEW — Go bench runner, writes JSON
├── internal/
│   ├── backend/                      NEW — Go Backend iface + 3 impls
│   │   ├── backend.go
│   │   ├── agentdb.go
│   │   ├── pg_naive.go
│   │   └── pg_tuned.go
│   ├── bench/                        NEW — 4 bench implementations
│   │   ├── histogram.go
│   │   ├── hotsession.go
│   │   ├── forkscaling.go
│   │   ├── largevalue.go
│   │   └── hybridretrieval.go
│   ├── fixture/                      NEW — loads ../benchmarks/fixtures/corpus.jsonl
│   └── client/                       (unchanged)
└── go.mod

demo/                                NEW top-level Python project
├── pyproject.toml                     deps: httpx, numpy, rich
├── README.md
├── agentdb_demo/
│   ├── __init__.py
│   ├── client.py                      sync httpx AgentDB client
│   ├── fixture.py                     loads ../benchmarks/fixtures/{corpus,queries}.jsonl
│   ├── tools.py                       scripted tool catalog — deterministic
│   ├── agent.py                       research-agent state machine
│   └── narrator.py                    rich stdout + JSONL to demo-out/
├── cli.py                             `python -m agentdb_demo`
└── tests/
    ├── test_agent.py                  golden trace assertion
    └── test_tools.py                  unit tests for scripted tools

benchmarks/                          NEW top-level data + orchestration
├── fixtures/
│   ├── generate.py                     sentence-transformers → jsonl
│   ├── corpus.jsonl                    ~10k docs + 384-dim embeddings (committed)
│   └── queries.jsonl                   canned queries + ground-truth doc ids (committed)
├── postgres/
│   ├── naive.sql                       sessions(id, messages jsonb)
│   ├── tuned.sql                       events + pgvector HNSW + tsvector GIN
│   ├── postgresql.conf                 modest tuning overrides
│   ├── docker-compose.yml              Postgres 17 + pgvector
│   └── README.md
├── run.sh                              orchestrator
└── README.md

crates/agentdb-dashboard/
├── static/
│   ├── index.html                    (unchanged)
│   ├── benchmarks.html               NEW — Chart.js page
│   └── benchmarks.js                 NEW
└── src/lib.rs                        (small) add /benchmarks route

docs/
├── DEMO.md                           NEW — narrative walkthrough
├── BENCHMARKS.md                     NEW — methodology + numbers + schema appendix
└── (existing docs unchanged)

README.md                            UPDATED — new "Demo" + "Benchmarks" sections

bench-out/                           gitignored
```

### Dependency direction

- `researchagent` and `bench` both depend only on `backend` (or the
  equivalent thin client in Python).
- `backend` depends only on `client` and `fixture`.
- No reverse edges. Python demo imports nothing from Go; Go bench imports
  nothing from Python.
- The only shared artifact across languages is
  `benchmarks/fixtures/*.jsonl` — data, not code.

### Wire fairness

Every backend talks over its native TCP protocol:
- AgentDB: REST over HTTP (`workload/internal/client/rest.go`, `httpx` in Python)
- Postgres naive and tuned: pgx/v5

No in-process shortcuts for AgentDB in the benches. The comparison is
honest at the wire layer viewers care about.

### Fixture pipeline

```
benchmarks/fixtures/generate.py
  ├── HuggingFace datasets → ~10k short docs (e.g., arXiv abstracts)
  ├── sentence-transformers/all-MiniLM-L6-v2 → 384-dim embedding per doc
  ├── ~200 canned queries with ground-truth doc ids (manually curated or BM25-seeded)
  └── writes → benchmarks/fixtures/{corpus,queries}.jsonl  (committed, ~50 MB)
```

The committed fixture IS the dataset. `generate.py` is kept so a curious
reader can regenerate with a different embedding model.

## Components

### `workload/internal/backend/` (Go)

Common interface for the three benchmark backends:

```go
type Backend interface {
    Name() string
    Reset(ctx) error
    Append(ctx, tenant, session, event) (seq int, err error)
    AppendAndFetch(ctx, tenant, session, event, lastN int) ([]Event, error)
    Fork(ctx, tenant, parent, child string) error
    FetchLastN(ctx, tenant, session string, n int) ([]Event, error)
    HybridQuery(ctx, tenant, session, text string, vec []float32, k int) ([]Hit, error)
    Close() error
}
```

- `agentdb.go` — reuses existing `workload/internal/client/rest.go`.
- `pg_naive.go` — pgx/v5, applies `naive.sql` on `Reset()`.
- `pg_tuned.go` — pgx/v5, applies `tuned.sql` on `Reset()`.
- Each impl owns its own pgxpool; no shared state.

### `workload/internal/bench/` (Go)

Four files, one per benchmark. Each exports `Run(backend Backend, cfg Config) Report`.

- `histogram.go` — HDR histogram wrapper (codahale/hdrhistogram-go);
  percentile helper.
- `hotsession.go`, `forkscaling.go`, `largevalue.go`,
  `hybridretrieval.go` — see §Benchmark methodology.

Reports carry: per-backend histogram, throughput, error count, config
echo. No cross-bench shared state.

### `workload/cmd/agentdb-bench` (Go binary)

CLI flags:
- `--backends=agentdb,pg_naive,pg_tuned`
- `--bench=all|hotsession|fork|largevalue|retrieval`
- `--concurrency=1,4,16,64` (default sweep)
- `--duration=15s`
- `--warmup-frac=0.1`
- `--out=bench-out/`
- `--agentdb-url`, `--pg-dsn`

Runs `bench.Run` for each `(bench, backend, concurrency)` cell in
sequence (not in parallel), merges results into `bench-out/data.json`.

### `demo/agentdb_demo/` (Python)

- **`client.py`** — sync `httpx` client wrapping `append_and_fetch`,
  `fork`, hybrid query. ~60 lines, reads like example code.
- **`fixture.py`** — memoized loaders for `corpus.jsonl` and
  `queries.jsonl`.
- **`tools.py`** — deterministic tool catalog:
  `web_search(query, seed) -> list[Doc]`,
  `read_doc(doc_id) -> str`,
  `summarize(text, seed) -> str`.
  Seeded RNG + fixture-backed; the agent's trace is reproducible from
  `--seed`.
- **`agent.py`** — state machine:
  ```
  run(question, seed):
      plans = propose_plans(question, seed, n=3)
      parent = start_session("research-" + hash(question))
      append_and_fetch(parent, QUESTION event, n=0)
      for i, plan in enumerate(plans):
          child = fork(parent, child_id=f"plan-{i}")
          for step in plan.steps:
              result = run_tool(step, seed)      # large payload: full doc text
              append_and_fetch(child, TOOL_RESULT event, n=0)
      hits = hybrid_query(tenant, text=question, vec=embed(question), k=10)
      winner = pick_winner(hits, plans)
      narrator.emit(...)
  ```
  No async, no threads.
- **`narrator.py`** — pair of sinks: rich-formatted stdout timeline +
  JSONL to `demo-out/run-{ts}.jsonl`.
- **`cli.py`** — `python -m agentdb_demo --question ... --seed N --agentdb-url ...`.

### `benchmarks/postgres/`

- `naive.sql`, `tuned.sql` — see §Postgres schemas.
- `postgresql.conf` — modest tuning overrides (committed).
- `docker-compose.yml` — Postgres 17.x + pgvector pinned.

### `benchmarks/run.sh`

Six steps:
1. Start/verify `agentdb-server`.
2. Start/verify Postgres via `docker compose up -d postgres`.
3. Apply `naive.sql` and `tuned.sql` to separate schemas.
4. Run `agentdb-bench --bench=all`.
5. Merge per-bench JSONs → `bench-out/data.json`; copy to
   `crates/agentdb-dashboard/static/data.json`.
6. Print summary table.

No Docker automation magic beyond standard `docker compose`.

### `crates/agentdb-dashboard/static/benchmarks.{html,js}`

- Chart.js via CDN. Four charts (one per bench). Fetches
  `/benchmarks/data.json`.
- If `data.json` is missing, renders a friendly
  "no run yet — run `bash benchmarks/run.sh`" banner.
- `lib.rs` gets a small change to nest a second `ServeDir` at
  `/benchmarks`.

## Data flow

### Demo run

```
$ python -m agentdb_demo --question "How does HNSW work?" --seed 42
```

1. CLI loads fixtures once into memory.
2. `append_and_fetch` with QUESTION event creates tenant + session on
   first write.
3. Agent synthesizes 3 plans deterministically from seed + corpus.
4. For each plan: `fork` at the QUESTION event; for each of N ∈ [5,10]
   tool steps (seeded), `append_and_fetch(TOOL_RESULT, n=0)` with the
   tool result's full doc text (16–64 KB) as payload.
5. One hybrid query across all branches; pick a winner; append FINAL
   event to parent; flush narrator.
6. Artifacts: `demo-out/run-{ts}.jsonl` + stdout. The running
   `/dashboard` shows the forks appearing as sessions and metric ticks
   live.

### Bench run

```
$ bash benchmarks/run.sh
```

1. `run.sh` brings up AgentDB + Postgres.
2. Applies `naive.sql` and `tuned.sql`.
3. Loads corpus into each Postgres schema's events table (setup, not
   measured).
4. For each `(bench, backend, concurrency)`: `Reset()`, warm-up (10% of
   duration, discarded), measured phase, HDR histogram recorded.
5. Writes `bench-out/{bench}.json` per bench, then merges to
   `bench-out/data.json`.
6. Viewer opens `/benchmarks` → Chart.js renders.

### Shared fixture

Both demo and benches read `benchmarks/fixtures/*.jsonl`. Event payloads
use doc texts from the corpus; retrieval queries come from
`queries.jsonl` with ground-truth doc ids.

### Explicitly NOT in the flow

- No cross-process coordination between demo and benches.
- No live-updating `bench-data.json`.
- Postgres is never queried by the demo — demo is AgentDB-only; comparison
  is the bench harness's job.

## Benchmark methodology

### Shared rig

- Same machine, same run. All backends run locally; bench runner targets
  each in sequence, not in parallel.
- Every bench: `for op in workload: histogram.Record(time(backend.Op(...)))`.
- `Reset()` before each bench; 10% warm-up discarded.
- Payloads come from `corpus.jsonl`, sampled by seed; payload sizes
  identical across backends for a given bench step.
- Concurrency sweep default: 1, 4, 16, 64. Each goroutine has its own
  connection.
- Reported per cell: p50, p90, p99, max latency; throughput; error count.
  No averaging across backends or concurrencies.

### Bench 1 — Hot session `append_and_fetch`

- Op: append one 512-byte event, fetch last 5. One tenant, one session;
  every goroutine hammers the same session.
- AgentDB: single `append_and_fetch` REST call.
- pg_naive: `UPDATE sessions SET messages = messages || $1 WHERE id = $2`
  + followup `SELECT jsonb_path_query_array(...)` for last-5. Row-lock
  contention on every update; full jsonb array rewrites.
- pg_tuned: `INSERT INTO events ... RETURNING seq` +
  `SELECT ... FROM events WHERE session_id = $1 ORDER BY seq DESC LIMIT 5`
  in one tx.
- Expected: AgentDB's single-roundtrip + box-lock advantage vs. Postgres
  row-lock contention.

### Bench 2 — Fork cost scaling

- Op: fork a parent session containing N ∈ {10, 100, 1 000, 10 000}
  events. Measure fork call alone; parent pre-populated in setup.
- AgentDB: `POST /api/sessions/.../fork` → one new session record with a
  `ForkRef`. Expected O(1).
- pg_naive: `INSERT INTO sessions(id, messages) SELECT $new_id, messages FROM sessions WHERE id = $1`.
  O(N) jsonb copy.
- pg_tuned: `INSERT INTO events ... SELECT ... FROM events WHERE session_id = $1`.
  O(N) row copy.
- Expected: AgentDB flat as N grows; both Postgres schemas climb
  linearly.

### Bench 3 — Append latency vs. payload size

- Op: append one event of size S ∈ {1 KB, 16 KB, 256 KB}. One tenant,
  one session per concurrency worker (no shared hot session — this bench
  isolates payload effect from contention).
- AgentDB: `append_and_fetch` with `fetch_last_n=0`.
- pg_naive: `UPDATE ... messages = messages || $1`. 256 KB forces
  TOAST decompression on jsonb read every write.
- pg_tuned: `INSERT INTO events ...`. TOASTs cleanly.
- Expected: AgentDB near-flat to 256 KB; pg_naive climbs hard;
  pg_tuned climbs slowly.

### Bench 4 — Hybrid retrieval (BM25 + vector + tenant filter)

- Op: given a query from `queries.jsonl`, return top-10 events matching
  text AND vector kNN, filtered to one tenant.
- AgentDB: `POST /api/query/hybrid` — pre-filters by tenant, then runs
  BM25 ∪ cosine.
- pg_naive:
  ```
  SELECT id FROM sessions
  WHERE tenant_id=$1
    AND to_tsvector(messages::text) @@ $2
  ORDER BY (messages -> 'embedding')::vector <=> $3
  LIMIT 10
  ```
  Honestly ugly; that's the point.
- pg_tuned:
  ```
  SELECT id FROM events
  WHERE tenant_id=$1 AND tsv @@ $2
  ORDER BY embedding <=> $3
  LIMIT 10
  ```
  with BTREE `(tenant_id)`, GIN `tsv`, HNSW `embedding`.
- Measured: latency + recall@10 vs. ground truth. Recall is disclosed,
  not scored as win/loss. HNSW parameters on the Postgres side
  (`m`, `ef_construction`) are set to match whatever AgentDB uses at
  bench time; exact values — and any env-var knobs needed on AgentDB
  (`AGENTDB_HNSW_EF`, etc.) to reach parity — land in `BENCHMARKS.md`
  after the first run. Where implementations differ in semantics
  (e.g., `ef_search` defaults), the delta is documented rather than
  hidden.

### What we NOT control

- No `postgresql.conf` tuning beyond committed defaults. No `pg_prewarm`.
- OS-level noise: one run per config. No statistical significance claims
  across runs — just percentile ranks within a single run.
- No absolute "which DB is faster" claim. Numbers move with hardware and
  tuning.

## Postgres schemas

### `naive.sql` — day-one PoC

```sql
CREATE SCHEMA IF NOT EXISTS bench_naive;
SET search_path TO bench_naive;

CREATE TABLE tenants (id TEXT PRIMARY KEY);

CREATE TABLE sessions (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants(id),
    parent_id  TEXT REFERENCES sessions(id),
    messages   JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_sessions_tenant ON sessions(tenant_id);
```

- One row per session; events inside `messages` jsonb array.
- No vector column, no FTS index.
- Append = `UPDATE ... messages = messages || $1` — row-lock contention.
- Fork = `INSERT ... SELECT` — copies the messages array.

This is deliberately the shortest reasonable day-one schema.

### `tuned.sql` — serious Postgres

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS bench_tuned;
SET search_path TO bench_tuned;

CREATE TABLE tenants (id TEXT PRIMARY KEY);

CREATE TABLE sessions (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id),
    parent_id   TEXT REFERENCES sessions(id),
    fork_seq    BIGINT,                   -- parent seq at fork, NULL if not forked
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_sessions_tenant ON sessions(tenant_id);

CREATE TABLE events (
    session_id  TEXT NOT NULL REFERENCES sessions(id),
    seq         BIGINT NOT NULL,
    tenant_id   TEXT NOT NULL,            -- denormalized for index-only retrieval
    payload     BYTEA NOT NULL,
    text        TEXT,
    tsv         tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(text,''))) STORED,
    embedding   vector(384),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, seq)
);

CREATE INDEX ix_events_tenant_seq ON events(tenant_id, seq DESC);
CREATE INDEX ix_events_session_seq ON events(session_id, seq DESC);
CREATE INDEX ix_events_tsv ON events USING GIN (tsv);
CREATE INDEX ix_events_vec ON events USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 200);
```

- Row-per-event; appends are INSERT.
- Fork copies events — unavoidable without CoW/parent-pointer (that's the
  benchmark's point).
- `tsv` generated column → full-text is free at write time.
- `embedding` HNSW index uses `m=16, ef_construction=200` as a starting
  point; final values are tuned to match AgentDB's at bench time and
  committed alongside the numbers (see §Benchmark methodology, Bench 4).
- `tenant_id` denormalized on events so retrieval hits one table with a
  covering index.

### `postgresql.conf` overrides

```
shared_buffers = 512MB
work_mem = 16MB
maintenance_work_mem = 128MB
max_connections = 200
random_page_cost = 1.1
effective_cache_size = 4GB
```

Committed; applied via docker-compose mount. Not exotic.

### What we don't do to Postgres on purpose

- No partitioning, no pre-aggregation, no bulk-load hacks.
- Setup uses normal COPY; measured ops use normal INSERT/UPDATE/SELECT.
- No prepared-statement warmup beyond pgx's default first-use preparation.

## Error handling

### Benchmarks (Go)

- Every op returns an error; bench loop counts errors but does not stop
  on a single failure.
- Final report includes `errors: N`. Nonzero N is surfaced prominently
  and fails CI.
- Setup-time errors (Reset, schema apply) are fatal.
- AgentDB 429s count as errors, not retries. Rationale: benches
  characterize DB behavior at offered load, not well-behaved-client
  throughput. Backoff masks the signal.
- Postgres SerializationFailure / DeadlockDetected also count as errors.

### Demo (Python)

- HTTP errors surface as exceptions; agent retries transient 5xx once,
  then propagates with a red-formatted narrator event.
- Fixture load failures are fatal at startup with a readable message
  pointing at `benchmarks/fixtures/generate.py`.
- Scripted tools are deterministic → no failures, no retry layer.

### Report page

- Missing/malformed `bench-data.json` → friendly
  "no run yet" banner, not a blank page or cryptic JS error.

### Explicitly not done

- No circuit breakers, no exponential backoff. Benches are meant to
  overrun; demos are meant to be deterministic; backoff masks both.

## Testing

### Benchmark harness

- `workload/internal/backend/*_test.go` — table-driven conformance tests.
  Every Backend impl passes the same ~10 assertions (append→fetch; fork
  then fetch child sees parent prefix; hybrid query returns ≥1 result
  for a query with ground-truth matches; etc.). Catches SQL bugs and
  silent no-ops.
- `workload/internal/bench/*_test.go` — smoke tests: run each bench
  against AgentDB with `duration=1s`, assert well-formed report
  (non-nil histogram, latency > 0, errors == 0).
- Run in CI against docker-compose Postgres. Fixture loading uses a
  10-doc subset for CI speed.

### Demo

- `demo/tests/test_agent.py` — given a fixed seed, `agent.run(question)`
  produces a deterministic JSONL trace. Compare to a committed golden
  file.
- `demo/tests/test_tools.py` — scripted tools are pure functions of
  `(inputs, seed)`; trivial unit tests.
- CI spins up AgentDB via `scripts/smoke.sh`-style pattern.

### Report page

- No automated tests for v1. Validated manually against sample
  `bench-data.json` fixtures. Playwright smoke test (pattern already in
  use for the main dashboard) is the obvious follow-up if it grows.

### What's NOT tested

- Actual benchmark numbers. Perf regressions in AgentDB stay caught by
  existing `crates/agentdb-core/benches/mvcc_bench.rs`. The comparison
  harness produces reports, not assertions — CI does not fail on
  "AgentDB got 10% slower."
- Postgres version pinning churn. `docker-compose.yml` pins Postgres 17.x
  + pgvector version; upgrades are intentional baseline refreshes.

### CI changes

```
cargo test --workspace                    # unchanged
cargo clippy --workspace -- -D warnings   # unchanged
cd workload && go test ./...              # gains backend + bench tests
cd demo && pytest                         # NEW
bash scripts/smoke.sh                     # unchanged
```

## Open questions

None at design sign-off. Follow-ups that are out of scope but worth
remembering:

- Live side-by-side demo UI (dashboard-style, both backends at once) —
  if a showcase opportunity arises.
- Real-LLM variant of the demo with an embedding API / local model — if
  we want to stop being deterministic for a talk.
- Additional benchmarks (e.g., warm→cold tier cost story, end-to-end
  agent-run wall time) — if the four benches leave gaps in the
  narrative.

## References

- `docs/ARCHITECTURE.md` — AgentDB primitives overview.
- `docs/PERFORMANCE.md` — AgentDB-alone numbers; this spec's Bench 1
  overlaps its throughput suite in design but is a broader comparison.
- `docs/agent-db.md` — original design doc; §1.1 characterizes the
  workload mix these benches instantiate.
