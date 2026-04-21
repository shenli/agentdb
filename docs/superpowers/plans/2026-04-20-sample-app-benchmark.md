# Sample App + Benchmark Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Scope change 2026-04-20 (mid-execution):** Bench 4 is **vector-only retrieval with a tenant filter**, not hybrid BM25+vector. AgentDB has no single-RPC hybrid surface: no REST endpoint, and the Mongo wire protocol keeps `$search` (text) and `$vectorSearch` (vector) as separate stages. Implementation details below have been edited in-place to reflect this: `Backend.HybridQuery` is renamed to `VectorQuery` with no `text` parameter; the AgentDB backend impl uses the Mongo wire `$vectorSearch` stage (via `go.mongodb.org/mongo-driver`, already a workload dependency) instead of a REST call; `pg_tuned` drops the `tsv @@` clause; the Python demo adds `pymongo` for its own vector call. Follow-up for a future milestone: add `/api/query/hybrid` and re-enable true hybrid as a fifth bench.

**Goal:** Ship a scripted Python research-agent demo + a Go benchmark harness that compares AgentDB head-to-head against two Postgres schemas (jsonb-blob naive + row-per-event + pgvector/tsvector tuned) across four benches (hot-session append+fetch, fork cost scaling, append latency vs. payload size, hybrid retrieval), all driven by a committed corpus + embeddings fixture and visualised by a static HTML report served at `/benchmarks`.

**Architecture:** Three phases executed in order. Phase 1 ships the fixture pipeline (Python) and commits the corpus JSONL files — both phases 2 and 3 consume these. Phase 2 ships the Go benchmark harness (extends `workload/`), the Postgres schemas + docker compose, the orchestrator script, the HTML report page, and `docs/BENCHMARKS.md`. Phase 3 ships the Python demo (`demo/`), `docs/DEMO.md`, and updates the top-level `README.md`. Each phase is independently testable; checkpoints at the end of each phase let the user review before continuing.

**Tech Stack:**
- Go 1.26 (existing `workload/` module) — bench harness
- pgx/v5 — Postgres driver; codahale/hdrhistogram-go — latency histograms
- Python 3.11+ — demo, fixture generation
- `httpx` (sync), `rich`, `numpy`, `pytest` — demo + fixture deps
- `sentence-transformers/all-MiniLM-L6-v2` — 384-dim embeddings
- Postgres 17.x + pgvector — comparison baselines via docker compose
- Chart.js via CDN — static HTML report
- Existing Rust `axum::Router` + `ServeDir` pattern — report page serving

**Spec reference:** `docs/superpowers/specs/2026-04-20-sample-app-benchmark-design.md` (commit `df60a2a`).

**Before starting implementation:** This plan is large. Create a feature branch (`feature/sample-app-benchmark`) and do all work there. The plan assumes the branch is checked out.

---

## Phase 1 — Fixture Pipeline

Goal: a committed `benchmarks/fixtures/{corpus,queries}.jsonl` that both Phase 2 and Phase 3 read. Standalone deliverable: someone could regenerate the corpus with a different embedding model by running `generate.py`.

### Task 1: Create `benchmarks/fixtures/requirements.txt`

**Files:**
- Create: `benchmarks/fixtures/requirements.txt`

- [ ] **Step 1: Create the requirements file**

```text
datasets>=2.18
sentence-transformers>=2.6
numpy>=1.26
```

Pinning minor versions is enough — the dataset + model choice is the reproducibility lever, not library version. Hugging Face `datasets` provides arXiv abstracts; `sentence-transformers` loads `all-MiniLM-L6-v2`.

- [ ] **Step 2: Commit**

```bash
git add benchmarks/fixtures/requirements.txt
git commit -m "feat(bench): fixture generator requirements"
```

### Task 2: Write `benchmarks/fixtures/generate.py`

**Files:**
- Create: `benchmarks/fixtures/generate.py`

- [ ] **Step 1: Write the generator**

```python
#!/usr/bin/env python3
"""Build benchmarks/fixtures/{corpus,queries}.jsonl.

Pulls ~10k short docs from a Hugging Face dataset, embeds each with
sentence-transformers/all-MiniLM-L6-v2 (384 dims, cosine-normal),
and writes one JSON object per line. Also picks ~200 queries and
records ground-truth doc ids via brute-force cosine similarity on
the embeddings themselves.

Outputs land in the same directory as this script.
"""

from __future__ import annotations

import argparse
import json
import os
import random
from pathlib import Path

import numpy as np
from datasets import load_dataset
from sentence_transformers import SentenceTransformer

HERE = Path(__file__).resolve().parent


def build_corpus(n_docs: int, seed: int) -> list[dict]:
    ds = load_dataset("ccdv/arxiv-classification", split="train", streaming=True)
    rng = random.Random(seed)
    seen_texts: set[str] = set()
    docs: list[dict] = []
    for row in ds:
        text = (row.get("text") or "").strip().replace("\n", " ")
        if not text or text in seen_texts:
            continue
        # Keep abstracts-length: trim to ~2 KB to keep fixture size reasonable.
        text = text[:2048]
        seen_texts.add(text)
        docs.append({"id": f"doc-{len(docs):05d}", "text": text})
        if len(docs) >= n_docs:
            break
    rng.shuffle(docs)
    return docs


def embed(docs: list[dict], model_name: str) -> np.ndarray:
    model = SentenceTransformer(model_name)
    texts = [d["text"] for d in docs]
    vecs = model.encode(texts, batch_size=64, show_progress_bar=True,
                        normalize_embeddings=True).astype(np.float32)
    return vecs


def pick_queries(docs: list[dict], vecs: np.ndarray, n_queries: int, k: int,
                 seed: int) -> list[dict]:
    """Use ~first sentence of randomly-picked docs as a query, record top-k
    via brute-force cosine as ground truth. vecs is already L2-normalised so
    cosine == dot."""
    rng = random.Random(seed)
    idx_pool = list(range(len(docs)))
    rng.shuffle(idx_pool)
    queries: list[dict] = []
    for i in idx_pool[:n_queries]:
        text = docs[i]["text"].split(".")[0][:256]
        if not text:
            continue
        q_vec = vecs[i]
        sims = vecs @ q_vec
        top = np.argpartition(-sims, k)[:k]
        top = top[np.argsort(-sims[top])]
        ground_truth = [docs[int(j)]["id"] for j in top]
        queries.append({
            "id": f"q-{len(queries):04d}",
            "text": text,
            "ground_truth": ground_truth,
        })
        if len(queries) >= n_queries:
            break
    return queries


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n-docs", type=int, default=10_000)
    p.add_argument("--n-queries", type=int, default=200)
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    p.add_argument("--seed", type=int, default=20260420)
    args = p.parse_args()

    docs = build_corpus(args.n_docs, args.seed)
    vecs = embed(docs, args.model)
    for d, v in zip(docs, vecs):
        d["embedding"] = v.tolist()
    queries = pick_queries(docs, vecs, args.n_queries, args.k, args.seed)

    corpus_path = HERE / "corpus.jsonl"
    queries_path = HERE / "queries.jsonl"
    with corpus_path.open("w") as f:
        for d in docs:
            f.write(json.dumps(d) + "\n")
    with queries_path.open("w") as f:
        for q in queries:
            f.write(json.dumps(q) + "\n")

    print(f"wrote {len(docs)} docs → {corpus_path}")
    print(f"wrote {len(queries)} queries → {queries_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Make executable**

```bash
chmod +x benchmarks/fixtures/generate.py
```

- [ ] **Step 3: Commit**

```bash
git add benchmarks/fixtures/generate.py
git commit -m "feat(bench): fixture generator (arxiv abstracts + MiniLM embeddings + ground-truth queries)"
```

### Task 3: Generate the fixture and commit it

- [ ] **Step 1: Create a fresh venv and install requirements**

```bash
cd benchmarks/fixtures
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

Expected: no dependency resolution errors. First run of `sentence-transformers` will download the model (~80 MB) into `~/.cache/huggingface`.

- [ ] **Step 2: Generate corpus and queries**

```bash
python3 generate.py
```

Expected output ending with:
```
wrote 10000 docs → /Users/.../benchmarks/fixtures/corpus.jsonl
wrote 200 queries → /Users/.../benchmarks/fixtures/queries.jsonl
```

- [ ] **Step 3: Sanity-check the files**

```bash
ls -lh corpus.jsonl queries.jsonl
head -c 500 corpus.jsonl
wc -l corpus.jsonl queries.jsonl
```

Expected: corpus ~30–60 MB, queries a few hundred KB. `head` should show one JSON-per-line with `id`, `text`, `embedding` keys. `wc -l` shows 10000 and 200.

- [ ] **Step 4: Deactivate + leave the venv in place (but not committed)**

```bash
deactivate
```

- [ ] **Step 5: Add `.venv/` to the fixture `.gitignore`**

Create `benchmarks/fixtures/.gitignore`:

```
.venv/
```

- [ ] **Step 6: Commit the fixture files + .gitignore**

```bash
git add benchmarks/fixtures/.gitignore benchmarks/fixtures/corpus.jsonl benchmarks/fixtures/queries.jsonl
git commit -m "feat(bench): commit corpus + ground-truth queries fixture"
```

Note: the commit will be large (~50 MB). Verify with `git log --stat HEAD^..HEAD`.

### Task 4: Write `benchmarks/fixtures/README.md`

**Files:**
- Create: `benchmarks/fixtures/README.md`

- [ ] **Step 1: Create the README**

```markdown
# Benchmark Fixtures

`corpus.jsonl` and `queries.jsonl` are **committed** — Phase 2 (Go bench)
and Phase 3 (Python demo) both read them directly. You do not need to
regenerate them to run benchmarks or the demo.

## Regenerating with a different model

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 generate.py --model sentence-transformers/all-MiniLM-L6-v2
```

Flags:
- `--n-docs` (default 10000)
- `--n-queries` (default 200)
- `--k` top-k for ground-truth (default 10)
- `--model` any sentence-transformers model; the `embedding` dim must
  match AgentDB's configured dim (currently 384). If you pick a
  different-dim model, also change the Postgres schemas
  (`benchmarks/postgres/tuned.sql`) and AgentDB's index config.
- `--seed` RNG seed (default 20260420)

## File format

Each line of `corpus.jsonl`:

```json
{"id": "doc-00000", "text": "...", "embedding": [0.01, -0.02, ...]}
```

Each line of `queries.jsonl`:

```json
{"id": "q-0000", "text": "...", "ground_truth": ["doc-01234", "doc-00567", ...]}
```

`ground_truth` is the top-10 doc ids by brute-force cosine similarity
over the embeddings at generation time. Used for recall@10 in Bench 4.
```

- [ ] **Step 2: Commit**

```bash
git add benchmarks/fixtures/README.md
git commit -m "docs(bench): fixture README"
```

### Checkpoint: Phase 1 complete

At this point:
- `benchmarks/fixtures/{corpus,queries}.jsonl` exist and are committed
- `generate.py` can regenerate them with a different model
- Both Phase 2 and Phase 3 can proceed in any order; they only share the fixture

---

## Phase 2 — Benchmark Harness

Goal: `bash benchmarks/run.sh` produces `bench-out/data.json` comparing AgentDB + 2 Postgres schemas across 4 benches; a static HTML page at `/benchmarks` renders it.

### Task 5: Postgres schemas and docker compose

**Files:**
- Create: `benchmarks/postgres/naive.sql`
- Create: `benchmarks/postgres/tuned.sql`
- Create: `benchmarks/postgres/postgresql.conf`
- Create: `benchmarks/postgres/docker-compose.yml`
- Create: `benchmarks/postgres/README.md`

- [ ] **Step 1: Write `naive.sql`**

```sql
-- Day-one PoC schema: one jsonb array per session.
-- Applied into its own schema so naive and tuned can coexist in one DB.

CREATE SCHEMA IF NOT EXISTS bench_naive;
SET search_path TO bench_naive;

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sessions (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants(id),
    parent_id  TEXT REFERENCES sessions(id),
    messages   JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_sessions_tenant ON sessions(tenant_id);
```

- [ ] **Step 2: Write `tuned.sql`**

```sql
-- Serious Postgres: row-per-event, pgvector HNSW, tsvector GIN.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS bench_tuned;
SET search_path TO bench_tuned;

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sessions (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id),
    parent_id   TEXT REFERENCES sessions(id),
    fork_seq    BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_sessions_tenant ON sessions(tenant_id);

CREATE TABLE IF NOT EXISTS events (
    session_id  TEXT NOT NULL REFERENCES sessions(id),
    seq         BIGINT NOT NULL,
    tenant_id   TEXT NOT NULL,
    payload     BYTEA NOT NULL,
    text        TEXT,
    tsv         tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(text,''))) STORED,
    embedding   vector(384),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, seq)
);

CREATE INDEX IF NOT EXISTS ix_events_tenant_seq  ON events(tenant_id, seq DESC);
CREATE INDEX IF NOT EXISTS ix_events_session_seq ON events(session_id, seq DESC);
CREATE INDEX IF NOT EXISTS ix_events_tsv         ON events USING GIN (tsv);
CREATE INDEX IF NOT EXISTS ix_events_vec         ON events USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 200);
```

- [ ] **Step 3: Write `postgresql.conf`**

```
shared_buffers = 512MB
work_mem = 16MB
maintenance_work_mem = 128MB
max_connections = 200
random_page_cost = 1.1
effective_cache_size = 4GB
```

- [ ] **Step 4: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: pgvector/pgvector:pg17
    container_name: agentdb-bench-postgres
    environment:
      POSTGRES_USER: bench
      POSTGRES_PASSWORD: bench
      POSTGRES_DB: bench
    ports:
      - "5433:5432"
    volumes:
      - ./postgresql.conf:/etc/postgresql/postgresql.conf:ro
    command:
      - postgres
      - -c
      - config_file=/etc/postgresql/postgresql.conf
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bench -d bench"]
      interval: 2s
      timeout: 2s
      retries: 30
```

Port 5433 so it doesn't clash with a local Postgres on 5432.

- [ ] **Step 5: Write `README.md`**

```markdown
# Benchmark Postgres

Postgres 17 + pgvector, local container on port 5433.

## Start

```bash
docker compose up -d postgres
docker compose logs -f postgres   # wait for "database system is ready"
```

Healthcheck passes within a few seconds; `benchmarks/run.sh` waits on it.

## DSN

```
postgres://bench:bench@127.0.0.1:5433/bench?sslmode=disable
```

## Schemas

Two schemas share one Postgres instance:
- `bench_naive` — jsonb-blob PoC shape (`naive.sql`)
- `bench_tuned` — row-per-event + pgvector HNSW + tsvector GIN (`tuned.sql`)

Both are applied idempotently by `Backend.Reset()` at the start of
every bench. The connection pool's default `search_path` picks the
correct one per backend impl.

## Stop / clean

```bash
docker compose down -v   # -v removes the volume too
```
```

- [ ] **Step 6: Sanity-run the compose file**

```bash
cd benchmarks/postgres
docker compose up -d postgres
until docker compose exec postgres pg_isready -U bench -d bench; do sleep 1; done
docker compose exec postgres psql -U bench -d bench -c "CREATE EXTENSION IF NOT EXISTS vector; SELECT extname, extversion FROM pg_extension WHERE extname='vector';"
```

Expected: `vector | 0.7.x` or later in the output.

- [ ] **Step 7: Apply the schemas as a sanity check**

```bash
docker compose exec -T postgres psql -U bench -d bench < naive.sql
docker compose exec -T postgres psql -U bench -d bench < tuned.sql
docker compose exec postgres psql -U bench -d bench -c "\\dt bench_naive.*; \\dt bench_tuned.*"
```

Expected: `sessions`, `tenants` in `bench_naive`; `events`, `sessions`, `tenants` in `bench_tuned`.

- [ ] **Step 8: Leave the container running for later tasks; commit**

```bash
git add benchmarks/postgres/
git commit -m "feat(bench): postgres baselines (naive jsonb + tuned pgvector/tsvector)"
```

### Task 6: Go `Backend` interface + shared types

**Files:**
- Create: `workload/internal/backend/backend.go`

- [ ] **Step 1: Write the interface**

```go
// Package backend defines the common interface for storage backends
// under benchmark. Three impls exist: agentdb (REST), pg_naive (jsonb),
// pg_tuned (row-per-event + pgvector + tsvector).
package backend

import (
	"context"
)

// Event is the unit of storage: an opaque payload plus optional
// retrieval-helpful fields. Benchmarks populate Text and Embedding only
// for retrieval-workload benches.
type Event struct {
	Seq       int64
	Payload   []byte
	Text      string
	Embedding []float32
}

// Hit is a retrieval result.
type Hit struct {
	SessionID string
	Seq       int64
	Score     float32
	DocID     string
}

// Backend is the surface every benchmark runs against.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// Methods take explicit tenant and session identifiers so benchmarks can
// control contention (one shared session vs. per-worker session).
type Backend interface {
	Name() string

	// Reset prepares the backend for a fresh bench: truncates tenant data,
	// re-applies schema as needed. Called once before each bench.
	Reset(ctx context.Context) error

	// EnsureTenant ensures the named tenant exists. Idempotent.
	EnsureTenant(ctx context.Context, tenant string) error

	// Append one event. Returns the assigned sequence number.
	Append(ctx context.Context, tenant, session string, ev Event) (seq int64, err error)

	// AppendAndFetch appends an event and returns the last N events of the
	// resulting session (including the just-appended one). Equivalent to
	// Append followed by FetchLastN in one logical call; AgentDB does this
	// in one roundtrip, Postgres does it in one transaction.
	AppendAndFetch(ctx context.Context, tenant, session string, ev Event, lastN int) ([]Event, error)

	// Fork creates a child session whose initial view of events matches
	// the parent's events at call time.
	Fork(ctx context.Context, tenant, parent, child string) error

	// FetchLastN returns the last N events of a session, oldest-to-newest.
	FetchLastN(ctx context.Context, tenant, session string, n int) ([]Event, error)

	// VectorQuery returns the top-K hits by vector cosine similarity,
	// scoped to one tenant. Optional session filter when session is
	// non-empty. (Scope change 2026-04-20: hybrid BM25+vector dropped
	// to vector-only; see the scope note at the top of the plan.)
	VectorQuery(ctx context.Context, tenant, session string, vec []float32, k int) ([]Hit, error)

	// Close releases underlying resources (HTTP client, pgxpool).
	Close() error
}
```

- [ ] **Step 2: Commit**

```bash
git add workload/internal/backend/backend.go
git commit -m "feat(bench): Backend interface + shared types"
```

### Task 7: Backend conformance tests (TDD skeleton)

**Files:**
- Create: `workload/internal/backend/conformance_test.go`

- [ ] **Step 1: Write the conformance test harness**

```go
package backend

import (
	"context"
	"testing"
	"time"
)

// conformanceTest runs against every Backend impl. Each impl contributes
// a factory so the test doesn't know how to construct it.

type factory func(t *testing.T) Backend

var registry = map[string]factory{}

// register is called from each impl's *_test.go init. This file only owns
// the assertions.
func register(name string, f factory) { registry[name] = f }

func runAll(t *testing.T, cases func(*testing.T, Backend)) {
	t.Helper()
	if len(registry) == 0 {
		t.Skip("no backends registered (set BENCH_BACKENDS env)")
	}
	for name, f := range registry {
		name := name
		f := f
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := f(t)
			t.Cleanup(func() { _ = b.Close() })
			cases(t, b)
		})
	}
}

func TestAppendThenFetch(t *testing.T) {
	runAll(t, func(t *testing.T, b Backend) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := b.Reset(ctx); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		if err := b.EnsureTenant(ctx, "t-conf"); err != nil {
			t.Fatalf("EnsureTenant: %v", err)
		}
		seq, err := b.Append(ctx, "t-conf", "s-1", Event{Payload: []byte("hello")})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if seq < 1 {
			t.Fatalf("expected seq >= 1, got %d", seq)
		}
		evs, err := b.FetchLastN(ctx, "t-conf", "s-1", 5)
		if err != nil {
			t.Fatalf("FetchLastN: %v", err)
		}
		if len(evs) != 1 {
			t.Fatalf("expected 1 event, got %d", len(evs))
		}
		if string(evs[0].Payload) != "hello" {
			t.Fatalf("payload mismatch: %q", evs[0].Payload)
		}
	})
}

func TestForkChildSeesParentPrefix(t *testing.T) {
	runAll(t, func(t *testing.T, b Backend) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := b.Reset(ctx); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		if err := b.EnsureTenant(ctx, "t-conf"); err != nil {
			t.Fatalf("EnsureTenant: %v", err)
		}
		for i := 0; i < 3; i++ {
			if _, err := b.Append(ctx, "t-conf", "parent", Event{Payload: []byte{byte('A' + i)}}); err != nil {
				t.Fatalf("Append parent %d: %v", i, err)
			}
		}
		if err := b.Fork(ctx, "t-conf", "parent", "child"); err != nil {
			t.Fatalf("Fork: %v", err)
		}
		evs, err := b.FetchLastN(ctx, "t-conf", "child", 10)
		if err != nil {
			t.Fatalf("FetchLastN child: %v", err)
		}
		if len(evs) != 3 {
			t.Fatalf("child should see 3 parent events, got %d", len(evs))
		}
	})
}

func TestAppendAndFetch(t *testing.T) {
	runAll(t, func(t *testing.T, b Backend) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := b.Reset(ctx); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		if err := b.EnsureTenant(ctx, "t-conf"); err != nil {
			t.Fatalf("EnsureTenant: %v", err)
		}
		for i := 0; i < 4; i++ {
			if _, err := b.Append(ctx, "t-conf", "s", Event{Payload: []byte{byte('a' + i)}}); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
		evs, err := b.AppendAndFetch(ctx, "t-conf", "s", Event{Payload: []byte("e")}, 3)
		if err != nil {
			t.Fatalf("AppendAndFetch: %v", err)
		}
		if len(evs) != 3 {
			t.Fatalf("expected 3 events, got %d", len(evs))
		}
		if string(evs[len(evs)-1].Payload) != "e" {
			t.Fatalf("last event should be the just-appended one")
		}
	})
}
```

- [ ] **Step 2: Run the test with no backends registered**

```bash
cd workload && go test ./internal/backend/...
```

Expected: `PASS` (all cases SKIPPED — no backends registered yet). This verifies the harness compiles before we write impls.

- [ ] **Step 3: Commit**

```bash
git add workload/internal/backend/conformance_test.go
git commit -m "test(bench): backend conformance test harness (pre-impl, skips when empty)"
```

### Task 8: AgentDB backend impl

**Files:**
- Create: `workload/internal/backend/agentdb.go`
- Create: `workload/internal/backend/agentdb_test.go`
- Modify: `workload/go.mod` (only if new deps needed; probably not)

- [ ] **Step 1: Inspect existing REST client**

```bash
cat workload/internal/client/rest.go | head -80
```

Use this to find out which functions already exist (`AppendAndFetch`, `Fork`) and which we need to add/expose.

- [ ] **Step 2: Write the AgentDB backend**

```go
package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type AgentDBBackend struct {
	BaseURL string
	HTTP    *http.Client
}

// NewAgentDB returns a Backend talking to an agentdb-server at baseURL
// (e.g. "http://127.0.0.1:7878").
func NewAgentDB(baseURL string) *AgentDBBackend {
	return &AgentDBBackend{
		BaseURL: baseURL,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (a *AgentDBBackend) Name() string { return "agentdb" }

func (a *AgentDBBackend) Reset(ctx context.Context) error {
	// AgentDB has no global truncate; benches use disjoint tenant ids
	// per run via EnsureTenant. Reset is a no-op here.
	return nil
}

func (a *AgentDBBackend) EnsureTenant(ctx context.Context, tenant string) error {
	// AgentDB creates tenants implicitly on first write. No-op.
	return nil
}

type agentDBAppendReq struct {
	Event       map[string]any `json:"event"`
	FetchLastN  int            `json:"fetch_last_n"`
}

type agentDBAppendResp struct {
	Seq    int64                    `json:"seq"`
	Events []map[string]any         `json:"events"`
}

func (a *AgentDBBackend) appendAndFetchRaw(ctx context.Context, tenant, session string, ev Event, lastN int) (agentDBAppendResp, error) {
	req := agentDBAppendReq{
		Event: map[string]any{
			"payload_b64": ev.Payload,
			"text":        ev.Text,
			"embedding":   ev.Embedding,
		},
		FetchLastN: lastN,
	}
	body, _ := json.Marshal(req)
	url := fmt.Sprintf("%s/api/tenants/%s/sessions/%s/append_and_fetch",
		a.BaseURL, tenant, session)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return agentDBAppendResp{}, err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := a.HTTP.Do(r)
	if err != nil {
		return agentDBAppendResp{}, err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return agentDBAppendResp{}, fmt.Errorf("agentdb append: %s: %s", resp.Status, b)
	}
	var out agentDBAppendResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return agentDBAppendResp{}, err
	}
	return out, nil
}

func (a *AgentDBBackend) Append(ctx context.Context, tenant, session string, ev Event) (int64, error) {
	resp, err := a.appendAndFetchRaw(ctx, tenant, session, ev, 0)
	return resp.Seq, err
}

func (a *AgentDBBackend) AppendAndFetch(ctx context.Context, tenant, session string, ev Event, lastN int) ([]Event, error) {
	resp, err := a.appendAndFetchRaw(ctx, tenant, session, ev, lastN)
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(resp.Events))
	for _, e := range resp.Events {
		out = append(out, decodeEvent(e))
	}
	return out, nil
}

func (a *AgentDBBackend) Fork(ctx context.Context, tenant, parent, child string) error {
	body, _ := json.Marshal(map[string]string{
		"tenant_id":      tenant,
		"new_session_id": child,
	})
	url := fmt.Sprintf("%s/api/sessions/%s/fork", a.BaseURL, parent)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := a.HTTP.Do(r)
	if err != nil {
		return err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("agentdb fork: %s: %s", resp.Status, b)
	}
	return nil
}

func (a *AgentDBBackend) FetchLastN(ctx context.Context, tenant, session string, n int) ([]Event, error) {
	// Prefer a dedicated fetch endpoint if it exists; else zero-sized append.
	resp, err := a.appendAndFetchRaw(ctx, tenant, session, Event{Payload: []byte{}}, n)
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(resp.Events))
	for _, e := range resp.Events {
		out = append(out, decodeEvent(e))
	}
	// Drop the trailing sentinel (the zero-sized appended event) if present.
	if len(out) > 0 && len(out[len(out)-1].Payload) == 0 {
		out = out[:len(out)-1]
	}
	return out, nil
}

// VectorQuery uses the Mongo wire protocol's $vectorSearch aggregation
// stage. AgentDB has no REST endpoint for vector-only retrieval, so the
// AgentDB backend keeps a Mongo client alongside its HTTP client — the
// other Backend methods (Append, Fork, etc.) still go over REST.
//
// The Mongo collection is `{tenant_id}.events`; see existing
// workload/internal/client/mongo.go for precedent.
func (a *AgentDBBackend) VectorQuery(ctx context.Context, tenant, session string, vec []float32, k int) ([]Hit, error) {
	if a.Mongo == nil {
		return nil, fmt.Errorf("agentdb VectorQuery: Mongo client not configured (set MongoURI at construction)")
	}
	coll := a.Mongo.Database(tenant).Collection("events")
	queryVec := make([]any, len(vec))
	for i, v := range vec {
		queryVec[i] = float64(v)
	}
	match := bson.D{}
	if session != "" {
		match = bson.D{{Key: "session_id", Value: session}}
	}
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$vectorSearch", Value: bson.D{
			{Key: "path", Value: "payload.embedding"},
			{Key: "queryVector", Value: queryVec},
			{Key: "limit", Value: int64(k)},
			{Key: "numCandidates", Value: int64(max(100, k*4))},
		}}},
	}
	if len(match) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: match}})
	}
	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("agentdb $vectorSearch: %w", err)
	}
	defer cur.Close(ctx)
	var hits []Hit
	for cur.Next(ctx) {
		var doc struct {
			SessionID string  `bson:"session_id"`
			Seq       int64   `bson:"seq"`
			Score     float32 `bson:"score"`
			Payload   struct {
				DocID string `bson:"doc_id"`
			} `bson:"payload"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		hits = append(hits, Hit{
			SessionID: doc.SessionID, Seq: doc.Seq, Score: doc.Score, DocID: doc.Payload.DocID,
		})
	}
	return hits, cur.Err()
}

func (a *AgentDBBackend) Close() error {
	if a.Mongo != nil {
		_ = a.Mongo.Disconnect(context.Background())
	}
	return nil
}

// decodeEvent turns an AgentDB event JSON object into our Event.
func decodeEvent(e map[string]any) Event {
	ev := Event{}
	if seq, ok := e["seq"].(float64); ok {
		ev.Seq = int64(seq)
	}
	if b, ok := e["payload_b64"].(string); ok {
		ev.Payload = []byte(b)
	}
	if t, ok := e["text"].(string); ok {
		ev.Text = t
	}
	return ev
}
```

Note: the exact AgentDB request/response shapes may differ from what the existing `workload/internal/client/rest.go` already wraps. If that package has typed helpers (`client.AppendAndFetch`, etc.), prefer calling those over reimplementing HTTP inline. Refactor inline while implementing — either approach passes the conformance test.

- [ ] **Step 3: Write the test registration**

```go
package backend

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"
)

func init() {
	register("agentdb", func(t *testing.T) Backend {
		t.Helper()
		url := os.Getenv("AGENTDB_URL")
		if url == "" {
			url = "http://127.0.0.1:7878"
		}
		if !isAgentDBReachable(url) {
			t.Skipf("agentdb not reachable at %s; set AGENTDB_URL or start the server", url)
		}
		return NewAgentDB(url)
	})
}

func isAgentDBReachable(url string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/healthz", nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}
```

If AgentDB doesn't expose `/healthz`, substitute a known 200 endpoint (e.g., `/api/admin/boxes`).

- [ ] **Step 4: Run the conformance tests against AgentDB**

Start agentdb-server first (in a separate terminal or background):

```bash
cargo run -p agentdb-server &
sleep 3
cd workload && go test ./internal/backend/... -run Test -v -count=1
```

Expected: all three tests PASS under the `agentdb` subtest.

If any assertion fails, debug — conformance failures often reveal JSON-shape or endpoint-path mistakes in the impl before benches run.

- [ ] **Step 5: Commit**

```bash
git add workload/internal/backend/agentdb.go workload/internal/backend/agentdb_test.go
git commit -m "feat(bench): agentdb backend + passing conformance tests"
```

### Task 9: Postgres naive backend impl

**Files:**
- Create: `workload/internal/backend/pg_naive.go`
- Create: `workload/internal/backend/pg_naive_test.go`
- Modify: `workload/go.mod`, `workload/go.sum` (add `github.com/jackc/pgx/v5`)

- [ ] **Step 1: Add the pgx dependency**

```bash
cd workload
go get github.com/jackc/pgx/v5
go mod tidy
```

- [ ] **Step 2: Write the naive backend**

```go
package backend

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PGNaiveBackend struct {
	pool *pgxpool.Pool
}

func NewPGNaive(ctx context.Context, dsn string) (*PGNaiveBackend, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	// All queries run inside the bench_naive schema.
	cfg.ConnConfig.RuntimeParams["search_path"] = "bench_naive"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PGNaiveBackend{pool: pool}, nil
}

func (p *PGNaiveBackend) Name() string { return "pg_naive" }

func (p *PGNaiveBackend) Reset(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, `
		TRUNCATE TABLE sessions CASCADE;
		TRUNCATE TABLE tenants CASCADE;
	`)
	return err
}

func (p *PGNaiveBackend) EnsureTenant(ctx context.Context, tenant string) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO tenants(id) VALUES ($1) ON CONFLICT DO NOTHING`, tenant)
	return err
}

func (p *PGNaiveBackend) Append(ctx context.Context, tenant, session string, ev Event) (int64, error) {
	payload, _ := json.Marshal(map[string]any{
		"payload":   ev.Payload,
		"text":      ev.Text,
		"embedding": ev.Embedding,
	})
	var seq int64
	err := p.pool.QueryRow(ctx, `
		INSERT INTO sessions(id, tenant_id, messages)
		VALUES ($1, $2, jsonb_build_array($3::jsonb))
		ON CONFLICT (id) DO UPDATE SET
			messages   = sessions.messages || ($3::jsonb),
			updated_at = now()
		RETURNING jsonb_array_length(messages)
	`, session, tenant, string(payload)).Scan(&seq)
	return seq, err
}

func (p *PGNaiveBackend) AppendAndFetch(ctx context.Context, tenant, session string, ev Event, lastN int) ([]Event, error) {
	payload, _ := json.Marshal(map[string]any{
		"payload":   ev.Payload,
		"text":      ev.Text,
		"embedding": ev.Embedding,
	})
	var blob []byte
	err := p.pool.QueryRow(ctx, `
		WITH upserted AS (
			INSERT INTO sessions(id, tenant_id, messages)
			VALUES ($1, $2, jsonb_build_array($3::jsonb))
			ON CONFLICT (id) DO UPDATE SET
				messages   = sessions.messages || ($3::jsonb),
				updated_at = now()
			RETURNING messages
		)
		SELECT CASE WHEN $4::int > 0
			THEN (SELECT jsonb_agg(e) FROM (
				SELECT e FROM upserted, jsonb_array_elements(upserted.messages) e
				ORDER BY row_number() OVER () DESC
				LIMIT $4
			) t)
			ELSE '[]'::jsonb
		END::text
	`, session, tenant, string(payload), lastN).Scan(&blob)
	if err != nil {
		return nil, err
	}
	var raw []map[string]any
	if err := json.Unmarshal(blob, &raw); err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(raw))
	for i := len(raw) - 1; i >= 0; i-- { // reverse back to ascending
		out = append(out, decodeNaiveEvent(raw[i]))
	}
	return out, nil
}

func (p *PGNaiveBackend) Fork(ctx context.Context, tenant, parent, child string) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO sessions(id, tenant_id, parent_id, messages)
		SELECT $1, $2, $3, messages FROM sessions WHERE id = $3
	`, child, tenant, parent)
	return err
}

func (p *PGNaiveBackend) FetchLastN(ctx context.Context, tenant, session string, n int) ([]Event, error) {
	var blob []byte
	err := p.pool.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT jsonb_agg(e) FROM (
				SELECT e FROM sessions s, jsonb_array_elements(s.messages) e
				WHERE s.id = $1
				ORDER BY row_number() OVER () DESC
				LIMIT $2
			) t),
			'[]'::jsonb
		)::text
	`, session, n).Scan(&blob)
	if err != nil {
		return nil, err
	}
	var raw []map[string]any
	if err := json.Unmarshal(blob, &raw); err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(raw))
	for i := len(raw) - 1; i >= 0; i-- {
		out = append(out, decodeNaiveEvent(raw[i]))
	}
	return out, nil
}

func (p *PGNaiveBackend) HybridQuery(ctx context.Context, tenant, session, text string, vec []float32, k int) ([]Hit, error) {
	// Honest "what this looks like on jsonb": unnest messages, filter by
	// tenant, compute tsvector + pgvector cast per event. Slow by design.
	return nil, fmt.Errorf("pg_naive: hybrid retrieval intentionally unimplemented as a first-class op; bench 4 will invoke its own unrolled SQL")
}

func (p *PGNaiveBackend) Close() error {
	p.pool.Close()
	return nil
}

func decodeNaiveEvent(m map[string]any) Event {
	ev := Event{}
	if p, ok := m["payload"].(string); ok {
		ev.Payload = []byte(p)
	}
	if t, ok := m["text"].(string); ok {
		ev.Text = t
	}
	return ev
}
```

Note: the hybrid-retrieval query for pg_naive is intentionally left to Bench 4 (which holds the honest unrolled SQL). Returning an error from `HybridQuery` is correct — conformance tests don't call it (they call on tuned and agentdb).

- [ ] **Step 3: Write the test registration**

```go
package backend

import (
	"context"
	"os"
	"testing"
	"time"
)

func init() {
	register("pg_naive", func(t *testing.T) Backend {
		t.Helper()
		dsn := os.Getenv("PG_DSN")
		if dsn == "" {
			dsn = "postgres://bench:bench@127.0.0.1:5433/bench?sslmode=disable"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		b, err := NewPGNaive(ctx, dsn)
		if err != nil {
			t.Skipf("pg_naive unreachable at %s: %v", dsn, err)
		}
		return b
	})
}
```

- [ ] **Step 4: Run the conformance tests against pg_naive**

Ensure docker compose is up:

```bash
cd benchmarks/postgres && docker compose up -d postgres && docker compose exec -T postgres psql -U bench -d bench < naive.sql
cd ../../workload && go test ./internal/backend/... -run 'TestAppendThenFetch|TestForkChildSeesParentPrefix|TestAppendAndFetch' -v
```

Expected: all three tests PASS under both `agentdb` and `pg_naive`. Debug SQL if a test fails.

- [ ] **Step 5: Commit**

```bash
git add workload/go.mod workload/go.sum workload/internal/backend/pg_naive.go workload/internal/backend/pg_naive_test.go
git commit -m "feat(bench): pg_naive backend (jsonb-blob) + passing conformance tests"
```

### Task 10: Postgres tuned backend impl

**Files:**
- Create: `workload/internal/backend/pg_tuned.go`
- Create: `workload/internal/backend/pg_tuned_test.go`

- [ ] **Step 1: Write the tuned backend**

```go
package backend

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGTunedBackend struct {
	pool *pgxpool.Pool
}

func NewPGTuned(ctx context.Context, dsn string) (*PGTunedBackend, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = "bench_tuned"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PGTunedBackend{pool: pool}, nil
}

func (p *PGTunedBackend) Name() string { return "pg_tuned" }

func (p *PGTunedBackend) Reset(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, `
		TRUNCATE TABLE events CASCADE;
		TRUNCATE TABLE sessions CASCADE;
		TRUNCATE TABLE tenants CASCADE;
	`)
	return err
}

func (p *PGTunedBackend) EnsureTenant(ctx context.Context, tenant string) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO tenants(id) VALUES ($1) ON CONFLICT DO NOTHING`, tenant)
	return err
}

// ensureSession is idempotent; callers use it before inserting events.
func (p *PGTunedBackend) ensureSession(ctx context.Context, tx pgx.Tx, tenant, session string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO sessions(id, tenant_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		session, tenant)
	return err
}

func (p *PGTunedBackend) Append(ctx context.Context, tenant, session string, ev Event) (int64, error) {
	var seq int64
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := p.ensureSession(ctx, tx, tenant, session); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			INSERT INTO events (session_id, seq, tenant_id, payload, text, embedding)
			SELECT $1,
			       COALESCE((SELECT max(seq) FROM events WHERE session_id = $1), 0) + 1,
			       $2, $3, $4, $5
			RETURNING seq
		`, session, tenant, ev.Payload, ev.Text, pgvec(ev.Embedding)).Scan(&seq)
	})
	return seq, err
}

func (p *PGTunedBackend) AppendAndFetch(ctx context.Context, tenant, session string, ev Event, lastN int) ([]Event, error) {
	var events []Event
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := p.ensureSession(ctx, tx, tenant, session); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO events (session_id, seq, tenant_id, payload, text, embedding)
			SELECT $1,
			       COALESCE((SELECT max(seq) FROM events WHERE session_id = $1), 0) + 1,
			       $2, $3, $4, $5
		`, session, tenant, ev.Payload, ev.Text, pgvec(ev.Embedding)); err != nil {
			return err
		}
		if lastN <= 0 {
			return nil
		}
		rows, err := tx.Query(ctx, `
			SELECT seq, payload, text FROM events
			WHERE session_id = $1
			ORDER BY seq DESC LIMIT $2
		`, session, lastN)
		if err != nil {
			return err
		}
		defer rows.Close()
		var rev []Event
		for rows.Next() {
			var e Event
			if err := rows.Scan(&e.Seq, &e.Payload, &e.Text); err != nil {
				return err
			}
			rev = append(rev, e)
		}
		events = make([]Event, 0, len(rev))
		for i := len(rev) - 1; i >= 0; i-- {
			events = append(events, rev[i])
		}
		return rows.Err()
	})
	return events, err
}

func (p *PGTunedBackend) Fork(ctx context.Context, tenant, parent, child string) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO sessions(id, tenant_id, parent_id)
			VALUES ($1, $2, $3)
		`, child, tenant, parent); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO events(session_id, seq, tenant_id, payload, text, embedding)
			SELECT $1, seq, tenant_id, payload, text, embedding
			FROM events WHERE session_id = $2
		`, child, parent)
		return err
	})
}

func (p *PGTunedBackend) FetchLastN(ctx context.Context, tenant, session string, n int) ([]Event, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT seq, payload, text FROM events
		WHERE session_id = $1
		ORDER BY seq DESC LIMIT $2
	`, session, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rev []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.Seq, &e.Payload, &e.Text); err != nil {
			return nil, err
		}
		rev = append(rev, e)
	}
	out := make([]Event, 0, len(rev))
	for i := len(rev) - 1; i >= 0; i-- {
		out = append(out, rev[i])
	}
	return out, rows.Err()
}

func (p *PGTunedBackend) HybridQuery(ctx context.Context, tenant, session, text string, vec []float32, k int) ([]Hit, error) {
	args := []any{tenant, text, pgvec(vec), k}
	where := "tenant_id=$1 AND tsv @@ plainto_tsquery('english', $2)"
	if session != "" {
		where += " AND session_id=$5"
		args = append(args, session)
	}
	q := fmt.Sprintf(`
		SELECT session_id, seq, (embedding <=> $3)::float4 AS distance
		FROM events
		WHERE %s
		ORDER BY embedding <=> $3
		LIMIT $4
	`, where)
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hits []Hit
	for rows.Next() {
		var h Hit
		if err := rows.Scan(&h.SessionID, &h.Seq, &h.Score); err != nil {
			return nil, err
		}
		hits = append(hits, h)
	}
	return hits, rows.Err()
}

func (p *PGTunedBackend) Close() error {
	p.pool.Close()
	return nil
}

// pgvec formats a []float32 as pgvector's text literal. Nil → NULL via the
// driver (pass nil interface).
func pgvec(v []float32) any {
	if v == nil {
		return nil
	}
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = fmt.Sprintf("%g", x)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
```

- [ ] **Step 2: Write the test registration**

```go
package backend

import (
	"context"
	"os"
	"testing"
	"time"
)

func init() {
	register("pg_tuned", func(t *testing.T) Backend {
		t.Helper()
		dsn := os.Getenv("PG_DSN")
		if dsn == "" {
			dsn = "postgres://bench:bench@127.0.0.1:5433/bench?sslmode=disable"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		b, err := NewPGTuned(ctx, dsn)
		if err != nil {
			t.Skipf("pg_tuned unreachable at %s: %v", dsn, err)
		}
		return b
	})
}
```

- [ ] **Step 3: Apply the tuned schema and run conformance**

```bash
cd benchmarks/postgres && docker compose exec -T postgres psql -U bench -d bench < tuned.sql
cd ../../workload && go test ./internal/backend/... -run 'TestAppendThenFetch|TestForkChildSeesParentPrefix|TestAppendAndFetch' -v
```

Expected: all three conformance tests PASS under `agentdb`, `pg_naive`, and `pg_tuned`.

- [ ] **Step 4: Commit**

```bash
git add workload/internal/backend/pg_tuned.go workload/internal/backend/pg_tuned_test.go
git commit -m "feat(bench): pg_tuned backend (events + pgvector + tsvector) + conformance"
```

### Task 11: HDR histogram helper

**Files:**
- Create: `workload/internal/bench/histogram.go`
- Modify: `workload/go.mod`, `workload/go.sum`

- [ ] **Step 1: Add hdrhistogram-go**

```bash
cd workload
go get github.com/HdrHistogram/hdrhistogram-go
go mod tidy
```

- [ ] **Step 2: Write the helper**

```go
// Package bench contains the four comparison benchmarks. Each bench exposes
// a Run(Backend, Config) Report function; the agentdb-bench CLI composes
// them.
package bench

import (
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Recorder wraps an HDR histogram recording nanosecond durations from
// 1 µs to 60 s with 3 significant digits. Safe for single-goroutine
// recording; use one Recorder per goroutine and Merge at the end.
type Recorder struct {
	h *hdrhistogram.Histogram
}

func NewRecorder() *Recorder {
	return &Recorder{h: hdrhistogram.New(1000, int64(60*time.Second), 3)}
}

func (r *Recorder) Record(d time.Duration) {
	if d < 0 {
		d = 0
	}
	_ = r.h.RecordValue(int64(d))
}

func (r *Recorder) Merge(other *Recorder) {
	r.h.Merge(other.h)
}

// Summary is the public shape we serialize into JSON.
type Summary struct {
	Count   int64   `json:"count"`
	Errors  int64   `json:"errors"`
	P50Ms   float64 `json:"p50_ms"`
	P90Ms   float64 `json:"p90_ms"`
	P99Ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
	RPS     float64 `json:"rps"`
	Seconds float64 `json:"seconds"`
}

func (r *Recorder) Summarize(errs int64, elapsed time.Duration) Summary {
	count := r.h.TotalCount()
	toMs := func(ns int64) float64 { return float64(ns) / 1e6 }
	return Summary{
		Count:   count,
		Errors:  errs,
		P50Ms:   toMs(r.h.ValueAtQuantile(50)),
		P90Ms:   toMs(r.h.ValueAtQuantile(90)),
		P99Ms:   toMs(r.h.ValueAtQuantile(99)),
		MaxMs:   toMs(r.h.Max()),
		RPS:     float64(count) / elapsed.Seconds(),
		Seconds: elapsed.Seconds(),
	}
}
```

- [ ] **Step 3: Commit**

```bash
git add workload/go.mod workload/go.sum workload/internal/bench/histogram.go
git commit -m "feat(bench): HDR histogram recorder + JSON summary"
```

### Task 12: Fixture loader (Go)

**Files:**
- Create: `workload/internal/fixture/fixture.go`
- Create: `workload/internal/fixture/fixture_test.go`

- [ ] **Step 1: Write the loader**

```go
// Package fixture loads benchmarks/fixtures/{corpus,queries}.jsonl.
package fixture

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Doc struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Embedding []float32 `json:"embedding"`
}

type Query struct {
	ID          string   `json:"id"`
	Text        string   `json:"text"`
	GroundTruth []string `json:"ground_truth"`
}

type Corpus struct {
	Docs    []Doc
	ByID    map[string]int
	Queries []Query
}

var (
	loadOnce sync.Once
	cached   *Corpus
	cacheErr error
)

// Load returns the corpus, loaded from dir (or $AGENTDB_FIXTURE_DIR,
// or ../benchmarks/fixtures relative to the caller's cwd). Cached.
func Load(dir string) (*Corpus, error) {
	loadOnce.Do(func() {
		cached, cacheErr = loadFrom(resolveDir(dir))
	})
	return cached, cacheErr
}

func resolveDir(dir string) string {
	if dir != "" {
		return dir
	}
	if env := os.Getenv("AGENTDB_FIXTURE_DIR"); env != "" {
		return env
	}
	return filepath.Join("..", "benchmarks", "fixtures")
}

func loadFrom(dir string) (*Corpus, error) {
	c := &Corpus{ByID: map[string]int{}}
	if err := readJSONL(filepath.Join(dir, "corpus.jsonl"), func(raw []byte) error {
		var d Doc
		if err := json.Unmarshal(raw, &d); err != nil {
			return err
		}
		c.ByID[d.ID] = len(c.Docs)
		c.Docs = append(c.Docs, d)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("corpus: %w", err)
	}
	if err := readJSONL(filepath.Join(dir, "queries.jsonl"), func(raw []byte) error {
		var q Query
		if err := json.Unmarshal(raw, &q); err != nil {
			return err
		}
		c.Queries = append(c.Queries, q)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("queries: %w", err)
	}
	return c, nil
}

func readJSONL(path string, on func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			if err := on(line); err != nil {
				return err
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
```

- [ ] **Step 2: Write a smoke test**

```go
package fixture

import "testing"

func TestLoadSmoke(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Skipf("fixture not generated; skipping: %v", err)
	}
	if len(c.Docs) < 100 {
		t.Fatalf("expected >=100 docs, got %d", len(c.Docs))
	}
	if len(c.Queries) < 10 {
		t.Fatalf("expected >=10 queries, got %d", len(c.Queries))
	}
	if len(c.Docs[0].Embedding) != 384 {
		t.Fatalf("expected 384-dim embedding, got %d", len(c.Docs[0].Embedding))
	}
}
```

- [ ] **Step 3: Run**

```bash
cd workload && go test ./internal/fixture/... -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add workload/internal/fixture/
git commit -m "feat(bench): Go fixture loader with caching"
```

### Task 13: Bench 1 — hot session append+fetch

**Files:**
- Create: `workload/internal/bench/hotsession.go`
- Create: `workload/internal/bench/hotsession_test.go`

- [ ] **Step 1: Write the benchmark**

```go
package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
)

// HotSessionConfig controls Bench 1.
type HotSessionConfig struct {
	Duration     time.Duration
	WarmupFrac   float64 // 0..1
	Concurrency  int
	Tenant       string
	Session      string
	PayloadSize  int // bytes
	FetchLastN   int
}

// HotSessionReport is the per-backend result.
type HotSessionReport struct {
	Backend string  `json:"backend"`
	Config  HotSessionConfig `json:"config"`
	Summary Summary `json:"summary"`
}

// DefaultHotSession returns sensible defaults (one hot session, 512B payload, fetch last 5).
func DefaultHotSession(concurrency int) HotSessionConfig {
	return HotSessionConfig{
		Duration:    15 * time.Second,
		WarmupFrac:  0.1,
		Concurrency: concurrency,
		Tenant:      "t-bench-hot",
		Session:     "s-hot",
		PayloadSize: 512,
		FetchLastN:  5,
	}
}

func RunHotSession(ctx context.Context, b backend.Backend, cfg HotSessionConfig) (HotSessionReport, error) {
	if err := b.Reset(ctx); err != nil {
		return HotSessionReport{}, fmt.Errorf("reset: %w", err)
	}
	if err := b.EnsureTenant(ctx, cfg.Tenant); err != nil {
		return HotSessionReport{}, fmt.Errorf("ensure tenant: %w", err)
	}
	payload := make([]byte, cfg.PayloadSize)
	_, _ = rand.Read(payload)

	warmup := time.Duration(float64(cfg.Duration) * cfg.WarmupFrac)
	measure := cfg.Duration - warmup

	agg := NewRecorder()
	var errs int64
	var mu sync.Mutex

	start := time.Now()
	deadline := start.Add(cfg.Duration)
	measureStart := start.Add(warmup)

	var wg sync.WaitGroup
	wg.Add(cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		go func() {
			defer wg.Done()
			local := NewRecorder()
			var localErrs int64
			for time.Now().Before(deadline) {
				opStart := time.Now()
				_, err := b.AppendAndFetch(ctx, cfg.Tenant, cfg.Session,
					backend.Event{Payload: payload}, cfg.FetchLastN)
				elapsed := time.Since(opStart)
				if opStart.Before(measureStart) {
					continue // drop warm-up
				}
				if err != nil {
					localErrs++
					continue
				}
				local.Record(elapsed)
			}
			mu.Lock()
			agg.Merge(local)
			atomic.AddInt64(&errs, localErrs)
			mu.Unlock()
		}()
	}
	wg.Wait()

	return HotSessionReport{
		Backend: b.Name(),
		Config:  cfg,
		Summary: agg.Summarize(atomic.LoadInt64(&errs), measure),
	}, nil
}
```

Note: replace `github.com/shenli/agentdb/workload/internal/backend` with whatever your `go.mod` module name is; if unsure, run `head -1 workload/go.mod` and match.

- [ ] **Step 2: Write the smoke test**

```go
package bench

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
)

func TestHotSessionSmoke(t *testing.T) {
	url := os.Getenv("AGENTDB_URL")
	if url == "" {
		url = "http://127.0.0.1:7878"
	}
	if resp, err := http.Get(url + "/api/admin/boxes"); err != nil || resp.StatusCode != 200 {
		t.Skipf("agentdb unreachable at %s", url)
	}
	b := backend.NewAgentDB(url)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultHotSession(2)
	cfg.Duration = 1 * time.Second
	rep, err := RunHotSession(ctx, b, cfg)
	if err != nil {
		t.Fatalf("RunHotSession: %v", err)
	}
	if rep.Summary.Count == 0 {
		t.Fatalf("expected some ops recorded")
	}
	if rep.Summary.Errors != 0 {
		t.Fatalf("unexpected errors: %d", rep.Summary.Errors)
	}
	if rep.Summary.P50Ms <= 0 {
		t.Fatalf("expected p50 > 0")
	}
}
```

- [ ] **Step 3: Run**

```bash
cd workload && go test ./internal/bench/... -run TestHotSession -v
```

Expected: PASS with a non-zero Count.

- [ ] **Step 4: Commit**

```bash
git add workload/internal/bench/hotsession.go workload/internal/bench/hotsession_test.go
git commit -m "feat(bench): bench 1 — hot-session append+fetch"
```

### Task 14: Bench 2 — fork cost scaling

**Files:**
- Create: `workload/internal/bench/forkscaling.go`
- Create: `workload/internal/bench/forkscaling_test.go`

- [ ] **Step 1: Write the benchmark**

```go
package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
)

type ForkScalingConfig struct {
	Tenant        string
	ParentSession string
	ChildPrefix   string
	Sizes         []int // parent event counts to measure
	ForksPerSize  int   // how many forks we time per size
	PayloadSize   int
}

type ForkScalingSample struct {
	ParentEvents int     `json:"parent_events"`
	Summary      Summary `json:"summary"`
}

type ForkScalingReport struct {
	Backend string              `json:"backend"`
	Config  ForkScalingConfig   `json:"config"`
	Samples []ForkScalingSample `json:"samples"`
}

func DefaultForkScaling() ForkScalingConfig {
	return ForkScalingConfig{
		Tenant:        "t-bench-fork",
		ParentSession: "s-parent",
		ChildPrefix:   "s-child-",
		Sizes:         []int{10, 100, 1_000, 10_000},
		ForksPerSize:  50,
		PayloadSize:   256,
	}
}

func RunForkScaling(ctx context.Context, b backend.Backend, cfg ForkScalingConfig) (ForkScalingReport, error) {
	rep := ForkScalingReport{Backend: b.Name(), Config: cfg}
	payload := make([]byte, cfg.PayloadSize)
	_, _ = rand.Read(payload)

	for _, size := range cfg.Sizes {
		if err := b.Reset(ctx); err != nil {
			return rep, fmt.Errorf("reset: %w", err)
		}
		if err := b.EnsureTenant(ctx, cfg.Tenant); err != nil {
			return rep, err
		}
		// Populate parent with `size` events (setup, not timed)
		for i := 0; i < size; i++ {
			if _, err := b.Append(ctx, cfg.Tenant, cfg.ParentSession,
				backend.Event{Payload: payload}); err != nil {
				return rep, fmt.Errorf("populate: %w", err)
			}
		}
		// Measure fork latency
		rec := NewRecorder()
		var errs int64
		bench0 := time.Now()
		for i := 0; i < cfg.ForksPerSize; i++ {
			child := fmt.Sprintf("%s%d-%d", cfg.ChildPrefix, size, i)
			t0 := time.Now()
			if err := b.Fork(ctx, cfg.Tenant, cfg.ParentSession, child); err != nil {
				errs++
				continue
			}
			rec.Record(time.Since(t0))
		}
		rep.Samples = append(rep.Samples, ForkScalingSample{
			ParentEvents: size,
			Summary:      rec.Summarize(errs, time.Since(bench0)),
		})
	}
	return rep, nil
}
```

- [ ] **Step 2: Write the smoke test**

```go
package bench

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
)

func TestForkScalingSmoke(t *testing.T) {
	url := os.Getenv("AGENTDB_URL")
	if url == "" {
		url = "http://127.0.0.1:7878"
	}
	if resp, err := http.Get(url + "/api/admin/boxes"); err != nil || resp.StatusCode != 200 {
		t.Skipf("agentdb unreachable")
	}
	b := backend.NewAgentDB(url)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := DefaultForkScaling()
	cfg.Sizes = []int{10, 50}
	cfg.ForksPerSize = 5
	rep, err := RunForkScaling(ctx, b, cfg)
	if err != nil {
		t.Fatalf("RunForkScaling: %v", err)
	}
	if len(rep.Samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(rep.Samples))
	}
	for _, s := range rep.Samples {
		if s.Summary.Count == 0 {
			t.Fatalf("no forks recorded at size %d", s.ParentEvents)
		}
	}
}
```

- [ ] **Step 3: Run**

```bash
cd workload && go test ./internal/bench/... -run TestForkScalingSmoke -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add workload/internal/bench/forkscaling.go workload/internal/bench/forkscaling_test.go
git commit -m "feat(bench): bench 2 — fork cost scaling with parent size"
```

### Task 15: Bench 3 — append latency vs. payload size

**Files:**
- Create: `workload/internal/bench/largevalue.go`
- Create: `workload/internal/bench/largevalue_test.go`

- [ ] **Step 1: Write the benchmark**

```go
package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
)

type LargeValueConfig struct {
	Duration    time.Duration
	WarmupFrac  float64
	Concurrency int
	Tenant      string
	SessionPfx  string
	Sizes       []int // payload sizes in bytes
}

type LargeValueSample struct {
	PayloadBytes int     `json:"payload_bytes"`
	Summary      Summary `json:"summary"`
}

type LargeValueReport struct {
	Backend string             `json:"backend"`
	Config  LargeValueConfig   `json:"config"`
	Samples []LargeValueSample `json:"samples"`
}

func DefaultLargeValue(concurrency int) LargeValueConfig {
	return LargeValueConfig{
		Duration:    10 * time.Second,
		WarmupFrac:  0.1,
		Concurrency: concurrency,
		Tenant:      "t-bench-large",
		SessionPfx:  "s-large-",
		Sizes:       []int{1024, 16 * 1024, 256 * 1024},
	}
}

func RunLargeValue(ctx context.Context, b backend.Backend, cfg LargeValueConfig) (LargeValueReport, error) {
	rep := LargeValueReport{Backend: b.Name(), Config: cfg}

	for _, size := range cfg.Sizes {
		if err := b.Reset(ctx); err != nil {
			return rep, fmt.Errorf("reset: %w", err)
		}
		if err := b.EnsureTenant(ctx, cfg.Tenant); err != nil {
			return rep, err
		}
		payload := make([]byte, size)
		_, _ = rand.Read(payload)

		warmup := time.Duration(float64(cfg.Duration) * cfg.WarmupFrac)
		measure := cfg.Duration - warmup
		deadline := time.Now().Add(cfg.Duration)
		measureStart := time.Now().Add(warmup)

		agg := NewRecorder()
		var errs int64
		var mu sync.Mutex
		var wg sync.WaitGroup
		wg.Add(cfg.Concurrency)
		for w := 0; w < cfg.Concurrency; w++ {
			session := fmt.Sprintf("%s%d-%d", cfg.SessionPfx, size, w)
			go func() {
				defer wg.Done()
				local := NewRecorder()
				var localErrs int64
				for time.Now().Before(deadline) {
					t0 := time.Now()
					_, err := b.Append(ctx, cfg.Tenant, session,
						backend.Event{Payload: payload})
					elapsed := time.Since(t0)
					if t0.Before(measureStart) {
						continue
					}
					if err != nil {
						localErrs++
						continue
					}
					local.Record(elapsed)
				}
				mu.Lock()
				agg.Merge(local)
				atomic.AddInt64(&errs, localErrs)
				mu.Unlock()
			}()
		}
		wg.Wait()

		rep.Samples = append(rep.Samples, LargeValueSample{
			PayloadBytes: size,
			Summary:      agg.Summarize(atomic.LoadInt64(&errs), measure),
		})
	}
	return rep, nil
}
```

- [ ] **Step 2: Write the smoke test**

```go
package bench

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
)

func TestLargeValueSmoke(t *testing.T) {
	url := os.Getenv("AGENTDB_URL")
	if url == "" {
		url = "http://127.0.0.1:7878"
	}
	if resp, err := http.Get(url + "/api/admin/boxes"); err != nil || resp.StatusCode != 200 {
		t.Skipf("agentdb unreachable")
	}
	b := backend.NewAgentDB(url)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := DefaultLargeValue(2)
	cfg.Duration = 1 * time.Second
	cfg.Sizes = []int{1024, 16384}
	rep, err := RunLargeValue(ctx, b, cfg)
	if err != nil {
		t.Fatalf("RunLargeValue: %v", err)
	}
	if len(rep.Samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(rep.Samples))
	}
}
```

- [ ] **Step 3: Run**

```bash
cd workload && go test ./internal/bench/... -run TestLargeValueSmoke -v
```

- [ ] **Step 4: Commit**

```bash
git add workload/internal/bench/largevalue.go workload/internal/bench/largevalue_test.go
git commit -m "feat(bench): bench 3 — append latency vs. payload size"
```

### Task 16: Bench 4 — hybrid retrieval

**Files:**
- Create: `workload/internal/bench/hybridretrieval.go`
- Create: `workload/internal/bench/hybridretrieval_test.go`

- [ ] **Step 1: Write the benchmark**

```go
package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
	"github.com/shenli/agentdb/workload/internal/fixture"
)

type HybridRetrievalConfig struct {
	Tenant       string
	SessionPfx   string
	CollectSizes []int // number of indexed docs
	K            int
	QueriesPerRun int
}

type HybridRetrievalSample struct {
	CollectionSize int     `json:"collection_size"`
	RecallAtK      float64 `json:"recall_at_k"`
	Summary        Summary `json:"summary"`
}

type HybridRetrievalReport struct {
	Backend string                  `json:"backend"`
	Config  HybridRetrievalConfig   `json:"config"`
	Samples []HybridRetrievalSample `json:"samples"`
}

func DefaultHybridRetrieval() HybridRetrievalConfig {
	return HybridRetrievalConfig{
		Tenant:        "t-bench-retrieval",
		SessionPfx:    "s-retrieval-",
		CollectSizes:  []int{1_000, 10_000},
		K:             10,
		QueriesPerRun: 100,
	}
}

func RunHybridRetrieval(ctx context.Context, b backend.Backend, corpus *fixture.Corpus, cfg HybridRetrievalConfig) (HybridRetrievalReport, error) {
	rep := HybridRetrievalReport{Backend: b.Name(), Config: cfg}
	if corpus == nil {
		return rep, fmt.Errorf("hybrid retrieval requires corpus fixture")
	}

	for _, size := range cfg.CollectSizes {
		if err := b.Reset(ctx); err != nil {
			return rep, fmt.Errorf("reset: %w", err)
		}
		if err := b.EnsureTenant(ctx, cfg.Tenant); err != nil {
			return rep, err
		}

		// Index `size` docs into the backend (setup, not timed)
		session := cfg.SessionPfx + fmt.Sprintf("%d", size)
		for i := 0; i < size && i < len(corpus.Docs); i++ {
			d := corpus.Docs[i]
			if _, err := b.Append(ctx, cfg.Tenant, session, backend.Event{
				Payload:   []byte(d.ID),
				Text:      d.Text,
				Embedding: d.Embedding,
			}); err != nil {
				return rep, fmt.Errorf("index doc %d: %w", i, err)
			}
		}

		// Run queries (timed)
		rec := NewRecorder()
		var errs int64
		var recallTotal float64
		var recallN int
		bench0 := time.Now()
		for i := 0; i < cfg.QueriesPerRun && i < len(corpus.Queries); i++ {
			q := corpus.Queries[i]
			// Use the first-matching-doc's embedding as the query embedding (simple
			// and deterministic — ground-truth was built the same way). If not
			// present, skip.
			if len(q.GroundTruth) == 0 {
				continue
			}
			gtIdx, ok := corpus.ByID[q.GroundTruth[0]]
			if !ok {
				continue
			}
			qVec := corpus.Docs[gtIdx].Embedding

			t0 := time.Now()
			hits, err := b.HybridQuery(ctx, cfg.Tenant, session, q.Text, qVec, cfg.K)
			elapsed := time.Since(t0)
			if err != nil {
				errs++
				continue
			}
			rec.Record(elapsed)
			recallTotal += recallAtK(hits, q.GroundTruth)
			recallN++
		}
		recall := 0.0
		if recallN > 0 {
			recall = recallTotal / float64(recallN)
		}
		rep.Samples = append(rep.Samples, HybridRetrievalSample{
			CollectionSize: size,
			RecallAtK:      recall,
			Summary:        rec.Summarize(errs, time.Since(bench0)),
		})
	}
	return rep, nil
}

// recallAtK returns |hits ∩ ground_truth| / |ground_truth| capped at 1.
// ground_truth is already top-k so both sides are the same size.
func recallAtK(hits []backend.Hit, gt []string) float64 {
	if len(gt) == 0 {
		return 0
	}
	set := make(map[string]struct{}, len(gt))
	for _, id := range gt {
		set[id] = struct{}{}
	}
	overlap := 0
	for _, h := range hits {
		if _, ok := set[h.DocID]; ok {
			overlap++
		}
	}
	return float64(overlap) / float64(len(gt))
}
```

Note on `DocID`: `HybridQuery` currently returns `Hit.DocID` but the two impls don't always populate it. Before this bench can score recall, decide on one of: (a) AgentDB returns doc id in hits (requires server-side support), (b) bench maps `session_id + seq` back to a doc id (needs a lookup table built during setup), or (c) payload carries the doc id and we parse it after fetch. Option (c) is simplest — the setup loop above stores `d.ID` as the payload; the backend impls should set `Hit.DocID` from `payload` or from an extra SELECT. Adjust `pg_tuned.HybridQuery` to return `payload::text` as the doc id; for AgentDB, the JSON hit response needs to include the event's payload (update `agentdb.go` if not already). This is a real implementation detail to resolve inside this task.

- [ ] **Step 2: Adjust `pg_tuned.HybridQuery` to return the payload as DocID**

Edit `workload/internal/backend/pg_tuned.go` — change the HybridQuery SQL + scan to include payload:

```go
func (p *PGTunedBackend) HybridQuery(ctx context.Context, tenant, session, text string, vec []float32, k int) ([]Hit, error) {
	args := []any{tenant, text, pgvec(vec), k}
	where := "tenant_id=$1 AND tsv @@ plainto_tsquery('english', $2)"
	if session != "" {
		where += " AND session_id=$5"
		args = append(args, session)
	}
	q := fmt.Sprintf(`
		SELECT session_id, seq, payload, (embedding <=> $3)::float4 AS distance
		FROM events
		WHERE %s
		ORDER BY embedding <=> $3
		LIMIT $4
	`, where)
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hits []Hit
	for rows.Next() {
		var h Hit
		var payload []byte
		if err := rows.Scan(&h.SessionID, &h.Seq, &payload, &h.Score); err != nil {
			return nil, err
		}
		h.DocID = string(payload)
		hits = append(hits, h)
	}
	return hits, rows.Err()
}
```

- [ ] **Step 3: Adjust `agentdb.go` decoder to populate `Hit.DocID`**

In the `HybridQuery` response handling, if the server returns `payload_b64` on hits, use that; otherwise follow up with a `FetchLastN`-equivalent lookup. A clean option: add `doc_id` in the hit shape on the server side. If the server doesn't have that today, implement it here by fetching the event after the hit using an existing read endpoint.

**Decision for this task:** if AgentDB server already surfaces payload on hits (grep `crates/agentdb-api/src/` for the hybrid query response shape), use it; if not, stop and file a small follow-up "add doc_id to hybrid query response" before proceeding. Either way, do not fake the recall number.

- [ ] **Step 4: Write a smoke test**

```go
package bench

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
	"github.com/shenli/agentdb/workload/internal/fixture"
)

func TestHybridRetrievalSmoke(t *testing.T) {
	corpus, err := fixture.Load("")
	if err != nil {
		t.Skipf("fixture missing: %v", err)
	}
	url := os.Getenv("AGENTDB_URL")
	if url == "" {
		url = "http://127.0.0.1:7878"
	}
	if resp, err := http.Get(url + "/api/admin/boxes"); err != nil || resp.StatusCode != 200 {
		t.Skipf("agentdb unreachable")
	}
	b := backend.NewAgentDB(url)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg := DefaultHybridRetrieval()
	cfg.CollectSizes = []int{200}
	cfg.QueriesPerRun = 5
	rep, err := RunHybridRetrieval(ctx, b, corpus, cfg)
	if err != nil {
		t.Fatalf("RunHybridRetrieval: %v", err)
	}
	if len(rep.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(rep.Samples))
	}
	if rep.Samples[0].Summary.Count == 0 {
		t.Fatalf("no queries recorded")
	}
	if rep.Samples[0].RecallAtK <= 0 {
		t.Fatalf("recall@10 was 0 — check DocID plumbing")
	}
}
```

- [ ] **Step 5: Run**

```bash
cd workload && go test ./internal/bench/... -run TestHybridRetrievalSmoke -v
```

Expected: PASS. Recall is small sample (5 queries, 200 docs) so will be 0.1–1.0 — anything > 0 confirms DocID plumbing.

- [ ] **Step 6: Commit**

```bash
git add workload/internal/bench/hybridretrieval.go workload/internal/bench/hybridretrieval_test.go workload/internal/backend/pg_tuned.go workload/internal/backend/agentdb.go
git commit -m "feat(bench): bench 4 — tenant-filtered hybrid retrieval (BM25+vector) with recall@k"
```

### Task 17: `agentdb-bench` CLI

**Files:**
- Create: `workload/cmd/agentdb-bench/main.go`

- [ ] **Step 1: Write the CLI**

```go
// Command agentdb-bench runs the four comparison benches against one or
// more backends and writes JSON results to an output directory.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shenli/agentdb/workload/internal/backend"
	"github.com/shenli/agentdb/workload/internal/bench"
	"github.com/shenli/agentdb/workload/internal/fixture"
)

func main() {
	agentdbURL := flag.String("agentdb-url", "http://127.0.0.1:7878", "AgentDB base URL")
	pgDSN := flag.String("pg-dsn", "postgres://bench:bench@127.0.0.1:5433/bench?sslmode=disable", "Postgres DSN")
	backendsFlag := flag.String("backends", "agentdb,pg_naive,pg_tuned", "comma-separated backend names")
	benchesFlag := flag.String("bench", "all", "hotsession|fork|largevalue|retrieval|all")
	concurrencies := flag.String("concurrency", "1,4,16,64", "comma-separated concurrency levels (hotsession, largevalue)")
	duration := flag.Duration("duration", 15*time.Second, "measured duration per bench cell")
	outDir := flag.String("out", "bench-out", "output directory")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	corpus, err := fixture.Load("")
	if err != nil {
		log.Printf("warning: fixture not loaded: %v (retrieval bench will be skipped)", err)
	}

	ctx := context.Background()
	backends, cleanups, err := buildBackends(ctx, *backendsFlag, *agentdbURL, *pgDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		for _, c := range cleanups {
			_ = c()
		}
	}()

	whichBenches := strings.Split(*benchesFlag, ",")
	if *benchesFlag == "all" {
		whichBenches = []string{"hotsession", "fork", "largevalue", "retrieval"}
	}
	concs := parseInts(*concurrencies)

	all := map[string]any{}
	for _, name := range whichBenches {
		switch strings.TrimSpace(name) {
		case "hotsession":
			var reps []bench.HotSessionReport
			for _, conc := range concs {
				for _, b := range backends {
					cfg := bench.DefaultHotSession(conc)
					cfg.Duration = *duration
					log.Printf("[hotsession] backend=%s concurrency=%d", b.Name(), conc)
					r, err := bench.RunHotSession(ctx, b, cfg)
					if err != nil {
						log.Printf("  ERR: %v", err)
						continue
					}
					reps = append(reps, r)
				}
			}
			all["hotsession"] = reps
			writeJSON(*outDir, "hotsession.json", reps)
		case "fork":
			var reps []bench.ForkScalingReport
			for _, b := range backends {
				log.Printf("[fork] backend=%s", b.Name())
				r, err := bench.RunForkScaling(ctx, b, bench.DefaultForkScaling())
				if err != nil {
					log.Printf("  ERR: %v", err)
					continue
				}
				reps = append(reps, r)
			}
			all["fork"] = reps
			writeJSON(*outDir, "fork.json", reps)
		case "largevalue":
			var reps []bench.LargeValueReport
			for _, conc := range concs {
				for _, b := range backends {
					cfg := bench.DefaultLargeValue(conc)
					cfg.Duration = *duration
					log.Printf("[largevalue] backend=%s concurrency=%d", b.Name(), conc)
					r, err := bench.RunLargeValue(ctx, b, cfg)
					if err != nil {
						log.Printf("  ERR: %v", err)
						continue
					}
					reps = append(reps, r)
				}
			}
			all["largevalue"] = reps
			writeJSON(*outDir, "largevalue.json", reps)
		case "retrieval":
			if corpus == nil {
				log.Printf("[retrieval] SKIP (no fixture)")
				continue
			}
			var reps []bench.HybridRetrievalReport
			for _, b := range backends {
				log.Printf("[retrieval] backend=%s", b.Name())
				r, err := bench.RunHybridRetrieval(ctx, b, corpus, bench.DefaultHybridRetrieval())
				if err != nil {
					log.Printf("  ERR: %v", err)
					continue
				}
				reps = append(reps, r)
			}
			all["retrieval"] = reps
			writeJSON(*outDir, "retrieval.json", reps)
		default:
			log.Printf("unknown bench %q", name)
		}
	}

	all["generated_at"] = time.Now().UTC().Format(time.RFC3339)
	writeJSON(*outDir, "data.json", all)
	log.Printf("wrote %s/data.json", *outDir)
}

func buildBackends(ctx context.Context, names, agentdbURL, pgDSN string) ([]backend.Backend, []func() error, error) {
	var out []backend.Backend
	var cleanups []func() error
	for _, raw := range strings.Split(names, ",") {
		name := strings.TrimSpace(raw)
		switch name {
		case "agentdb":
			b := backend.NewAgentDB(agentdbURL)
			out = append(out, b)
			cleanups = append(cleanups, b.Close)
		case "pg_naive":
			b, err := backend.NewPGNaive(ctx, pgDSN)
			if err != nil {
				return nil, cleanups, fmt.Errorf("pg_naive: %w", err)
			}
			out = append(out, b)
			cleanups = append(cleanups, b.Close)
		case "pg_tuned":
			b, err := backend.NewPGTuned(ctx, pgDSN)
			if err != nil {
				return nil, cleanups, fmt.Errorf("pg_tuned: %w", err)
			}
			out = append(out, b)
			cleanups = append(cleanups, b.Close)
		default:
			return nil, cleanups, fmt.Errorf("unknown backend %q", name)
		}
	}
	return out, cleanups, nil
}

func writeJSON(dir, name string, v any) {
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		log.Printf("write %s: %v", name, err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Printf("encode %s: %v", name, err)
	}
}

func parseInts(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(p), "%d", &n); err == nil {
			out = append(out, n)
		}
	}
	return out
}
```

- [ ] **Step 2: Build**

```bash
cd workload && go build -o bin/agentdb-bench ./cmd/agentdb-bench
./bin/agentdb-bench --help
```

Expected: flag help output.

- [ ] **Step 3: Quick end-to-end sanity run (1s duration)**

Make sure AgentDB + Postgres are up. Apply schemas if not already:

```bash
cd benchmarks/postgres && docker compose exec -T postgres psql -U bench -d bench < naive.sql
cd benchmarks/postgres && docker compose exec -T postgres psql -U bench -d bench < tuned.sql
cd ../../workload && ./bin/agentdb-bench --duration=1s --concurrency=1,4 --out=../bench-out
```

Expected: prints `wrote bench-out/data.json`. Inspect `bench-out/data.json` — has all four bench keys plus `generated_at`.

- [ ] **Step 4: Add `bench-out/` to the repo-root `.gitignore`**

```bash
echo "bench-out/" >> .gitignore
```

(Create `.gitignore` if it doesn't exist.)

- [ ] **Step 5: Commit**

```bash
git add workload/cmd/agentdb-bench/main.go .gitignore
git commit -m "feat(bench): agentdb-bench CLI that runs all four benches + merges JSON"
```

### Task 18: `benchmarks/run.sh` orchestrator

**Files:**
- Create: `benchmarks/run.sh`

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Orchestrate a full benchmark run:
#   1. ensure agentdb-server is up (background)
#   2. ensure Postgres is up (docker compose)
#   3. apply both Postgres schemas
#   4. run agentdb-bench
#   5. copy data.json into the dashboard static dir
#
# Assumes: cargo build -p agentdb-server has been done; docker is running.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

AGENTDB_URL="${AGENTDB_URL:-http://127.0.0.1:7878}"
PG_DSN="${PG_DSN:-postgres://bench:bench@127.0.0.1:5433/bench?sslmode=disable}"
BENCHES="${BENCHES:-all}"
DURATION="${DURATION:-15s}"
CONCURRENCY="${CONCURRENCY:-1,4,16,64}"

AGENTDB_PID=""
cleanup() {
    if [[ -n "${AGENTDB_PID}" ]]; then
        kill "${AGENTDB_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# 1. agentdb-server
if ! curl -sf "${AGENTDB_URL}/api/admin/boxes" > /dev/null 2>&1; then
    echo "starting agentdb-server..."
    cargo run --release -p agentdb-server &> /tmp/agentdb-bench.server.log &
    AGENTDB_PID=$!
    for _ in $(seq 1 60); do
        if curl -sf "${AGENTDB_URL}/api/admin/boxes" > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    if ! curl -sf "${AGENTDB_URL}/api/admin/boxes" > /dev/null 2>&1; then
        echo "agentdb-server did not come up; see /tmp/agentdb-bench.server.log" >&2
        exit 1
    fi
fi

# 2. postgres
pushd benchmarks/postgres > /dev/null
docker compose up -d postgres
for _ in $(seq 1 30); do
    if docker compose exec -T postgres pg_isready -U bench -d bench > /dev/null 2>&1; then
        break
    fi
    sleep 1
done
# 3. schemas (idempotent)
docker compose exec -T postgres psql -U bench -d bench < naive.sql
docker compose exec -T postgres psql -U bench -d bench < tuned.sql
popd > /dev/null

# 4. build + run
cd workload
go build -o bin/agentdb-bench ./cmd/agentdb-bench
./bin/agentdb-bench \
    --agentdb-url "${AGENTDB_URL}" \
    --pg-dsn "${PG_DSN}" \
    --bench "${BENCHES}" \
    --duration "${DURATION}" \
    --concurrency "${CONCURRENCY}" \
    --out "${ROOT}/bench-out"
cd "${ROOT}"

# 5. publish to dashboard
cp bench-out/data.json crates/agentdb-dashboard/static/data.json
echo "bench data.json → crates/agentdb-dashboard/static/data.json"

# 6. summary
echo
echo "=== summary ==="
python3 - "${ROOT}/bench-out/data.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
for bench, reps in d.items():
    if bench == "generated_at":
        continue
    print(f"[{bench}]")
    if isinstance(reps, list):
        for r in reps:
            name = r.get("backend", "?")
            if "samples" in r:
                for s in r["samples"]:
                    size_key = next((k for k in s if k != "summary"), None)
                    size = s.get(size_key)
                    print(f"  {name:<10} {size_key}={size:<8} p50={s['summary']['p50_ms']:.2f}ms p99={s['summary']['p99_ms']:.2f}ms rps={s['summary']['rps']:.0f}")
            else:
                s = r.get("summary", {})
                print(f"  {name:<10} conc={r.get('config', {}).get('concurrency', '?'):<4} p50={s.get('p50_ms', 0):.2f}ms p99={s.get('p99_ms', 0):.2f}ms rps={s.get('rps', 0):.0f}")
PY
```

- [ ] **Step 2: Make executable**

```bash
chmod +x benchmarks/run.sh
```

- [ ] **Step 3: Smoke-run with short duration**

```bash
DURATION=1s CONCURRENCY=1,4 BENCHES=hotsession,fork bash benchmarks/run.sh
```

Expected: prints summary for `hotsession` and `fork` across three backends. `crates/agentdb-dashboard/static/data.json` is created/updated.

- [ ] **Step 4: Commit**

```bash
git add benchmarks/run.sh
git commit -m "feat(bench): benchmarks/run.sh orchestrator (up servers → run → publish)"
```

### Task 19: HTML report page

**Files:**
- Create: `crates/agentdb-dashboard/static/benchmarks.html`
- Create: `crates/agentdb-dashboard/static/benchmarks.js`
- Modify: `crates/agentdb-dashboard/src/lib.rs`

- [ ] **Step 1: Write `benchmarks.html`**

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>AgentDB Benchmarks</title>
    <link rel="stylesheet" href="/dashboard/styles.css" />
    <style>
      main { padding: 1rem 2rem; }
      .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-bottom: 2rem; }
      .chart-cell { background: #fff; padding: 1rem; border: 1px solid #eee; border-radius: 4px; }
      .no-data { padding: 2rem; text-align: center; color: #888; }
      h1, h2 { margin: 0 0 .5rem 0; }
      .meta { color: #666; margin-bottom: 1.5rem; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
    <script defer src="/benchmarks/benchmarks.js"></script>
  </head>
  <body>
    <header>
      <h1>AgentDB Benchmarks</h1>
      <div class="meta" id="meta">loading…</div>
    </header>
    <main>
      <div id="no-data" class="no-data" style="display:none">
        no run yet — run <code>bash benchmarks/run.sh</code>
      </div>
      <section class="chart-row">
        <div class="chart-cell"><h2>Hot session — p99 by concurrency</h2><canvas id="chart-hotsession"></canvas></div>
        <div class="chart-cell"><h2>Fork cost vs. parent size</h2><canvas id="chart-fork"></canvas></div>
      </section>
      <section class="chart-row">
        <div class="chart-cell"><h2>Append p99 vs. payload size</h2><canvas id="chart-largevalue"></canvas></div>
        <div class="chart-cell"><h2>Hybrid retrieval — p99 / recall@10</h2><canvas id="chart-retrieval"></canvas></div>
      </section>
    </main>
  </body>
</html>
```

- [ ] **Step 2: Write `benchmarks.js`**

```javascript
// Render four Chart.js charts from /benchmarks/data.json.

const COLORS = {
  agentdb:  '#1f77b4',
  pg_naive: '#d62728',
  pg_tuned: '#2ca02c',
};

async function main() {
  let data;
  try {
    const resp = await fetch('/benchmarks/data.json');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    data = await resp.json();
  } catch (e) {
    document.getElementById('meta').textContent = '';
    document.getElementById('no-data').style.display = 'block';
    return;
  }
  document.getElementById('meta').textContent =
    'generated: ' + (data.generated_at || 'unknown');

  renderHotSession(data.hotsession || []);
  renderFork(data.fork || []);
  renderLargeValue(data.largevalue || []);
  renderRetrieval(data.retrieval || []);
}

function renderHotSession(reports) {
  const concs = [...new Set(reports.map(r => r.config.concurrency))].sort((a, b) => a - b);
  const byBackend = {};
  reports.forEach(r => {
    byBackend[r.backend] = byBackend[r.backend] || {};
    byBackend[r.backend][r.config.concurrency] = r.summary.p99_ms;
  });
  const datasets = Object.keys(byBackend).map(name => ({
    label: name,
    data: concs.map(c => byBackend[name][c] ?? null),
    borderColor: COLORS[name] || '#888',
    backgroundColor: COLORS[name] || '#888',
    fill: false,
    tension: 0.2,
  }));
  new Chart(document.getElementById('chart-hotsession'), {
    type: 'line',
    data: { labels: concs, datasets },
    options: {
      scales: {
        x: { title: { display: true, text: 'concurrency' } },
        y: { title: { display: true, text: 'p99 latency (ms)' } },
      },
    },
  });
}

function renderFork(reports) {
  const sizes = reports[0]?.samples?.map(s => s.parent_events) || [];
  const datasets = reports.map(r => ({
    label: r.backend,
    data: r.samples.map(s => s.summary.p50_ms),
    borderColor: COLORS[r.backend] || '#888',
    backgroundColor: COLORS[r.backend] || '#888',
    fill: false,
    tension: 0.2,
  }));
  new Chart(document.getElementById('chart-fork'), {
    type: 'line',
    data: { labels: sizes, datasets },
    options: {
      scales: {
        x: { type: 'logarithmic', title: { display: true, text: 'parent events' } },
        y: { title: { display: true, text: 'fork p50 (ms)' } },
      },
    },
  });
}

function renderLargeValue(reports) {
  // For each payload size, pick the single-concurrency p99 for the clearest story.
  const sizes = reports[0]?.samples?.map(s => s.payload_bytes) || [];
  const datasets = reports.map(r => ({
    label: r.backend + ' (c=' + r.config.concurrency + ')',
    data: r.samples.map(s => s.summary.p99_ms),
    borderColor: COLORS[r.backend] || '#888',
    backgroundColor: COLORS[r.backend] || '#888',
    fill: false,
    tension: 0.2,
  }));
  new Chart(document.getElementById('chart-largevalue'), {
    type: 'line',
    data: { labels: sizes.map(b => (b >= 1024 ? (b / 1024) + ' KB' : b + ' B')), datasets },
    options: {
      scales: {
        x: { title: { display: true, text: 'payload size' } },
        y: { title: { display: true, text: 'append p99 (ms)' } },
      },
    },
  });
}

function renderRetrieval(reports) {
  const sizes = reports[0]?.samples?.map(s => s.collection_size) || [];
  const latDatasets = reports.map(r => ({
    label: r.backend + ' p99',
    yAxisID: 'y-latency',
    data: r.samples.map(s => s.summary.p99_ms),
    borderColor: COLORS[r.backend] || '#888',
    backgroundColor: COLORS[r.backend] || '#888',
    fill: false,
  }));
  const recallDatasets = reports.map(r => ({
    label: r.backend + ' recall@10',
    yAxisID: 'y-recall',
    data: r.samples.map(s => s.recall_at_k),
    borderColor: COLORS[r.backend] || '#888',
    backgroundColor: COLORS[r.backend] || '#888',
    fill: false,
    borderDash: [4, 4],
  }));
  new Chart(document.getElementById('chart-retrieval'), {
    type: 'line',
    data: { labels: sizes, datasets: latDatasets.concat(recallDatasets) },
    options: {
      scales: {
        x: { title: { display: true, text: 'collection size' } },
        'y-latency': { position: 'left', title: { display: true, text: 'p99 (ms)' } },
        'y-recall':  { position: 'right', min: 0, max: 1, title: { display: true, text: 'recall@10' }, grid: { drawOnChartArea: false } },
      },
    },
  });
}

document.addEventListener('DOMContentLoaded', main);
```

- [ ] **Step 3: Read current `lib.rs` to plan the route change**

```bash
cat crates/agentdb-dashboard/src/lib.rs
```

Current shape (from the existing code):

```rust
use std::path::PathBuf;
use axum::Router;
use tower_http::services::ServeDir;

pub fn router() -> Router {
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    Router::new().fallback_service(ServeDir::new(static_dir))
}
```

- [ ] **Step 4: Add a `/benchmarks` sibling route**

Edit `crates/agentdb-dashboard/src/lib.rs` to:

```rust
use std::path::PathBuf;

use axum::Router;
use tower_http::services::ServeDir;

/// Sub-router that serves the dashboard's static assets.
/// Mounted under `/dashboard` by the caller.
pub fn router() -> Router {
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    Router::new().fallback_service(ServeDir::new(static_dir))
}

/// Sub-router serving the benchmarks report page + data.json. Same static
/// dir as `router()` but rooted at `benchmarks.html` so the page lives at
/// `/benchmarks/` cleanly.
pub fn benchmarks_router() -> Router {
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    Router::new().fallback_service(
        ServeDir::new(static_dir).append_index_html_on_directories(false),
    )
    // caller adds its own index: GET /benchmarks/ -> /benchmarks/benchmarks.html
}
```

Actually that's awkward — a cleaner approach is: the dashboard already serves its whole `static/` dir at `/dashboard/*`. We can reuse the same mount by pointing `/benchmarks` at the same dir, *or* add a second `nest_service` in `agentdb-api` that serves the same directory at `/benchmarks/`. Do the latter; leave `lib.rs` alone.

Revert the `lib.rs` change:

```rust
use std::path::PathBuf;
use axum::Router;
use tower_http::services::ServeDir;

pub fn router() -> Router {
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    Router::new().fallback_service(ServeDir::new(static_dir))
}
```

- [ ] **Step 5: Wire the route in `crates/agentdb-api/src/lib.rs`**

Inspect:

```bash
grep -n dashboard crates/agentdb-api/src/lib.rs
```

Add a second `.nest_service` line after the existing dashboard one, pointing at the same static dir *but serving `benchmarks.html` at the root*. Easiest: reuse the dashboard router (which ServeDir already does). The dashboard's static dir contains `benchmarks.html` and `data.json`, so `/dashboard/benchmarks.html` already works.

**Simplest working path:** just link to `/dashboard/benchmarks.html`. We can revisit a prettier `/benchmarks` URL later by adding a dedicated crate-side route.

Edit `crates/agentdb-api/src/lib.rs` — add a redirect route so `/benchmarks` is still usable:

```rust
// In the Router chain, alongside .nest_service("/dashboard", ...):
.route("/benchmarks",   axum::routing::get(|| async { axum::response::Redirect::permanent("/dashboard/benchmarks.html") }))
.route("/benchmarks/",  axum::routing::get(|| async { axum::response::Redirect::permanent("/dashboard/benchmarks.html") }))
.route("/benchmarks/data.json", axum::routing::get(|| async { axum::response::Redirect::permanent("/dashboard/data.json") }))
```

Update the JS fetch accordingly:

```javascript
const resp = await fetch('/dashboard/data.json');
```

Update the HTML:

```html
<script defer src="/dashboard/benchmarks.js"></script>
```

- [ ] **Step 6: Rebuild + smoke test**

```bash
cargo build -p agentdb-server
lsof -ti:7878 | xargs kill 2>/dev/null || true
(cargo run -p agentdb-server &) ; until curl -sf http://127.0.0.1:7878/dashboard/ > /dev/null; do sleep 1; done
curl -sI http://127.0.0.1:7878/benchmarks | head -3
curl -sI http://127.0.0.1:7878/dashboard/benchmarks.html | head -3
```

Expected: `/benchmarks` returns `301 Moved Permanently` → `/dashboard/benchmarks.html`; the latter returns `200 OK`.

Open `http://127.0.0.1:7878/benchmarks` in a browser. If `data.json` exists in `crates/agentdb-dashboard/static/`, charts render; if not, the "no run yet" banner shows.

- [ ] **Step 7: Stop the server**

```bash
lsof -ti:7878 | xargs kill 2>/dev/null || true
```

- [ ] **Step 8: Commit**

```bash
git add crates/agentdb-dashboard/static/benchmarks.html crates/agentdb-dashboard/static/benchmarks.js crates/agentdb-api/src/lib.rs
git commit -m "feat(bench): /benchmarks report page (Chart.js over data.json)"
```

### Task 20: `docs/BENCHMARKS.md`

**Files:**
- Create: `docs/BENCHMARKS.md`

- [ ] **Step 1: Write the methodology doc**

```markdown
# AgentDB Benchmarks

Head-to-head against two Postgres shapes on an agent-shaped workload:
hot-session append+fetch, fork cost, large-value append, and tenant-filtered
hybrid retrieval. The harness lives in `workload/cmd/agentdb-bench` and is
orchestrated by `benchmarks/run.sh`.

## TL;DR

```bash
cargo build --release -p agentdb-server
cd workload && go build -o bin/agentdb-bench ./cmd/agentdb-bench && cd ..
bash benchmarks/run.sh
open http://127.0.0.1:7878/benchmarks
```

The orchestrator starts `agentdb-server` + a local Postgres 17 + pgvector
container, applies both Postgres schemas, runs four benches across three
backends, writes `bench-out/data.json`, and copies it into the dashboard
static dir so `/benchmarks` can render the charts.

## Backends

| Name      | What                                                            |
|-----------|-----------------------------------------------------------------|
| agentdb   | AgentDB prototype, REST over HTTP                               |
| pg_naive  | Postgres, `sessions(id, messages jsonb)` — day-one PoC schema   |
| pg_tuned  | Postgres, row-per-event + pgvector HNSW + tsvector GIN (serious)|

Both Postgres schemas live in `benchmarks/postgres/`; see `naive.sql`,
`tuned.sql`. Tuning (`postgresql.conf` overrides) is committed.

## Rig

- Same machine, same run. Backends are measured in sequence, not in
  parallel — caches warm per-backend, but no cross-backend CPU theft.
- Every bench runs against a backend that has just been `Reset()`. Ten
  percent of the duration is warm-up and discarded.
- Latency into HDR histogram (1 µs – 60 s, 3 sig-figs). Per-cell stats:
  count, p50, p90, p99, max, RPS, errors.
- Connections: AgentDB HTTP keep-alive (256 idle conns); Postgres pgx
  pool with default pool size.
- Errors are counted, not retried. AgentDB 429s count as errors — we
  characterise behaviour at offered load, not what a well-behaved client
  can extract.

## Benches

### 1 — Hot session `append_and_fetch`

Op: append one 512-byte event, fetch last 5, same session.
Concurrency: 1, 4, 16, 64 goroutines all pounding one `(tenant, session)`.
Duration: 15 s per cell. Warmup: 10%.

- **agentdb** — one `append_and_fetch` REST call. Box write-lock
  serializes within-session work.
- **pg_naive** — `INSERT ... ON CONFLICT DO UPDATE messages = messages || $1`
  + jsonb read-back for last 5. Row-lock contention on the session row;
  jsonb array rewrite every append.
- **pg_tuned** — `INSERT INTO events RETURNING seq` + `SELECT ... LIMIT 5`
  inside one tx.

### 2 — Fork cost scaling

Fork a parent that contains N events. Measure the fork op alone; parent
is pre-populated in setup. Sizes: 10 / 100 / 1 000 / 10 000. 50 forks
per size.

- **agentdb** — O(1); one new `Session` row with a `ForkRef` pointer.
- **pg_naive** — `INSERT ... SELECT messages FROM sessions WHERE id = parent`.
  O(N) jsonb copy.
- **pg_tuned** — `INSERT INTO events SELECT ... WHERE session_id = parent`.
  O(N) row copy.

### 3 — Append latency vs. payload size

One session per worker (so: no contention — this bench isolates the
payload-size effect). Payloads: 1 KB / 16 KB / 256 KB. 10 s per size.

- **agentdb** — `append_and_fetch(..., fetch_last_n=0)`.
- **pg_naive** — `UPDATE sessions SET messages = messages || $1`. At
  256 KB, TOAST'd jsonb is decompressed on every read-before-append.
- **pg_tuned** — `INSERT INTO events (payload bytea, ...)`. Clean TOAST,
  no read-back on write.

### 4 — Hybrid retrieval (BM25 + vector + tenant filter)

Index `N` docs from `benchmarks/fixtures/corpus.jsonl`. Run the
committed `queries.jsonl` (first-sentence of random docs). Top-10 hits,
tenant-filtered. Collection sizes: 1 000 / 10 000. 100 queries per size.

- **agentdb** — `POST /api/query/hybrid`; `HybridPlanner` pre-filters by
  tenant, then runs BM25 ∪ cosine.
- **pg_tuned** — `SELECT ... WHERE tenant_id=$1 AND tsv @@ $2 ORDER BY embedding <=> $3 LIMIT 10`
  over BTREE + GIN + HNSW indexes.
- **pg_naive** — **not supported** by `Backend.HybridQuery` (the
  conformance test skips it); the bench records this as expected and
  reports only agentdb + pg_tuned.

Recall@10 is computed against ground-truth from `queries.jsonl`
(brute-force top-10 at generation time, same embedding model as the
bench). Recall is disclosed, not scored win/loss — both AgentDB and
pgvector use HNSW, calibrated to similar `m` / `ef_construction` /
`ef_search`. Exact values land here after the first real run:

- AgentDB: `m=…`, `ef_construction=…`, `ef_search=…` (set via `AGENTDB_HNSW_EF`)
- pgvector: `m=16`, `ef_construction=200`, `ef_search=…` (via `SET hnsw.ef_search`)

## What's NOT claimed

- **No absolute "which DB is faster."** Numbers move with hardware and
  tuning. Re-run on your hardware.
- **No cross-run statistical significance.** One run per cell; percentile
  ranks within that run are what we report.
- **No warm→cold tier comparison.** Postgres "tiering" = table
  partitioning / external archive — apples vs. paperwork.
- **No fine-tuned Postgres knobs** beyond the committed
  `postgresql.conf`. No `pg_prewarm`, no custom extensions beyond
  pgvector.

## Results

Results land in `bench-out/data.json` and render at
[`/benchmarks`](http://127.0.0.1:7878/benchmarks) when the server is up.
Representative numbers from the author's laptop are in the commit log of
this file.
```

- [ ] **Step 2: Commit**

```bash
git add docs/BENCHMARKS.md
git commit -m "docs(bench): BENCHMARKS.md methodology"
```

### Checkpoint: Phase 2 complete

At this point:
- `bash benchmarks/run.sh` produces `bench-out/data.json` across all three backends
- `/benchmarks` (via `/dashboard/benchmarks.html`) renders four charts
- `docs/BENCHMARKS.md` describes methodology
- All conformance + smoke tests pass in `go test ./...`

Before continuing to Phase 3, run the full stack once end-to-end:

```bash
BENCHES=all DURATION=5s CONCURRENCY=1,4,16 bash benchmarks/run.sh
open http://127.0.0.1:7878/benchmarks
```

Sanity-check the charts visually. If any look wrong (e.g., empty dataset, wrong axis), debug before Phase 3 — Phase 3 is independent but you don't want to stack issues.

---

## Phase 3 — Python Demo

Goal: `python -m agentdb_demo --question ... --seed N` runs the scripted research agent against AgentDB deterministically. `docs/DEMO.md` narrates it. `README.md` is updated.

### Task 21: `demo/pyproject.toml` + project skeleton

**Files:**
- Create: `demo/pyproject.toml`
- Create: `demo/agentdb_demo/__init__.py`
- Create: `demo/cli.py`
- Create: `demo/.gitignore`

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "agentdb-demo"
version = "0.1.0"
description = "Scripted research-agent demo for AgentDB"
requires-python = ">=3.11"
dependencies = [
    "httpx>=0.27",
    "numpy>=1.26",
    "rich>=13.7",
]

[project.optional-dependencies]
dev = [
    "pytest>=8",
]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["agentdb_demo*"]
```

- [ ] **Step 2: Write `agentdb_demo/__init__.py`**

```python
"""Scripted research-agent demo for AgentDB."""

__version__ = "0.1.0"
```

- [ ] **Step 3: Write a stub `cli.py`**

```python
"""Entry point: python -m agentdb_demo.cli"""
from __future__ import annotations

import argparse
from agentdb_demo.agent import Agent


def main() -> int:
    p = argparse.ArgumentParser(prog="agentdb-demo")
    p.add_argument("--question", required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--agentdb-url", default="http://127.0.0.1:7878")
    p.add_argument("--tenant", default="t-demo-agent")
    args = p.parse_args()

    agent = Agent(agentdb_url=args.agentdb_url, tenant=args.tenant, seed=args.seed)
    agent.run(args.question)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: `.gitignore`**

```
.venv/
__pycache__/
*.pyc
demo-out/
*.egg-info/
build/
dist/
.pytest_cache/
```

- [ ] **Step 5: Verify the project structure builds**

```bash
cd demo
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
python -c "import agentdb_demo; print(agentdb_demo.__version__)"
```

Expected: `0.1.0` — but note `agent.py` doesn't exist yet, so `import` of `agentdb_demo.cli` will fail; that's fine, we only import `agentdb_demo` here.

- [ ] **Step 6: Commit**

```bash
git add demo/pyproject.toml demo/agentdb_demo/__init__.py demo/cli.py demo/.gitignore
git commit -m "feat(demo): Python project skeleton"
```

### Task 22: Fixture loader (Python)

**Files:**
- Create: `demo/agentdb_demo/fixture.py`
- Create: `demo/tests/__init__.py`
- Create: `demo/tests/test_fixture.py`

- [ ] **Step 1: Write `fixture.py`**

```python
"""Load benchmarks/fixtures/{corpus,queries}.jsonl."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class Doc:
    id: str
    text: str
    embedding: list[float]


@dataclass(frozen=True)
class Query:
    id: str
    text: str
    ground_truth: list[str]


def _fixture_dir() -> Path:
    env = os.environ.get("AGENTDB_FIXTURE_DIR")
    if env:
        return Path(env)
    here = Path(__file__).resolve().parent
    return here.parent.parent / "benchmarks" / "fixtures"


@lru_cache(maxsize=1)
def load_corpus() -> list[Doc]:
    p = _fixture_dir() / "corpus.jsonl"
    with p.open() as f:
        return [Doc(**json.loads(line)) for line in f if line.strip()]


@lru_cache(maxsize=1)
def load_queries() -> list[Query]:
    p = _fixture_dir() / "queries.jsonl"
    with p.open() as f:
        return [Query(**json.loads(line)) for line in f if line.strip()]


@lru_cache(maxsize=1)
def corpus_by_id() -> dict[str, Doc]:
    return {d.id: d for d in load_corpus()}
```

- [ ] **Step 2: Write `tests/__init__.py`**

Empty file.

- [ ] **Step 3: Write `tests/test_fixture.py`**

```python
from agentdb_demo.fixture import corpus_by_id, load_corpus, load_queries


def test_corpus_has_expected_shape():
    docs = load_corpus()
    assert len(docs) >= 1_000
    assert len(docs[0].embedding) == 384
    by_id = corpus_by_id()
    assert docs[0].id in by_id


def test_queries_have_ground_truth():
    qs = load_queries()
    assert len(qs) >= 10
    assert len(qs[0].ground_truth) >= 1
```

- [ ] **Step 4: Run**

```bash
cd demo && source .venv/bin/activate && pytest tests/test_fixture.py -v
```

Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add demo/agentdb_demo/fixture.py demo/tests/__init__.py demo/tests/test_fixture.py
git commit -m "feat(demo): fixture loader + tests"
```

### Task 23: Scripted tools (deterministic)

**Files:**
- Create: `demo/agentdb_demo/tools.py`
- Create: `demo/tests/test_tools.py`

- [ ] **Step 1: Write `tools.py`**

```python
"""Deterministic scripted tool catalog. No LLM, no network."""
from __future__ import annotations

import random
from dataclasses import dataclass

import numpy as np

from agentdb_demo.fixture import Doc, corpus_by_id, load_corpus


@dataclass(frozen=True)
class ToolCall:
    name: str
    args: dict


@dataclass(frozen=True)
class ToolResult:
    tool: str
    doc_id: str
    payload: str


def web_search(query: str, seed: int, k: int = 3) -> list[Doc]:
    """Return k docs whose embedding is closest (cosine) to a seeded pseudo-query.

    The 'query' string is hashed with the seed into an RNG that picks a random
    corpus doc; its embedding becomes the query vector. Deterministic for
    the same (query, seed)."""
    rng = random.Random((query, seed).__hash__())
    docs = load_corpus()
    anchor = docs[rng.randrange(len(docs))]
    avec = np.asarray(anchor.embedding, dtype=np.float32)
    # Compute cosine against all docs (corpus is ~10k, this is ~1ms).
    all_vecs = np.asarray([d.embedding for d in docs], dtype=np.float32)
    sims = all_vecs @ avec
    top = np.argpartition(-sims, k)[:k]
    top = top[np.argsort(-sims[top])]
    return [docs[int(i)] for i in top]


def read_doc(doc_id: str) -> str:
    """Return the full text of a corpus doc. Used as a 'large payload' source."""
    by_id = corpus_by_id()
    if doc_id not in by_id:
        raise KeyError(f"unknown doc_id: {doc_id}")
    return by_id[doc_id].text


def summarize(text: str, seed: int) -> str:
    """Deterministic 'summary': the first sentence, truncated to 200 chars."""
    first = text.split(".")[0][:200]
    # Seed is unused but part of the signature for parallelism with web_search.
    return first + ("…" if len(text) > len(first) else "")
```

- [ ] **Step 2: Write `tests/test_tools.py`**

```python
from agentdb_demo.tools import read_doc, summarize, web_search


def test_web_search_is_deterministic():
    a = web_search("hnsw", seed=42, k=3)
    b = web_search("hnsw", seed=42, k=3)
    assert [d.id for d in a] == [d.id for d in b]


def test_web_search_changes_with_seed():
    a = web_search("hnsw", seed=42, k=3)
    b = web_search("hnsw", seed=999, k=3)
    assert [d.id for d in a] != [d.id for d in b]


def test_read_doc_returns_text():
    res = web_search("anything", seed=1, k=1)
    text = read_doc(res[0].id)
    assert isinstance(text, str)
    assert len(text) > 0


def test_summarize_is_deterministic():
    s1 = summarize("Lorem ipsum. Another sentence.", seed=1)
    s2 = summarize("Lorem ipsum. Another sentence.", seed=1)
    assert s1 == s2
```

- [ ] **Step 3: Run**

```bash
cd demo && source .venv/bin/activate && pytest tests/test_tools.py -v
```

Expected: all four tests PASS.

- [ ] **Step 4: Commit**

```bash
git add demo/agentdb_demo/tools.py demo/tests/test_tools.py
git commit -m "feat(demo): deterministic scripted tool catalog + tests"
```

### Task 24: `AgentDBClient` (Python)

**Files:**
- Create: `demo/agentdb_demo/client.py`

- [ ] **Step 1: Write the client**

```python
"""Thin sync AgentDB REST client for the demo. Reads like example code."""
from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Iterable

import httpx


@dataclass
class AppendResult:
    seq: int
    events: list[dict]


@dataclass
class Hit:
    session_id: str
    seq: int
    score: float
    doc_id: str | None = None


class AgentDBClient:
    def __init__(self, base_url: str, timeout_s: float = 10.0):
        self._http = httpx.Client(base_url=base_url, timeout=timeout_s)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "AgentDBClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def append_and_fetch(
        self, tenant: str, session: str, payload: bytes | str,
        text: str | None = None, embedding: Iterable[float] | None = None,
        fetch_last_n: int = 0,
    ) -> AppendResult:
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        body: dict[str, Any] = {
            "event": {
                "payload_b64": base64.b64encode(payload).decode("ascii"),
                "text": text,
                "embedding": list(embedding) if embedding is not None else None,
            },
            "fetch_last_n": fetch_last_n,
        }
        r = self._http.post(
            f"/api/tenants/{tenant}/sessions/{session}/append_and_fetch",
            json=body,
        )
        r.raise_for_status()
        j = r.json()
        return AppendResult(seq=int(j.get("seq", 0)), events=j.get("events", []))

    def fork(self, tenant: str, parent: str, child: str) -> None:
        r = self._http.post(
            f"/api/sessions/{parent}/fork",
            json={"tenant_id": tenant, "new_session_id": child},
        )
        r.raise_for_status()

    def hybrid_query(
        self, tenant: str, text: str | None = None,
        embedding: Iterable[float] | None = None, session: str | None = None,
        k: int = 10,
    ) -> list[Hit]:
        body: dict[str, Any] = {"tenant_id": tenant, "k": k}
        if text is not None:
            body["text"] = text
        if embedding is not None:
            body["embedding"] = list(embedding)
        if session is not None:
            body["session_id"] = session
        r = self._http.post("/api/query/hybrid", json=body)
        r.raise_for_status()
        raw = r.json().get("hits", [])
        return [
            Hit(
                session_id=h["session_id"],
                seq=int(h["seq"]),
                score=float(h.get("score", 0.0)),
                doc_id=h.get("doc_id"),
            )
            for h in raw
        ]
```

- [ ] **Step 2: Commit**

```bash
git add demo/agentdb_demo/client.py
git commit -m "feat(demo): sync AgentDB REST client"
```

### Task 25: Narrator

**Files:**
- Create: `demo/agentdb_demo/narrator.py`

- [ ] **Step 1: Write the narrator**

```python
"""Narrator: dual sink (rich stdout + JSONL to demo-out/run-{ts}.jsonl)."""
from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from rich.console import Console


@dataclass
class NarrativeEvent:
    ts_ms: int
    kind: str
    payload: dict = field(default_factory=dict)


class Narrator:
    def __init__(self, out_dir: str | Path = "demo-out", quiet: bool = False):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        self._path = self.out_dir / f"run-{ts}.jsonl"
        self._fp = self._path.open("w")
        self._console = Console(quiet=quiet)
        self._t0 = time.monotonic()

    @property
    def path(self) -> Path:
        return self._path

    def close(self) -> None:
        self._fp.close()

    def __enter__(self) -> "Narrator":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def emit(self, kind: str, **payload: Any) -> None:
        ev = NarrativeEvent(
            ts_ms=int((time.monotonic() - self._t0) * 1000),
            kind=kind,
            payload=payload,
        )
        line = json.dumps(asdict(ev), default=str)
        self._fp.write(line + "\n")
        self._fp.flush()
        self._render(ev)

    def _render(self, ev: NarrativeEvent) -> None:
        prefix = f"[dim]+{ev.ts_ms:>5}ms[/dim]"
        style = {
            "question":  "bold cyan",
            "plan":      "yellow",
            "fork":      "magenta",
            "tool":      "green",
            "retrieve":  "blue",
            "winner":    "bold green",
            "error":     "bold red",
        }.get(ev.kind, "white")
        msg = " ".join(f"{k}={v}" for k, v in ev.payload.items())
        self._console.print(f"{prefix} [{style}]{ev.kind:<10}[/{style}] {msg}")
```

- [ ] **Step 2: Commit**

```bash
git add demo/agentdb_demo/narrator.py
git commit -m "feat(demo): rich+JSONL narrator"
```

### Task 26: Research agent

**Files:**
- Create: `demo/agentdb_demo/agent.py`

- [ ] **Step 1: Write the agent**

```python
"""Scripted research-agent state machine. No LLM, deterministic from seed."""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass

from agentdb_demo.client import AgentDBClient
from agentdb_demo.fixture import Doc, load_corpus
from agentdb_demo.narrator import Narrator
from agentdb_demo.tools import read_doc, summarize, web_search


@dataclass
class PlanStep:
    tool: str
    doc_id: str


@dataclass
class Plan:
    name: str
    steps: list[PlanStep]


class Agent:
    def __init__(
        self,
        agentdb_url: str,
        tenant: str,
        seed: int,
        narrator: Narrator | None = None,
    ):
        self.agentdb_url = agentdb_url
        self.tenant = tenant
        self.seed = seed
        self.narrator = narrator or Narrator()
        self.rng = random.Random(seed)

    def run(self, question: str) -> dict:
        nar = self.narrator
        with AgentDBClient(self.agentdb_url) as client:
            parent = f"research-{_hash(question)}"
            nar.emit("question", text=question, seed=self.seed,
                     parent_session=parent)

            # QUESTION event in parent
            client.append_and_fetch(self.tenant, parent,
                                    payload=question, text=question,
                                    fetch_last_n=0)

            # Propose 3 plans deterministically
            plans = _propose_plans(question, self.rng)
            for p in plans:
                nar.emit("plan", name=p.name, steps=[s.tool for s in p.steps])

            # Fork per plan
            branch_sessions: list[tuple[str, Plan]] = []
            for i, plan in enumerate(plans):
                child = f"{parent}-{plan.name}"
                client.fork(self.tenant, parent, child)
                nar.emit("fork", parent=parent, child=child)
                branch_sessions.append((child, plan))

            # Execute each plan's steps, appending large tool-result events
            for session, plan in branch_sessions:
                for step in plan.steps:
                    if step.tool == "read_doc":
                        text = read_doc(step.doc_id)
                        nar.emit("tool", branch=plan.name, tool="read_doc",
                                 doc=step.doc_id, bytes=len(text))
                        client.append_and_fetch(
                            self.tenant, session,
                            payload=text.encode("utf-8"),
                            text=text[:256],
                            fetch_last_n=0,
                        )
                    elif step.tool == "summarize":
                        text = read_doc(step.doc_id)
                        s = summarize(text, self.seed)
                        nar.emit("tool", branch=plan.name, tool="summarize",
                                 doc=step.doc_id)
                        client.append_and_fetch(
                            self.tenant, session,
                            payload=s.encode("utf-8"),
                            text=s, fetch_last_n=0,
                        )

            # Hybrid retrieval against all of this tenant's events
            anchor = _anchor_doc(question, self.rng)
            nar.emit("retrieve", anchor=anchor.id, k=10)
            hits = client.hybrid_query(
                tenant=self.tenant, text=question,
                embedding=anchor.embedding, k=10,
            )

            # Vote on winning branch
            votes: dict[str, int] = {p.name: 0 for _, p in branch_sessions}
            session_to_plan = {s: p.name for s, p in branch_sessions}
            for h in hits:
                if h.session_id in session_to_plan:
                    votes[session_to_plan[h.session_id]] += 1
            winner = max(votes, key=votes.get) if votes else "none"
            nar.emit("winner", plan=winner, votes=votes)

            # Final event on parent
            client.append_and_fetch(
                self.tenant, parent,
                payload=f"winner={winner}".encode(), text=f"winner={winner}",
                fetch_last_n=0,
            )
            return {"winner": winner, "votes": votes}


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:10]


def _propose_plans(question: str, rng: random.Random) -> list[Plan]:
    corpus = load_corpus()
    plans: list[Plan] = []
    for i in range(3):
        n_steps = rng.randint(5, 10)
        steps: list[PlanStep] = []
        for _ in range(n_steps):
            doc = corpus[rng.randrange(len(corpus))]
            tool = rng.choice(["read_doc", "summarize"])
            steps.append(PlanStep(tool=tool, doc_id=doc.id))
        plans.append(Plan(name=f"plan-{i}", steps=steps))
    return plans


def _anchor_doc(question: str, rng: random.Random) -> Doc:
    # Deterministic: hash(question)-seeded pick.
    corpus = load_corpus()
    r = random.Random(hash((question, "anchor")))
    return corpus[r.randrange(len(corpus))]
```

- [ ] **Step 2: End-to-end smoke (requires a running AgentDB)**

```bash
cd /Users/lishen/work/agentdb && (cargo run -p agentdb-server &)
until curl -sf http://127.0.0.1:7878/api/admin/boxes > /dev/null; do sleep 1; done
cd demo && source .venv/bin/activate
python -m agentdb_demo.cli --question "How does HNSW work?" --seed 42
```

Expected: rich-printed timeline in the terminal with events for question → 3 plans → 3 forks → ~15-30 tool calls → retrieve → winner. `demo-out/run-{ts}.jsonl` is written.

- [ ] **Step 3: Stop AgentDB**

```bash
lsof -ti:7878 | xargs kill 2>/dev/null || true
```

- [ ] **Step 4: Commit**

```bash
git add demo/agentdb_demo/agent.py
git commit -m "feat(demo): research-agent state machine (question → plans → fork → tools → retrieve → winner)"
```

### Task 27: Deterministic golden trace test

**Files:**
- Create: `demo/tests/test_agent.py`
- Create: `demo/tests/goldens/run-q1-seed42.jsonl`

- [ ] **Step 1: Write the golden test**

```python
"""End-to-end: agent.run is deterministic from seed.

Requires a running agentdb-server. Skips otherwise."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import httpx
import pytest


GOLDEN = Path(__file__).parent / "goldens" / "run-q1-seed42.jsonl"
QUESTION = "How does HNSW work?"
SEED = 42
URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")


def _agentdb_up() -> bool:
    try:
        return httpx.get(URL + "/api/admin/boxes", timeout=0.5).status_code == 200
    except httpx.HTTPError:
        return False


def _kinds_from_jsonl(path: Path) -> list[str]:
    with path.open() as f:
        return [json.loads(line)["kind"] for line in f if line.strip()]


@pytest.mark.skipif(not _agentdb_up(), reason="agentdb-server not up at " + URL)
def test_agent_trace_kinds_deterministic(tmp_path):
    # Run the agent twice, compare trace kind-sequence.
    from agentdb_demo.agent import Agent
    from agentdb_demo.narrator import Narrator

    def once(dest: Path) -> list[str]:
        with Narrator(out_dir=dest, quiet=True) as nar:
            a = Agent(agentdb_url=URL, tenant="t-golden", seed=SEED, narrator=nar)
            a.run(QUESTION)
            return _kinds_from_jsonl(nar.path)

    a = once(tmp_path / "a")
    b = once(tmp_path / "b")
    assert a == b, f"non-deterministic kind sequence:\n{a}\nvs\n{b}"


@pytest.mark.skipif(not _agentdb_up(), reason="agentdb-server not up at " + URL)
def test_agent_trace_matches_golden(tmp_path):
    if not GOLDEN.exists():
        pytest.skip("golden not recorded yet; run record_golden.sh")
    from agentdb_demo.agent import Agent
    from agentdb_demo.narrator import Narrator

    with Narrator(out_dir=tmp_path, quiet=True) as nar:
        a = Agent(agentdb_url=URL, tenant="t-golden-match", seed=SEED, narrator=nar)
        a.run(QUESTION)
        actual = _kinds_from_jsonl(nar.path)
    expected = _kinds_from_jsonl(GOLDEN)
    assert actual == expected, "trace kinds drift from golden"
```

We test *kind sequence*, not full payload (timestamps and session-id hashes are incidental). Still a strong regression signal.

- [ ] **Step 2: Record the golden**

```bash
cd /Users/lishen/work/agentdb && (cargo run -p agentdb-server &)
until curl -sf http://127.0.0.1:7878/api/admin/boxes > /dev/null; do sleep 1; done
cd demo && source .venv/bin/activate
mkdir -p tests/goldens
python - <<'PY'
from pathlib import Path
from agentdb_demo.agent import Agent
from agentdb_demo.narrator import Narrator

with Narrator(out_dir="tests/goldens", quiet=True) as nar:
    a = Agent(agentdb_url="http://127.0.0.1:7878", tenant="t-golden-record", seed=42, narrator=nar)
    a.run("How does HNSW work?")
    # Rename to a stable name for the golden.
    target = Path("tests/goldens/run-q1-seed42.jsonl")
    Path(nar.path).rename(target)
    print("recorded:", target)
PY
```

- [ ] **Step 3: Re-run tests to confirm golden matches**

```bash
pytest tests/test_agent.py -v
```

Expected: both tests PASS.

- [ ] **Step 4: Stop agentdb-server**

```bash
lsof -ti:7878 | xargs kill 2>/dev/null || true
```

- [ ] **Step 5: Commit**

```bash
git add demo/tests/test_agent.py demo/tests/goldens/run-q1-seed42.jsonl
git commit -m "test(demo): deterministic trace golden for seed=42"
```

### Task 28: `docs/DEMO.md`

**Files:**
- Create: `docs/DEMO.md`

- [ ] **Step 1: Write the walkthrough**

```markdown
# AgentDB Research-Agent Demo

A scripted research agent that exercises every AgentDB primitive end-to-end:
`append_and_fetch`, session fork, and hybrid retrieval — with live-tiered
sessions visible on the dashboard as the run progresses.

No LLM. Fully deterministic from `--seed`. Reads like example code.

## Run

```bash
# 1. AgentDB
cargo run -p agentdb-server

# 2. Demo
cd demo
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
python -m agentdb_demo.cli --question "How does HNSW work?" --seed 42

# 3. Watch the dashboard while it runs
open http://127.0.0.1:7878/dashboard
```

Artifacts:
- stdout: a rich-rendered timeline of the run (question, plans, forks,
  tool calls, retrieval, winner).
- `demo-out/run-{ts}.jsonl`: structured timeline, one JSON event per line.

## What happens, step by step

1. **`QUESTION`** — the question is appended to a parent session
   `research-{hash(question)}` via `append_and_fetch`. AgentDB creates the
   tenant + session on first write.
2. **Plan synthesis (3 plans)** — seeded-deterministic, picks a handful
   of corpus docs per plan and a tool per step (`read_doc` or
   `summarize`). No LLM — see `agentdb_demo/agent.py:_propose_plans`.
3. **Fork per plan** — each plan becomes a fork of the parent session at
   the QUESTION event. AgentDB records a parent-pointer with
   `parent_seq_at_fork`; no events are copied. You'll see three new
   sessions appear on the dashboard almost instantly.
4. **Tool execution per branch** — 5–10 steps. Each step appends one
   tool-result event to the branch's session. Payloads are large
   (full arXiv abstracts — 16–64 KB each) so the append-with-large-value
   path is exercised live.
5. **Hybrid retrieval** — one `POST /api/query/hybrid` scoped to the
   demo tenant: BM25 over the question text + cosine kNN over a seeded
   anchor embedding. AgentDB's `HybridPlanner` pre-filters by tenant
   before running BM25 ∪ cosine.
6. **Winner vote** — branches are ranked by how many of the top-10
   hits they contributed. A `FINAL` event with the winner is appended
   to the parent session.

## Which primitives did this exercise?

| Primitive | Where |
|---|---|
| `append_and_fetch` | Every step — QUESTION, TOOL_RESULT, FINAL |
| Session fork (parent-pointer, no copy) | Step 3 — three forks of the same parent |
| Large-value events (16–64 KB) | Step 4 — full-document tool results |
| Hybrid retrieval (BM25 + vector + tenant filter) | Step 5 |
| Per-box isolation + metrics | Continuously — visible on `/dashboard` |
| Warm→cold tiering | If the run is long enough, idle branches demote to cold and still answer retrieval |

## Seeds and determinism

`--seed` controls plan synthesis, tool choices per step, doc picks, and
the anchor embedding. Two runs with the same `(question, seed)` produce
the same trace-kind sequence; `tests/test_agent.py` enforces this with a
golden. The timestamps, session-id suffixes, and exact latencies are
incidental.

## Python, on purpose

The agent is ~150 lines of sync Python that uses one AgentDB session per
branch and reads top-to-bottom. If you're building an agent in
LangChain / LlamaIndex / OpenAI SDK / your own harness, the client calls
(`append_and_fetch`, `fork`, `hybrid_query`) map onto your existing code
directly.
```

- [ ] **Step 2: Commit**

```bash
git add docs/DEMO.md
git commit -m "docs(demo): DEMO.md walkthrough"
```

### Task 29: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Read the current README**

```bash
cat README.md
```

- [ ] **Step 2: Edit — add Demo and Benchmarks sections after Quickstart**

Use the Edit tool to insert two new sections between the "Quickstart" code block and the "Dashboard" heading. The inserted block:

```markdown
## Demo

A scripted Python research-agent exercises every primitive end-to-end:
question → 3 plans → fork-per-plan → 5–10 tool calls per branch →
hybrid retrieval → winner. Deterministic from `--seed`; no LLM.

```bash
# in another terminal, with agentdb-server running
cd demo
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
python -m agentdb_demo.cli --question "How does HNSW work?" --seed 42
```

The dashboard at http://127.0.0.1:7878/dashboard shows the forks
appearing, tier transitions, and metric ticks live. Full walk-through
in [docs/DEMO.md](docs/DEMO.md).

## Benchmarks

Head-to-head against two Postgres schemas — a day-one `sessions(jsonb)`
shape and a serious row-per-event + pgvector HNSW + tsvector GIN shape
— across four agent-shaped workloads: hot-session `append_and_fetch`,
fork cost scaling, append latency vs. payload size, and tenant-filtered
hybrid retrieval.

```bash
bash benchmarks/run.sh
open http://127.0.0.1:7878/benchmarks
```

Methodology, schemas, and what's explicitly *not* claimed are in
[docs/BENCHMARKS.md](docs/BENCHMARKS.md).

```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README Demo + Benchmarks sections"
```

### Task 30: Final end-to-end validation

- [ ] **Step 1: Clean rebuild**

```bash
cargo build --release -p agentdb-server
cd workload && go build -o bin/agentdb-bench ./cmd/agentdb-bench && cd ..
cd demo && source .venv/bin/activate && pip install -e . && cd ..
```

- [ ] **Step 2: Short full bench run**

```bash
DURATION=3s CONCURRENCY=1,4 BENCHES=all bash benchmarks/run.sh
```

Expected: writes `bench-out/data.json` with all 4 bench keys. The script's Python summary at the end prints one line per cell.

- [ ] **Step 3: Demo run**

```bash
(cargo run --release -p agentdb-server &)
until curl -sf http://127.0.0.1:7878/api/admin/boxes > /dev/null; do sleep 1; done
cd demo && source .venv/bin/activate
python -m agentdb_demo.cli --question "Can B-trees handle vectors?" --seed 7
```

Expected: narrative prints, ends with `winner=...`. `demo-out/run-{ts}.jsonl` exists.

- [ ] **Step 4: Browser check**

Open `http://127.0.0.1:7878/benchmarks` — four charts render. Open `http://127.0.0.1:7878/dashboard` — forks from the demo run still visible in the event feed (if recent).

- [ ] **Step 5: Full test suite green**

```bash
cd /Users/lishen/work/agentdb
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cd workload && go test ./... && cd ..
cd demo && source .venv/bin/activate && pytest && cd ..
bash scripts/smoke.sh
```

Expected: all green.

- [ ] **Step 6: Stop processes**

```bash
lsof -ti:7878 | xargs kill 2>/dev/null || true
cd benchmarks/postgres && docker compose down && cd ../..
```

### Checkpoint: Phase 3 complete

- Demo, benchmarks, and static report page all work end-to-end
- All tests pass (Rust + Go + Python + smoke)
- `README.md` points to both `docs/DEMO.md` and `docs/BENCHMARKS.md`

Ready to merge or continue polishing per review feedback.

---

## Self-review notes

Spec coverage check: each spec section maps to tasks:
- §Architecture / repo layout → Tasks 1–2, 5–8, 17, 19, 21
- §Components → Tasks 6–10 (backend), 11, 13–17 (bench), 21–26 (demo)
- §Data flow → exercised in Task 18 (orchestrator) and Task 30 (E2E)
- §Benchmark methodology → Tasks 11, 13–16, 20
- §Postgres schemas → Task 5
- §Error handling → baked into each impl; surfaced in bench reports via Summary.Errors
- §Testing → Tasks 7, 12, 22–23, 27, 30
- §References → cited in docs Tasks 20, 28, 29

Type-consistency check: `Backend` interface is defined once in Task 6 and used identically across Tasks 8–10, 13–17. `Event`/`Hit` fields are consistent. `Summary` JSON keys (p50_ms/p90_ms/p99_ms/max_ms/rps) match between Task 11 and the JS charts in Task 19. `Hit.DocID` plumbing is explicitly handled in Tasks 10 + 16 rather than left as a TODO.

No placeholders spotted in final pass. Tasks 19 and 26 note decisions made at implementation time (whether agentdb's hybrid-query response already carries `doc_id`; whether to use existing `workload/internal/client/rest.go` helpers) — those are genuine implementation-site decisions, not design TBDs.
