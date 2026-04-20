# AgentDB

A prototype agent-native operational database. The premise and design
motivation are in [docs/agent-db.md](docs/agent-db.md); this repo is
the showcase-grade prototype of what the primitives look like when
you actually build them.

## What's here

- **Document storage** keyed by tenant + session, served over both
  native REST and a subset of the MongoDB wire protocol
- **`append_and_fetch`** — the one-round-trip write primitive that
  appends an event and returns the last N in the same call
- **Session fork** — cross-session parent pointer so children see a
  frozen snapshot of the parent's events at fork time, without
  copying
- **Hybrid retrieval** — BM25 text + brute-force vector cosine,
  planned together with a tenant/session filter
- **Warm → cold tiering** — a lifecycle engine flushes idle sessions
  out of KV into object-storage segments, transparent to readers
- **Per-box isolation** — tenants are routed to boxes via FNV-1a;
  each box has its own metrics, quota, and fault-injection knob
- **Live dashboard** — Alpine.js page at `/dashboard` driven by
  `/api/admin/boxes` + a Server-Sent Events stream of tier changes,
  forks, fault injections, and per-second metric ticks
- **Go workload generator** — goroutine-per-tenant persona simulator
  that drives REST appends and Mongo `$search` aggregations

## Quickstart

```bash
# 1. start the database (listens on 7878 REST / 7879 Mongo)
cargo run -p agentdb-server

# 2. in a second terminal, open the dashboard
open http://127.0.0.1:7878/dashboard

# 3. in a third terminal, drive some traffic
cd workload && go run ./cmd/agentdb-workload -config config.yaml
```

The dashboard's three demo buttons talk directly to the REST API —
fork a session, inject a latency fault on box 0, and force a session
into the cold tier. For scripted traffic shaping, hit the workload's
control server on `:9090` (`/demo/burst`, `/demo/fork`, `/demo/cold`).

## Dashboard

<!-- screenshot placeholder: http://127.0.0.1:7878/dashboard with
     workload running. The box grid shows warm/cold dots per box;
     the event feed on the right shows live TierChange / ForkCreated
     / FaultInjected entries. -->

`docs/screenshots/dashboard.png` — not checked in yet; capture one
with the workload running and drop it here.

## Repo layout

```
bin/agentdb-server           binary: serves HTTP (:7878) + Mongo (:7879)
crates/agentdb-storage       KvStore trait + RocksDB impl
crates/agentdb-core          types, primitives, lifecycle, fork, metrics
crates/agentdb-index         per-box BM25 + cosine vector indexes
crates/agentdb-wire          MongoDB wire protocol subset (OP_MSG)
crates/agentdb-api           native REST: append_and_fetch, fork, admin, SSE
crates/agentdb-dashboard     static HTML/CSS/JS for /dashboard
workload/                    Go workload generator + demo control server
scripts/smoke.sh             end-to-end health check
docs/                        design doc, architecture, perf
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how the pieces
fit together, and [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for
numbers from a run on the author's laptop.

## Tests

```bash
cargo test --workspace                  # 161 tests, all async-free
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
bash scripts/smoke.sh                   # end-to-end: serve, append, fetch dashboard
cd workload && go build ./... && go vet ./...
```

## Status

Prototype. Every primitive in the design doc that touches a single
box is implemented; anything that requires real distribution (cross-
shard transactions, replication, real ANN, global admission control)
is explicitly out of scope — see the "What the prototype does NOT do"
section in `docs/ARCHITECTURE.md`.

## License

MIT OR Apache-2.0 (see [LICENSE](LICENSE)).
