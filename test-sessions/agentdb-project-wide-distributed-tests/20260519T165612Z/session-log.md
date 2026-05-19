# AgentDB Distributed Test Completion Session

**Plan:** `testing-plans/agentdb-project-wide-distributed-tests.md`
**Session:** `test-sessions/agentdb-project-wide-distributed-tests/20260519T165612Z`
**Goal:** complete residual depth from the project-wide distributed test plan.
**Local branch:** `codex-s9-routing-hints`
**Remote worktree:** `li-dev:/home/lishen/work/agentdb-finish-20260519T165612Z`
**Remote commit:** `ee0e339`

## Toolbox Discovered

- Real cluster compose harness: `ops/smoke/scripts/real-cluster-smoke-compose.sh`
- Stability suite wrapper: `ops/smoke/scripts/real-cluster-stability-suite.sh`
- Author-mode S8/S9 wrappers: `ops/smoke/tests/auto/S8_...sh`, `S9_...sh`
- Live S5 HTTP isolation pytest: `adapters/tests/auto/test_S5_...py`
- S11 libFuzzer target: `crates/agentdb-wire/fuzz/fuzz_targets/S11_...rs`
- S12 upgrade wrapper: `ops/local-ci/tests/auto/S12_...sh`
- S13 benchmark wrapper: `tools/agentdb-bench/tests/auto/S13_...sh`
- S14 TLC model: `docs/formal/auto/S14_...tla`

## Environment Probe

| Requirement | Result |
|---|---|
| Remote host | `li-dev` |
| Remote work dir | `/home/lishen/work/agentdb-finish-20260519T165612Z` |
| Docker compose | present, `Docker Compose version v5.1.3` |
| Rust | `cargo 1.95.0`, `rustc 1.95.0` |
| Java/TLC runtime | `java` present |
| Python | `python3 3.10.12` |
| Node/npm | present |

## Run Log

| Time UTC | Scenario | Action | Result |
|---|---|---|---|
| 2026-05-19T16:56:12Z | setup | created local session and remote detached worktree at `ee0e339` | PASS |
| 2026-05-19T17:00:00Z | S8 | seeded chaos failover, seeds 101 and 202 | PASS |
| 2026-05-19T17:36:00Z | S8 | seeded chaos seed 303 before retry-hint fix | FAIL: stale owner hint used when `leader_raft_node_id` was null |
| 2026-05-19T18:05:00Z | S8 | seed 303 rerun after null-leader rediscovery fix | PASS |
| 2026-05-19T18:35:00Z | S9 | replacement/split/merge cycle 1 before active-node fix | FAIL: split/merge catch-up targeted stopped replacement source |
| 2026-05-19T19:00:00Z | S9 | cycle 1 rerun after catch-up scope patch | PASS |
| 2026-05-19T19:13:54Z | S9 | cycle 2 before active-node handoff fix | FAIL: split/merge still received the original full node list |
| 2026-05-19T19:32:43Z | S9 | cycles 2-rerun, 3, 4, 5 after active-node handoff fix | PASS |
| 2026-05-19T20:26:00Z | S5 | live HTTP tenant-isolation author test | PASS, partial S5 surface coverage |
| 2026-05-19T20:27:00Z | S11 | libFuzzer target, 1,000,000 runs | PASS |
| 2026-05-19T20:27:00Z | S12 | old binary to current binary persisted-state upgrade | PASS |
| 2026-05-19T20:27:32Z | S13 | benchmark smoke, concurrency 1 and 4, 10s duration | PASS, partial release-grade coverage |
| 2026-05-19T20:28:32Z | S14 | TLC model check | PASS, bounded/skeletal model |
