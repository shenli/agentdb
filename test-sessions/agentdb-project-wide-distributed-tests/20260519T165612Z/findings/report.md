# AgentDB Distributed Test Completion Report

**Plan:** `testing-plans/agentdb-project-wide-distributed-tests.md`
**Session:** `test-sessions/agentdb-project-wide-distributed-tests/20260519T165612Z`
**Remote execution:** `li-dev:/home/lishen/work/agentdb-finish-20260519T165612Z`
**Base commit:** `ee0e339`
**Verdict:** PASS for the executable residual scenarios after two S9 harness fixes; several broad plan claims remain partial by scenario design.

## Summary

| Scenario | Run | Verdict |
|---|---|---|
| S8 | seeded chaos failover seeds 101, 202, 303-rerun | PASS |
| S9 | replacement/split/merge cycles 1-rerun, 2-rerun, 3, 4, 5 | PASS |
| S5 | live HTTP tenant isolation | PASS, partial |
| S11 | `cargo +nightly fuzz`, 1,000,000 runs | PASS |
| S12 | old binary to current binary single-node upgrade | PASS |
| S13 | benchmark smoke, concurrency 1 and 4, 10s duration | PASS, partial |
| S14 | TLC bounded model check | PASS, partial |

## Findings

### F1: S8 stale owner hint on null Raft leader

Initial S8 seed 303 failed when the harness retried a `RAFT_NOT_LEADER` response that included `owner_node_id:"node-b"` but `leader_raft_node_id:null`. The retry loop pinned to the metadata owner even though the response did not advertise a valid Raft leader.

Fix: `retry_owner_hint_from_error` now ignores owner hints when `leader_raft_node_id` is explicitly null, forcing owner rediscovery. `APPEND_RECOVERY_ATTEMPTS` was also raised from 8 to 16 to give leader churn enough time to settle.

Evidence:

- Failure log: `logs/S8-chaos-303.log`
- Passing rerun: `logs/S8-chaos-303-rerun.log`
- Passing controls: `logs/S8-chaos-101.log`, `logs/S8-chaos-202.log`

### F2: S9 split/merge used stale full node set after replacement

Initial S9 cycles failed after replacement stopped the source node. The subsequent split/merge phase still received the original full node list, so catch-up tried to send snapshots to the intentionally stopped source.

Fix: `run_explicit_replacement` now returns the active survivor node set, and `run_smoke` passes that active set into split/merge. The catch-up helper also scopes expected failures to currently reachable nodes.

Evidence:

- Failure logs: `logs/S9-cycle-1.log`, `logs/S9-cycle-2.log`
- Passing reruns: `logs/S9-cycle-1-rerun.log`, `logs/S9-cycle-2-rerun.log`
- Additional passing cycles: `logs/S9-cycle-3.log`, `logs/S9-cycle-4.log`, `logs/S9-cycle-5.log`

## Scenario Evidence

S8 produced three passing seeded chaos runs after the retry-hint fix. Each passing run ended with `real cluster smoke passed` and retained diagnostics under `artifacts/S8-chaos-*`.

S9 produced five passing hardening cycles after the active-node fix. The runs covered setup, idempotency replay/conflict, record-index visibility, watch/export cursor replay, restart persistence, checkpoint install, AZ failure, rolling restart, replacement, split prepare writes, and merge cutover.

S5 live HTTP isolation passed with `1 passed in 0.15s`. This does not cover the full SDK/Mongo/adapter/export matrix requested in the prose, so the S5 claim remains partial.

S11 completed `1,000,000` libFuzzer runs with no crash:

```text
#1000000 DONE cov: 1490 ft: 4494 corp: 1155/67Kb
```

S12 verified persisted state across an older binary and the current binary:

```text
old-write verified ... seqs=[0,1]
new-verify verified ... seqs=[0,1,2]
PASS: single-node upgrade preserved fetch/watch/export state and accepted a post-upgrade append
```

S13 produced zero-error benchmark smoke results:

```text
c=1 ops/s=228.80 ops=2288 errors=0 p50=4.371ms p95=10.100ms p99=12.282ms
c=4 ops/s=241.50 ops=2415 errors=0 p50=15.400ms p95=44.705ms p99=73.444ms
```

S14 TLC completed a full bounded state graph check:

```text
Model checking completed. No error has been found.
913 states generated, 144 distinct states found
```

## Residual Uncertainty

S5 remains partial because the authored executable test currently exercises native HTTP tenant isolation only, not the full Python SDK, TypeScript SDK, Mongo listener, adapter, and export isolation matrix.

S13 remains partial because the wrapper is a smoke benchmark. It does not execute the full release-grade matrix from the plan: three-repeat tolerance gates, noisy-neighbor tenant fairness, Zipf/cache-pressure profiles, strict fsync, and MongoDB/Postgres baseline comparisons.

S14 remains partial because the TLA model is intentionally small. It checks the current abstract invariants, but it does not yet encode the full routing epoch, split/merge, quorum membership, and export lease protocol surface from the prose.

No remaining executable task from the authored plan is blocked. The remaining work is to broaden the partial scenarios, not to rerun a known failing path.

## Confidence

Confidence is moderate for the S8/S9 harness-covered evaluation-cluster paths because they now have multiple remote passes with retryable Raft timeouts, owner churn, replacement, split/merge, and cursor checks. Confidence is moderate for the S12 single-node upgrade fixture covered here. Confidence remains low-to-moderate for project-wide isolation, performance, and formal claims until S5, S13, and S14 are expanded to match the full plan prose.
