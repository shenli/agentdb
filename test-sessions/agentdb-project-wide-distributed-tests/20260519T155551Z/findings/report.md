# AgentDB Distributed Test Continuation Report

**Plan:** `testing-plans/agentdb-project-wide-distributed-tests.md`
**Session:** `test-sessions/agentdb-project-wide-distributed-tests/20260519T155551Z`
**Base commit under review:** `862f4c7`
**Remote execution checkout:** `li-dev:/home/lishen/work/agentdb` at `3c291f6` with synced working-tree fixes
**Verdict:** PASS for all continuation runs; one routing observability gap remains

## Summary

This session extended the committed author-mode execution with repeated high-value paths:

| Scenario | Scope | Verdict |
|---|---|---|
| S11 | `cargo +nightly fuzz run ... -- -runs=100000` | PASS |
| S9 | six-node/two-cell full hardening repeat | PASS |
| S8 | six-node/two-cell failover repeat | PASS |
| S13 | benchmark smoke, concurrency `1,4`, 2s warmup, 10s duration | PASS |

## Evidence

S11 increased the fuzz budget from 1000 to 100000 runs:

```text
#100000 DONE cov: 1153 ft: 3181 corp: 719/21Kb
Done 100000 runs in 0 second(s)
```

S9 repeated the full replacement/split/merge path. It exercised multiple retryable Raft commit timeouts, explicit replacement, split prepare writes, and split/merge cutover. The critical prior-failure path now passed:

```text
split-merge: appending event 15 while split is prepared
smoke: append seq 15 retry 1 after convergence error: HTTP 500 ...
smoke: append seq 15 attempt 2 via node-b
split-merge: completed metadata split and merge cutover
real cluster smoke passed
```

S8 repeated the failover path. It killed initial writer `node-a`, observed owner change, wrote after failover, and later observed `dead_nodes_marked_total=1`:

```text
smoke: killing initial writer node-a
smoke: waiting for tablet owner change
smoke: appending event 5 after failover
smoke: appending secondary cell event 5 after primary failover
real cluster smoke passed
```

S13 produced zero-error benchmark smoke results:

```text
c=1 ops/s=230.30 ops=2303 errors=0 p50=4.262ms p95=10.090ms p99=12.736ms
c=4 ops/s=242.80 ops=2428 errors=0 p50=15.447ms p95=46.360ms p99=63.538ms
```

## Residual Finding

During S9 rolling restart, the harness recovered from `RAFT_NOT_LEADER`, but the error response still lacked leader/owner routing detail:

```text
"leader_raft_node_id":null,"owner_node_id":null,"owner_address":null
```

This did not cause a correctness failure in the run, but it remains an operability/routing observability gap during leader churn.

## Artifacts

- `logs/S11-cargo-fuzz-nightly-100k.log`
- `logs/S8-real-cluster-smoke.log`
- `logs/S8-repeat-1.wrapper.log`
- `logs/S9-full-hardening.log`
- `logs/S9-repeat-1.wrapper.log`
- `logs/S13-agentdb-bench-local.log`
- `artifacts/S8-repeat-1-diagnostics/`
- `artifacts/S8-repeat-1-node-logs/`
- `artifacts/S9-repeat-1-diagnostics/`
- `artifacts/S9-repeat-1-node-logs/`
- `artifacts/S13-benchmark-author-s13-20260519-092506/`

## Confidence

Confidence is now moderate for the specific S8 failover and S9 replacement/split/merge paths because each has two full post-fix remote passes, including retry-heavy runs. Confidence remains low-to-moderate for broader distributed recovery until repeated seeded chaos, larger cluster sizes, rolling upgrade matrices, and release-grade benchmark profiles run.
