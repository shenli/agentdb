# Continued Distributed Test Findings

Validation branch: `codex-expand-s5-typescript-isolation`
Validation head: `2508945`
PR: https://github.com/shenli/agentdb/pull/280
Remote host: `li-dev`

## Summary

The continued run found and fixed one test-harness bug in S8: the cluster smoke reachability probe treated `/health/ready` as JSON, so it misclassified healthy survivor nodes as unreachable and included a killed owner in raft catch-up scope. The fix changes the probe to use HTTP status only.

After that fix, all executable authored scenarios passed in local or remote runs. S12 remains INCONCLUSIVE because it requires an older executable server binary through `AGENTDB_OLD_SERVER_BIN`; no such binary was provided in the environment.

Continuation update: added an end-to-end MongoDB driver regression test that verifies unsupported Mongo mutations remain rejected without side effects. Re-ran the wire suite and server check locally and on `li-dev`, then extended the S11 fuzz smoke from 1M to 5M executions.

## PR Changes Added During This Continuation

- `544e163 Fix cluster smoke reachability probe`
- `2508945 Wire distributed auto tests into Cargo`
- `796d402 Add Mongo wire unsupported mutation regression`

## Scenario Results

| Scenario | Verdict | Evidence |
|---|---:|---|
| S1-S4, S6 | PASS | `cargo test -q -p agentdb-core --test auto`: 6 passed locally and on `li-dev` |
| S5 | PASS | `AGENTDB_AUTHOR_LIVE=1 python3 -m pytest adapters/tests/auto/test_S5_tenant_isolation_across_http_sdk_mongo_adapters_and_export.py -q -s`: 3 passed locally and on `li-dev`; `npm test` in `clients/typescript`: 25 passed, 2 skipped |
| S7, S10 | PASS | `cargo test -q -p agentdb-api --test auto`: 2 passed locally and on `li-dev` |
| S8 | PASS after harness fix | First run failed on harness reachability/catch-up scope; rerun passed at `test-sessions/s8-current-20260520T213431Z` |
| S9 | PASS | Full hardening passed at `test-sessions/s9-current-20260520T220233Z` |
| S11 | PASS within smoke budget | `cargo +nightly fuzz run S11_wire_sdk_adapter_and_config_boundaries -- -runs=5000000`: 5,000,000 runs, no crash |
| S12 | INCONCLUSIVE | `test-sessions/s12-current-20260521T020830Z`: missing `AGENTDB_OLD_SERVER_BIN` |
| S13 | PASS | Release-grade benchmark gate passed at `test-sessions/s13-release-current-20260520T183253Z` |
| S14 | PASS | TLC completed with no errors at `test-sessions/s14-current-20260520T184000Z/logs/S14-tlc.log` |

## Additional Verification Added

Mongo wire unsupported mutation regression:

- Test: `mongodb_driver_rejects_unsupported_mutations_without_side_effects`
- File: `crates/agentdb-wire/tests/driver.rs`
- Commands:
  - Local: `cargo test -q -p agentdb-wire` passed: 8 unit tests, 4 driver integration tests.
  - Remote: `cargo test -q -p agentdb-wire` passed on `li-dev`: 8 unit tests, 4 driver integration tests.
  - Local and remote: `cargo check -q -p agentdb-server` passed.
- Coverage: `create`, arbitrary `users` collection insert, `update`, and `delete` fail through the MongoDB driver; fixed AgentDB collections remain listed; supported `events` insert/count still works afterward.

## Notable Evidence

S8 rerun after fix:

- Session: `/home/lishen/work/agentdb/test-sessions/s8-current-20260520T213431Z`
- Work dir: `/home/lishen/work/agentdb-author-s8-current-20260520T213431Z`
- Result line: `real cluster smoke passed`
- Fault evidence: killed `node-a`; post-failover append seq 5 succeeded via `node-b`; secondary cell append seq 5 succeeded via `node-d`; surviving nodes reported degraded rather than ready/ok after one node was dead.

S9 full hardening:

- Session: `/home/lishen/work/agentdb/test-sessions/s9-current-20260520T220233Z`
- Work dir: `/home/lishen/work/agentdb-author-s9-current-20260520T220233Z`
- Result line: `PASS: hardening smoke reported pass`
- Covered restart persistence, checkpoint replacement, AZ failure/restart, rolling restart, tablet replacement, and split/merge cutover.

S13 release benchmark:

- Session: `/home/lishen/work/agentdb/test-sessions/s13-release-current-20260520T183253Z`
- Output root: `/home/lishen/work/agentdb-bench-runs/s13-release-current-20260520T183253Z`
- Repeat gate: 3 repeats, errors 0, ops/s mean 2692.56, p95 mean 47.030 ms, p99 mean 68.272 ms.
- Noisy-neighbor gate: errors 0, ops/s 4143.42, p95 15.590 ms, p99 20.291 ms.

S14 TLC:

- Log: `/home/lishen/work/agentdb/test-sessions/s14-current-20260520T184000Z/logs/S14-tlc.log`
- Result: no error found.
- State space: 225,060,100 generated states, 14,164,992 distinct states, depth 29.

## Residual Risk

S12 is the only remaining unexecuted plan scenario. To convert it from INCONCLUSIVE to PASS/FAIL, provide an older `agentdb` server binary and run:

```bash
AGENTDB_OLD_SERVER_BIN=/path/to/old/agentdb \
AGENTDB_DIST_SESSION_DIR=/home/lishen/work/agentdb/test-sessions/s12-current-<timestamp> \
bash ops/local-ci/tests/auto/S12_single_node_upgrade_preserves_runtime_state_and_client_compatibility.sh
```

The S11 fuzz result is a smoke-budget result, not the plan's full 24 CPU-hour fuzz budget. It is useful regression evidence, but not full fuzz confidence.
