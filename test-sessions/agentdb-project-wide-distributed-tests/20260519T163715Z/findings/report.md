# S9 Routing Hint Follow-Up

**Plan:** `testing-plans/agentdb-project-wide-distributed-tests.md`
**Session:** `test-sessions/agentdb-project-wide-distributed-tests/20260519T163715Z`
**Scenario:** `S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors`
**Verdict:** PASS; prior null owner-address residual fixed

## Fix

`RAFT_NOT_LEADER` responses now populate owner routing hints from the best available source:

1. Existing route headers from the routing guard.
2. Current metadata tablet owner and node address.
3. Raft leader mapping when the local Raft status can name a leader.

If the owner changes and the replacement source has no address, the code clears the stale address rather than pairing a new owner with an old address.

## Verification

Local:

```text
cargo test -q -p agentdb-api raft_not_leader_owner_hint -- --nocapture
3 passed

cargo check -q -p agentdb-server
PASS

cargo test -q -p agentdb-api
PASS

git diff --check
PASS
```

Remote on `li-dev`:

```text
cargo test -q -p agentdb-api raft_not_leader_owner_hint -- --nocapture
3 passed
```

Full S9 rerun:

```text
AGENTDB_RUN_AUTHOR_HARDENING=1
AGENTDB_CLUSTER_SMOKE_WORK_DIR=/home/lishen/work/agentdb-s9-routing-hint
AGENTDB_CLUSTER_SMOKE_COMPOSE_PROJECT=agentdb-cluster-smoke-s9hint
ops/smoke/tests/auto/S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors.sh
```

Result:

```text
PASS: hardening smoke reported pass
```

## Routing Evidence

The rerun reproduced `RAFT_NOT_LEADER` during rolling restart. The previous residual was:

```text
"leader_raft_node_id":null,"owner_node_id":null,"owner_address":null
```

The fixed response now keeps the owner routing hint populated even when the Raft leader id is unavailable:

```text
"leader_raft_node_id":null,"owner_node_id":"node-b","owner_address":"http://127.0.0.1:30101"
```

The log contains this corrected response for seq 9 retries 2-6 and seq 11 retries 2-6.

## Artifacts

- `logs/S9-full-hardening.log`
- `artifacts/S9-routing-hint-diagnostics/`
- `artifacts/S9-routing-hint-node-logs/`

## Residual Uncertainty

This closes the specific S9 routing-observability residual from the prior continuation session. It does not expand S9 to the plan's full target of repeated seeded movement cycles; that broader adequacy gap remains part of the project-wide test-plan residuals.
