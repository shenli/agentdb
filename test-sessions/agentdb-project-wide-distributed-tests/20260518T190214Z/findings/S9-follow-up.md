# S9 Follow-Up

**Scenario:** `S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors`

**Follow-up verdict:** PASS after fix

## Root Cause

The local tablet movement executor installed split/merge destination snapshots with full runtime rehydration while the source tablet was still the active writer. During a prepared split, that allowed a pre-cutover destination snapshot to regress the node's global in-memory session state. The next append could reuse the previous sequence number after an ambiguous Raft timeout, producing the observed `expected appended seq 15, got 14` failure.

## Fix

Changed split/merge movement snapshot installs to update the destination tablet store without global rehydrate. Raft/replacement snapshot paths still use rehydrate.

Regression coverage added:

```bash
cargo test -q -p agentdb-core split_destination_snapshot_install_does_not_regress_source_session_sequence
```

Local and remote focused checks passed:

- `cargo test -q -p agentdb-core split_destination_snapshot_install_does_not_regress_source_session_sequence`
- `cargo check -q -p agentdb-server`

## Remote Verification

Reran the full S9 author-mode hardening wrapper on `li-dev` with a fresh work directory and compose project:

```bash
AGENTDB_DIST_SESSION_DIR=/home/lishen/work/agentdb/test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z \
AGENTDB_RUN_AUTHOR_HARDENING=1 \
AGENTDB_CLUSTER_SMOKE_WORK_DIR=/home/lishen/work/agentdb-author-s9-after-fix \
AGENTDB_CLUSTER_SMOKE_COMPOSE_PROJECT=agentdb-cluster-smoke-s9fix \
ops/smoke/tests/auto/S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors.sh
```

Result:

```text
split-merge: appending event 15 while split is prepared
smoke: append seq 15 attempt 1 via node-a
split-merge: completed metadata split and merge cutover
real cluster smoke passed; diagnostics kept under /home/lishen/work/agentdb-author-s9-after-fix
PASS: hardening smoke reported pass
```

Artifacts:

- `logs/reruns/S9-after-fix-full-pass.log`
- `artifacts/S9-after-fix-full-pass/logs/node-{a,b,c,d,e,f}.log`
- `artifacts/S9-after-fix-full-pass/diagnostics/{compose-ps.txt,minio-init.log,minio.log,postgres.log}`
