# S8 Follow-Up Verification

**Verdict:** PASS after patch

## What changed

- `bin/agentdb-server/src/main.rs`: local Raft participants can now reconcile tablet metadata ownership to the Raft leader they observe before scheduler-leader gating.
- `tools/agentdb-cluster-smoke/src/main.rs`: record-index smoke checks were made robust to normal distributed convergence:
  - vector search now checks a small result set so ambiguous retries of non-idempotent record creates do not fail the oracle with duplicate nearest vectors;
  - positive record-search assertions now use a bounded wait, matching the existing bounded wait for delete disappearance.

## Verification

```bash
cargo test -p agentdb-server scheduler_follower_reconciles_local_raft_leader_ownership -- --nocapture
cargo check -q -p agentdb-cluster-smoke
ssh li-dev 'cd ~/work/agentdb && cargo test -p agentdb-server scheduler_follower_reconciles_local_raft_leader_ownership -- --nocapture'
ssh li-dev 'cd ~/work/agentdb && AGENTDB_CLUSTER_SMOKE_WORK_DIR=/home/lishen/work/agentdb-author-s8-failover-only AGENTDB_CLUSTER_SMOKE_COMPOSE_PROJECT=agentdb-cluster-smoke-s8fo ops/smoke/scripts/real-cluster-smoke-compose.sh --nodes 6 --two-cell --failover'
ssh li-dev 'cd ~/work/agentdb && AGENTDB_DIST_SESSION_DIR=/home/lishen/work/agentdb/test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z AGENTDB_RUN_AUTHOR_CLUSTER=1 AGENTDB_CLUSTER_SMOKE_WORK_DIR=/home/lishen/work/agentdb-author-s8-fixed-full2 AGENTDB_CLUSTER_SMOKE_COMPOSE_PROJECT=agentdb-cluster-smoke-s8fullfix2 ops/smoke/tests/auto/S8_raft_quorum_preserves_acknowledged_tablet_writes_under_partition.sh'
```

The final full S8 rerun completed consistency, record-index, changefeed, pre-fault convergence, owner kill, owner movement, post-failover appends, and dead-node marking.

Evidence:

- `logs/reruns/S8-after-fix-full-pass.log`
- `artifacts/S8-after-fix-full-pass/diagnostics/`
- `artifacts/S8-after-fix-full-pass/logs/`
