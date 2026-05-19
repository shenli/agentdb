# S9 Execution

**Scenario:** `S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors`

**Initial verdict:** FAIL
**Follow-up verdict:** PASS after fix

## Execution

Command launched on `li-dev`:

```bash
ssh li-dev 'cd ~/work/agentdb && AGENTDB_DIST_SESSION_DIR=/home/lishen/work/agentdb/test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z AGENTDB_RUN_AUTHOR_HARDENING=1 AGENTDB_CLUSTER_SMOKE_WORK_DIR=/home/lishen/work/agentdb-author-s9-full-hardening AGENTDB_CLUSTER_SMOKE_COMPOSE_PROJECT=agentdb-cluster-smoke-s9hard ops/smoke/tests/auto/S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors.sh'
```

Observed progress before the SSH session stopped producing output:

- setup writes completed for both cells;
- primary and secondary consistency checks ran;
- primary and secondary record-index probes completed;
- primary and secondary changefeed probes completed;
- raft catch-up and convergence completed;
- restart-persistence completed and accepted a post-restart append;
- checkpoint install completed;
- AZ failure stopped `node-a` and `node-d`, writes continued, then the AZ restarted;
- rolling restart progressed across nodes with retryable `RAFT_NOT_LEADER` windows;
- explicit replacement prepared and completed tablet 1 handoff from `node-b` to `node-a`;
- split/merge began and appended event 14 before split prepare plus event 15 while split was prepared.

Last observed output:

```text
split-merge: appending event 15 while split is prepared
smoke: append seq 15 attempt 1 via node-a
smoke: append seq 15 retry 1 after convergence error: HTTP 500 Internal Server Error ... raft-rs command did not commit for tablet 1 before timeout
```

After that, the session was silent for a sustained interval and new `ssh li-dev` connections timed out. I stopped the local SSH wrapper to regain control.

On retry, `li-dev` became reachable and the remote log plus diagnostics were retrieved.

## Assessment

The retrieved log closes S9 as a real failure. Split/merge prepared writes can return a stale sequence after an ambiguous Raft timeout retry:

```text
split-merge: appending event 14 before split prepare
smoke: append seq 14 attempt 1 via node-a
split-merge: appending event 15 while split is prepared
smoke: append seq 15 attempt 1 via node-a
smoke: append seq 15 retry 1 after convergence error: HTTP 500 Internal Server Error ... raft-rs command did not commit for tablet 1 before timeout
smoke: append seq 15 attempt 2 via node-a
Error: expected appended seq 15, got 14
```

Artifacts:

- `logs/S9-full-hardening.log`
- `artifacts/S9-full-hardening/logs/node-{a,b,c,d,e,f}.log`
- `artifacts/S9-full-hardening/diagnostics/{compose-ps.txt,minio-init.log,minio.log,postgres.log}`

Positive evidence before the failure still matters: the same run completed setup, consistency checks, record-index probes, changefeed probes, raft convergence, restart-persistence, checkpoint install, AZ failure, rolling restart, and explicit replacement. The failure is specifically in the split/merge prepared-write path after a retryable Raft command timeout.

## Retrieval retry

Retried on request after the session:

```bash
ssh li-dev 'echo reachable; date; cd ~/work/agentdb && git status --short --branch'
ssh li-dev 'ps -axo pid,ppid,stat,etime,command | grep -E "agentdb-cluster-smoke|real-cluster-smoke|agentdb serve" | grep -v grep || true'
```

Both attempts timed out connecting to `192.168.1.235:18080`, so S9 remote logs still could not be retrieved.

Second retry succeeded:

```bash
ssh li-dev 'echo reachable; date; test -f /home/lishen/work/agentdb/test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z/logs/S9-full-hardening.log && wc -l /home/lishen/work/agentdb/test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z/logs/S9-full-hardening.log || echo no_s9_log'
```

The remote log had 172 lines and was copied into the local session directory with node logs and diagnostics.

## Follow-Up Fix

The fix stopped split/merge movement destination snapshot installs from globally rehydrating runtime state before cutover. A stale pre-cutover destination snapshot could otherwise regress the active source tablet's in-memory session sequence.

Follow-up evidence is in `findings/S9-follow-up.md` and `logs/reruns/S9-after-fix-full-pass.log`. The full S9 hardening wrapper passed on `li-dev` after the fix.
