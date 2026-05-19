# Distributed Test Continuation Session

**Plan:** `testing-plans/agentdb-project-wide-distributed-tests.md`
**Session:** `test-sessions/agentdb-project-wide-distributed-tests/20260519T155551Z`
**Mode:** continuation after commit `862f4c7`
**Remote host:** `li-dev:/home/lishen/work/agentdb`

## Focus

- Re-run the high-value S8/S9 real-cluster paths after the fixes.
- Increase S11 fuzz budget beyond the initial 1000-run smoke.
- Re-run S13 with a longer two-concurrency smoke profile if the cluster runs leave enough time.
- Keep evidence separate from the committed author-mode session.

## Timeline

| Time (UTC) | Scenario | Event | Notes |
|---|---|---|---|
| 2026-05-19T15:55:51Z | all | continuation session created | local commit `862f4c7`; remote checkout `3c291f6` with synced working-tree fixes |
| 2026-05-19T15:56:00Z | S9 | started repeat full-hardening run | remote pid `2536098`; compose project `agentdb-cluster-smoke-s9cont1`; work dir `/home/lishen/work/agentdb-cont-s9-1` |
| 2026-05-19T15:57:00Z | S11 | 100k nightly fuzz run | PASS: `#100000 DONE`, `cov: 1153`, `ft: 3181`, `corp: 719/21Kb`, no crash |
| 2026-05-19T16:15:00Z | S9 | repeat full-hardening run completed | PASS: seq 15 split-prepared append retried once and returned seq 15; split/merge cutover completed |
| 2026-05-19T16:16:00Z | S8 | started repeat failover run | remote pid `2550442`; compose project `agentdb-cluster-smoke-s8cont1`; work dir `/home/lishen/work/agentdb-cont-s8-1` |
| 2026-05-19T16:31:00Z | S8 | repeat failover run completed | PASS: killed initial writer `node-a`, observed owner change, post-failover appends succeeded, `dead_nodes_marked_total=1` |
| 2026-05-19T16:35:00Z | S13 | longer benchmark smoke | PASS: concurrency 1 and 4, 2s warmup, 10s duration, 0 errors; c=1 p99 12.736ms, c=4 p99 63.538ms |
