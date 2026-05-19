# AgentDB Distributed Test Execution Report

**Plan:** `testing-plans/agentdb-project-wide-distributed-tests.md`
**Session:** `test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z`
**Mode:** AUTHOR MODE
**Local authoring commit:** `8b89250b94c4b6f33bf69e42df32af61fcdb936a`
**Remote execution commit:** `3c291f6f3264352b7fd549be6c60774883a21b44`
**Overall verdict:** PASS at executed author-mode scope; S8 and S9 failures fixed and verified, S13/S14 remain scoped smoke/bounded evidence

## Executive summary

Author mode produced all 14 scenario targets declared by the plan. The in-process executable-spec harness for S1, S2, S3, S4, S6, S7, and S10 compiled and passed on `li-dev`.

The initial session failed because S8, the real cluster quorum/failover scenario, failed on `li-dev`: after killing the initial writer `node-a`, tablet 1 did not move away from `node-a` within 45 seconds. This was a real liveness/failover finding against the evaluation-cluster path.

Follow-up work fixed the S8 owner-failover bug and hardened two brittle record-index smoke assertions. The full S8 author-mode wrapper then passed on `li-dev`.

Continued S9 execution later produced a second real failure. During split/merge, appending sequence 15 while split was prepared hit a retryable Raft command timeout; the retry returned appended sequence 14 instead of 15. The S9 log and node diagnostics were retrieved from `li-dev` after the host became reachable again.

Follow-up work fixed the S9 split/merge rehydrate bug. The full S9 author-mode hardening wrapper then passed on `li-dev`. S11 later passed a 1000-run nightly `cargo-fuzz` execution, S12 passed a single-node old-binary to new-binary upgrade harness from `8b89250` to `3c291f6`, S13 passed its smoke benchmark after fixing a wrapper report-file lookup, and S14 passed bounded TLC model checking after adding a model config and bounding owner epoch movement.

## Authored target files

- `crates/agentdb-core/tests/auto/S1_contiguous_session_seq_and_append_fetch_under_high_concurrency.rs`
- `crates/agentdb-core/tests/auto/S2_idempotent_replay_after_lost_ack_restart_and_handoff.rs`
- `crates/agentdb-core/tests/auto/S3_acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries.rs`
- `crates/agentdb-core/tests/auto/S4_fork_and_tiered_session_history_remains_ordered_across_lifecycle_faults.rs`
- `crates/agentdb-core/tests/auto/S6_record_visibility_and_policy_gc_match_committed_model_under_lag.rs`
- `crates/agentdb-api/tests/auto/S7_export_and_watch_cursors_are_gap_free_and_sink_ack_safe.rs`
- `crates/agentdb-api/tests/auto/S10_routing_epoch_residency_and_readiness_reject_unsafe_cluster_states.rs`
- `adapters/tests/auto/test_S5_tenant_isolation_across_http_sdk_mongo_adapters_and_export.py`
- `ops/smoke/tests/auto/S8_raft_quorum_preserves_acknowledged_tablet_writes_under_partition.sh`
- `ops/smoke/tests/auto/S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors.sh`
- `crates/agentdb-wire/fuzz/fuzz_targets/S11_wire_sdk_adapter_and_config_boundaries.rs`
- `crates/agentdb-wire/fuzz/Cargo.toml`
- `ops/local-ci/tests/auto/S12_single_node_upgrade_preserves_runtime_state_and_client_compatibility.sh`
- `tools/agentdb-bench/tests/auto/S13_benchmark_claims_remain_reproducible_under_skew_and_noisy_neighbors.sh`
- `docs/formal/auto/S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.tla`
- `docs/formal/auto/S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.cfg`

Because Cargo does not discover `tests/auto/*.rs` as integration tests, the executable-spec Rust scenarios were also included through an ephemeral session harness under `artifacts/harness/`.

## Execution matrix

| Scenario | Claim focus | Execution | Verdict |
|---|---|---:|---|
| S1 | C1/C2 contiguous session seq and append-fetch | local and remote Rust harness | PASS |
| S2 | C3/C4 idempotency after lost ack/restart | local and remote Rust harness | PASS |
| S3 | C4/C8/C10 crash-boundary durability model | local and remote Rust harness | PASS |
| S4 | C5/C6 fork and tiered history ordering | local and remote Rust harness | PASS |
| S5 | C7/C14/C20 adapter isolation | local live HTTP pytest after sandbox escalation | PASS |
| S6 | C8/C9 policy/GC record visibility | local and remote Rust harness | PASS |
| S7 | C11/C12 watch/export cursor ordering | remote Rust harness | PASS |
| S8 | C1/C15/C17/C18 cluster quorum/failover | remote real cluster smoke on `li-dev`; follow-up rerun after fix | PASS after fix |
| S9 | C11/C15/C16/C17 tablet replacement/split/merge | remote full-hardening on `li-dev`; follow-up rerun after fix | PASS after fix |
| S10 | C16/C18 routing/readiness rejection | remote Rust harness | PASS |
| S11 | C14/C20 wire/SDK boundary fuzzing | remote `cargo +nightly fuzz run ... -- -runs=1000` | PASS |
| S12 | C4/C18/C20 single-node upgrade | remote old binary `8b89250` writes; current binary `3c291f6` verifies and appends | PASS |
| S13 | C19 benchmark reproducibility | remote smoke benchmark, concurrency 1, 1s warmup, 3s duration | PASS (smoke only) |
| S14 | C15/C16 formal protocol model | remote TLC 2.19 bounded model, 913 generated states, 144 distinct states | PASS (bounded model) |

## Commands run

Representative checks:

```bash
rustfmt --edition 2021 <authored rust files>
python3 -m py_compile adapters/tests/auto/test_S5_tenant_isolation_across_http_sdk_mongo_adapters_and_export.py
bash -n ops/smoke/tests/auto/S8_raft_quorum_preserves_acknowledged_tablet_writes_under_partition.sh
cargo test --manifest-path test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z/artifacts/harness/Cargo.toml -- --nocapture
ssh li-dev 'cd ~/work/agentdb && AGENTDB_DIST_SESSION_DIR=/home/lishen/work/agentdb/test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z AGENTDB_RUN_AUTHOR_CLUSTER=1 AGENTDB_CLUSTER_SMOKE_WORK_DIR=/home/lishen/work/agentdb-author-s8 ops/smoke/tests/auto/S8_raft_quorum_preserves_acknowledged_tablet_writes_under_partition.sh'
```

## Finding summary

### F1: Tablet owner failover did not move tablet 1 off killed owner

S8 failed in the remote real cluster smoke. Before the fault, all six nodes reported healthy Raft summaries with no leaderless, multi-leader, drift, lagging, or unreachable tablets. The harness then killed initial writer `node-a`, but tablet 1 did not move away from `node-a` within 45 seconds.

Evidence is in `findings/S8.md` and `logs/S8-real-cluster-smoke.log`.

Follow-up resolution:

- Server fix: allow local Raft participants to reconcile tablet metadata ownership to the observed Raft leader before scheduler-leader gating.
- Smoke fix: tolerate duplicate vector records from ambiguous non-idempotent record-create retries by checking a small result set rather than exactly top two.
- Smoke fix: use a bounded positive-search visibility wait for committed records, matching the existing bounded wait for deleted records to disappear.
- Final evidence: `logs/reruns/S8-after-fix-full-pass.log`.

### F2: Split/merge prepared write returned a stale sequence after retry

S9 failed in the remote full-hardening smoke during split/merge. The run completed setup, consistency checks, record-index probes, changefeed probes, raft convergence, restart-persistence, checkpoint install, AZ failure, rolling restart, and explicit replacement. It then appended event 14 before split prepare, attempted event 15 while split was prepared, hit a retryable Raft command timeout, retried, and received sequence 14 instead of the expected sequence 15.

Evidence is in `findings/S9-partial.md`, `logs/S9-full-hardening.log`, and `artifacts/S9-full-hardening/`.

Ancillary diagnostic: during the rolling-restart portion, several retryable `RAFT_NOT_LEADER` responses reported `leader_raft_node_id:null`, `owner_node_id:null`, and `owner_address:null`. The harness recovered from those windows, so they are not the direct failure, but they indicate a routing/ownership observability gap during leadership churn.

Follow-up resolution:

- Server fix: split/merge movement destination snapshot installs no longer globally rehydrate runtime state before cutover.
- Regression test: `cargo test -q -p agentdb-core split_destination_snapshot_install_does_not_regress_source_session_sequence`.
- Final evidence: `findings/S9-follow-up.md` and `logs/reruns/S9-after-fix-full-pass.log`.

### F3: S13 wrapper expected the wrong benchmark report filename

The first S13 smoke benchmark produced `report-mixedagentloop.json`, but the authored wrapper checked only for `report.json` and incorrectly failed after a successful run. The wrapper now accepts `report*.json`.

Final smoke evidence is in `logs/S13-agentdb-bench-local.log` and `artifacts/S13-benchmark-smoke/author-s13-20260518-204140/`.

## Coverage adequacy

The authored executable specs provide useful regression coverage for the single-process runtime, idempotency, persistence-model, fork/lifecycle, record-policy, watch/export, and route/readiness claims. They are not a substitute for exhaustive distributed validation because most pass results are in-process or model-level checks.

The remote S8 and S9 runs give high-value evidence because they used the repository's real cluster compose harness with Docker, Postgres, MinIO, multiple AgentDB nodes, Raft, routing, consistency, record-index, changefeed, failover, replacement, and split/merge enabled. S8 and S9 each now have one full passing rerun after targeted fixes. That supports the specific evaluated fault paths, but not a broad claim over all seeded schedules or larger clusters.

The remaining follow-up runs close the earlier execution gaps at limited depth: S11 exercised 1000 libFuzzer inputs under nightly, S12 crossed one old-to-new single-node binary boundary with persistent runtime/content dirs, S13 covered only a short smoke workload, and S14 checked a bounded TLA model with owner epochs limited to one movement.

## Residual uncertainty

- S5 now covers live HTTP tenant isolation, but still does not exercise SDK, Mongo, adapter, and export surfaces together.
- S9 now passes one full follow-up hardening run after the targeted fix, but repeated seeded split/merge/replacement runs are still needed.
- S11 passed 1000 fuzz runs, but this is a short smoke fuzz budget rather than sustained corpus growth or CI-grade fuzzing.
- S12 covered only one old commit (`8b89250`) to the current remote checkout (`3c291f6`) and only a single-node HTTP fetch/watch/export compatibility path.
- S13 passed only a smoke profile; release-grade repeat/noisy-neighbor thresholds remain unvalidated.
- S14 passed only a bounded model with small constant sets and one owner-epoch increment per owner; it does not prove the full implementation.

## Confidence statement

Conservative confidence is moderate for the authored in-process runtime claims covered by S1, S2, S3, S4, S6, S7, and S10 on the remote host.

After the follow-up fixes, confidence is moderate for the specific S8 owner-failover path and the specific S9 replacement/split/merge path, each covered by one full six-node two-cell smoke pass. Confidence is low-to-moderate for broader distributed recovery because repeated seeded fault runs, longer fuzzing, broader upgrade matrices, and larger formal models remain unrun.

## Continued local verification

Additional checks run after the S8 fix:

- `AGENTDB_AUTHOR_LIVE=1 python3 -m pytest adapters/tests/auto/test_S5_tenant_isolation_across_http_sdk_mongo_adapters_and_export.py -q -s` - PASS after allowing localhost bind.
- `cargo test --manifest-path test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z/artifacts/harness/Cargo.toml -- --nocapture` - PASS, 7 tests.
- `cargo check -q -p agentdb-server` - PASS.
- `cargo test -q -p agentdb-core` - PASS.
- `cargo test -q -p agentdb-api` - PASS.
- `cargo test -q -p agentdb-wire` - PASS.
- `PYTHONPATH=clients/python python3 -m pytest clients/python/tests -q` - PASS, 27 passed and 14 skipped.
- `npm test` in `clients/typescript` - PASS, 25 passed and 2 skipped.
- S9 follow-up: `cargo test -q -p agentdb-core split_destination_snapshot_install_does_not_regress_source_session_sequence` - PASS locally and on `li-dev`.
- S9 follow-up: `cargo check -q -p agentdb-server` - PASS locally and on `li-dev`.
- S9 follow-up: full remote S9 hardening wrapper - PASS.
- S13 follow-up: remote smoke benchmark - PASS, `ops/s=379.33`, `errors=0`, `p99=5.779ms` for concurrency 1, 1s warmup, 3s duration.
- S11 follow-up: `cargo +nightly fuzz run S11_wire_sdk_adapter_and_config_boundaries -- -runs=1000` - PASS, `#1000 DONE`, `cov: 315`, `ft: 506`, `corp: 68/185b`.
- S12 follow-up: `AGENTDB_OLD_SERVER_BIN=/home/lishen/work/agentdb-s12-old/target/debug/agentdb ... S12_single_node_upgrade_preserves_runtime_state_and_client_compatibility.sh` - PASS, old server verified seqs `[0,1]`; new server verified seqs `[0,1,2]`.
- S14 follow-up: `java -cp /home/lishen/work/tla-tools/tla2tools.jar tlc2.TLC ...` - PASS, 913 generated states, 144 distinct states, 0 states left on queue.
