# Remaining Scenario Continuation

## S11

**Verdict:** PASS for 1000-run nightly fuzz smoke.

Added the missing fuzz manifest at `crates/agentdb-wire/fuzz/Cargo.toml` and ran on `li-dev` with the installed nightly toolchain:

```bash
cd /home/lishen/work/agentdb/crates/agentdb-wire
cargo +nightly fuzz run S11_wire_sdk_adapter_and_config_boundaries -- -runs=1000
```

Result:

```text
#1000 DONE cov: 315 ft: 506 corp: 68/185b
Done 1000 runs in 0 second(s)
```

Evidence:

- `logs/S11-cargo-fuzz-nightly.log`

This is a smoke fuzz budget, not sustained fuzzing.

## S12

**Verdict:** PASS for one old-to-new single-node upgrade path.

Replaced the placeholder wrapper with an executable upgrade harness. The run built an old detached worktree at `8b89250`, started that old binary with persistent `data` and `content` dirs, created a tenant/agent/session, appended seqs `[0,1]`, and verified fetch/watch/export. It then stopped the old server, started the current `3c291f6` binary over the same dirs, verified seqs `[0,1]`, appended seq `2`, and verified fetch/watch/export for `[0,1,2]`.

Command:

```bash
AGENTDB_DIST_SESSION_DIR=/home/lishen/work/agentdb/test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z \
AGENTDB_OLD_SERVER_BIN=/home/lishen/work/agentdb-s12-old/target/debug/agentdb \
AGENTDB_NEW_SERVER_BIN=/home/lishen/work/agentdb/target/debug/agentdb \
ops/local-ci/tests/auto/S12_single_node_upgrade_preserves_runtime_state_and_client_compatibility.sh
```

Result:

```text
old-write verified tenant=ten_019e40ee-ace0-7ab0-883f-c18e97232dcd session=ses_019e40ee-ace2-7de0-8f96-1420c6cff6df seqs=[0,1]
new-verify verified tenant=ten_019e40ee-ace0-7ab0-883f-c18e97232dcd session=ses_019e40ee-ace2-7de0-8f96-1420c6cff6df seqs=[0,1,2]
PASS: single-node upgrade preserved fetch/watch/export state and accepted a post-upgrade append
```

Evidence:

- `logs/S12-upgrade-client.log`
- `logs/S12-old-server.log`
- `logs/S12-new-server.log`
- `artifacts/S12-upgrade/metrics.json`

This does not cover multi-node upgrade, rolling upgrade, metadata schema migration across older release tags, or SDK/Mongo compatibility.

## S13

**Verdict:** PASS for smoke profile.

The first S13 run exposed a wrapper bug: the benchmark produced `report-mixedagentloop.json`, while the wrapper only looked for `report.json`. The wrapper was fixed to accept `report*.json`.

Rerun command:

```bash
AGENTDB_DIST_SESSION_DIR=/home/lishen/work/agentdb/test-sessions/agentdb-project-wide-distributed-tests/20260518T190214Z \
AGENTDB_RUN_AUTHOR_BENCH=1 \
AGENTDB_AUTHOR_BENCH_OUT=/home/lishen/work/agentdb-author-bench-s13 \
PROFILE=smoke \
CONCURRENCY=1 \
WARMUP=1s \
DURATION=3s \
tools/agentdb-bench/tests/auto/S13_benchmark_claims_remain_reproducible_under_skew_and_noisy_neighbors.sh
```

Result:

```text
c=1 ops/s=379.33 ops=1138 errors=0 p50=2.797ms p95=5.500ms p99=5.779ms
PASS: benchmark smoke produced /home/lishen/work/agentdb-author-bench-s13/runs/author-s13-20260518-204140/report-mixedagentloop.json
```

Evidence:

- `logs/S13-agentdb-bench-local.log`
- `artifacts/S13-benchmark-smoke/author-s13-20260518-204140/`
- `artifacts/S13-benchmark-smoke/author-s13-20260518-204140/report-mixedagentloop.json`
- `artifacts/S13-benchmark-smoke/author-s13-20260518-204140/report.md`

This is only a smoke-profile pass. Release-grade repeat/noisy-neighbor thresholds remain unvalidated.

## S14

**Verdict:** PASS for bounded TLC model.

Downloaded TLC 2.19 to `li-dev` under `/home/lishen/work/tla-tools/tla2tools.jar`, added a config file, fixed the module name to match the filename, removed an invalid primed-state invariant, added `TypeOK`, and bounded owner movements with `ownerEpoch[o] = 0`.

Command:

```bash
cd /home/lishen/work/agentdb/docs/formal/auto
java -cp /home/lishen/work/tla-tools/tla2tools.jar tlc2.TLC \
  -deadlock \
  -config S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.cfg \
  S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.tla
```

Result:

```text
Model checking completed. No error has been found.
913 states generated, 144 distinct states found, 0 states left on queue.
```

Evidence:

- `logs/S14-tlc.log`
- `docs/formal/auto/S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.tla`
- `docs/formal/auto/S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.cfg`

This bounded model is useful protocol evidence, not an implementation proof.
