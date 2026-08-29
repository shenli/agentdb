# Test Plan: agentdb-project-wide-distributed-tests

**Date:** 2026-05-18
**Plan mode:** project-wide
**SUT:** AgentDB
**Change under test:** N/A - project-wide
**Plan author:** Codex
**Status:** draft

## 0. Architectural summary

AgentDB is a multi-tenant runtime database for AI agent state: tenants own agents, agents own sessions, and sessions own ordered append-only events. The canonical user surface is the native `/api/...` HTTP API through `agentdb-api`, with maintained Python and TypeScript SDKs layered over it and a deliberately narrow MongoDB wire listener for client/tool compatibility. The hot request path is append or append-and-fetch: API validation and routing call `agentdb-core`, which serializes session mutation, stamps idempotency/policy metadata, persists runtime state through `agentdb-storage`, emits watch/export envelopes, and updates record-index outboxes consumed by `agentdb-index`. Persistent single-node deployments store runtime metadata in RocksDB and artifact/segment/archive bytes in a local or S3-compatible content backend; in-memory mode is dev/test only, and SlateDB/object-store metadata is an evaluation backend. Session lifecycle moves history among warm rows, sealed cold segments, and archive bundles while preserving logical ordered reads. Records have durable base rows plus owner-local and async global indexes; local searches with `sharding_key` are the read-after-write path, while global searches are eventually consistent and repairable with commit-sequence fences. Watch, subscribe, and export surfaces expose committed runtime changes through scoped cursors; managed export consumers claim leases and ack only after downstream sink acceptance. Policy controls quotas, backpressure, lifecycle, retention, legal hold, and GC. Evaluation cluster mode uses `agentdb-meta` for Postgres-backed control-plane metadata, physical tablets with local grouped RocksDB state, `raft-rs` quorum logs for tablet writes, and internal HTTP transport; docs state this is not yet the primary production deployment target. Trust boundaries are tenant IDs, admin/internal auth for operational and Raft HTTP paths, optional Mongo PLAIN listener auth, and connector boundaries for ClickHouse/ClickStack/JSONL/Parquet exports.

```text
SDK / HTTP / Mongo adapter / framework adapters / CLI
        |
        v
agentdb-api: native routes, Mongo listener, admin/routing/export surfaces
        |
        v
agentdb-core: tenants -> agents -> sessions -> ordered events
        |        records, artifacts, skills, policy, lifecycle, context,
        |        fork/replay, watch/export cursors
        |
        +--> agentdb-storage: RocksDB/SlateDB KV, content backend, Raft log
        +--> agentdb-index: local/global record search helpers
        +--> agentdb-meta: cluster metadata, routing, scheduler, quotas
        |
        v
single-node RocksDB/local FS or evaluation cluster tablets/Raft/Postgres/MinIO
```

## 1. Scope

In scope: all subsystems in this repository. That includes `bin/agentdb-server`, `bin/agentdbctl`, `crates/agentdb-api`, `crates/agentdb-core`, `crates/agentdb-storage`, `crates/agentdb-index`, `crates/agentdb-meta`, `crates/agentdb-wire`, `crates/agentdb-dashboard`, maintained Python and TypeScript SDKs, first-party framework adapters, examples that exercise supported surfaces, smoke/cluster/benchmark/workload tools, observability export scripts, deployment examples, and CI/release workflows.

Out of scope: none by request. The plan still distinguishes supported single-node claims from evaluation-cluster claims so confidence remains proportional to the repository's documented support level.

## 1b. Claims under test

| ID | Claim | Category | Source | Inferred? |
|---|---|---|---|---|
| C1 | Session append is serialized per session and produces contiguous session-local `seq` values. | safety | `docs/design/design.md`, `docs/design/architecture.md` | no |
| C2 | `append-and-fetch` writes one event and returns a recent-event slice from the same runtime operation. | safety | `docs/design/design.md`, `docs/reference/api-sdk.md` | no |
| C3 | Mutating calls with the same idempotency key replay the same result; the same key with a conflicting body returns an idempotency conflict and does not commit a second effect. | idempotency | `docs/design/architecture.md`, `crates/agentdb-core/tests/runtime_append.rs` | no |
| C4 | Persistent single-node RocksDB/content-backend state preserves acknowledged tenants, agents, sessions, events, next sequence numbers, idempotency records, records, skills, artifacts, lifecycle metadata, and export cursors across restart. | durability | `docs/operations/deployment.md`, `crates/agentdb-core/tests/runtime_restart.rs` | partial |
| C5 | Fork creates a child session with child-local sequence numbers and replay visibility over parent events through the fork point. | safety | `docs/design/design.md`, `crates/agentdb-core/tests/v2_fork_context.rs` | no |
| C6 | Logical session reads preserve ordered history across warm rows, sealed cold segments, archive bundles, restore, and later appends. | durability | `docs/design/architecture.md`, `crates/agentdb-core/tests/lifecycle_gc.rs` | no |
| C7 | Runtime data is tenant-scoped: reads, writes, search, artifacts, watch, export, policy, storage accounting, adapters, and routing must not leak or mutate another tenant's data. | isolation | `docs/design/architecture.md`, `docs/design/design.md`, `crates/agentdb-core/tests/runtime_isolation.rs` | no |
| C8 | Record search with `sharding_key` is the owner-local read-after-write path; global search is eventually consistent and repairable with `min_visible_record_commit_seq`. | safety | `docs/design/architecture.md`, `docs/reference/api-sdk.md` | no |
| C9 | Record indexing policy, delete, expiry, retention, and GC prevent deleted, expired, private, or disallowed record data from appearing in get/search results. | safety / privacy | `crates/agentdb-core/tests/record_runtime.rs`, `crates/agentdb-core/tests/lifecycle_gc.rs` | partial |
| C10 | Artifact metadata and content bytes are stored separately, retrievable through metadata/blob APIs, checksum/crypto validated where configured, and purged according to policy. | durability / operational | `docs/design/architecture.md`, `crates/agentdb-core/tests/artifact_runtime.rs` | partial |
| C11 | Subscribe, watch, and export cursors expose ordered committed changes without duplicates, gaps, or cross-scope leakage within their tenant/session/agent filters. | safety | `docs/design/architecture.md`, `crates/agentdb-core/tests/v2_fork_context.rs` | no |
| C12 | Observability export is based on committed runtime truth; managed consumers claim leases and ack only after external sink acceptance so worker crash can replay unacked ranges. | durability / operational | `docs/design/architecture.md`, `docs/operations/agentdbctl.md` | no |
| C13 | Policy enforces quota, backpressure, lifecycle, retention, legal hold, and GC per tenant without failing open under concurrency. | operational / isolation | `docs/design/design.md`, `crates/agentdb-core/tests/policy.rs` | no |
| C14 | The MongoDB listener is a narrow adapter onto AgentDB runtime primitives, supports only documented commands/collections/auth, and returns Mongo-shaped errors for unsupported behavior. | safety / compatibility | `docs/reference/mongodb-compatibility.md`, `crates/agentdb-wire/tests/driver.rs` | no |
| C15 | In evaluation cluster mode, acknowledged tablet writes commit through a Raft quorum; if quorum survives, acknowledged writes are not lost, and loss of quorum rejects or times out rather than accepting unsafe writes. | durability / safety | `docs/operations/cluster-status.md`, `ops/smoke/README.md`, `crates/agentdb-core/tests/runtime_quorum.rs` | no |
| C16 | Cluster routing maps tenant/session or sharding-key requests to the correct cell/tablet/owner, rejects stale epochs or unassigned tablets, and preserves placement/residency policy. | safety / isolation | `docs/design/architecture.md`, `crates/agentdb-api/src/routing_http.rs` | no |
| C17 | Cluster smoke hardening paths for node loss, owner failover, restart, checkpoint restore, AZ loss with quorum, rolling restart, replacement, split, and merge preserve exact event sequence and cursor visibility in the evaluation harness. | liveness / durability | `docs/stability-test-plan.md`, `ops/smoke/README.md` | no |
| C18 | Health, readiness, admin, dashboard asset, and `agentdbctl` operational surfaces expose enough status to distinguish ready, degraded, stale, leaderless, lagging, draining, failed, and misconfigured states. | operational | `docs/operations/*`, `crates/agentdb-api/tests/api_smoke.rs`, `crates/agentdb-dashboard/tests/assets.rs` | partial |
| C19 | Published performance/benchmark claims are tied to exact workload, host, durability profile, skew, duration, concurrency, and caveats; release comparisons are withheld or updated when not reproducible. | performance-SLO | `README.md`, `docs/operations/agentdb-bench.md`, `tools/agentdb-bench/README.md` | no |
| C20 | Maintained Python and TypeScript SDKs map to tenant-scoped native HTTP routes, generate or propagate idempotency keys for mutating calls, preserve record commit-sequence visibility, and parse SSE/subscribe streams. | compatibility / idempotency | `docs/design/architecture.md`, `clients/python/tests`, `clients/typescript/tests` | no |
| C21 | First-party adapters and examples map framework concepts to AgentDB primitives without reimplementing or weakening core runtime semantics. | compatibility | `adapters/*/tests`, `examples/agents/*` | partial |

## 1c. Missing claims discovered

| ID | Behavior the code relies on | Source / evidence | Suggested action |
|---|---|---|---|
| M1 | Exact idempotency retention window and post-GC retry semantics. | `crates/agentdb-core/tests/runtime_restart.rs` covers idempotency GC, but public docs do not state what a retry after retention expiry means. | Document default retention, configurability, and whether post-retention retry may commit as new work. |
| M2 | Cursor expiration and oldest-available-cursor recovery semantics for watch/export ledger pruning. | Cursor tests assert invalid-cursor recovery details; API docs do not fully define client recovery. | Document cursor retention, error code/body, and client recovery flow. |
| M3 | Record text tokenization, Unicode normalization, and ranking policy. | Record/index tests cover simple matching; docs do not commit to normalization/ranking semantics. | Document "best-effort simple search" or a stable text-search contract, then add property tests. |
| M4 | Artifact/content atomicity when content PUT, checksum validation, or encryption fails midway. | Content backend and crypto tests exist; user docs do not state whether metadata can commit if content write fails. | Document atomicity and rollback/repair behavior for metadata/content divergence. |
| M5 | Watch/export cursor compatibility across tablet split/merge and epoch changes. | Cluster smoke checks movement visibility, but API docs do not define cursor translation/invalidation during movement. | Document whether movement preserves cursors or returns a recovery cursor. |
| M6 | Single-node upgrade fixture compatibility and cluster mixed-version compatibility. | Operations docs cover backup/restart; cluster rolling upgrade is not claimed. | Add explicit version compatibility matrix and fixture corpus before release gating on upgrade. |
| M7 | Dashboard operational status contract. | Dashboard asset tests pin bundled files; docs do not state a UI data freshness or status interpretation guarantee. | Keep dashboard tests to asset/API compatibility unless a user-facing UI status claim is documented. |

- **Tenancy / isolation:** tenant ID is the server-side runtime boundary for routing, policy, quotas, lifecycle, storage accounting, watch/changefeed visibility, export, Mongo database mapping, SDK workspace binding, and future placement.
- **Persistence:** in-memory dev state is not durable. Persistent single-node mode uses RocksDB for runtime metadata and a content backend for artifacts, sealed segments, and archive bundles. Evaluation cluster mode adds tablet-local grouped RocksDB, Raft logs, Postgres metadata, and MinIO/object-store content.
- **Replication / consensus:** supported single-node mode has no replication. Evaluation cluster mode uses per-tablet `raft-rs` groups and quorum commit before acknowledging tablet writes.
- **Ordering:** clients see contiguous per-session `seq` values. Forked sessions have child-local sequence numbers while replay composes parent history through the fork point.
- **Network boundaries:** native HTTP/SDK routes, MongoDB TCP listener, SSE subscribe, watch/export APIs, internal Raft HTTP message/snapshot routes, `agentdbctl` admin/export clients, framework adapters, and external export sinks.
- **Retry / idempotency contract:** mutating calls may carry `Idempotency-Key`; identical retries replay the original result, conflicting retries return an idempotency conflict, and clients should retry ambiguous timeouts with the same key.
- **Observability:** tests can use API responses, SSE frames, watch/export cursors, admin JSON, `/health/ready`, `agentdbctl` output, Raft status, per-node logs, smoke diagnostics, index watermarks, export consumer status, benchmark JSON/Markdown, and sink row counts/checksums.

## 3. Existing test inventory

| Test / harness | Subsystem | Invariant pinned | Failure modes it catches |
|---|---|---|---|
| `crates/agentdb-core/tests/runtime_append.rs`, `runtime_fetch.rs` | session runtime | contiguous seq, append/fetch ordering, idempotency replay/conflict | single-process races, duplicate seq, stale fetch, simple retry conflicts |
| `crates/agentdb-core/tests/runtime_restart.rs`, `runtime_slate.rs` | persistence | runtime state, idempotency, records, skills, artifacts, tiered history survive reopen | cache-only durability bugs, same-version restart loss |
| `crates/agentdb-core/tests/v2_fork_context.rs` | fork/context/watch/export | fork replay, budgeted context, watch/export cursor paging, consumer rehydrate | fork boundary errors, cursor gaps in example histories |
| `crates/agentdb-core/tests/record_runtime.rs`, `record.rs`, `global_index.rs` coverage through core tests | records/index | local/global search, commit-seq repair, delete/expiry filtering, tenant/namespace isolation | stale record reads, leaked deleted records, index repair regressions |
| `crates/agentdb-core/tests/artifact_runtime.rs`, `artifact_content_crypto.rs`, `runtime_artifact_refs.rs` | artifacts/content | metadata/blob round trip, crypto/checksum behavior, artifact refs | metadata/content mismatch in happy-path and crypto cases |
| `crates/agentdb-core/tests/lifecycle_gc.rs`, `policy.rs`, `policy_worker.rs` | policy/lifecycle/GC | retention, legal hold, quotas, backpressure, lifecycle/GC worker behavior | premature purge, missing admission checks, stale deleted data |
| `crates/agentdb-core/tests/runtime_isolation.rs`, `runtime_alias_uniqueness.rs` | isolation/aliases | tenant and alias separation | simple cross-tenant lookup leaks |
| `crates/agentdb-core/tests/runtime_quorum.rs`, `tablet_checkpoint_restore.rs` | Raft/tablet runtime | quorum commit, catch-up, snapshot/checkpoint restore | no-quorum write acceptance, stale follower state in unit harness |
| `crates/agentdb-api/tests/api_*.rs` | HTTP API | route contracts for events, artifacts, records, skills, policy, watch/export, admin/routing | DTO drift, route-level tenant mistakes, status shape regressions |
| `crates/agentdb-wire/tests/driver.rs` and command unit tests | MongoDB adapter | driver handshake, PLAIN auth, fixed commands/collections, errors | unsupported command drift, auth mismatch, adapter/runtime divergence |
| `crates/agentdb-meta/tests/postgres_smoke.rs` and meta unit tests | control plane | migrations, nodes/tablets/routing/session locators, scheduler/rebalance helpers | schema drift, metadata state-machine regressions |
| `crates/agentdb-storage/tests/*` | KV/content backends | scan ordering, prefix isolation, RocksDB/SlateDB/content contracts | backend-specific persistence and range bugs |
| `crates/agentdb-dashboard/tests/assets.rs` | dashboard | static assets are embedded/servable | packaging regressions |
| `clients/python/tests`, `clients/typescript/tests` | SDKs | route mapping, smoke/session-first flow, async flow, SSE parsing, commit token propagation | SDK URL/header drift, parser regressions |
| `adapters/*/tests`, `examples/agents/python/test_*.py`, Vercel template tests | adapters/examples | framework event/checkpoint/store mapping | adapter drift from runtime primitives |
| `ops/smoke/scripts/real-cluster-smoke-compose.sh`, `tools/agentdb-cluster-smoke` | real-process cluster | seeded topology, exact sequence writes, failover, hardening, chaos, split/merge probes | process crash, owner loss, replica catch-up, movement bugs |
| `docs/stability-test-plan.md` | stability record | documents deterministic-surface, hardening, seeded-chaos coverage and known hardening failures | gap visibility for cluster validation |
| `ops/observability/smoke-clickhouse-otel.sh`, `smoke-clickstack-hyperdx.sh` | observability export | ClickHouse schema and ClickStack-compatible projections | connector/schema projection drift |
| `tools/agentdb-bench`, `ops/perf/scripts/*`, `tools/workload` | performance/workload | reproducible benchmark/reporting machinery | throughput/tail regressions when explicitly run |
| `.github/workflows/ci.yml`, `ops/local-ci/scripts/local-ci.sh` | CI/release | workspace checks/tests, Docker/release paths, local CI orchestration | normal PR regression signal, not fault coverage |

## 4. Failure-mode hypotheses

### Pitfall walk from `references/common-distributed-systems-pitfalls.md`

| Pitfall | Applies? | AgentDB interpretation | Hypotheses |
|---|---|---|---|
| 1. Lost updates under concurrent write + partition | yes | cluster tablet append may accept unsafe writes if quorum/routing is wrong | H15 |
| 2. Stale reads from followers / secondaries | yes | routed reads/searches may hit stale owner/follower after write or handoff | H11, H16 |
| 3. Replica divergence after partition / restart | yes | Raft replicas/checkpoints/tablet stores may diverge after fault | H15, H17 |
| 4. Linearizability under wall-clock skew or timing | maybe | wall-clock must not determine append order; leases/readiness may still be timing-sensitive | H18 |
| 5. Lost ack vs lost commit | yes | post-commit timeout must be resolved by idempotency replay | H2 |
| 6. Reconfiguration / membership-change races | yes | replacement, split/merge, owner movement, rebalance may race writes | H17 |
| 7. Crash-recovery divergence | yes | RocksDB/content/index/outbox/cache rehydrate can lose acknowledged state | H3, H4 |
| 8. Schema migration during traffic | maybe | Postgres metadata migrations and upgrade fixtures can change state while traffic runs | H20 |
| 9. ID / sequence number collision under partition | yes | session seq/id generation can reuse values after restart/handoff | H1, H15 |
| 10. Watch/change-feed event loss or duplication | yes | watch/export cursors can gap/duplicate across restart/movement/prune | H10 |
| 11. Cross-shard transaction non-atomicity | maybe | no generic multi-key transactions are claimed, but split/merge and metadata+runtime handoffs are cross-component | H17, M5 |
| 12. Clock-skew safety violations | maybe | cluster leases/status may depend on time; append safety should not | H18 |
| 13. Auth/quota/metadata divergence under partition | yes | tenant policy/quota/routing metadata can diverge from data plane | H9, H18 |
| 14. Lease expiry under contention | yes | meta-layer load can expire live owner/export/scheduler leases | H12, H18 |
| 15. Idempotency replay bypassed by cold cache | yes | retries after restart/handoff/eviction can double-apply | H2 |
| 16. Async outbox head-of-line block | yes | record index/export outbox can stall on missing referent or GC race | H7, H10 |

### Session runtime and durability

H1. **sequence_collision_or_gap_under_concurrency_restart_or_handoff** - concurrent appends, restart, or cluster handoff can allocate duplicate, skipped, or reordered session `seq` values.
- Could falsify: C1, C2, C15, C17
- Suspected because: per-session serialization spans hot cache, persistence, and tablet ownership paths.
- Subsystem: `agentdb-core` session runtime, tablet runtime

H2. **idempotency_replay_bypassed_after_lost_ack_cold_cache_or_movement** - an operation that committed but timed out can be retried after restart, cache eviction, or movement and either double-commit or replay the wrong result.
- Could falsify: C3, C4, C15, C17, C20
- Suspected because: lost-ack/cold-cache idempotency is a common pitfall and prior stability notes identify retry-around-timeout risks.
- Subsystem: idempotency, persistence, SDK retry, Raft tablet commit

H3. **single_node_restart_loses_or_misorders_runtime_state** - same-version restart can lose acknowledged events, next sequence, idempotency, records, skills, artifacts, lifecycle state, or export cursor state.
- Could falsify: C4, C6, C10, C12
- Suspected because: runtime data spans RocksDB, content backend, index outbox, export ledger, and in-memory rehydration.
- Subsystem: persistence/recovery

H4. **metadata_content_index_atomicity_breaks_at_crash_boundary** - crash or write failure between runtime metadata, content bytes, and index/export outbox can leave visible metadata without bytes, bytes without metadata, or invisible committed records.
- Could falsify: C4, C8, C10, C12
- Suspected because: base truth, content, and derived indexes are separate physical stores.
- Subsystem: storage/content/index/export

H5. **fork_or_tiered_lifecycle_logical_history_diverges_from_model** - fork replay or warm/cold/archive/restore transitions can omit, duplicate, or reorder parent/child events.
- Could falsify: C5, C6
- Suspected because: logical reads merge multiple physical forms and fork boundaries.
- Subsystem: fork/replay/lifecycle

### Records, policy, tenants, adapters

H6. **tenant_scope_escape_through_secondary_surface** - watch/export/Mongo/SDK/adapters/blob/search/dashboard/admin surfaces can leak or mutate another tenant's data.
- Could falsify: C7, C11, C14, C20, C21
- Suspected because: multiple protocols and adapters map onto the same tenant-scoped runtime.
- Subsystem: API, SDKs, Mongo, adapters, dashboard/admin

H7. **record_index_lag_repair_or_outbox_stall_hides_or_leaks_records** - async global-index lag, missing referents, GC, or outbox head-of-line blocking can hide same-client committed records, leak deleted records, or stall watermarks.
- Could falsify: C8, C9, C18, C20
- Suspected because: record visibility intentionally depends on local/global index split and durable outbox repair.
- Subsystem: records/index worker

H8. **policy_quota_lifecycle_gc_fails_open_or_purges_held_state** - concurrent admission, retention, legal hold, lifecycle sweep, or GC can over-admit work, purge held data, or leave deleted data searchable.
- Could falsify: C9, C13
- Suspected because: policy intersects admission, storage accounting, lifecycle workers, indexes, and content purging.
- Subsystem: policy/lifecycle/GC

H9. **auth_quota_metadata_config_diverges_across_surfaces** - server config, Mongo auth, tenant policy/quotas, and metadata/routing state can be interpreted differently by nodes or tools.
- Could falsify: C7, C13, C14, C16, C18
- Suspected because: config-file defaults, CLI overrides, optional env vars, Postgres metadata, and compatibility listeners all influence runtime safety.
- Subsystem: config/meta/auth/quota

### Watch, export, operations, protocols

H10. **watch_export_subscribe_cursor_gap_duplicate_or_bad_recovery** - paged resume, restart, prune, split/merge, or wrong-scope cursor can skip, duplicate, mis-scope, or wrongly reject committed changes.
- Could falsify: C11, C12, C17
- Suspected because: cursors encode scope, ledger position, tablet movement context, and retention windows.
- Subsystem: subscribe/watch/export

H11. **export_consumer_acks_before_sink_accept_or_loses_takeover_range** - connector crash before/after sink write or lease takeover can advance AgentDB ack before the sink durably accepts data.
- Could falsify: C12
- Suspected because: no-loss export depends on connector ordering and lease enforcement, not only server paging.
- Subsystem: `agentdbctl` export connectors

H12. **lease_renewal_or_readiness_fails_under_metadata_contention** - scheduler, owner, or export leases can expire or readiness can report ready while metadata/control-plane resources are degraded.
- Could falsify: C16, C17, C18
- Suspected because: cluster and export lease paths share metadata/control-plane resources and are time-sensitive.
- Subsystem: `agentdb-meta`, scheduler, health/readiness

### Cluster and routing

H13. **routing_epoch_or_residency_accepts_wrong_owner_mutation** - stale routing snapshots, wrong owner forwarding, unassigned tablet state, or policy changes can send mutations to the wrong tablet/cell.
- Could falsify: C7, C16, C17
- Suspected because: routing combines tenant policy, session locators, tablet maps, epochs, owners, and forwarding.
- Subsystem: `agentdb-api` routing, `agentdb-meta`

H14. **raft_ack_without_quorum_or_replica_divergence_after_fault** - a tablet can acknowledge without durable quorum, lose committed entries, or diverge committed/applied state after partition/restart.
- Could falsify: C15, C17, C18
- Suspected because: cluster mode is real but evaluation-oriented and includes catch-up/snapshot paths.
- Subsystem: Raft/tablet runtime

H15. **tablet_reconfiguration_drops_state_or_reuses_sequence** - replacement, split, merge, checkpoint install, or owner movement can miss writes committed during prepare/cutover or reuse stale session sequence/idempotency state.
- Could falsify: C1, C3, C11, C15, C16, C17
- Suspected because: movement crosses metadata, routing, Raft, snapshots, runtime state, watch/export, and retry logic.
- Subsystem: tablet movement/reconfiguration

### Compatibility, performance, upgrade, fuzzing

H16. **sdk_adapter_retry_commit_token_or_route_drift** - SDKs/adapters can omit stable idempotency keys, lose record commit tokens, parse SSE incorrectly, or map framework objects to wrong tenants/sessions.
- Could falsify: C3, C8, C20, C21
- Suspected because: clients add ergonomic aliases and retry behavior above canonical HTTP routes.
- Subsystem: SDKs/adapters/examples

H17. **mongo_protocol_accepts_unsupported_or_unsafe_semantics** - Mongo listener can accidentally accept arbitrary Mongo behavior, use wrong auth rules, parse malformed OP_MSG/BSON unsafely, or diverge from runtime semantics.
- Could falsify: C7, C14
- Suspected because: compatibility adapters tend to grow beyond their documented subset.
- Subsystem: `agentdb-wire`

H18. **parser_or_config_fuzz_crash_or_silent_unsafe_start** - BSON/OP_MSG/SSE/cursor/API/config parsers can panic, hang, or accept unsafe unsupported states.
- Could falsify: C14, C18, C20
- Suspected because: these are byte/protocol/config trust boundaries.
- Subsystem: wire, SDK parsers, API DTOs, server config

H19. **performance_or_noisy_neighbor_tail_collapse_invalidates_claims** - aggregate throughput can look healthy while p99/p99.9 latency, tenant fairness, or resource usage violates benchmark caveats.
- Could falsify: C7, C19
- Suspected because: benchmark docs call out workload/durability caveats and noisy-neighbor risk.
- Subsystem: benchmark/workload/runtime

H20. **upgrade_or_migration_breaks_persisted_state_or_public_compatibility** - binary upgrade, Postgres metadata migration, or SDK/server version skew can fail to read existing state or silently change API behavior.
- Could falsify: C4, C6, C12, C16, C20, C21
- Suspected because: current tests are mostly same-version and cluster mixed-version support is not documented.
- Subsystem: upgrade/migration/compatibility

H21. **formal_protocol_invariant_not_stated_or_wrong** - quorum commit, routing epoch fencing, split/merge, lease/ack, and export protocols may rely on assumptions not covered by examples.
- Could falsify: C12, C15, C16, C17
- Suspected because: these protocols span components and have small enough safety cores to model.
- Subsystem: protocol design

All minimum categories apply in project-wide scope: correctness (H1, H5, H6), durability (H2-H4, H14), liveness (H12, H14, H15), partial failure (H10-H15), idempotency/replay (H2, H15, H16), upgrade/rollback (H20), configuration (H9, H18), and performance/fairness (H19).

## 5. Coverage matrix

| Claim | Hypothesis | Existing tests | Verdict | Gap kind | Recommended scenario |
|---|---|---|---|---|---|
| C15 | H14 | `runtime_quorum.rs`, cluster smoke | partial | no asymmetric partition / disk fault oracle | S8 |
| C17 | H15 | `docs/stability-test-plan.md`, hardening smoke | partial | movement/idempotency oracle too shallow | S9 |
| C16 | H13 | API routing tests, cluster smoke | partial | no generated topology/epoch coverage | S10 |
| C12 | H11 | export API tests, `agentdbctl` unit coverage, ClickHouse smoke | partial | no sink crash/takeover fault matrix | S7 |
| C3 | H2 | runtime/API idempotency tests | partial | no deterministic post-commit timeout injection | S2 |
| C4 | H3 | `runtime_restart.rs` | partial | same-version examples only | S3, S12 |
| C4 | H4 | storage/content/index tests | partial | no crash-at-IO-boundary matrix | S3 |
| C7 | H6 | isolation tests per surface | partial | no unified cross-surface tenant oracle | S5 |
| C8 | H7 | record runtime tests | partial | no randomized lag/outbox/repair campaign | S6 |
| C11 | H10 | watch/export tests and smoke | partial | no prune/restart/movement cursor oracle | S7, S9 |
| C13 | H8 | policy/lifecycle tests | partial | no generated concurrency/clock/GC matrix | S6 |
| C14 | H17 | Mongo driver tests | partial | no negative protocol/property/fuzz suite | S11 |
| C18 | H12 | API/admin/dashboard tests and smoke | partial | no false-ready metadata fault campaign | S8, S10 |
| C19 | H19 | benchmark scripts/docs | partial | no release-grade noisy-neighbor gate | S13 |
| C20 | H16 | Python/TS tests | partial | no live fault/retry and adapter cross-check | S5, S11 |
| C1 | H1 | append tests, quorum tests | partial | no high-contention plus restart/handoff | S1, S8 |
| C2 | H1 | append-and-fetch tests | partial | no hot-tail fault/stress variant | S1 |
| C5 | H5 | fork/context tests | partial | no generated fork+lifecycle model | S4 |
| C6 | H5 | lifecycle/restart tests | partial | no crash during transition matrix | S4, S12 |
| C9 | H7/H8 | record/policy/lifecycle tests | partial | no generated policy/search input space | S6 |
| C10 | H4 | artifact/content tests | partial | no partial content backend failure injection | S3 |
| C15 | H21 | no formal model in repo | not covered | no model | S14 |
| C16 | H21 | no formal model in repo | not covered | no model | S14 |
| C12 | H21 | no formal export ack model | not covered | no model | S14 |
| C21 | H16/H20 | adapter/example tests | partial | no cross-version/live fault adapter run | S5, S12 |

Verdict legend: covered / partial / not covered. Gap-kind legend: no test / shallow test / oracle too weak / no fault-injection variant / no scale variant.

## 6. Technique selection

- **Jepsen and Elle**
  - **Hypotheses it addresses:** H1, H6, H10, H13, H14, H15
  - **What it would catch that other techniques miss:** lost acknowledged writes, duplicate/gapped session histories, stale reads, divided ownership, and cursor anomalies across concurrent clients and injected partitions.
  - **Reference:** `references/jepsen-and-elle.md`
  - **Cost / wall-clock estimate:** 30 minutes to multiple hours per scenario; target at least 10k operations per history.
- **Deterministic simulation testing**
  - **Hypotheses it addresses:** H1, H2, H10, H13, H15, H21
  - **What it would catch that other techniques miss:** reproducible commit/timeout/retry interleavings, stale epoch races, and movement cutover ordering bugs from a single seed.
  - **Reference:** `references/deterministic-simulation.md`
  - **Cost / wall-clock estimate:** high upfront plumbing; seconds to minutes per seed once simulated IO/clock hooks exist.
- **Chaos and fault injection**
  - **Hypotheses it addresses:** H2, H4, H10, H11, H12, H14, H15, H19
  - **What it would catch that other techniques miss:** real process, kernel, network, metadata, object-store, disk, limping-node, and connector crash behavior.
  - **Reference:** `references/chaos-and-fault-injection.md`
  - **Cost / wall-clock estimate:** minutes to hours per scenario; Linux fault privileges required for full coverage.
- **Crash recovery and upgrade testing**
  - **Hypotheses it addresses:** H2, H3, H4, H5, H10, H15, H20
  - **What it would catch that other techniques miss:** crash-at-boundary data loss, double-apply on replay, WAL/content/index atomicity holes, and versioned state incompatibility.
  - **Reference:** `references/crash-recovery-and-upgrade.md`
  - **Cost / wall-clock estimate:** minutes to hours per matrix cell; high planning cost because boundary count grows quickly.
- **Property-based and metamorphic testing**
  - **Hypotheses it addresses:** H5, H6, H7, H8, H9, H13, H16, H18, H19
  - **What it would catch that other techniques miss:** generated fork/lifecycle histories, cross-surface tenant equivalence, record visibility under lag, config-space safety, route-planner state-space bugs, and SDK/adapters metamorphic drift.
  - **Reference:** `references/property-and-metamorphic.md`
  - **Cost / wall-clock estimate:** seconds to minutes per property; main cost is good generators and shrinkable models.
- **Performance and benchmarking**
  - **Hypotheses it addresses:** H12, H19
  - **What it would catch that other techniques miss:** p99/p99.9 collapse, head-of-line blocking, tenant unfairness, and benchmark claims unsupported by repeatable evidence.
  - **Reference:** `references/performance-and-benchmarking.md`
  - **Cost / wall-clock estimate:** smoke cells 10-15 minutes; release-grade cells 30-90 minutes each across repeats.
- **Fuzzing**
  - **Hypotheses it addresses:** H17, H18
  - **What it would catch that other techniques miss:** malformed BSON/OP_MSG/SSE/cursor/config/API input crashes, hangs, or unsupported command acceptance.
  - **Reference:** `references/fuzzing.md`
  - **Cost / wall-clock estimate:** CPU-hours per target; useful for nightly or continuous fuzzing.
- **Formal methods**
  - **Hypotheses it addresses:** H11, H13, H14, H15, H21
  - **What it would catch that other techniques miss:** protocol-level missing invariants in export ack, routing epoch fencing, quorum, replacement, split, and merge before sampled tests happen to hit them.
  - **Reference:** `references/formal-methods-tla.md`
  - **Cost / wall-clock estimate:** days to weeks to write useful models; minutes to hours per TLC run.

## 6b. Environment requirements

| Requirement | Version floor | Used by scenarios | Install hint |
|---|---|---|---|
| Rust toolchain | repo-pinned, Rust 1.75+ | S1-S4, S6, S8-S12 | `rustup show`; build with `cargo test --workspace --locked` |
| Cargo workspace dependencies | `Cargo.lock` | S1-S4, S6, S8-S12 | downloaded by Cargo as needed |
| Python | 3.12+ | S5, S11 | `python -m pip install -e clients/python pytest hypothesis` |
| Node.js | 20+ | S5, S11 | `npm ci` in `clients/typescript`; add `fast-check` when implementing properties |
| Go | 1.21+ | S13 | `go version`; used by `tools/workload` if selected as load driver |
| Docker + Compose | Compose plugin 2.20+ | S7-S10, S13 | cluster/observability compose stacks are in repo |
| Linux host or VM | modern kernel with cgroups/netns | S7-S10, S13 | required for realistic `tc`, `iptables`/`nft`, and process pause/kill |
| `tc` / `netem` | any modern `iproute2` | S8-S10 | needs `CAP_NET_ADMIN` |
| `iptables` or `nftables` | any modern version | S8-S10 | needs root/CAP_NET_ADMIN |
| Postgres | 16 preferred | S8-S10, S12 | brought up by `ops/smoke/scripts/real-cluster-smoke-compose.sh` |
| MinIO / S3-compatible object store | current stable | S3, S4, S8-S10 | brought up by SUT compose or configured object-store backend |
| ClickHouse or ClickStack | current stable | S7 | `ops/observability/*compose.yml` or equivalent service |
| Jepsen/Elle or Porcupine | current | S8-S10 | Clojure/Leiningen Jepsen or language-local history checker |
| `proptest`, Hypothesis, fast-check | compatible current | S4-S6, S10-S11 | add dev dependencies in target crates/packages when implementing |
| `cargo-fuzz` / libFuzzer | current | S11 | `cargo install cargo-fuzz`; LLVM toolchain required |
| TLA+ TLC | current | S14 | `tla2tools.jar` or TLA+ Toolbox |
| Benchmark host | 8+ cores, NVMe, 32 GiB RAM preferred | S13 | dedicated host; otherwise mark performance cells inconclusive |
| Metrics/log collection | shell + JSON tooling; Prometheus optional | S7-S10, S13 | collect per-node logs, admin JSON, benchmark JSON/Markdown, sink checksums |

## 7. Scenarios

### Scenario S1: contiguous_session_seq_and_append_fetch_under_high_concurrency

- **Falsifies if it FAILs:** C1, C2
- **Closes:** H1
- **Technique:** property-based/metamorphic plus performance stress
- **Workload:** generate 1-256 sessions per tenant, 1-128 concurrent clients per hot session, and a 70% `append-and-fetch`, 20% append, 10% fetch/replay mix over uniform, Zipf-1.2, and hotset-90-10 selectors for 5-15 minutes.
- **Faults:** baseline has no external faults; repeat after process restart and with bounded hot-tail/cache settings.
- **Oracle:** for every session, final replay sorted by seq equals `0..n-1`, event IDs are unique, and every `append-and-fetch` response with `fetch_last_n > 0` contains its own event and no event that disappears after restart.
- **Observability required:** operation history, response seq/event_id, final replay/fetch dump, server logs and latency histograms.
- **Exit criteria:** PASS if 3 seeds per selector complete with zero gaps/duplicates/stale tails and at least 10k successful appends; FAIL on first invariant violation; INCONCLUSIVE if restart cannot be performed.
- **Target test file:** `crates/agentdb-core/tests/auto/S1_contiguous_session_seq_and_append_fetch_under_high_concurrency.rs`
- **Skeleton language:** rust
- **Skeleton:**

  ```rust
  //! AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  //! Scenario: S1 contiguous_session_seq_and_append_fetch_under_high_concurrency
  //! Falsifies: C1, C2
  //!
  //! REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  use agentdb_core::Runtime;

  #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
  async fn contiguous_session_seq_and_append_fetch_under_high_concurrency() {
      // 1. Workload - generate concurrent append/append-and-fetch operations.
      // TODO: build seeded tenants/sessions and record every response.

      // 2. Faults - restart persistent runtime between phases and vary cache limits.
      // TODO: reopen runtime from the same temp directory and continue appends.

      // 3. Oracle - assert final replay has unique contiguous seqs and own-write tails.
      // TODO: compare replay dumps with the operation model.
  }
  ```

### Scenario S2: idempotent_replay_after_lost_ack_restart_and_handoff

- **Falsifies if it FAILs:** C3, C4, C15, C17, C20
- **Closes:** H2
- **Technique:** deterministic simulation, crash recovery, chaos/fault injection
- **Workload:** issue append, append-and-fetch, record write, skill register/bind, artifact create, and SDK mutating calls with stable idempotency keys; inject at least 1,000 ambiguous outcomes per operation class.
- **Faults:** force timeout after durable commit but before HTTP response; restart single-node runtime; in cluster mode kill or hand off owner immediately after commit; repeat during split/merge prepare.
- **Oracle:** retry with same key returns exact original ID/seq/result; conflicting retry returns idempotency conflict; final state contains exactly one committed effect per key.
- **Observability required:** forced-fault markers, operation history, durable replay/search/export state, per-node Raft status for cluster cells.
- **Exit criteria:** PASS if all operation classes preserve one effect per key across 3 deterministic seeds and 1 real-cluster seed; FAIL on duplicate effect, wrong replayed seq, or conflict on same body; INCONCLUSIVE if no post-commit timeout hook exists.
- **Target test file:** `crates/agentdb-core/tests/auto/S2_idempotent_replay_after_lost_ack_restart_and_handoff.rs`
- **Skeleton language:** rust
- **Skeleton:**

  ```rust
  //! AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  //! Scenario: S2 idempotent_replay_after_lost_ack_restart_and_handoff
  //! Falsifies: C3, C4, C15, C17, C20
  //!
  //! REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  use agentdb_core::Runtime;

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn idempotent_replay_after_lost_ack_restart_and_handoff() {
      // 1. Workload - submit mutating calls with stable idempotency keys.
      // TODO: cover append, records, skills, artifacts, and SDK-equivalent requests.

      // 2. Faults - inject post-commit response loss and restart/handoff.
      // TODO: add or call a fault hook that drops the response after durable commit.

      // 3. Oracle - retry same/conflicting bodies and assert one committed effect per key.
      // TODO: compare replayed result and final runtime state.
  }
  ```

### Scenario S3: acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries

- **Falsifies if it FAILs:** C4, C8, C10, C12
- **Closes:** H3, H4
- **Technique:** crash recovery and fault injection
- **Workload:** concurrent artifact uploads, record writes with commit tokens, appends with artifact refs, skill bindings, and export ledger writes in persistent RocksDB with local and object-store content backends.
- **Faults:** crash before metadata commit, after metadata before blob, after blob before response, after record base before outbox, after outbox before index apply, and during checksum/crypto validation.
- **Oracle:** after restart, no visible artifact metadata lacks retrievable bytes; checksum mismatch rejects without committed metadata; committed records are visible with commit token; export ledger resumes without skipping committed envelopes.
- **Observability required:** filesystem/object-store listing, KV scan summary, index watermarks/counters, API readbacks, export cursor status.
- **Exit criteria:** PASS if 100 injected crash points per backend recover to a valid state; FAIL on orphan visible metadata, missing committed record under token, checksum acceptance, or unrecoverable ledger/index stall.
- **Target test file:** `crates/agentdb-core/tests/auto/S3_acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries.rs`
- **Skeleton language:** rust
- **Skeleton:**

  ```rust
  //! AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  //! Scenario: S3 acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries
  //! Falsifies: C4, C8, C10, C12
  //!
  //! REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  use agentdb_core::Runtime;

  #[tokio::test]
  async fn acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries() {
      // 1. Workload - interleave artifacts, records, appends, skills, and exports.
      // TODO: create a seeded persistent runtime and capture acknowledged operations.

      // 2. Faults - crash/fail at metadata, content, index, and ledger boundaries.
      // TODO: wire failpoints or a test content/index backend.

      // 3. Oracle - reopen and assert visible state is complete or absent as documented.
      // TODO: check blob retrieval, commit-token search, and export cursor continuity.
  }
  ```

### Scenario S4: fork_and_tiered_session_history_remains_ordered_across_lifecycle_faults

- **Falsifies if it FAILs:** C5, C6
- **Closes:** H5
- **Technique:** property-based testing plus crash recovery
- **Workload:** generate parent/child sessions with 0-10k events and random lifecycle operations: fork at cutoff, append to child, transition cold, archive, restore, reopen warm, append after restore, fetch/replay/context.
- **Faults:** crash before/after segment write, manifest update, hot-row deletion, archive bundle creation, restore materialization, and post-archive append.
- **Oracle:** logical replay equals a pure model history for every generated sequence; seqs are contiguous within each session; storage summary matches physical blobs/manifests.
- **Observability required:** final replay/fetch dump, lifecycle/storage summary, content listing, transition logs.
- **Exit criteria:** PASS for 1,000 generated operation sequences across local and object-store content backends; FAIL on model mismatch; INCONCLUSIVE if lifecycle crash hooks are unavailable.
- **Target test file:** `crates/agentdb-core/tests/auto/S4_fork_and_tiered_session_history_remains_ordered_across_lifecycle_faults.rs`
- **Skeleton language:** rust
- **Skeleton:**

  ```rust
  //! AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  //! Scenario: S4 fork_and_tiered_session_history_remains_ordered_across_lifecycle_faults
  //! Falsifies: C5, C6
  //!
  //! REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  use agentdb_core::Runtime;

  #[tokio::test]
  async fn fork_and_tiered_session_history_remains_ordered_across_lifecycle_faults() {
      // 1. Workload - generate fork/lifecycle/event sequences and a model history.
      // TODO: use proptest or a seeded generator with shrinkable operations.

      // 2. Faults - crash around segment, archive, restore, and warm-head boundaries.
      // TODO: inject failpoints and reopen persistent runtime.

      // 3. Oracle - compare replay/fetch/context against the model.
      // TODO: assert storage manifests and content blobs match lifecycle state.
  }
  ```

### Scenario S5: tenant_isolation_across_http_sdk_mongo_adapters_and_export

- **Falsifies if it FAILs:** C7, C11, C14, C20, C21
- **Closes:** H6, H16, H17
- **Technique:** property-based/metamorphic cross-surface testing
- **Workload:** create 3-20 tenants with overlapping aliases, namespaces, tags, event kinds, artifact names, Mongo database names, and framework adapter sessions; randomly mix native HTTP, Python SDK, TypeScript SDK, Mongo adapter, Vercel/LangGraph/OpenAI adapter flows, watch/export, and blob reads.
- **Faults:** wrong-tenant IDs, malformed prefix-valid IDs, global-index lag, delete/expiry/GC, cursor from another tenant, Mongo database alias collision, SDK `/api/me` failure.
- **Oracle:** each tenant's reads/search/watch/export/Mongo/adapters/blob results equal only that tenant's model; cross-tenant requests return not found/invalid cursor without existence leaks; adapters produce the same core event/record model as native HTTP.
- **Observability required:** model state per tenant, operation history, adapter request logs, index stats, response status/error codes.
- **Exit criteria:** PASS for 10k operations over 20 seeds with zero isolation violations; FAIL on any cross-tenant object, cursor item, index result, or successful wrong-tenant mutation.
- **Target test file:** `adapters/tests/auto/test_S5_tenant_isolation_across_http_sdk_adapters_and_export.py`
- **Skeleton language:** python
- **Skeleton:**

  ```python
  # AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  # Scenario: S5 tenant_isolation_across_http_sdk_mongo_adapters_and_export
  # Falsifies: C7, C11, C14, C20, C21
  #
  # REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  import pytest


  @pytest.mark.asyncio
  async def test_tenant_isolation_across_http_sdk_mongo_adapters_and_export():
      # 1. Workload - generate tenants and mixed native/SDK/Mongo/adapter operations.
      # TODO: start a dev server with HTTP and Mongo listeners and build a tenant model.

      # 2. Faults - inject wrong-tenant IDs, cursor reuse, index lag, and auth/user-info failures.
      # TODO: run negative requests and induced lag/delete/GC operations.

      # 3. Oracle - compare every surface against the per-tenant model.
      # TODO: assert no cross-tenant reads or mutations and adapter/native equivalence.
      raise NotImplementedError
  ```

### Scenario S6: record_visibility_and_policy_gc_match_committed_model_under_lag

- **Falsifies if it FAILs:** C8, C9, C13, C18
- **Closes:** H7, H8
- **Technique:** property-based/metamorphic testing
- **Workload:** generate tenant policies, namespaces, sharding keys, text/vector records, delete/expiry events, lifecycle/GC sweeps, and record searches with/without commit tokens.
- **Faults:** global-index worker lag, outbox entry with missing referent, GC while outbox pending, clock jumps around expiry, legal hold toggles, quota boundary concurrency.
- **Oracle:** local search with `sharding_key` is read-after-write; global search with commit token repairs to include committed records; deleted/expired/private records never appear; outbox advances past unreparable entries with visible counters; policy reports match physical state.
- **Observability required:** record model, index watermarks and skip counters, policy report, response headers, storage summary.
- **Exit criteria:** PASS for 5k generated policies/operation sequences; FAIL on missing committed record under token, stale deleted result, head-of-line outbox stall, over-limit committed work, or legal-hold purge.
- **Target test file:** `crates/agentdb-core/tests/auto/S6_record_visibility_and_policy_gc_match_committed_model_under_lag.rs`
- **Skeleton language:** rust
- **Skeleton:**

  ```rust
  //! AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  //! Scenario: S6 record_visibility_and_policy_gc_match_committed_model_under_lag
  //! Falsifies: C8, C9, C13, C18
  //!
  //! REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  use agentdb_core::Runtime;

  #[tokio::test]
  async fn record_visibility_and_policy_gc_match_committed_model_under_lag() {
      // 1. Workload - generate record writes/searches and policy/GC operations.
      // TODO: construct a model for committed record visibility and policy state.

      // 2. Faults - lag global index, remove referents, jump clocks, and race GC/admission.
      // TODO: add hooks or test workers for lag and missing-referent injection.

      // 3. Oracle - assert search, counters, policy reports, and storage match the model.
      // TODO: verify commit-token repair and no deleted/private record visibility.
  }
  ```

### Scenario S7: export_and_watch_cursors_are_gap_free_and_sink_ack_safe

- **Falsifies if it FAILs:** C11, C12, C17
- **Closes:** H10, H11
- **Technique:** property-based cursor model plus connector chaos/fault injection
- **Workload:** append 1k-100k mixed events/records/skills/artifacts across sessions; page subscribe/watch/export/backfill with small limits; run two `agentdbctl export clickhouse` or JSONL connectors sharing a managed consumer.
- **Faults:** restart between pages, prune old ledger entries, wrong-scope cursor, cursor below oldest available, kill connector before sink write, after partial sink write before ack, after full sink write before ack, during lease renewal, and after ack.
- **Oracle:** concatenated pages equal committed model with no duplicates/gaps/wrong-scope items; expired cursor returns documented recovery metadata; sink contains every acked range and AgentDB never advances `last_acked_cursor` past sink-accepted data.
- **Observability required:** cursor history, committed model, export consumer status, sink row count/checksums, connector metrics, kill markers.
- **Exit criteria:** PASS if 20 cursor seeds and all six connector crash points preserve no-loss semantics; FAIL on missing/duplicate cursor item or sink-missing acked range; INCONCLUSIVE if no sink is available.
- **Target test file:** `crates/agentdb-api/tests/auto/S7_export_and_watch_cursors_are_gap_free_and_sink_ack_safe.rs`
- **Skeleton language:** rust
- **Skeleton:**

  ```rust
  //! AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  //! Scenario: S7 export_and_watch_cursors_are_gap_free_and_sink_ack_safe
  //! Falsifies: C11, C12, C17
  //!
  //! REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn export_and_watch_cursors_are_gap_free_and_sink_ack_safe() {
      // 1. Workload - generate committed mutations and page watch/export/subscribe.
      // TODO: create an HTTP test server and optional managed export consumer.

      // 2. Faults - restart between pages, prune cursors, and kill connector at ack boundaries.
      // TODO: integrate process/sink fault hooks for connector mode.

      // 3. Oracle - compare cursor streams and sink ack state with the committed model.
      // TODO: assert no gaps, duplicates, wrong-scope items, or ack-before-sink ranges.
  }
  ```

### Scenario S8: raft_quorum_preserves_acknowledged_tablet_writes_under_partition

- **Falsifies if it FAILs:** C1, C15, C17, C18
- **Closes:** H1, H14
- **Technique:** Jepsen/Elle or Porcupine plus chaos/fault injection
- **Workload:** 3-node and 6-node/two-cell clusters; clients append with idempotency keys and read/replay exact seqs from random API nodes; minimum 10k operations per run.
- **Faults:** leader kill, follower kill, asymmetric partition, delayed/lossy links, `kill -STOP` limping leader, one-AZ loss with quorum, two-voter loss without quorum, and restart after heal.
- **Oracle:** acknowledged appends form a linearizable per-session history with contiguous seqs; quorum-surviving cases remain writable within smoke windows; no-quorum cases reject/time out without acknowledged writes; readiness reports leaderless/lagging states accurately.
- **Observability required:** Elle/Porcupine history, per-node logs, `/api/admin/raft`, readiness, exact replay dumps.
- **Exit criteria:** PASS if no linearizability anomalies and no unsafe no-quorum writes across 3 seeds per fault; FAIL on lost acknowledged write, duplicate seq, divergent replica, or false-ready.
- **Target test file:** `ops/smoke/tests/auto/S8_raft_quorum_preserves_acknowledged_tablet_writes_under_partition.sh`
- **Skeleton language:** shell
- **Skeleton:**

  ```bash
  #!/usr/bin/env bash
  # AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  # Scenario: S8 raft_quorum_preserves_acknowledged_tablet_writes_under_partition
  # Falsifies: C1, C15, C17, C18
  #
  # REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  set -euo pipefail

  # 1. Workload - start real cluster and record append/read history.
  # TODO: invoke ops/smoke/scripts/real-cluster-smoke-compose.sh or a Jepsen harness.

  # 2. Faults - inject leader/follower kill, asymmetric partition, STOP/CONT, and AZ loss.
  # TODO: use tc/iptables/kill and record evidence the fault took effect.

  # 3. Oracle - run linearizability/contiguous-seq checker and readiness assertions.
  # TODO: analyze operation history and final replay dumps.
  ```

### Scenario S9: tablet_replacement_split_merge_preserves_state_idempotency_and_cursors

- **Falsifies if it FAILs:** C1, C3, C11, C15, C16, C17
- **Closes:** H10, H15
- **Technique:** deterministic simulation plus real-cluster crash recovery
- **Workload:** continuous append/read/watch/export workload during tablet replacement, checkpoint install, prepare/complete split, prepare/complete merge, and source retirement in a two-cell six-node cluster.
- **Faults:** commit timeouts, owner restart, target restart, stale snapshot install, crash between prepare and complete, delayed movement completion, retry storm using stable idempotency keys.
- **Oracle:** final per-session event history equals operation model; watch/export contains all writes committed during movement windows; idempotent retries replay exact committed seq; routing decisions after cutover target only valid tablets.
- **Observability required:** operation history, movement rows, routing decisions, watch/export cursors, snapshot metadata, final replay.
- **Exit criteria:** PASS if 5 movement cycles per seed complete with model equality and no cursor gaps; FAIL on stale seq replay, missed prepared-window write, stale owner mutation, or lost cursor.
- **Target test file:** `ops/smoke/tests/auto/S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors.sh`
- **Skeleton language:** shell
- **Skeleton:**

  ```bash
  #!/usr/bin/env bash
  # AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  # Scenario: S9 tablet_replacement_split_merge_preserves_state_idempotency_and_cursors
  # Falsifies: C1, C3, C11, C15, C16, C17
  #
  # REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  set -euo pipefail

  # 1. Workload - run continuous appends, reads, watch, and export during movement.
  # TODO: extend real-cluster smoke with operation history capture.

  # 2. Faults - restart owners/targets and inject commit timeout/retry storms.
  # TODO: schedule faults around replacement and split/merge prepare/complete.

  # 3. Oracle - compare model history, idempotency replay, routing, and cursor streams.
  # TODO: fail on stale owner mutation, missed movement-window write, or cursor gap.
  ```

### Scenario S10: routing_epoch_residency_and_readiness_reject_unsafe_cluster_states

- **Falsifies if it FAILs:** C7, C16, C17, C18
- **Closes:** H9, H12, H13
- **Technique:** property-based route model plus metadata/control-plane chaos
- **Workload:** generate cells, placement scopes, tenants, tablet ranges, owners, epochs, session locators, stale snapshots, drain/maintenance states, residency policies, metadata pool pressure, and readiness polls every second while writes run every 100 ms.
- **Faults:** stale routing snapshot, unassigned tablet, owner outside allowed location, tenant policy update during request, forwarded stale epoch, Postgres restart, metadata pool limited to 1-2 connections, dropped LISTEN/NOTIFY, scheduler pause, slow metadata queries.
- **Oracle:** route planner either routes to correct owner/cell/tablet or rejects with documented reason; stale epoch/residency-denied mutations never commit; readiness never reports ready during leaderless/multi-leader/unassigned unsafe states; routing recovers after metadata returns.
- **Observability required:** generated topology, route decisions, API status/errors, routing headers, admin scheduler/nodes/tablets/routing JSON, metadata logs, write timeline.
- **Exit criteria:** PASS for 50k generated topologies plus 3 live metadata-fault seeds; FAIL on incorrect mutation route, missing rejection, false-ready, unsafe mutation, or unrecovered routing after metadata recovery.
- **Target test file:** `crates/agentdb-api/tests/auto/S10_routing_epoch_residency_and_readiness_reject_unsafe_cluster_states.rs`
- **Skeleton language:** rust
- **Skeleton:**

  ```rust
  //! AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  //! Scenario: S10 routing_epoch_residency_and_readiness_reject_unsafe_cluster_states
  //! Falsifies: C7, C16, C17, C18
  //!
  //! REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  #[tokio::test]
  async fn routing_epoch_residency_and_readiness_reject_unsafe_cluster_states() {
      // 1. Workload - generate route topologies and poll readiness during writes.
      // TODO: build property generators for cells/tablets/epochs/policies.

      // 2. Faults - stale snapshots, metadata outage/pool pressure, scheduler pauses.
      // TODO: run both model-level and live-cluster metadata fault cases.

      // 3. Oracle - assert reject-or-correct-route and no false-ready unsafe states.
      // TODO: compare decisions with the generated model and admin status.
  }
  ```

### Scenario S11: wire_sdk_adapter_and_config_boundaries_reject_malformed_or_unsupported_states

- **Falsifies if it FAILs:** C14, C18, C20, C21
- **Closes:** H16, H17, H18
- **Technique:** fuzzing plus property-based compatibility checks
- **Workload:** coverage-guided fuzz targets for BSON/OP_MSG decoding, Mongo command dispatch, API DTO JSON validation, cursor parsing, config parsing, SDK SSE frame parsing, and adapter event/checkpoint conversion.
- **Faults:** malformed bytes, truncated UTF-8, nested/oversized BSON, invalid cursors, unknown Mongo commands, mixed auth payloads, boundary integers, huge/empty config arrays, SDK connection drops, adapter duplicate checkpoint/event conversions.
- **Oracle:** no panic, hang, OOM, unsupported command acceptance, wrong auth fallback, or adapter/runtime model divergence; errors are structured and supported valid corpus still parses.
- **Observability required:** fuzz corpus, crash artifacts, sanitizer output, minimized repros, SDK/adapter request logs.
- **Exit criteria:** PASS after 24 CPU-hours per Rust target and 1M generated SDK/adapter cases with zero crashes/divergence; FAIL on panic, hang, OOM, unsafe unsupported Mongo semantics, or wrong-tenant fallback.
- **Target test file:** `crates/agentdb-wire/fuzz/fuzz_targets/S11_wire_sdk_adapter_and_config_boundaries.rs`
- **Skeleton language:** rust
- **Skeleton:**

  ```rust
  //! AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  //! Scenario: S11 wire_sdk_adapter_and_config_boundaries_reject_malformed_or_unsupported_states
  //! Falsifies: C14, C18, C20, C21
  //!
  //! REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  #![no_main]

  use libfuzzer_sys::fuzz_target;

  fuzz_target!(|data: &[u8]| {
      // 1. Workload - feed bytes/cases to Mongo, cursor, DTO, config, and SSE parsers.
      // TODO: dispatch into the smallest meaningful parser/handler entry points.

      // 2. Faults - malformed bytes and generated unsupported states are the input.
      // TODO: seed with real protocol examples and invalid command/auth variants.

      // 3. Oracle - no panic/hang/OOM and unsupported states reject safely.
      // TODO: assert parser/handler result categories where possible.
      let _ = data;
  });
  ```

### Scenario S12: single_node_upgrade_preserves_runtime_state_and_client_compatibility

- **Falsifies if it FAILs:** C4, C6, C12, C16, C20, C21
- **Closes:** H20
- **Technique:** crash recovery and upgrade testing
- **Workload:** build fixture corpora with current release-1 binary or saved fixtures: tenants, sessions, warm/cold/archive histories, records, skills, artifacts, export consumers, durable cursors, SDK-created aliases, adapter-created checkpoints/events, and Postgres metadata fixtures.
- **Faults:** upgrade after clean shutdown, upgrade after `kill -9`, interrupted migration before restart, SDK/server minor version skew, and downgrade validation only if documented as supported.
- **Oracle:** new binary reads all prior state; next append seq is correct; lifecycle reads match pre-upgrade snapshot; export consumers resume; routing metadata migrates or rejects safely; SDK and adapter smoke tests pass against upgraded server.
- **Observability required:** pre/post state manifests, replay/search/export snapshots, server version, migration logs, SDK/adapter smoke output.
- **Exit criteria:** PASS if all fixtures round-trip and new mutations continue; FAIL on unreadable state, changed IDs/seqs, cursor loss, unsafe metadata migration, or undocumented manual step.
- **Target test file:** `ops/local-ci/tests/auto/S12_single_node_upgrade_preserves_runtime_state_and_client_compatibility.sh`
- **Skeleton language:** shell
- **Skeleton:**

  ```bash
  #!/usr/bin/env bash
  # AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  # Scenario: S12 single_node_upgrade_preserves_runtime_state_and_client_compatibility
  # Falsifies: C4, C6, C12, C16, C20, C21
  #
  # REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  set -euo pipefail

  # 1. Workload - create or load versioned fixtures with runtime, export, SDK, adapter, and metadata state.
  # TODO: run old binary/fixture loader and snapshot replay/search/export results.

  # 2. Faults - upgrade after clean shutdown, kill -9, and interrupted migration.
  # TODO: switch binaries and restart from the same data directories.

  # 3. Oracle - compare pre/post snapshots and run SDK/adapter smoke against upgraded server.
  # TODO: fail on seq/id/cursor/migration incompatibility.
  ```

### Scenario S13: benchmark_claims_remain_reproducible_under_skew_and_noisy_neighbors

- **Falsifies if it FAILs:** C7, C19
- **Closes:** H19
- **Technique:** performance and benchmarking
- **Workload:** run `agentdb-bench` single-node WAL, strict fsync, working-set-exceeds-cache, Zipf-1.2 repeated, Mongo adapter, MongoDB/Postgres baselines when versions match docs, and noisy-neighbor multi-tenant cells with one hot tenant and several quiet tenants.
- **Faults:** no correctness fault for baseline; add limping tenant with hot sessions and large artifacts; add compaction/cache pressure by growing state beyond memory.
- **Oracle:** reports include git SHA, hardware, versions, profile, duration, warmup, concurrency, repeats, durability settings, p50/p95/p99/p99.9/max, tenant-level p99, errors, and caveats; published claims are updated or withheld when confidence criteria fail.
- **Observability required:** benchmark report markdown/JSONL, system CPU/disk metrics, dataset size, per-tenant histograms.
- **Exit criteria:** PASS if results reproduce within declared tolerance over 3 repeats and no tenant-level p99 exceeds 2x aggregate p99 under noisy-neighbor cell; FAIL for release gating if claimed ratio regresses >15%, errors occur, or metadata is incomplete.
- **Target test file:** `tools/agentdb-bench/tests/auto/S13_benchmark_claims_remain_reproducible_under_skew_and_noisy_neighbors.sh`
- **Skeleton language:** shell
- **Skeleton:**

  ```bash
  #!/usr/bin/env bash
  # AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  # Scenario: S13 benchmark_claims_remain_reproducible_under_skew_and_noisy_neighbors
  # Falsifies: C7, C19
  #
  # REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  set -euo pipefail

  # 1. Workload - run benchmark matrix with skew, repeats, baselines, and noisy neighbors.
  # TODO: call tools/agentdb-bench/scripts wrappers with pinned profiles and seeds.

  # 2. Faults - add limping/hot tenant and cache/compaction pressure cells.
  # TODO: configure workload dimensions and collect system metrics.

  # 3. Oracle - validate report metadata, error-free runs, reproducibility, and tenant p99 fairness.
  # TODO: parse JSON/Markdown and enforce release-gate thresholds.
  ```

### Scenario S14: protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants

- **Falsifies if it FAILs:** C12, C15, C16, C17
- **Closes:** H21
- **Technique:** formal methods
- **Workload:** TLA+ models for export consumer claim/write/ack, routing epoch fencing, tablet replacement, split/merge cutover, and quorum commit/apply.
- **Faults:** message delay/drop/reorder, crash/restart, stale owner, duplicate retry, lease expiry, learner lag, movement completion race.
- **Oracle:** TLC invariants: no ack before sink accept, no committed write without quorum, no stale epoch mutation after handoff, no lost committed key during movement, no two active owners for same tablet epoch; liveness checked under explicit fairness assumptions.
- **Observability required:** model files, TLC configs, counterexample traces, and mapping doc from model state to AgentDB code/docs.
- **Exit criteria:** PASS if all models check within bounded state space and mapping review is complete; FAIL on counterexample not covered by implementation tests; INCONCLUSIVE if reductions omit a fault used by S7-S10.
- **Target test file:** `docs/formal/auto/S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.tla`
- **Skeleton language:** tla
- **Skeleton:**

  ```tla
  \* AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
  \* Scenario: S14 protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants
  \* Falsifies: C12, C15, C16, C17
  \*
  \* REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

  ---- MODULE S14_ProtocolModels ----
  EXTENDS Naturals, Sequences, FiniteSets

  VARIABLES state

  Init ==
      /\ state = [committed |-> {}, acked |-> {}, owners |-> {}, sink |-> {}]

  Next ==
      \* 1. Workload - model quorum writes, routing changes, movements, and export claims.
      \* TODO: add abstract actions for write, ack, movement, handoff, and retry.
      /\ UNCHANGED state

  Safety ==
      \* 3. Oracle - no ack before sink accept, no write without quorum, no stale owner mutation.
      \* TODO: encode invariants from the scenario prose.
      TRUE

  ====
  ```

## 7b. Coverage adequacy argument

| Claim | Threat model (how it could fail) | Scenarios that exercise the threats | Why these are sufficient |
|---|---|---|---|
| C1 | concurrent append races, restart after high-throughput writes, sequence reuse after tablet handoff | S1, S8, S9 | S1 stresses local session serialization and restart; S8 checks distributed acknowledged histories under partition; S9 targets movement windows where stale sequence state is most likely. |
| C2 | append-and-fetch omits own write, returns stale tail, or returns uncommitted data | S1, S2 | S1 checks every fused response against final committed history; S2 makes lost-ack ambiguity explicit so returned-but-lost events are caught. |
| C3 | retry double-applies, conflicting retry succeeds, SDK omits key, movement replays wrong seq | S2, S9, S11 | S2 targets server post-commit ambiguity, S9 covers movement retry storms, and S11 includes SDK/adapter retry compatibility. |
| C4 | restart loses base state, idempotency, content, records, lifecycle metadata, or upgrade cannot read it | S2, S3, S12 | S2 verifies retry state, S3 crashes IO boundaries, and S12 checks versioned fixture compatibility. |
| C5 | fork replay includes too many/few parent events or child seq collides | S4 | S4's generated fork+lifecycle model directly falsifies incorrect fork replay boundaries. |
| C6 | warm/cold/archive/restore loses or reorders events, or upgrade breaks tiered state | S4, S12 | S4 crashes lifecycle boundaries; S12 proves stored lifecycle forms remain readable after upgrade. |
| C7 | tenant data leaks through protocols/adapters/routing or noisy tenant starves others | S5, S10, S13 | S5 is a unified cross-surface isolation oracle; S10 adds routing/residency faults; S13 measures fairness isolation under load. |
| C8 | local read-after-write fails, global repair misses committed records, SDK drops token | S3, S6, S11 | S3 covers base/outbox atomicity, S6 injects lag/repair/GC, and S11 checks SDK/adapter parser and compatibility boundaries. |
| C9 | private/deleted/expired records remain searchable or policy hides allowed records | S6 | S6 models indexing policy, delete, expiry, legal hold, GC, and outbox lag together. |
| C10 | artifact metadata and bytes diverge, checksum mismatch commits, purge breaks metadata | S3 | S3 injects failures at metadata/content/checksum boundaries and verifies readback after restart. |
| C11 | subscribe/watch/export cursors skip, duplicate, leak scope, or break during movement | S5, S7, S9 | S5 covers tenant scope, S7 covers paging/prune/restart/sink ack, and S9 covers movement windows. |
| C12 | export emits uncommitted state, ack loses data, cursor state breaks after upgrade, protocol invariant is wrong | S3, S7, S12, S14 | S3 verifies ledger durability, S7 exercises connector crash/takeover, S12 covers upgrade, and S14 checks ack protocol invariants. |
| C13 | quota admits over-limit work, backpressure wrong, GC purges held state or leaves deleted data | S6 | S6 directly generates policy and concurrent operation sequences with model oracles. |
| C14 | Mongo accepts unsupported semantics, wrong auth, parser crash, or tenant leak | S5, S11 | S5 checks Mongo/native tenant equivalence; S11 fuzzes and property-checks wire/auth/unsupported command boundaries. |
| C15 | acknowledged write without quorum, divergent replicas, stale snapshot/movement state | S2, S8, S9, S14 | S8 is the distributed history oracle, S2 covers ambiguous commit, S9 covers reconfiguration, and S14 checks protocol-level safety. |
| C16 | wrong owner/cell/tablet mutation, stale epoch accepted, metadata migration/routing state unsafe | S9, S10, S12, S14 | S10 generated route model covers stale states; S9 movement cutover; S12 migration; S14 epoch-fencing proof. |
| C17 | cluster loses acknowledged writes or exceeds recovery safety during failover/restart/replacement/split/merge | S8, S9, S10, S14 | These cover quorum partitions, movement, readiness/control-plane faults, and protocol invariants. Passing supports only the documented evaluation harness. |
| C18 | health/admin/dashboard/CLI surfaces hide degraded state or accept unsafe config/input | S6, S8, S10, S11 | S8/S10 poll status during faults; S6 checks policy/index counters; S11 fuzzes config/admin parser boundaries. |
| C19 | benchmark result is not reproducible or aggregate masks tenant p99 collapse | S13 | S13 is dedicated to repeatability metadata, workload caveats, noisy-neighbor p99, and release-gate thresholds. |
| C20 | SDK route/idempotency/commit-token/SSE compatibility drifts | S2, S5, S11, S12 | S2 covers retry semantics, S5 cross-surface model equality, S11 parser/adapter properties, and S12 upgraded server compatibility. |
| C21 | adapters/examples reimplement or weaken runtime semantics | S5, S11, S12 | S5 checks adapter/native equivalence, S11 targets adapter conversion properties, and S12 covers compatibility across upgraded server state. |

## 7c. Residual uncertainty

| Uncertainty | Why uncovered | Why acceptable today | When to revisit |
|---|---|---|---|
| Production multi-node cluster SLA is not established. | Docs describe the distributed control plane as evaluation-oriented; scenarios use local process/compose faults, not a full production deployment. | Supported product emphasis is persistent single-node; cluster scenarios validate implemented evaluation paths, not a production SLA. | Before advertising cluster as supported or publishing RTO/RPO/SLOs. |
| Geo-distributed or real cloud AZ failure is not exercised. | Harness uses local AZ labels and local networking/storage. | No multi-region production claim is documented. | When cells map to real regions or cloud multi-AZ deployment becomes supported. |
| True power-loss/fsync media failure is approximated. | Crash/fail hooks do not replace block-level tools such as ALICE/CrashMonkey. | Application-level atomicity and restart recovery are the first release gate; filesystem-level power-loss claims are not currently precise. | Before claiming host-crash durability beyond the configured RocksDB/fsync contract. |
| Object-store consistency anomalies are only partially covered. | MinIO/local object-store tests do not reproduce every S3 latency, throttling, or consistency behavior. | Object-store metadata is evaluation, and object-store content is separate from hot append. | Before recommending object-store-backed runtime metadata for production hot writes. |
| Full hosted authentication/authorization is not covered. | The plan checks tenant isolation and Mongo PLAIN behavior, not an external identity provider or full authz product. | Docs frame tenant binding/user-info but do not define a complete hosted authz guarantee in this repo. | Before hosted authenticated deployment is release-gated. |
| Long-duration soak effects are not fully covered. | Scenarios run minutes to hours, not weeks. | This plan focuses on falsifying main correctness and release claims; separate soak can extend confidence. | Add 24-72h soak before high-retention or high-traffic production use. |
| Mixed-version cluster upgrade remains residual. | Cluster rolling upgrade is not documented as supported. | S12 covers single-node and metadata fixture upgrade; cluster mixed-version is intentionally not claimed. | Before cluster day-2 operations are documented as supported. |
| Formal models can abstract away implementation details. | S14 checks protocol models, not the code. | S8-S10 test implementation behavior; formal models complement rather than replace them. | Revisit whenever model state cannot be mapped cleanly to implementation state. |

## 7d. Confidence statement

If every scenario in section 7 passes against a named validation SHA, a reviewer should believe that AgentDB did not violate its documented single-node session, idempotency, persistence, tenant isolation, record visibility, lifecycle, artifact, export, Mongo-adapter, SDK, adapter, policy, and benchmark claims under the workloads and fault schedules in this plan. The reviewer should also believe that the evaluation cluster Raft/routing paths did not lose acknowledged writes, accept unsafe no-quorum progress, or hide degraded state under the local process, network, metadata, and movement faults tested here. That confidence is deliberately bounded: passing this plan would not prove production cluster support, multi-region behavior, arbitrary object-store failure semantics, full hosted authz, or long-soak reliability. The strongest defensible statement is that C1-C21 survived these stated threat models and oracles on the validation environment; it is not evidence that the claims cannot fail under untested schedules or larger deployments.
