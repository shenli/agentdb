# AgentDB V1 → V2 Gap Analysis

Status: draft

Date: 2026-04-28

Inputs:

- [`agent-runtime-database.md`](agent-runtime-database.md) — V2 architecture
- [`minimal-single-machine-implementation.md`](minimal-single-machine-implementation.md) — V2 single-machine prototype build guide
- Current `main` branch as of M28 v1 cleanup

This document compares the V1 prototype to the V2 spec. It does not propose
ordering. The implementation plan is a separate document.

## 1. Headline

V1 validated several V2 keep-list primitives — ordered session history,
per-session `seq`, append-and-fetch, copy-free fork, durable SSE subscribe,
warm/cold/archive lifecycle, tenant-first storage keys. The V2 runtime model is
broader: it requires first-class Agent, Artifact, Searchable Record, Skill, and
Policy objects; typed global IDs; explicit indexing policy; idempotency on the
runtime path; and a clean-break `/api` API surface.

V2 is a clean break. Under V2:

- the Mongo wire facade (`agentdb-wire`, 2279 LoC) is deleted,
- the Go SDK is deleted,
- old `/api/...` route shapes are replaced by the new native `/api/...` route
  shapes,
- typed prefixed IDs (`ten_`, `agt_`, `ses_`, `evt_`, `art_`, `rec_`, `skl_`,
  `pol_`) become the wire format.

V1's storage layer, lifecycle tiering, fork lineage, and SSE subscribe survive
mostly intact and should be reused, not rewritten.

## 2. Naming clarification

Recent commits (M27/M28) refer to "v2 routes." Those routes are V1-shape routes
with auth-derived tenant, not the V2-spec runtime. To avoid confusion in the
plan and PRs:

- "auth-derived V1" or just "V1 routes" — old `/api/...` paths
- "V2 runtime" or "V2 spec" — the design in `docs/v2_design/`
- "native runtime routes" — `/api/tenants/{tenant_id}/...` paths from §8 of the
  impl guide

## 3. Keep-list (§11.1) — V2 says these V1 ideas remain right

| V2 keep-list item | V1 location | Decision |
|---|---|---|
| Session as core execution boundary | `agentdb-core/src/types.rs:65-85` | Keep, add agent ownership |
| Per-session ordered `seq` | `Event.seq: u64` | Keep |
| Append-and-fetch | `/api/sessions/:id/append_and_fetch` | Keep semantics, move to the new `/api/...` shape |
| Copy-free fork | `Session.forked_from: Option<ForkRef>` | Keep, expose at V2 path |
| Durable + ephemeral subscribe | SSE in `agentdb-api/src/subscribe.rs`, broadcast channels | Keep, expose at V2 path |
| Warm/cold/archive lifecycle | `agentdb-core/src/tiering.rs` + segment cold storage | Keep, surface as policy-driven |
| Object storage for cold + large | `AGENTDB_OBJECT_STORE` (local/s3) | Keep as artifact content backend; artifact metadata stays with runtime metadata/events |
| Tenant-aware admission | `agentdb-core/src/tenant_quota.rs` (max_qps, burst) | Keep, extend per-agent and per-resource |

## 4. Change-list (§11.2) — V2 says these V1 choices should change

| V2 change | V1 reality | Required change |
|---|---|---|
| Agent as only metadata | True — `Event::actor_id` free string only | Build first-class Agent: `agt_*` ID, registration, policies, skill bindings, session ownership |
| Events as only main data model | True — no Artifact / Record / Skill / Policy primitives | Add Artifact, Searchable Record, Skill, Policy as runtime peers connected by source refs |
| Large payloads inline | True — `Event.payload: serde_json::Value` always inline | Enforce inline vs artifact thresholds; add `artifact_refs: []` |
| Last-N context only | True — only `fetch_recent` | Add `/api/.../context` with byte/token budgets and inclusion filters |
| Recursive indexing | True — `InMemoryInverted` indexes per-path across full payload | Add explicit indexing policy: `no_indexing`/`metadata_only`/`summary_only`/`selected_fields[]`; vector and artifact-derived allowed/denied |
| Operator-first dashboard | True — `agentdb-dashboard` is an 11-line placeholder | Session-first UI: timeline, fork tree, artifacts, records, skills, replay |
| Document-store adapters as primary API | True — `agentdb-wire` (2279 LoC) is the Mongo wire facade | Delete `agentdb-wire`. The native runtime `/api` surface is the only surface |

## 5. First-class object coverage (§5)

| Object | V1 coverage | Action |
|---|---|---|
| Tenant | Present (`agentdb-core/src/types.rs:44-51`, `agentdb-meta/src/tenants.rs`) | Keep; add policy attachment, retention/residency hooks |
| Agent | None — `actor_id` is a free string on events | Build whole subsystem |
| Session/Run | Full (`types.rs:65-85`) | Keep; add `agent_id` ownership, schema_version |
| Event | No `event_id` global, no `schema_version`, no `artifact_refs`, no `source_refs`, no `idempotency_key` | Extend envelope per impl guide §6.1 |
| Artifact | None | Build whole subsystem |
| Searchable Record | None — events are the only searchable unit | Build whole subsystem |
| Skill | None | Build whole subsystem |
| Policy | Only `TenantQuota` (max_qps, burst) | Build full policy model |
| Subscription | SSE durable per-session + tenant-keyed | Keep; add changefeed generalization |

## 6. ID model (§13)

V1 uses opaque external strings on the wire (e.g., `s-123`) and `u64` internally.
V2 mandates UUIDv7/ULID with type prefixes for all eight runtime objects.

Breaking changes:

- Wire IDs: opaque → typed prefix
- Internal u64 IDs may persist inside storage key encoding for compactness, but
  must be paired with prefixed external IDs in API responses
- Aliases (tenant external_key, agent alias, session alias) remain as
  human-friendly secondary keys, scoped per impl guide §13

V1's tenant-first 31-byte fixed-width key encoding (`Namespace + box_id +
tenant_id + session_id + branch_id + seq`) is compatible with prefix-typed
external IDs at the API layer; no storage layout change is forced by ID
prefixing alone.

## 7. Storage capabilities (§10) coverage

| §10 capability | V1 status | Gap |
|---|---|---|
| Ordered streams (append/scan/tail/fork) | Present (`KvStore` + RocksDB/SlateDB; tenant-first keys) | None |
| Transactional metadata (CAS, txn) | Partial — no exposed CAS API | Add CAS / multi-op transaction helpers |
| Artifact store | None | Build artifact metadata in the metadata/event store plus tenant-first content backend refs; local content backend may use `artifact_content/{tenant_id}/{artifact_id}/blob` with sha256 verify + tmp-then-rename |
| Indexing hooks (text/vector) | Auto on whole payload | Add scope/policy parameter; vector accepts caller-supplied embeddings |
| Snapshot reads / MVCC | Index snapshots only | Add stream/metadata snapshot reads for fork+replay+cross-object consistency |
| Lifecycle primitives (seal/move/archive/hydrate) | Present (segment-based) | Surface as policy-driven actions |
| Changefeed | Subscribe is the changefeed today | Generalize: `watch(stream_id | tenant_id | agent_id, from_cursor)` |

## 8. Indexing (§9.4, §10.4, §12.7)

V1 indexes recursively across the full event payload via in-memory BM25
(`agentdb-index/src/text.rs`). V2 makes indexing explicit policy with these
modes:

- `no_indexing`
- `metadata_only`
- `selected_fields`
- `summary_only`
- vector indexing allowed / denied
- artifact-derived indexing allowed / denied
- region-bound indexing

Plus: deletion or redaction of a source object must invalidate derived index
entries unless derivative-only is explicitly allowed. V1 has no such hook.

## 9. Policy model (§17)

V1 enforces only per-tenant rate limit (max_qps, burst). V2 needs:

- quota / admission (per agent, per session, per resource)
- lifecycle (cold_after_minutes, archive_after_days,
  delete_raw_artifacts_after_days)
- retention / delete (raw retention windows, derivative-only mode)
- indexing (modes from §8 above)
- artifact size (inline_event_max_bytes, artifact_required_after_bytes,
  max_artifact_bytes)
- approval / human-in-the-loop
- tool / skill access
- tenant data access (residency, region routing)

Precedence (impl guide §11): agent override > tenant > built-in defaults.

Policy versioning is required. Session events should record the
`policy_version` evaluated at append time so replay can reconstruct context.

## 10. Lifecycle (§18)

V1: Warm → Cold → Archive. V2 expands:

- Active (currently appending) — distinct from Warm
- Derivative-only (compliance/cost mode: raw artifacts deleted, derivatives
  retained, provenance preserved)

Existing `tiering.rs` primitives are reusable. Additions are policy-driven
triggers and derivative-only plumbing through Artifact metadata.

## 11.bis Deletion model (cross-cutting)

V2 spec covers retention policy (§17) and artifact lifecycle/redaction (§12.8)
but does not explicitly specify the synchronous-delete vs GC contract. Plan
decision: **soft-delete + GC** for every runtime object. Synchronous delete
writes a `deleted_at_ms` marker; a separate GC worker (V2.4) removes content
backend bytes, index entries, and the metadata record itself once the
retention window elapses. Artifacts also carry `raw_purged_at_ms` for
derivative-only mode. Reads filter `deleted_at_ms IS NULL` by default.
Tenant-first prefix scans make range-delete a single tombstone-marker write,
expanded by GC.

V1 reality: no soft-delete model — no `deleted_at_ms` field on session, event,
or any other type; no GC worker; no content-backend orphan cleanup. Building
this in V2 is foundational, not optional, because:

- replay/audit invariants require the existence record to outlive the data,
- derivative-only mode (V2 §12.8) requires the metadata/content split to be
  destruct-able independently,
- multi-store consistency (metadata + content backend + indexes) requires a
  coordinated GC pass,
- range-delete via prefix is powerful and dangerous; markers add a recovery
  window,
- changefeed subscribers must see deletes as events.

The `deleted_at_ms` field is added in V2.0 so subsequent milestones don't
retrofit it. The GC worker lives in V2.4 (Policy + Lifecycle + GC).

## 11. Idempotency (impl guide §9)

V1: not implemented on the runtime path. M27 session-locator INSERT OR IGNORE is
idempotent at the meta layer only.

V2 P0 requires `Idempotency-Key` on:

- event append, append-and-fetch
- artifact put, redact, delete, lifecycle transition
- searchable record write/update/delete
- skill register/version create/bind mutation
- policy create/version update/attachment mutation

Scope: `(tenant_id, operation, target, key)`. Replay returns original result or
structured conflict if payload differs. Storage path: impl guide §7 specifies
`idempotency/{tenant_id}/{operation}/{target}/{key} -> result_hash + result`.

## 12. Subscription (§19)

V1 SSE matches V2 §19 except:

- No explicit consumer offset / ack protocol — current is at-least-once
  client-tracked (client stores last delivered seq, reconnects with
  `from_seq=N+1`). V2 §19 explicitly accepts this and warns against claiming
  exactly-once. Keep as-is.
- Heartbeat comments on idle: present in V1.
- Tenant-wide changefeed (`watch(tenant_id, ...)`) is partial via
  `/api/subscribe`. Generalize to §10.7 form.

## 13. Adapter strategy (§16.1, §16.2)

V1 has no framework adapters. The `agentdb-wire` MongoDB facade does not count
and is deleted under V2.

V2 P1 priorities:

1. LangGraph adapter (checkpoints, store, vector search)
2. OpenAI Agents SDK session adapter
3. MCP server exposing AgentDB sessions/artifacts/search/replay
4. Vercel AI SDK template

The validation rule from §16.2: "A production-quality framework adapter should
be mostly mapping code, not infrastructure code." If the adapter has to invent
ordering, idempotency, fork lineage, artifact offload, durable subscribe,
search, lifecycle, or index policy — AgentDB is missing a primitive.

## 14. SDK posture (impl guide §3)

| SDK | V1 | V2 disposition |
|---|---|---|
| Python | `clients/python/`, migrated to v2 routes (M27) | Rewrite for clean-break `/api` surface; remains P0 |
| TypeScript | `clients/typescript/`, migrated to v2 routes (M27) | Rewrite for clean-break `/api` surface; P1 |
| Go | `clients/go/`, present | **Delete** — V2 §22 non-goal |

Go callers may use the HTTP API directly; no maintained Go client.

## 15. Wire surface

Old V1 routes (`/api/...`) are auth-derived but not V2-shape. Native runtime
routes from impl guide §8:

```text
/api/tenants/{tenant_id}
/api/tenants/{tenant_id}/agents
/api/tenants/{tenant_id}/agents/{agent_id}
/api/tenants/{tenant_id}/agents/{agent_id}/sessions
/api/tenants/{tenant_id}/sessions/{session_id}
/api/tenants/{tenant_id}/sessions/{session_id}/events
/api/tenants/{tenant_id}/sessions/{session_id}/append-and-fetch
/api/tenants/{tenant_id}/sessions/{session_id}/replay
/api/tenants/{tenant_id}/sessions/{session_id}/fork
/api/tenants/{tenant_id}/sessions/{session_id}/subscribe       (SSE)
/api/tenants/{tenant_id}/sessions/{session_id}/context
/api/tenants/{tenant_id}/artifacts
/api/tenants/{tenant_id}/artifacts/{artifact_id}
/api/tenants/{tenant_id}/artifacts/{artifact_id}/blob
/api/tenants/{tenant_id}/records
/api/tenants/{tenant_id}/records/search
/api/tenants/{tenant_id}/records/search                       (POST for vector)
/api/tenants/{tenant_id}/records/{record_id}
/api/tenants/{tenant_id}/skills
/api/tenants/{tenant_id}/agents/{agent_id}/skills/bind
/api/tenants/{tenant_id}/agents/{agent_id}/skills
```

Decision: build the new native `/api/...` shape directly. No `/v2` path is
needed because V2 is a clean break.

## 16. Crate-by-crate disposition

| Crate | V2 action |
|---|---|
| `agentdb-core` | Extend. Add `agent`, `artifact`, `record`, `skill`, `policy`, `id_prefix` modules. Extend Event envelope. |
| `agentdb-storage` | Keep. KV trait stable. Add artifact content backend trait with filesystem and object-store impls (storage engine decides inline vs spill), CAS API, generalized changefeed. |
| `agentdb-index` | Extend. Add indexing-policy parameter. Vector accepts caller-supplied embedding. Add invalidation hook for source deletion. |
| `agentdb-api` | Replace old handlers with the new native `/api/*` routes + DTOs. |
| `agentdb-wire` | **Delete.** V2 explicitly drops the MongoDB facade. |
| `agentdb-meta` | Extend. Add tables for Agent registration, Skill registry, Policy attachments, Idempotency results, Artifact metadata. |
| `agentdb-dashboard` | Build. Session-first UI per V2 spec. |
| `bin/agentdb-server` | No `--v2` flag. Serve the native runtime API directly. |
| `clients/go` | **Delete.** |
| `clients/python` | Rewrite for `/api`. |
| `clients/typescript` | Rewrite for `/api`. |

## 17. Decisions to confirm before planning

1. **Cutover model.** Confirmed clean break. Build the new native `/api/...`
   shape directly; do not add `/v2` routes or a `--v2` flag.
2. **`agentdb-wire` deletion.** V2 spec confirms this. Confirm no external
   user is depending on the Mongo facade. Recommendation: delete in the V2
   cutover milestone.
3. **`clients/go` deletion.** V2 spec confirms. Recommendation: delete same
   milestone as `agentdb-wire`.
4. **Storage backend continuity.** V2 spec is permissive (RocksDB / SlateDB /
   object store / Postgres). Recommendation: keep current backends.
   Postgres remains the meta plane only; RocksDB+SlateDB+object store carry
   runtime data.
5. **Policy persistence.** Where do policy records live — `agentdb-meta`
   (Postgres) or in `agentdb-storage` keyspace? Recommendation: meta for
   long-lived attachments and versioning, storage keyspace for runtime-cached
   policy state to keep the hot path fast (impl guide §12.2).

## 18. What does not change

- Tenant-first key encoding stays.
- RocksDB and SlateDB backends stay.
- Object store integration for cold/archive stays.
- SSE subscribe protocol stays (at-least-once + client dedupe by `(session_id,
  seq)`).
- Auth model (Basic, JWT, admin bearer) stays — auth derives tenant; V2 adds
  agent scope on top of authenticated tenant.
- Postgres meta plane stays for users, tokens, audit, session locators, tenant
  quotas.

## 19. Bottom line

V1's storage and durability primitives are V2-ready. The runtime model is not.
V2 is a runtime-layer rewrite on top of mostly-unchanged storage, plus a wire
break (typed IDs and the new native `/api/...` shape), plus deletion of two non-V2-aligned
subsystems (`agentdb-wire`, `clients/go`).
