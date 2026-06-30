# AgentDB Product Roadmap

AgentDB is still pre-release. This roadmap tracks product progress and gaps in
plain product terms rather than internal milestone names. For the current design
contract, see [`architecture/design.md`](architecture/design.md).
For the conceptual motivation and positioning, see
[`architecture/agentdb-product-thesis.md`](architecture/agentdb-product-thesis.md).

## Implemented

- Native runtime objects: tenants, agents, sessions, events, artifacts, records,
  skills, policies, tenant metadata, and record graph edges.
- Per-session append semantics: serialized append, contiguous session-local
  sequence numbers, append-and-fetch, idempotency, and backpressure reporting.
- Native tenant-scoped HTTP API plus maintained Python and TypeScript SDKs.
- Session-first dashboard mounted at `/dashboard`.
- Local single-node runtime modes: in-memory development and RocksDB-backed
  metadata with separate content backends for artifacts, segments, archives, and
  replay prefixes.
- Fork and replay: pointer forks, fork-aware replay, bounded ancestor reads,
  replay scan stats, and durable replay-prefix materialization exposed through
  core, HTTP, and SDK methods.
- Sandbox snapshot references: `SnapshotRef` / `SnapshotLayer` metadata on
  sessions, resume resolution across fork chains, dangling/expired ref fallback
  semantics, and the external sandbox-service contract.
- Watch and export feeds: session subscribe, tenant/agent/session `watch`,
  frontier cursors, fork-edge control items, durable `export_watch`, export
  backfill, and managed export consumers with claim/ack/rewind.
- Record retrieval substrate: durable base records, source refs, local/global
  indexes, commit-sequence visibility repair, metadata filters, record edges,
  and SDK commit-sequence propagation.
- Policy, lifecycle, and cleanup: quota/backpressure, warm/cold/archive
  lifecycle, archive restore, retention, legal hold, derivative-only artifact
  purge, and policy/GC workers.
- Credential boundary hardening: opaque event-level `credential_refs`, SDK raw
  secret payload validators, optional server-side raw-secret field rejection,
  and runtime tool-result sanitization.
- Optional MongoDB wire listener for a deliberately narrow driver/tool
  compatibility subset.
- Framework integration proofs for LangGraph, OpenAI Agents SDK, MCP, and
  Vercel AI SDK style persistence.
- Evaluation cluster components: metadata-backed routing, session locators,
  forwarding contracts, tablets, shard ranges, tablet movement, tablet
  checkpoints, and Raft-backed tablet write paths.
- KMS/key availability signals and pluggable envelope adapters at metadata and
  content boundaries.

## Partial / Needs Hardening

- **Single-node operations.** Config-first server flow, Docker/Compose/Helm
  examples, backup manifest/checkpoint APIs, and storage runbooks exist. The
  remaining work is a boring operator story for restore apply, upgrade,
  compaction, backup verification, disk-full recovery, and routine maintenance.
- **Dashboard.** A baseline dashboard exists. It still needs richer replay
  visualization, fork lineage, context provenance, artifact previews, policy
  state, export lag, and adapter convention warnings.
- **Observability exporters.** Durable export primitives are implemented.
  First-party ClickHouse/ClickStack/JSONL/Parquet exporters and their operational
  diagnostics need more polish and end-to-end examples.
- **Framework adapters.** Proofs exist. More adapters should show real recovery:
  crash/restart, idempotent retry, artifact recovery, fork, and watch/export
  consumption in one documented flow.
- **Distributed/tablet operation.** The codebase has substantial evaluation
  machinery, but the production-default posture is still single-node. The
  distributed story needs a clear OSS/managed boundary, stable operator
  procedures, failure-mode runbooks, and sustained smoke coverage.
- **Sandbox snapshot integration.** AgentDB owns refs and resolution; the
  external sandbox service still must implement capture, upload, copy-on-write
  fork, restore, byte retention, metrics, and dangling-ref handling.
- **Replay-prefix policy.** Manual materialization exists. Automatic triggers
  still need product policy: fork depth, replay scan count, byte threshold,
  explicit caller request, lifecycle sweep budget, or a combination.

## Deliberately Outside AgentDB

- Model runtime, agent harness, workflow scheduler, browser/sandbox/tool
  execution, and MCP server hosting.
- Credential storage and secret resolution. AgentDB stores refs; vault/control
  plane/broker components inject real credentials at egress.
- Memory derivation policy: summarization, extraction prompts, embedding spend,
  importance scoring, ranking, and "dreaming" jobs.
- Application OLTP state such as users, billing, orders, CRM objects, and
  product transactions.
- Full MongoDB compatibility, arbitrary MongoDB CRUD, MongoDB auth
  administration, or MongoDB-compatible indexing.

## Open Product Questions

- Whether hot data-plane calls need a lower-overhead binary transport after the
  HTTP API and SDK shape stabilize.
- How much distributed behavior should be part of the open-source server versus
  a managed control plane.
- Which operator tasks must be first-class before calling single-node production
  use boring: online restore apply, schema migration, compaction control, backup
  verification, artifact lifecycle verification, and upgrade rollback.
- Which additional SDKs justify maintenance after Python and TypeScript.
- How much built-in scoped retrieval should AgentDB own before dedicated
  search/vector systems are a better integration point.
