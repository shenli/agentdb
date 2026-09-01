# AgentDB Product Roadmap

AgentDB is still pre-release. This roadmap tracks product progress and gaps in
plain product terms rather than internal milestone names. For the current design
contract, see [`design/design.md`](design/design.md).
For the conceptual motivation and positioning, see
[`design/product-thesis.md`](design/product-thesis.md).

## Implemented

- Native runtime objects: tenants, agents, sessions, events, artifacts, records,
  skills, policies, tenant metadata, and record graph edges.
- Per-session append semantics: serialized append, contiguous session-local
  sequence numbers, append-and-fetch, idempotency, and backpressure reporting.
- Native tenant-scoped HTTP API plus maintained Python and TypeScript SDKs;
  mandatory rotating installation bearer authentication on non-loopback binds,
  metadata-backed admin/read-only tokens, and separate internal Raft
  authentication.
- Session-first dashboard mounted at `/dashboard`, with assets embedded in the
  binary and bearer credentials kept only in page memory.
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
  frontier cursors, fork-edge control items, commit-ordered durable
  `export_watch`, export backfill, and managed export consumers with validated
  retained cursors plus claim/ack/rewind.
- Record retrieval substrate: durable base records, source refs, local/global
  indexes, commit-sequence visibility repair, metadata filters, record edges,
  and SDK commit-sequence propagation.
- Policy, lifecycle, and cleanup: quota/backpressure, warm/cold/archive
  lifecycle, archive restore, retention, legal hold, derivative-only artifact
  purge, and policy/GC workers.
- Single-node operations: config-first server flow, Docker/Compose/Helm
  examples, backup verification, offline restore/recovery, single-node
  upgrade, storage maintenance, disk-full recovery, routine maintenance,
  CDC/export, tiered-storage lifecycle runbooks, read-only artifact lifecycle
  verification, offline local restore apply, ClickHouse export schema rollback,
  production preflight checklist, distinct live/ready probes, local artifact
  failure cleanup/startup orphan reconciliation, and explicit startup hydration
  row/byte limits.
- Release packaging: Docker images and tagged binary archives include both
  `agentdb` and `agentdbctl`, so diagnostic, backup/restore, and export commands
  do not require a source checkout.
- Credential boundary hardening: opaque event-level `credential_refs`, SDK raw
  secret payload validators, optional server-side raw-secret field rejection,
  and runtime tool-result sanitization.
- Framework integration proofs for LangGraph, OpenAI Agents SDK, MCP, and
  Vercel AI SDK style persistence.
- Evaluation cluster components: metadata-backed routing, session locators,
  forwarding contracts, tablets, shard ranges, tablet movement, tablet
  checkpoints, and Raft-backed tablet write paths.
- KMS/key availability signals and pluggable envelope adapters at metadata and
  content boundaries.

## Partial / Needs Hardening

- **Authentication and authorization.** Static bearer auth and scoped admin
  tokens protect non-loopback deployments, but loopback development can remain
  open and the static token remains installation-wide. Per-principal tenant
  authorization, identity-provider integration, and audit attribution belong
  to future hosted or control-plane work.
- **Startup and content scale.** Single-node startup hydration is now bounded at
  512 MiB of serialized keys/values and explicit per-prefix row defaults, but
  decoded state can consume more memory and startup still scans each prefix
  before scanning it again to hydrate metadata/in-memory record indexes. Local
  orphan reconciliation performs a full listing bounded to 500,000 entries by
  default and removes crash-left temporary files; object-store content lacks
  that listing primitive entirely.
- **Dashboard.** A baseline dashboard exists. It still needs richer replay
  visualization, fork lineage, context provenance, artifact previews, policy
  state, export lag, and adapter convention warnings.
- **Observability exporters.** Durable export primitives are implemented for
  single-tablet runtimes. Snapshot-routed clusters fail export closed until a
  cross-tablet merge frontier and cursor version exist. First-party
  ClickHouse/ClickStack/JSONL/Parquet operator guidance exists, but
  sink-specific diagnostics and examples still need more polish.
- **Framework adapters.** Proofs exist. More adapters should show real recovery:
  crash/restart, idempotent retry, artifact recovery, fork, and watch/export
  consumption in one documented flow.
- **CodeMate local engineering review.** Product research points to reviewer
  trust, verification cost, repeated agent mistakes, and uncaptured best
  practices as stronger wedges than generic agent memory. The proposed demo is a
  third-party local web app that imports coding-agent session history into
  AgentDB, then renders PR review packets, identifies common issues, and
  suggests project guidance or skill updates. See
  [`design/codemate-product-note.md`](design/codemate-product-note.md).
- **Distributed/tablet operation.** The codebase has substantial evaluation
  machinery, but the production-default posture is still single-node. The
  distributed story needs a clear OSS/managed boundary, stable operator
  procedures, failure-mode runbooks, and sustained smoke coverage.
- **S3-native committed-log recovery.** AgentDB has object-store content,
  periodic whole-state recovery manifests, and retained Raft snapshot objects,
  but it does not continuously archive committed tablet-log ranges by Raft
  index. Durable upload intents, immutable log segments, regular per-tablet S3
  checkpoints, archive-aware compaction, and checkpoint-plus-log bucket restore
  remain proposed in
  [`design/s3-committed-log-archive.md`](design/s3-committed-log-archive.md).
- **Encryption at rest and BYOK.** KMS availability, key-ID resolution, crypto
  adapter seams, and envelope framing exist, but the default adapters preserve
  plaintext. Production authenticated encryption, durable-path coverage, a
  wrapped-DEK catalog and bounded cache, rotation and revocation workflows,
  backup/restore validation, and benchmark qualification remain. See
  [`design/encryption-at-rest.md`](design/encryption-at-rest.md).
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
- Which single-node operator workflows should become automated product
  features instead of documented procedures: online restore apply, schema
  migration for core runtime metadata, lifecycle dashboard visibility, and
  online binary/data upgrade rollback.
- Which additional SDKs justify maintenance after Python and TypeScript.
- How much built-in scoped retrieval should AgentDB own before dedicated
  search/vector systems are a better integration point.
