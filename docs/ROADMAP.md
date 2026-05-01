# AgentDB Roadmap

AgentDB is still pre-release, so this roadmap describes implementation state
without exposing internal milestone names as public product versions.

## Implemented Prototype Scope

- Tenant, agent, session, event, artifact, record, skill, policy, replay, fork,
  watch, context, and idempotency primitives in the Rust runtime.
- Native tenant-scoped HTTP API plus Python and TypeScript SDKs.
- Session-first dashboard mounted at `/dashboard`.
- Local single-node operation with in-memory dev mode, RocksDB runtime state,
  filesystem artifact content, Docker, Compose, and Helm evaluation assets.
- Framework integration proofs for LangGraph, OpenAI Agents SDK, MCP, and
  Vercel AI SDK style persistence.
- Metadata-backed routing planner and HTTP forwarding contracts for future
  multi-node operation.
- KMS/key availability signals and pluggable envelope adapters at storage
  boundaries.

## Near-Term Product Work

- Make single-node installation, backup, upgrade, and troubleshooting feel
  boring and reliable.
- Keep the SDKs as the primary developer surface; raw HTTP remains documented
  for operators, tests, and custom integrations.
- Add more end-to-end adapter examples that show durable recovery from real
  agent harness restarts.
- Improve dashboard inspection for replay, fork lineage, context provenance,
  artifact previews, and adapter convention warnings.

## Open Architecture Questions

- Whether hot data-plane calls need a lower-overhead binary transport after the
  HTTP API and SDK shape stabilize.
- How much multi-node behavior belongs in the open-source server versus a
  managed control plane.
- The exact operator story for online backup, compaction, schema migration, and
  artifact lifecycle verification.
- Which additional SDKs justify maintenance after Python and TypeScript.

