# AgentDB Docs

This directory contains the public AgentDB documentation. Start with the current
docs below; historical planning artifacts are intentionally not part of the main
public docs path.

## Start Here

- [quickstart.md](quickstart.md) - shortest path from checkout to a stored and
  replayable agent session
- [design/product-thesis.md](design/product-thesis.md) - product thesis,
  workload requirements, positioning, and systems rationale
- [design/design.md](design/design.md) - durable design commitments,
  implementation status, constraints, and non-goals
- [product-roadmap.md](product-roadmap.md) - product progress, remaining gaps,
  deliberately out-of-scope work, and open questions
- [design/architecture.md](design/architecture.md) - current crate
  boundaries, request flow, storage, routing, and operational model
- [design/tablet-storage-engine.md](design/tablet-storage-engine.md) - hot
  tablet storage engine, Raft commit path, and split-LSM improvement plan
- [design/cluster-storage-topology.md](design/cluster-storage-topology.md)
  - next-step design for cells, AZ-aware placement, tenant blast radius, and
  distributed storage validation
- [design/distributed-safety-hardening.md](design/distributed-safety-hardening.md)
  - prioritized plan for read consistency, partition testing, cluster/store
  identity, manifest safety, and bounded tenant backpressure
- [agentdb-messaging-house.md](agentdb-messaging-house.md)
  - product positioning and value-prop messaging framework
- [design/codemate-product-note.md](design/codemate-product-note.md)
  - research findings and local-first demo plan for CodeMate, a developer app
  that uses AgentDB to review agent sessions and distill best practices
- [design/codemate-prd.md](design/codemate-prd.md)
  - simple PRD for CodeMate as a third-party local web app

## Run And Operate

- [operations/deployment.md](operations/deployment.md) - supported deployment
  paths, packaging modes, and current cluster status
- [operations/operations.md](operations/operations.md) - start/stop, backup,
  restore, upgrade, and troubleshooting for the supported single-node shape
- [operations/single-node-production-checklist.md](operations/single-node-production-checklist.md)
  - preflight gate before exposing a persistent single node
- [operations/recovery.md](operations/recovery.md) - object storage as the
  recovery root: recovery manifests, `recovery_lag_ms`, and
  `agentdb restore-from-bucket`.
- [operations/backup-verification.md](operations/backup-verification.md) -
  backup manifest validation, tablet checkpoint checks, scratch install drills,
  and recovery boundaries
- [operations/restore-recovery.md](operations/restore-recovery.md) - offline
  single-node restore and recovery procedure
- [operations/single-node-upgrade.md](operations/single-node-upgrade.md) -
  single-node upgrade, verification, and rollback procedure
- [operations/storage-maintenance.md](operations/storage-maintenance.md) -
  storage inspection, compaction, and sizing checks
- [operations/disk-full-recovery.md](operations/disk-full-recovery.md) -
  response procedure for full runtime or backup volumes
- [operations/routine-maintenance.md](operations/routine-maintenance.md) -
  daily, weekly, and pre-maintenance checklist
- [operations/cdc-export.md](operations/cdc-export.md) - CDC/export operation,
  ClickHouse, JSONL/Parquet archives, cursors, and consumer recovery
- [operations/tiered-storage.md](operations/tiered-storage.md) -
  warm/cold/archive/derivative-only lifecycle operation and verification
- [operations/agentdbctl.md](operations/agentdbctl.md) - human and AI-agent
  friendly admin workflows
- [operations/performance.md](operations/performance.md) - benchmark commands
  and current performance notes
- [operations/agentdb-bench.md](operations/agentdb-bench.md) - design and
  testing plan for comparing AgentDB with MongoDB and Postgres on agent-runtime
  workloads
- [operations/local-ci.md](operations/local-ci.md) - local validation profiles
  used instead of always-on hosted CI
- [runbooks/README.md](runbooks/README.md) - operator runbooks for API error
  codes and `doc_url` links

Operational assets live outside `docs/`: cloud provisioning is under
`infra/`, local CI and smoke/perf operations are under `ops/`, benchmark tooling
is under `tools/agentdb-bench/`, and top-level `scripts/` is reserved for
small repo-wide entrypoints.

## Cluster Work

- [operations/cluster-status.md](operations/cluster-status.md) - honest status
  of multi-node support and missing operator workflows today
- [operations/cluster-deployment.md](operations/cluster-deployment.md) -
  forward-looking target deployment model for a real AgentDB cluster
- [operations/cluster-operations.md](operations/cluster-operations.md) -
  forward-looking day-2 workflows for add/remove/replace/drain/upgrade
- [operations/cluster-alerts.md](operations/cluster-alerts.md) - evaluation
  cluster alert triage
- [operations/real-cluster-smoke.md](operations/real-cluster-smoke.md) -
  local multi-process Raft smoke harness

## Reference

- [reference/api-sdk.md](reference/api-sdk.md) - native HTTP API and SDK surface
- [reference/session-event-conventions.md](reference/session-event-conventions.md)
  - canonical event payload conventions
- [reference/sandbox-snapshot-contract.md](reference/sandbox-snapshot-contract.md)
  - contract between AgentDB snapshot refs and an external sandbox service
- [reference/adapter-readiness-checklist.md](reference/adapter-readiness-checklist.md)
  - adapter behavior and validation expectations

## Validation And Release

- [stability-test-plan.md](stability-test-plan.md) - deterministic, hardening,
  and seeded-chaos validation plan
- [stability-findings-2026-05.md](stability-findings-2026-05.md) - May 2026
  stability findings and mitigation status
- [release-checklist.md](release-checklist.md) - public-release and tag
  readiness checklist

## Forward-Looking Designs

- [design/observability-export.md](design/observability-export.md)
  - design for exporting AgentDB runtime truth into observability and analytics
  sinks
- [design/lifecycle-storage.md](design/lifecycle-storage.md)
  - warm/cold/archive session storage semantics and large artifact
  content handling
- [../THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) - dependency license
  and release notice guidance
