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
- [design/storage-engine.md](design/storage-engine.md) - current
  storage engine and replicated tablet/split-LSM improvement plan
- [design/distributed-storage-cluster.md](design/distributed-storage-cluster.md)
  - next-step design for cells, AZ-aware placement, tenant blast radius, and
  distributed storage validation
- [agentdb-messaging-house.md](agentdb-messaging-house.md)
  - product positioning and value-prop messaging framework

## Run And Operate

- [operations/deployment.md](operations/deployment.md) - supported deployment
  paths, packaging modes, and current cluster status
- [operations/operations.md](operations/operations.md) - start/stop, backup,
  restore, upgrade, and troubleshooting for the supported single-node shape
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

- [reference/api-sdk.md](reference/api-sdk.md) - native HTTP API, SDK surface,
  and MongoDB compatibility subset
- [reference/mongodb-compatibility.md](reference/mongodb-compatibility.md) -
  MongoDB client tutorial, mapping rules, limitations, and AI coding-agent
  guidance
- [../examples/mongodb/README.md](../examples/mongodb/README.md) - copyable
  MongoDB client examples for Node and Python
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
- [design/tiered-storage-redesign.md](design/tiered-storage-redesign.md)
  - redesign for real hot/cold/archive session storage and large artifact
  content handling
- [../THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) - dependency license
  and release notice guidance
