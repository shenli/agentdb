# AgentDB Documentation

AgentDB is alpha software. Persistent single-node operation is the primary
release path; multi-node and Cell isolation work remain experimental. Start
with the guides below, and check each design's implementation status before
using it as a deployment contract.

## Start Here

- [Quickstart](quickstart.md): run AgentDB and store a replayable session.
- [Native API and SDKs](reference/api-sdk.md): application integration with
  Python, TypeScript, and HTTP.
- [Agent runtime manual](../LLMS.md): the runtime API in one document for LLM agents.
- [Product thesis](design/product-thesis.md): workload fit and non-goals.
- [Roadmap](product-roadmap.md): progress, remaining gaps, and open questions.

## API And Integration Contracts

- [Session event conventions](reference/session-event-conventions.md)
- [Sandbox snapshot contract](reference/sandbox-snapshot-contract.md)
- [Adapter readiness checklist](reference/adapter-readiness-checklist.md)

## Deploy And Operate

Start with [deployment](operations/deployment.md), the
[operations guide](operations/operations.md), and the
[single-node preflight checklist](operations/single-node-production-checklist.md).

| Task | Guides |
|---|---|
| Administer and maintain | [agentdbctl](operations/agentdbctl.md), [routine maintenance](operations/routine-maintenance.md), [single-node upgrades](operations/single-node-upgrade.md) |
| Back up and restore | [Backup verification](operations/backup-verification.md), [offline restore](operations/restore-recovery.md), [object-store recovery](operations/recovery.md) |
| Manage storage | [Storage maintenance](operations/storage-maintenance.md), [backpressure](operations/storage-backpressure.md), [disk-full recovery](operations/disk-full-recovery.md), [tiered storage](operations/tiered-storage.md) |
| Export events | [CDC and export](operations/cdc-export.md) |
| Investigate API errors | [Error runbooks](runbooks/README.md) |
| Measure performance | [Performance notes](operations/performance.md), [benchmark methodology](operations/agentdb-bench.md) |

Operational tooling lives outside this directory: [infra](../infra/) holds cloud
provisioning, [ops](../ops/) holds local validation and smoke/performance
operations, [tools/agentdb-bench](../tools/agentdb-bench/) holds the benchmark
suite, and [scripts](../scripts/) holds repository-wide entrypoints.

## Architecture And Design

The [design overview](design/design.md) records commitments and implementation
status; the [architecture map](design/architecture.md) describes crate
boundaries, request flow, and runtime storage.

- [Tablet storage engine](design/tablet-storage-engine.md): storage, replication,
  movement, and remaining hardening work.
- [Encryption at rest](design/encryption-at-rest.md): implemented surface,
  threat model, key lifecycle, and qualification requirements.
- [Clustered read semantics](design/read-semantics.md): consistency labels,
  barriers, and endpoint classification.
- [Tablet attachment evaluation](design/tablet-attachment-evaluation.md):
  opt-in SST movement evidence and deferred lazy attachment.

## Experimental Cluster Work

Read [cluster status](operations/cluster-status.md) first for current support
boundaries. The target topology and operational workflows below do not establish
production Cell-level failure isolation.

- [Cluster storage topology](design/cluster-storage-topology.md): Cells,
  placement, and failure domains.
- [Distributed safety hardening](design/distributed-safety-hardening.md):
  correctness requirements and validation work.
- [Cluster deployment](operations/cluster-deployment.md)
- [Cluster operations](operations/cluster-operations.md)
- [Cluster alert triage](operations/cluster-alerts.md)
- [Multi-process cluster smoke](operations/real-cluster-smoke.md)

## Proposals And Design Background

These documents contain target behavior and implementation history. Consult
linked API and operations guides for supported behavior today.

- [Lifecycle storage](design/lifecycle-storage.md): lifecycle design background;
  current operation is documented in [tiered storage](operations/tiered-storage.md).
- [Observability export](design/observability-export.md): export design and
  delivery notes; current operation is documented in [CDC/export](operations/cdc-export.md).
- [S3 committed-log archive](design/s3-committed-log-archive.md): proposed,
  unimplemented continuous log archiving.

## Contribute, Validate, And Release

- [Contribution guide](../CONTRIBUTING.md)
- [Local and hosted CI](operations/local-ci.md)
- [Testing evidence contract](testing/testing-evidence-contract.md)
- [Protocol model](formal/auto/S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.tla)
  and [TLC configuration](formal/auto/S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants.cfg)
- [Release checklist](release-checklist.md)
- [Third-party notices](../THIRD_PARTY_NOTICES.md)

## Historical Records

Archived notes preserve earlier results and implementation stages. Their
“current” statements, defaults, and plans are historical, not operational guidance.

- [Benchmark evaluation history](archive/benchmark-evaluation-history.md)
- [Encryption implementation history](archive/encryption-implementation-history.md)

When updating documentation, keep current instructions in the relevant guide,
label proposed behavior explicitly, and move superseded run logs or milestone
narratives into `archive/`. Preserve canonical guide paths and update links when
moving material.
