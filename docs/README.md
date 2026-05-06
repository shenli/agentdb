# AgentDB Docs

This directory separates current project documentation from historical planning
material. Start with the current docs unless you are looking for design history
or implementation plans.

## Start Here

- [architecture/agent-db.md](architecture/agent-db.md) - product thesis,
  workload requirements, positioning, and systems rationale
- [architecture/DESIGN.md](architecture/DESIGN.md) - durable design commitments,
  constraints, and non-goals
- [architecture/TRADEOFFS.md](architecture/TRADEOFFS.md) - what AgentDB is
  optimized for, where it should win, and where it should not compete
- [architecture/ARCHITECTURE.md](architecture/ARCHITECTURE.md) - current crate
  boundaries, request flow, storage, routing, and operational model
- [architecture/STORAGE_ENGINE.md](architecture/STORAGE_ENGINE.md) - current
  storage engine and replicated tablet/split-LSM improvement plan
- [agentdb-messaging-house.md](agentdb-messaging-house.md)
  - product positioning and value-prop messaging framework

## Run And Operate

- [operations/DEPLOYMENT.md](operations/DEPLOYMENT.md) - supported deployment
  paths, packaging modes, and current cluster status
- [operations/OPERATIONS.md](operations/OPERATIONS.md) - start/stop, backup,
  restore, upgrade, and troubleshooting for the supported single-node shape
- [operations/AGENTDBCTL.md](operations/AGENTDBCTL.md) - human and AI-agent
  friendly admin workflows
- [operations/PERFORMANCE.md](operations/PERFORMANCE.md) - benchmark commands
  and current performance notes
- [runbooks/README.md](runbooks/README.md) - operator runbooks for API error
  codes and `doc_url` links

## Cluster Work

- [operations/CLUSTER_STATUS.md](operations/CLUSTER_STATUS.md) - honest status
  of multi-node support and missing operator workflows today
- [operations/CLUSTER_DEPLOYMENT.md](operations/CLUSTER_DEPLOYMENT.md) -
  forward-looking target deployment model for a real AgentDB cluster
- [operations/CLUSTER_OPERATIONS.md](operations/CLUSTER_OPERATIONS.md) -
  forward-looking day-2 workflows for add/remove/replace/drain/upgrade

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
- [reference/adapter-readiness-checklist.md](reference/adapter-readiness-checklist.md)
  - adapter behavior and validation expectations

## Forward-Looking Designs

- [architecture/TIERED_STORAGE_REDESIGN.md](architecture/TIERED_STORAGE_REDESIGN.md)
  - redesign for real hot/cold/archive session storage
- [architecture/ARTIFACT_STORAGE_OPTIMIZATION.md](architecture/ARTIFACT_STORAGE_OPTIMIZATION.md)
  - optimization plan for large artifact blobs and content backends
- [roadmap.md](roadmap.md) - milestone direction and open work

## Planning Archive

- [superpowers/specs](superpowers/specs) - design specs
- [superpowers/plans](superpowers/plans) - implementation plans
- [superpowers/notes](superpowers/notes) - investigation notes and decision
  records
