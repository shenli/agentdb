# AgentDB Docs

This directory separates current project documentation from historical planning
material.

## Current Docs

- [architecture/ARCHITECTURE.md](architecture/ARCHITECTURE.md) - current crate
  boundaries, request flow, storage, routing, and operational model
- [architecture/DESIGN.md](architecture/DESIGN.md) - product constraints,
  non-goals, and durable design decisions
- [architecture/agent-db.md](architecture/agent-db.md) - long-form product and
  systems design
- [architecture/TIERED_STORAGE_REDESIGN.md](architecture/TIERED_STORAGE_REDESIGN.md)
  - forward-looking redesign for real hot/cold/archive session storage
- [architecture/ARTIFACT_STORAGE_OPTIMIZATION.md](architecture/ARTIFACT_STORAGE_OPTIMIZATION.md)
  - forward-looking optimization plan for large artifact blobs and content backends
- [operations/DEPLOYMENT.md](operations/DEPLOYMENT.md) - supported deployment
  paths and operational checklist
- [operations/SINGLE_NODE.md](operations/SINGLE_NODE.md) - single-node commands,
  config, Docker, backup, and troubleshooting
- [operations/PERFORMANCE.md](operations/PERFORMANCE.md) - benchmark commands
  and current performance notes
- [reference/session-event-conventions.md](reference/session-event-conventions.md)
  - canonical event payload conventions
- [reference/adapter-readiness-checklist.md](reference/adapter-readiness-checklist.md)
  - adapter behavior and validation expectations
- [reference/api-sdk.md](reference/api-sdk.md) - native HTTP API and SDK surface
  reference
- [runbooks/README.md](runbooks/README.md) - operator runbooks for API error
  codes
- [roadmap.md](roadmap.md) - milestone direction and open work

## Planning Archive

- [superpowers/specs](superpowers/specs) - design specs
- [superpowers/plans](superpowers/plans) - implementation plans
- [superpowers/notes](superpowers/notes) - investigation notes and decision
  records
- [superpowers/plans/2026-05-02-tiered-storage-redesign-implementation.md](superpowers/plans/2026-05-02-tiered-storage-redesign-implementation.md)
  - phased implementation plan for the tiered-storage redesign
