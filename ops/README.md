# Operations

This directory contains operational workflows for validating and running
AgentDB after a checkout already exists.

- `local-ci/` contains the local CI runner, dashboard, pruning helpers, and
  systemd user units.
- `smoke/` contains real-cluster and API smoke-test scripts.
- `perf/` contains low-level profiling and performance helper scripts.

Cloud provisioning assets belong under `infra/`. Developer tools with their own
code or benchmark harnesses belong under `tools/`.
