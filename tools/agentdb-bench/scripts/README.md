# AgentDB Bench Scripts

These scripts wrap the `agentdb-bench` Rust CLI for common benchmark runs:

- `agentdb-bench-local.sh` runs AgentDB with the native HTTP API.
- `agentdb-bench-agentdb-mongodb.sh` runs AgentDB through its MongoDB adapter.
- `agentdb-bench-mongodb.sh` and `agentdb-bench-postgres.sh` run baseline
  comparisons.
- `agentdb-bench-value-matrix.sh` and
  `agentdb-bench-longrun-value-matrix.sh` run comparison matrices.
- `agentdb-bench-repeat.sh`, `agentdb-bench-status.sh`,
  `agentdb-bench-finalize-matrix.sh`, and `agentdb-bench-prune.sh` manage
  repeat runs and output directories.
