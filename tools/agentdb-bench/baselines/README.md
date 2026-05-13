# Benchmark Baseline Fixtures

This directory is not a second benchmark runner. It contains support files used
by benchmark tools:

- `mongodb/`: Docker Compose infrastructure for the MongoDB event-log baseline.
- `postgres/`: Docker Compose infrastructure and SQL schema for the Postgres
  event-log baseline.
- `fixtures/`: generated/static fixture data for benchmark scenarios that need
  an external corpus.

The executable benchmark harness lives in `tools/agentdb-bench`. The wrapper
scripts under `tools/agentdb-bench/scripts/agentdb-bench-*.sh` wire that harness to the baseline
infrastructure here.
