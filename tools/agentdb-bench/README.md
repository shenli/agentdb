# agentdb-bench

`agentdb-bench` prepares and runs synthetic agent-runtime workloads against
AgentDB. The first implemented backend is `agentdb-native-rocksdb`: native HTTP
API plus local RocksDB storage.

Start a local persistent AgentDB server:

```bash
cargo run -p agentdb-server -- serve \
  --bind 127.0.0.1:7890 \
  --data-dir /tmp/agentdb-bench-data \
  --content-dir /tmp/agentdb-bench-content
```

Prepare a dataset:

```bash
cargo run -p agentdb-bench -- prepare \
  --base-url http://127.0.0.1:7890 \
  --run-id smoke-local \
  --out bench-out \
  --profile smoke \
  --events-per-session 10 \
  --records-per-session 2 \
  --artifact-rate 0.2 \
  --artifact-large-mode inline
```

Run a workload:

```bash
cargo run -p agentdb-bench -- run \
  --base-url http://127.0.0.1:7890 \
  --run-id smoke-local \
  --out bench-out \
  --workload mixed-agent-loop \
  --concurrency 1,4,16 \
  --warmup 5s \
  --duration 30s
```

Outputs are written under `bench-out/runs/<run-id>/`:

- `dataset.json` records prepared tenant/agent/session IDs.
- `host-manifest.json` records hardware and OS information.
- `software-manifest.json` records toolchain and runtime versions.
- `report-*.json` contains machine-readable latency summaries.
- `report.md` contains a compact human-readable report.

