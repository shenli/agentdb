# AgentDB Benchmark Report

## Run Configuration

- Run ID: `author-s13-20260518-204140`
- Backend: `agentdb-native-rocksdb`
- Workload: `MixedAgentLoop`
- Storage engine: `rocksdb`
- Content engine: `localfs`
- RocksDB data dir: `/home/lishen/work/agentdb-author-bench-s13/state/author-s13-20260518-204140/rocksdb`
- Content dir: `/home/lishen/work/agentdb-author-bench-s13/state/author-s13-20260518-204140/content`
- Dataset profile: `smoke`
- Tenants: `1`
- Agents per tenant: `1`
- Sessions per agent: `2`
- Events per session: `5`
- Total sessions: `2`
- Total prepared events: `10`
- Total prepared records: `4`
- Estimated prepared event payload bytes: `2560`
- Artifact mode: `Reference`
- Session selection: `uniform`
- Zipf exponent: `1.2`
- Content-engine artifacts: `0` artifacts, `0` bytes
- External artifact references: `1` references, `1048576` bytes
- Host: `li-dev`
- Kernel: `Linux li-dev 6.8.0-90-generic #91~22.04.1-Ubuntu SMP PREEMPT_DYNAMIC Thu Nov 20 15:20:45 UTC 2 x86_64 x86_64 x86_64 GNU/Linux`

## Results

| concurrency | ops/s | ops | errors | p50 ms | p95 ms | p99 ms | max ms |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 379.33 | 1138 | 0 | 2.797 | 5.500 | 5.779 | 8.086 |

## Operation Breakdown

| concurrency | operation | ops | errors | p50 ms | p95 ms | p99 ms | max ms |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | `append_fetch` | 593 | 0 | 4.121 | 5.668 | 6.152 | 8.086 |
| 1 | `fetch_recent` | 272 | 0 | 0.814 | 1.110 | 2.500 | 2.617 |
| 1 | `record_write` | 83 | 0 | 0.595 | 0.868 | 1.602 | 2.189 |
| 1 | `record_search` | 71 | 0 | 0.654 | 0.758 | 0.795 | 0.931 |
| 1 | `fork` | 78 | 0 | 0.486 | 0.561 | 0.583 | 0.612 |
| 1 | `artifact` | 41 | 0 | 3.978 | 5.082 | 6.577 | 6.577 |
