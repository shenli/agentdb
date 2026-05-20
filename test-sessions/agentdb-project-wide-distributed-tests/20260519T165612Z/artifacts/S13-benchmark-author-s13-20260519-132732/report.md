# AgentDB Benchmark Report

## Run Configuration

- Run ID: `author-s13-20260519-132732`
- Backend: `agentdb-native-rocksdb`
- Workload: `MixedAgentLoop`
- Storage engine: `rocksdb`
- Content engine: `localfs`
- RocksDB data dir: `/home/lishen/work/agentdb-finish-s13-bench/state/author-s13-20260519-132732/rocksdb`
- Content dir: `/home/lishen/work/agentdb-finish-s13-bench/state/author-s13-20260519-132732/content`
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
| 1 | 228.80 | 2288 | 0 | 4.371 | 10.100 | 12.282 | 17.921 |
| 4 | 241.50 | 2415 | 0 | 15.400 | 44.705 | 73.444 | 140.533 |

## Operation Breakdown

| concurrency | operation | ops | errors | p50 ms | p95 ms | p99 ms | max ms |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | `append_fetch` | 1132 | 0 | 7.569 | 10.577 | 13.666 | 17.921 |
| 1 | `fetch_recent` | 557 | 0 | 0.919 | 1.164 | 1.694 | 2.088 |
| 1 | `record_write` | 192 | 0 | 0.697 | 0.797 | 0.919 | 1.633 |
| 1 | `record_search` | 166 | 0 | 0.759 | 0.920 | 1.536 | 2.421 |
| 1 | `fork` | 171 | 0 | 0.594 | 0.716 | 1.024 | 5.736 |
| 1 | `artifact` | 70 | 0 | 6.732 | 9.750 | 11.616 | 14.021 |
| 4 | `append_fetch` | 1213 | 0 | 27.780 | 52.868 | 86.066 | 140.533 |
| 4 | `fetch_recent` | 565 | 0 | 1.003 | 14.762 | 17.831 | 19.545 |
| 4 | `record_write` | 222 | 0 | 0.792 | 8.202 | 17.234 | 18.772 |
| 4 | `record_search` | 165 | 0 | 0.864 | 16.405 | 17.925 | 19.393 |
| 4 | `fork` | 178 | 0 | 0.703 | 13.872 | 16.503 | 18.015 |
| 4 | `artifact` | 72 | 0 | 28.855 | 55.253 | 86.183 | 87.233 |
