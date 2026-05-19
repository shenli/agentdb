# AgentDB Benchmark Report

## Run Configuration

- Run ID: `author-s13-20260519-092506`
- Backend: `agentdb-native-rocksdb`
- Workload: `MixedAgentLoop`
- Storage engine: `rocksdb`
- Content engine: `localfs`
- RocksDB data dir: `/home/lishen/work/agentdb-cont-bench-s13/state/author-s13-20260519-092506/rocksdb`
- Content dir: `/home/lishen/work/agentdb-cont-bench-s13/state/author-s13-20260519-092506/content`
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
| 1 | 230.30 | 2303 | 0 | 4.262 | 10.090 | 12.736 | 26.519 |
| 4 | 242.80 | 2428 | 0 | 15.447 | 46.360 | 63.538 | 161.722 |

## Operation Breakdown

| concurrency | operation | ops | errors | p50 ms | p95 ms | p99 ms | max ms |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | `append_fetch` | 1143 | 0 | 7.328 | 10.653 | 14.859 | 26.519 |
| 1 | `fetch_recent` | 559 | 0 | 0.912 | 1.271 | 1.938 | 3.006 |
| 1 | `record_write` | 193 | 0 | 0.687 | 0.907 | 1.396 | 1.908 |
| 1 | `record_search` | 167 | 0 | 0.749 | 1.138 | 1.696 | 4.506 |
| 1 | `fork` | 171 | 0 | 0.585 | 0.752 | 0.876 | 1.085 |
| 1 | `artifact` | 70 | 0 | 6.443 | 10.242 | 12.467 | 12.900 |
| 4 | `append_fetch` | 1230 | 0 | 27.582 | 53.308 | 79.778 | 161.722 |
| 4 | `fetch_recent` | 564 | 0 | 1.002 | 14.289 | 18.591 | 27.390 |
| 4 | `record_write` | 219 | 0 | 0.788 | 15.269 | 17.902 | 18.755 |
| 4 | `record_search` | 168 | 0 | 0.847 | 14.315 | 18.033 | 20.459 |
| 4 | `fork` | 178 | 0 | 0.687 | 10.161 | 17.752 | 18.777 |
| 4 | `artifact` | 69 | 0 | 27.698 | 56.301 | 73.680 | 129.789 |
