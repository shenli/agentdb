# Infrastructure

This directory contains assets for provisioning external infrastructure that can
run AgentDB.

- `aws/terraform/` provisions demo AWS infrastructure. Set `node_count` and
  `subnet_ids` to create multiple nodes across AZs for cluster experiments.
- `aws/scripts/` contains helper scripts for bootstrapping and smoke-testing the
  AWS demo deployment.

AWS bootstrap templates emit the bounded `grouped_rocksdb` tablet-store layout
by default. Before testing larger node counts, size
`storage.tablet.max_open_domains`, `domain_write_buffer_size_mib`, and
`domain_max_write_buffers` against the EC2 instance memory budget.
The Terraform variables `tablet_store_tablets_per_domain` and
`tablet_store_max_open_domains` make it easy to switch between the default
grouped layout and high-isolation `tablets_per_domain = 1` backup/restore test
mode without editing the generated `agentdb.toml`.
For cluster-mode experiments, set `deployment_mode = "cluster"`, provide a
shared Postgres `meta_url`, set `internal_auth_token`, and use `cell_id` plus
`capacity_class` to label the nodes. The generated config binds AgentDB to the
instance private IP so Raft and movement RPC metadata does not advertise
`0.0.0.0`.
After deployment, `agentdbctl status` reports the configured memtable budget,
open tablet-store domain count, and pending compaction bytes so AWS experiments
can verify domain sizing before increasing isolation.

The AWS Terraform template can also write optional ClickHouse export settings.
Set `clickhouse_url` and `clickhouse_export_tenant_id` after creating a tenant;
`infra/aws/scripts/deploy-demo.sh` will build `agentdbctl`, initialize the
ClickHouse schema with `agentdbctl export clickhouse migrate --apply`, and
start `agentdbctl export clickhouse run --backfill --tail` with a managed
consumer lease. Set `clickhouse_export_mode` to
`native,otel` to write the native ClickHouse tables plus
`agentdb_otel_logs`/`agentdb_otel_traces` projections and
ClickStack-compatible views. Point HyperDX sources at
`agentdb_clickstack_logs` and `agentdb_clickstack_traces`, or at the `otel_*`
aliases in a dedicated AgentDB observability database. Run the same deploy flow
on multiple nodes with the same `clickhouse_export_consumer` to get
active/passive exporter HA; each node derives a distinct lease owner from its
hostname.

The same template can also start a bounded JSONL archive worker. Set
`jsonl_export_tenant_id` and `jsonl_export_uri`, for example
`s3://<bucket>/agentdb-exports/tenants/<tenant>/events.jsonl`; deploy will run
`agentdbctl export jsonl run --backfill --tail --max-batches ...` with a
managed consumer. Use `agentdbctl export parquet from-jsonl` or
`ops/observability/duckdb-jsonl-demo.sql` after the archive lands to inspect or
convert it.

Runtime operations that do not provision cloud infrastructure belong under
`ops/`.
