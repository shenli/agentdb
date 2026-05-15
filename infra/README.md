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
ClickHouse schema, and start `agentdbctl export clickhouse run --backfill
--tail` with a managed consumer lease. Run the same deploy flow on multiple
nodes with the same `clickhouse_export_consumer` to get active/passive exporter
HA; each node derives a distinct lease owner from its hostname.

Runtime operations that do not provision cloud infrastructure belong under
`ops/`.
