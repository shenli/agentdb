# Infrastructure

This directory contains assets for provisioning external infrastructure that can
run AgentDB.

- `aws/terraform/` provisions demo AWS infrastructure.
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

Runtime operations that do not provision cloud infrastructure belong under
`ops/`.
