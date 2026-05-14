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

Runtime operations that do not provision cloud infrastructure belong under
`ops/`.
