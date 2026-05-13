# Infrastructure

This directory contains assets for provisioning external infrastructure that can
run AgentDB.

- `aws/terraform/` provisions demo AWS infrastructure.
- `aws/scripts/` contains helper scripts for bootstrapping and smoke-testing the
  AWS demo deployment.

Runtime operations that do not provision cloud infrastructure belong under
`ops/`.
