variable "aws_region" {
  type        = string
  description = "AWS region for the demo stack."
}

variable "name" {
  type        = string
  description = "Name prefix for AWS resources."
  default     = "agentdb-tiered-demo"
}

variable "vpc_id" {
  type        = string
  description = "Existing VPC id."
}

variable "subnet_id" {
  type        = string
  description = "Existing subnet id for the EC2 instance. Used when subnet_ids is empty."
  default     = null
}

variable "subnet_ids" {
  type        = list(string)
  description = "Existing subnet ids for AgentDB nodes. Provide subnets in different AZs for multi-AZ cluster tests."
  default     = []
}

variable "node_count" {
  type        = number
  description = "Number of AgentDB EC2 nodes to create."
  default     = 1

  validation {
    condition     = var.node_count >= 1
    error_message = "node_count must be at least 1."
  }
}

variable "http_port" {
  type        = number
  description = "AgentDB HTTP port."
  default     = 7878
}

variable "instance_type" {
  type        = string
  description = "EC2 instance type."
  default     = "t3.small"
}

variable "key_name" {
  type        = string
  description = "EC2 key pair name for SSH access."
}

variable "ssh_cidr" {
  type        = string
  description = "CIDR allowed to SSH to the instance."
}

variable "ami_id" {
  type        = string
  description = "AMI id for the EC2 instance."
}

variable "deployment_mode" {
  type        = string
  description = "AgentDB deployment mode written to agentdb.toml. Use cluster only with a shared metadata backend."
  default     = "single_node"

  validation {
    condition     = contains(["single_node", "cluster"], var.deployment_mode)
    error_message = "deployment_mode must be single_node or cluster."
  }
}

variable "node_id_prefix" {
  type        = string
  description = "Prefix for generated AgentDB routing node ids."
  default     = "node"
}

variable "cell_id" {
  type        = string
  description = "AgentDB cell id written to cluster routing config."
  default     = "default"
}

variable "capacity_class" {
  type        = string
  description = "AgentDB capacity class for node placement: stable, mixed, or preemptible."
  default     = "stable"

  validation {
    condition     = contains(["stable", "mixed", "preemptible"], var.capacity_class)
    error_message = "capacity_class must be stable, mixed, or preemptible."
  }
}

variable "internal_auth_token" {
  type        = string
  description = "Shared internal token for cluster-mode Raft and movement RPCs. Leave empty for single-node."
  default     = ""
  sensitive   = true
}

variable "meta_url" {
  type        = string
  description = "Postgres metadata URL for cluster mode. Leave empty for local SQLite/single-node."
  default     = ""
  sensitive   = true
}

variable "tablet_store_tablets_per_domain" {
  type        = number
  description = "Number of tablets per local RocksDB tablet-store domain. Use 1 for high-isolation backup/restore testing."
  default     = 32
}

variable "tablet_store_max_open_domains" {
  type        = number
  description = "Maximum number of local RocksDB tablet-store domains open at once."
  default     = 16
}

variable "tablet_store_domain_write_buffer_size_mib" {
  type        = number
  description = "Write buffer size per local RocksDB tablet-store domain."
  default     = 16
}

variable "tablet_store_domain_max_write_buffers" {
  type        = number
  description = "Maximum write buffers per local RocksDB tablet-store domain."
  default     = 2
}

variable "tablet_store_domain_max_background_jobs" {
  type        = number
  description = "Maximum RocksDB background jobs per local tablet-store domain."
  default     = 1
}

variable "tablet_store_domain_max_open_files" {
  type        = number
  description = "Maximum open files per local RocksDB tablet-store domain."
  default     = 128
}

variable "clickhouse_url" {
  type        = string
  description = "Optional ClickHouse HTTP URL for AgentDB observability export. Leave empty to skip exporter setup."
  default     = ""
  sensitive   = true
}

variable "clickhouse_export_tenant_id" {
  type        = string
  description = "Tenant id to export to ClickHouse when clickhouse_url is set."
  default     = ""
}

variable "clickhouse_export_consumer" {
  type        = string
  description = "Managed export consumer name used by ClickHouse exporter workers."
  default     = "prod-clickhouse"
}

variable "clickhouse_export_mode" {
  type        = string
  description = "Comma-separated ClickHouse export modes: native, otel, or native,otel."
  default     = "native"

  validation {
    condition     = contains(["native", "otel", "native,otel"], var.clickhouse_export_mode)
    error_message = "clickhouse_export_mode must be native, otel, or native,otel."
  }
}

variable "clickhouse_export_lease_ms" {
  type        = number
  description = "Managed export consumer lease duration for ClickHouse exporter workers."
  default     = 30000
}

variable "clickhouse_export_max_retries" {
  type        = number
  description = "Maximum ClickHouse insert retries before the exporter exits and lets the process manager restart it."
  default     = 6
}

variable "clickhouse_export_retry_initial_backoff_ms" {
  type        = number
  description = "Initial ClickHouse insert retry backoff in milliseconds. Retries use exponential backoff."
  default     = 250
}

variable "jsonl_export_tenant_id" {
  type        = string
  description = "Tenant id to export to JSONL when jsonl_export_uri is set."
  default     = ""
}

variable "jsonl_export_uri" {
  type        = string
  description = "Optional JSONL export output path or object-store URI such as s3://bucket/prefix/events.jsonl. Leave empty to skip JSONL exporter setup."
  default     = ""
}

variable "jsonl_export_consumer" {
  type        = string
  description = "Managed export consumer name used by JSONL exporter workers."
  default     = "prod-jsonl"
}

variable "jsonl_export_include" {
  type        = string
  description = "Comma-separated JSONL export include list."
  default     = "events"
}

variable "jsonl_export_lease_ms" {
  type        = number
  description = "Managed export consumer lease duration for JSONL exporter workers."
  default     = 30000
}

variable "jsonl_export_max_batches" {
  type        = number
  description = "Maximum JSONL export batches for bounded object-store archive runs."
  default     = 1000
}
