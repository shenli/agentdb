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
  description = "Existing subnet id for the EC2 instance."
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
