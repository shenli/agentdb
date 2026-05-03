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
