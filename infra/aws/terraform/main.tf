provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  effective_subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : (
    var.subnet_id == null ? [] : [var.subnet_id]
  )
}

data "aws_subnet" "agentdb" {
  count = length(local.effective_subnet_ids)
  id    = local.effective_subnet_ids[count.index]
}

resource "aws_s3_bucket" "agentdb" {
  bucket = "${var.name}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "agentdb" {
  bucket = aws_s3_bucket.agentdb.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_iam_role" "agentdb" {
  name = "${var.name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "agentdb_s3" {
  name = "${var.name}-s3"
  role = aws_iam_role.agentdb.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.agentdb.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = ["${aws_s3_bucket.agentdb.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "agentdb" {
  name = "${var.name}-profile"
  role = aws_iam_role.agentdb.name
}

resource "aws_security_group" "agentdb" {
  name        = "${var.name}-sg"
  description = "Security group for AgentDB SlateDB/S3 demo"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  ingress {
    from_port   = var.http_port
    to_port     = var.http_port
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  ingress {
    from_port = var.http_port
    to_port   = var.http_port
    protocol  = "tcp"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "agentdb" {
  count = var.node_count

  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = local.effective_subnet_ids[count.index % length(local.effective_subnet_ids)]
  vpc_security_group_ids = [aws_security_group.agentdb.id]
  iam_instance_profile   = aws_iam_instance_profile.agentdb.name
  key_name               = var.key_name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    deployment_mode                            = var.deployment_mode
    node_id                                    = format("%s-%02d", var.node_id_prefix, count.index + 1)
    http_port                                  = var.http_port
    cell_id                                    = var.cell_id
    availability_zone                          = data.aws_subnet.agentdb[count.index % length(local.effective_subnet_ids)].availability_zone
    capacity_class                             = var.capacity_class
    internal_auth_token                        = var.internal_auth_token
    meta_url                                   = var.meta_url
    bucket                                     = aws_s3_bucket.agentdb.bucket
    region                                     = var.aws_region
    tablet_store_tablets_per_domain            = var.tablet_store_tablets_per_domain
    tablet_store_max_open_domains              = var.tablet_store_max_open_domains
    tablet_store_domain_write_buffer_size_mib  = var.tablet_store_domain_write_buffer_size_mib
    tablet_store_domain_max_write_buffers      = var.tablet_store_domain_max_write_buffers
    tablet_store_domain_max_background_jobs    = var.tablet_store_domain_max_background_jobs
    tablet_store_domain_max_open_files         = var.tablet_store_domain_max_open_files
    clickhouse_url                             = var.clickhouse_url
    clickhouse_export_tenant_id                = var.clickhouse_export_tenant_id
    clickhouse_export_consumer                 = var.clickhouse_export_consumer
    clickhouse_export_mode                     = var.clickhouse_export_mode
    clickhouse_export_lease_ms                 = var.clickhouse_export_lease_ms
    clickhouse_export_max_retries              = var.clickhouse_export_max_retries
    clickhouse_export_retry_initial_backoff_ms = var.clickhouse_export_retry_initial_backoff_ms
    jsonl_export_tenant_id                     = var.jsonl_export_tenant_id
    jsonl_export_uri                           = var.jsonl_export_uri
    jsonl_export_consumer                      = var.jsonl_export_consumer
    jsonl_export_include                       = var.jsonl_export_include
    jsonl_export_lease_ms                      = var.jsonl_export_lease_ms
    jsonl_export_max_batches                   = var.jsonl_export_max_batches
  })

  tags = {
    Name                  = format("%s-%02d", var.name, count.index + 1)
    AgentDBCell           = var.cell_id
    AgentDBNodeId         = format("%s-%02d", var.node_id_prefix, count.index + 1)
    AgentDBCapacityClass  = var.capacity_class
    AgentDBDeploymentMode = var.deployment_mode
  }
}
