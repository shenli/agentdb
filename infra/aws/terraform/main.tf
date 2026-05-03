provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

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
    from_port   = 7878
    to_port     = 7878
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "agentdb" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.agentdb.id]
  iam_instance_profile   = aws_iam_instance_profile.agentdb.name
  key_name               = var.key_name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    bucket = aws_s3_bucket.agentdb.bucket
    region = var.aws_region
  })

  tags = {
    Name = var.name
  }
}
