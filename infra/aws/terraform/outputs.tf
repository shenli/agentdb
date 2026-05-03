output "bucket" {
  value = aws_s3_bucket.agentdb.bucket
}

output "instance_public_ip" {
  value = aws_instance.agentdb.public_ip
}

output "ssh_target" {
  value = "ec2-user@${aws_instance.agentdb.public_ip}"
}
