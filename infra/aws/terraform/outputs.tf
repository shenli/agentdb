output "bucket" {
  value = aws_s3_bucket.agentdb.bucket
}

output "instance_public_ip" {
  value = aws_instance.agentdb[0].public_ip
}

output "instance_public_ips" {
  value = aws_instance.agentdb[*].public_ip
}

output "instance_private_ips" {
  value = aws_instance.agentdb[*].private_ip
}

output "ssh_target" {
  value = "ec2-user@${aws_instance.agentdb[0].public_ip}"
}

output "ssh_targets" {
  value = [for instance in aws_instance.agentdb : "ec2-user@${instance.public_ip}"]
}
