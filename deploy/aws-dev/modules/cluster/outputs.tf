output "endpoint" {
  description = "K8S API server"
  value       = "https://${aws_eip.k3s_master_eip.public_ip}:6443"
}

output "ca_certificate" {
  description = "The CA Certificate for the K3s cluster"
  value       = data.aws_ssm_parameter.ca_certificate.value
}

output "client_certificate" {
  description = "The client certificate for the K3s cluster"
  value       = data.aws_ssm_parameter.client_certificate.value
}

output "client_key" {
  description = "The client key for the K3s cluster"
  value       = data.aws_ssm_parameter.client_key.value
}

output "cluster_name" {
  description = "The k3s cluster name"
  value       = var.k3s_cluster_name
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "public_subnets" {
  description = "Public subnets"
  value       = "${aws_subnet.public-us-east-1a.id},${aws_subnet.public-us-east-1b.id}"
}