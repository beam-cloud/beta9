variable "k3s_cluster_config" {
  type = object({
    endpoint           = object({ description = string, value = string })
    ca_certificate     = object({ description = string, value = string })
    client_certificate = object({ description = string, value = string })
    client_key         = object({ description = string, value = string })
    cluster_name       = object({ description = string, value = string })
  })
  description = "K3S Cluster config."
}

variable "vpc_config" {
  type = object({
    vpc_id         = object({ description = string, value = string })
    public_subnets = object({ description = string, value = string })
  })
  description = "VPC config."
}

variable "db_config" {
  type = object({
    host     = object({ value = string })
    username = object({ value = string })
    password = object({ value = string })
  })
  description = "Postgres db config."
  sensitive   = true
}

variable "bucket_user_credentials" {
  type = object({
    access_key = string
    secret_key = string
  })
  description = "IAM credentials with access to buckets needed for images/juicefs"
  sensitive   = true
}

variable "s3_buckets" {
  type = object({
    image_bucket_name   = string
    juicefs_bucket_name = string
  })
}

variable "aws_region" {
  type = string
}

variable "domain" {
  type        = string
  description = "Domain name"
}

variable "domain_hosted_zone_id" {
  type        = string
  description = "Hosted zone ID in AWS Route 53."
}

variable "prefix" {
  type        = string
  description = "Identifier prefix for resource naming."
}
