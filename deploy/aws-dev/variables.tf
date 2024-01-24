variable "prefix" {
  description = "Global prefix for all resources"
  type        = string
}

variable "domain" {
  type        = string
  description = "Domain name"
}

variable "domain_hosted_zone_id" {
  type        = string
  description = "Hosted zone ID in AWS Route 53"
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS Region to deploy all resources"
}
