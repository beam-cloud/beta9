variable "prefix" {
  description = "Global prefix for all resources"
  type        = string
  default     = "beamtest" # Set your desired prefix here
}

variable "domain" {
  type        = string
  default     = "eng-stage.slai.io"
  description = "Domain name"
}

variable "domain_hosted_zone_id" {
  type        = string
  default     = "Z07081541B2HAA9KWC78W"
  description = "Hosted zone ID in AWS Route 53"
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS Region to deploy all resources"
}
