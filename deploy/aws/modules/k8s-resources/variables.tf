variable "domain" {
  type        = string
  default     = "eng-stage.slai.io"
  description = "Domain name, defaults to 'eng-stage.slai.io'."
}

variable "domain_hosted_zone_id" {
  type        = string
  default     = "Z07081541B2HAA9KWC78W"
  description = "Hosted zone ID in AWS Route 53."
}

variable "prefix" {
  type        = string
  description = "Identifier prefix for resource naming."
}

variable "cluster_endpoint" {
  type        = string
  description = "Endpoint URL of the cluster."
}

variable "cluster_name" {
  type        = string
  description = "Name of the cluster."
}

variable "cluster_ca_certificate" {
  type        = string
  description = "CA certificate for the cluster."
}

variable "cluster_client_key" {
  type        = string
  description = "Private key for cluster client."
}

variable "cluster_client_certificate" {
  type        = string
  description = "Client certificate for cluster access."
}

variable "vpc_id" {
  type        = string
  description = "ID of the VPC."
}

variable "public_subnets" {
  type        = string
  description = "Public subnets in the VPC."
}