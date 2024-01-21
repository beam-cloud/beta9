variable "prefix" {
  type = string
}

variable "cluster_endpoint" {
  type = string
}

variable "cluster_name" {
  type = string
}

variable "cluster_ca_certificate" {
  type = string
}

variable "cluster_client_key" {
  type = string
}

variable "cluster_client_certificate" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "public_subnets" {
  type = string
}

variable "domain" {
  default = "eng-stage.slai.io"
}

variable "domain_hosted_zone_id" {
  default = "Z07081541B2HAA9KWC78W"
}
