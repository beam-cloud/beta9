variable "prefix" {
  description = "Global prefix for all resources"
  type        = string
}

variable "domain" {
  default = "eng-stage.slai.io"
}

variable "domain_hosted_zone_id" {
  default = "Z07081541B2HAA9KWC78W"
}

variable "k3s_cluster_ami" {
  default = "ami-027a754129abb5386" # ubuntu 20.04
}

variable "k3s_cluster_name" {
  default = "beamtest-cluster"
}