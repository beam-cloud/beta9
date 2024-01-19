variable "prefix" {
  description = "Global prefix for all resources"
  type        = string
  default     = "beamtest" # Set your desired prefix here
}

variable "cluster_name" {
  default = "demo2"
}

variable "cluster_version" {
  default = "1.28"
}

variable "domain" {
  default = "eng-stage.slai.io"
}

variable "domain_hosted_zone_id" {
  default = "Z07081541B2HAA9KWC78W"
}

variable "k3s_cluster_ami" {
  default = "ami-00b56546df5a8bc0e"
}