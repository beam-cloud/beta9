variable "prefix" {
  description = "Global prefix for all resources"
  type        = string
}

variable "k3s_cluster_ami" {
  default = "ami-027a754129abb5386" # ubuntu 20.04
}

variable "k3s_instance_type" {
  default = "t3.small"
}

variable "k3s_worker_count" {
  default = 2
}

variable "k3s_cluster_name" {
  default = "beta9test-cluster"
}