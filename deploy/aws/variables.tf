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
