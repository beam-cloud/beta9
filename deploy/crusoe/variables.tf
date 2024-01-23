variable "project_id" {
  description = "UUID of your project."
  type        = string
}

variable "ssh_key_path" {
  description = "Path to your public SSH key."
  type        = string
}

variable "location" {
  description = "Location to deploy your resources."
  type        = string
  default     = "us-northcentral1-a"
}

variable "instance_type" {
  type    = string
  default = "a40.1x"
}
