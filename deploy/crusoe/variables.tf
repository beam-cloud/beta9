variable "ssh_key_path" {
  description = "Path to your public SSH key."
  type        = string
}

variable "location" {
  description = <<-EOF
    Location to deploy your resources. Run the CLI command to show a list of locations.
      ```
      crusoe locations list
      ```
  EOF
  type        = string
  default     = "us-northcentral1-a"
}

variable "instance_type" {
  description = <<-EOF
    Type of instance to run. Run the CLI command to show a list of instance types.
      ```
      crusoe compute vms types
      ```
  EOF
  type        = string
  default     = "a100.1x"
}
