output "public_ipv4" {
  value = crusoe_compute_instance.this.network_interfaces[0].public_ipv4.address
}
