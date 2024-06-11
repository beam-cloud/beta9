locals {
  name            = "beam-beta9"
  ssh_key_content = file(var.ssh_key_path)
}

resource "crusoe_compute_instance" "this" {
  name     = local.name
  image    = "ubuntu20.04-nvidia-pcie-docker:latest"
  type     = var.instance_type
  ssh_key  = local.ssh_key_content
  location = var.location

  startup_script = <<-EOF
  #!/bin/bash

  exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

  mkdir /data
  mkfs.ext4 /dev/vda
  mount -t ext4 /dev/vda /data

  cd /data
  git clone https://github.com/beam-cloud/beta9.git
  cd beta9
  make setup
  EOF

  disks = [
    {
      id              = crusoe_storage_disk.data.id
      mode            = "read-write"
      attachment_type = "data"
    }
  ]

  depends_on = [crusoe_storage_disk.data]
}

resource "crusoe_storage_disk" "data" {
  name     = local.name
  size     = "400GiB"
  location = var.location
}

data "crusoe_vpc_networks" "this" {}

resource "crusoe_vpc_firewall_rule" "ingress" {
  network           = data.crusoe_vpc_networks.this.vpc_networks[0].id
  name              = local.name
  action            = "allow"
  direction         = "ingress"
  protocols         = "tcp"
  source            = "0.0.0.0/0"
  source_ports      = "1993-1994"
  destination       = data.crusoe_vpc_networks.this.vpc_networks[0].cidr
  destination_ports = "1-65535"

  depends_on = [data.crusoe_vpc_networks.this]
}
