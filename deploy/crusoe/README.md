# Beta9 on Crusoe Cloud

This will help you run a single instance of Beta9 on [Crusoe Cloud](https://docs.crusoecloud.com/).

> [!NOTE]
> This setup runs everything on a single node. It is not recommended for production use.

## Prereqs

1. Install terraform.
1. Install Crusoe CLI and setup your config ([docs](https://docs.crusoecloud.com/quickstart/installing-the-cli/index.html#configure-the-cli)).

## Getting started

1. Make a new SSH key, or point to an existing one.
1. Create a `terraform.tfvars` file in this directory.
    ```
    ssh_key_path  = "~/.ssh/id_crusoecloud.pub"
    ```
1. Apply your Terraform.
    ```sh
    terraform init
    terraform apply
    ```
1. To monitor the setup, tail the logs over SSH
    ```sh
    ssh -i ~/.ssh/id_crusoecloud ubuntu@<instance-ip> 'tail -n+1 -f /var/log/user-data.log'
    ```

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_crusoe"></a> [crusoe](#requirement\_crusoe) | 0.5.18 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_crusoe"></a> [crusoe](#provider\_crusoe) | 0.5.18 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [crusoe_compute_instance.this](https://registry.terraform.io/providers/crusoecloud/crusoe/0.5.18/docs/resources/compute_instance) | resource |
| [crusoe_storage_disk.data](https://registry.terraform.io/providers/crusoecloud/crusoe/0.5.18/docs/resources/storage_disk) | resource |
| [crusoe_vpc_firewall_rule.ingress](https://registry.terraform.io/providers/crusoecloud/crusoe/0.5.18/docs/resources/vpc_firewall_rule) | resource |
| [crusoe_vpc_networks.this](https://registry.terraform.io/providers/crusoecloud/crusoe/0.5.18/docs/data-sources/vpc_networks) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_instance_type"></a> [instance\_type](#input\_instance\_type) | Type of instance to run. Run the CLI command to show a list of instance types.<pre>crusoe compute vms types</pre> | `string` | `"a100-80gb.1x"` | no |
| <a name="input_location"></a> [location](#input\_location) | Location to deploy your resources. Run the CLI command to show a list of locations.<pre>crusoe locations list</pre> | `string` | `"us-northcentral1-a"` | no |
| <a name="input_ssh_key_path"></a> [ssh\_key\_path](#input\_ssh\_key\_path) | Path to your public SSH key. | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_public_ipv4"></a> [public\_ipv4](#output\_public\_ipv4) | n/a |
