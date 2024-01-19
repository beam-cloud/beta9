# Beam Arc on Crusoe Cloud

This will help you run a single instance of Beam Arc on [Crusoe Cloud](https://docs.crusoecloud.com/).

## Prereqs

1. Install terraform.
1. Configure your Crusoe config ([docs](https://docs.crusoecloud.com/quickstart/installing-the-cli/index.html#configure-the-cli)).

## Getting started

1. Find your project ID.
1. Make a new SSH key, or point to an existing one.
1. Create a `terraform.tfvars` file in this directory.
    ```
    project_id    = "<uuid of your project>"
    ssh_key_path  = "~/.ssh/id_crusoecloud.pub"
    ```
1. Apply your Terraform.

    ```sh
    terraform init
    terraform apply
    ```
