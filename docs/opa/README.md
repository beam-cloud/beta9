# On-Prem Agent

Beam's control plane can be extended to manage external resources, with the same developer experience and performance benefits as the managed product.

> **Note**: this feature is available on-request and needs to be enabled on your Beam account to use.

## How it Works

The on-prem agent is built on top of these components:

- The Control Plane (Gateway)
- Tailscale (Wireguard)
- An agent service

![beam-opa](https://github.com/beam-cloud/beta9/blob/main/docs/opa/opa.png?raw=true)

## Managing On-Premise Worker Pools

In Beam, we have a concept of a "worker pool", which is a worker with certain attributes. For example, you might create a worker pool
for a certain set of GPUs. This can be done via CLI:

```sh
beam pool create --name my-h100-pool --gpu-type A100 --default-gpu-count 1
```

## Adding Machines to a Worker Pool

Now that we have a worker pool, we need to add workers to it. To do this, we must add a "machine" to the pool. This is just a debian based system (if you want to use a GPU on this machine, the server must have NVIDIA drivers installed). 

To create a new machine, you'll use the `beam machine create` command:

```sh
$ beam machine create --pool my-h100-pool

=> Created machine with ID: 'c11d3030'. Use the following command to setup the node:
# -- Agent setup
sudo curl -L -o agent https://release.beam.cloud/agent/agent && \
sudo chmod +x agent && \
sudo ./agent --token "MY_TOKEN" \
    --machine-id "c11d3030" \
    --tailscale-url "" \
    --tailscale-auth "tskey-auth" \
    --pool-name "my-h100-pool" \
    --provider-name "generic"
```

This command can now be copied and pasted into the machine to setup the agent. It will automatically join the Tailscale network and register with Beam's control plane.

## Running Workloads

With the hardware connected, we can validate it using the `machine list` command:

```sh
$ beam machine list

| ID       | CPU     | Memory     | GPU     | Status     | Pool         |
|----------|---------|------------|---------|------------|--------------|
| edc9c2d2 | 30,000m | 222.16 GiB | H100    | registered | my-h100-pool |
```

This will show you all the machines in your inventory.

Once the machine is registered, you can use it in the same way you would use a managed machine on Beam:

```python
from beam import function

@function(gpu="H100")
def handler():
    return {"message": "This is running on your worker pool!"}

if __name__ == "__main__":
    handler.remote()
```

When invoked, this function will run on the on-prem worker pool specified above.
