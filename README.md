<div align="center">
<p align="center">
<img alt="Logo" src="static/beam-logo.jpeg" width="20%">
</p>

---

### AI Infrastructure for Developers

Run AI workloads anywhere with zero complexity. One line of Python, global GPUs, full control.

<p align="center">
  <a href="https://github.com/beam-cloud/beta9/stargazers">
    <img alt="⭐ Star the Repo" src="https://img.shields.io/github/stars/beam-cloud/beta9">
  </a>
  <a href="https://docs.beam.cloud">
    <img alt="Documentation" src="https://img.shields.io/badge/docs-quickstart-purple">
  </a>
  <a href="https://join.slack.com/t/beam-cloud/shared_invite/zt-2uiks0hc6-UbBD97oZjz8_YnjQ2P7BEQ">
    <img alt="Join Slack" src="https://img.shields.io/badge/Beam-Join%20Slack-orange?logo=slack">
  </a>
    <a href="https://twitter.com/beam_cloud">
    <img alt="Twitter" src="https://img.shields.io/twitter/follow/beam_cloud.svg?style=social&logo=twitter">
  </a>
  <a href="https://github.com/beam-cloud/beta9/actions">
    <img alt="Tests Passing" src="https://github.com/beam-cloud/beta9/actions/workflows/test.yml/badge.svg">
  </a>
</p>

---

[English](https://github.com/beam-cloud/beta9/blob/master/README.md) | [简体中文](https://github.com/beam-cloud/beta9/blob/master/docs/zh/zh_cn/README.md) | [繁體中文](https://github.com/beam-cloud/beta9/blob/master/docs/zh/zh_cw/README.md) | [Türkçe](https://github.com/beam-cloud/beta9/blob/master/docs/tr/README.md) | [हिंदी](https://github.com/beam-cloud/beta9/blob/master/docs/in/README.md) | [Português (Brasil)](https://github.com/beam-cloud/beta9/blob/master/docs/pt/README.md) | [Italiano](https://github.com/beam-cloud/beta9/blob/master/docs/it/README.md) | [Español](https://github.com/beam-cloud/beta9/blob/master/docs/es/README.md) | [한국어](https://github.com/beam-cloud/beta9/blob/master/docs/kr/README.md) | [日本語](https://github.com/beam-cloud/beta9/blob/master/docs/jp/README.md)

---

</div>

## Features

- Run serverless workloads using a friendly Python interface
- Autoscaling and automatic scale-to-zero
- Read large files at the edge using distributed, cross-region storage
- Connect bare-metal nodes to your cluster with a single cURL command
- Manage your fleet of servers using a Tailscale-powered service mesh
- Securely run workloads with end-to-end encryption through WireGuard

## How Does It Work?

### Run Serverless AI Workloads

Add an `endpoint` decorator to your code, and you'll get a load-balanced HTTP endpoint (with auth!) to invoke your code.

You can also run long-running functions with `@function`, deploy task queues using `@task_queue`, and schedule jobs with `@schedule`:

```python
from beta9 import endpoint


# This will run on a remote A100-40 in your cluster
@endpoint(cpu=1, memory=128, gpu="A100-40")
def square(i: int):
    return i**2
```

Deploy with a single command:

```
$ beta9 deploy app.py:square --name inference
=> Building image
=> Using cached image
=> Deployed 🎉

curl -X POST 'https://inference.beam.cloud/v1' \
-H 'Authorization: Bearer [YOUR_AUTH_TOKEN]' \
-H 'Content-Type: application/json' \
-d '{}'
```

### Run on Bare-Metal Servers Around the World

Connect any GPU to your cluster with one CLI command and a cURL.

```sh
$ beta9 machine create --pool lambda-a100-40

=> Created machine with ID: '9541cbd2'. Use the following command to set up the node:

#!/bin/bash
sudo curl -L -o agent https://release.beam.cloud/agent/agent && \
sudo chmod +x agent && \
sudo ./agent --token "AUTH_TOKEN" \
  --machine-id "9541cbd2" \
  --tailscale-url "" \
  --tailscale-auth "AUTH_TOKEN" \
  --pool-name "lambda-a100-40" \
  --provider-name "lambda"
```

You can run this install script on your VM to connect it to your cluster.

### Manage Your CPU or GPU Fleet

Manage your distributed cross-region cluster using a centralized control plane.

```sh
$ beta9 machine list

| ID       | CPU     | Memory     | GPU     | Status     | Pool        |
|----------|---------|------------|---------|------------|-------------|
| edc9c2d2 | 30,000m | 222.16 GiB | A10G    | registered | lambda-a10g |
| d87ad026 | 30,000m | 216.25 GiB | A100-40 | registered | gcp-a100-40 |

```

# Local Installation

You can run Beta9 locally, or in an existing Kubernetes cluster using our [Helm chart](https://github.com/beam-cloud/beta9/tree/main/deploy/charts/beta9).

### Setting Up the Server

k3d is used for local development. You'll need Docker to get started.

To use our fully automated setup, run the `setup` make target.

```bash
make setup
```

#### Local DNS

This is required to use an external file service for mulitpart uploads and range downloads. Its optional for using the subdomain middlware (host-based URLs).

```shell
brew install dnsmasq
echo 'address=/cluster.local/127.0.0.1' >> /opt/homebrew/etc/dnsmasq.conf
sudo bash -c 'mkdir -p /etc/resolver'
sudo bash -c 'echo "nameserver 127.0.0.1" > /etc/resolver/cluster.local'
sudo brew services start dnsmasq
```

To use subdomain or host-based URLs, add this to the config and rebuild the Beta9 gateway.

```yaml
gateway:
  invokeURLType: host
```

You should now be able to access your local k3s instance via a domain.

```shell
curl http://beta9-gateway.beta9.svc.cluster.local:1994/api/v1/health
```

### Setting Up the SDK

The SDK is written in Python. You'll need Python 3.8 or higher. Use the `setup-sdk` make target to get started.

```bash
make setup-sdk
```

### Using the SDK

After you've setup the server and SDK, check out the SDK readme [here](sdk/README.md).

## Contributing

We welcome contributions big or small. These are the most helpful things for us:

- Submit a [feature request](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=) or [bug report](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=bug-report.md&title=)
- Open a PR with a new feature or improvement

## Community & Support

If you need support, you can reach out through any of these channels:

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(Chat live with maintainers and community members\)
- [GitHub issues](https://github.com/beam-cloud/issues) \(Bug reports, feature requests, and anything roadmap related)
- [Twitter](https://twitter.com/beam_cloud) \(Updates on releases and more)

## Thanks to Our Contributors

<a href="https://github.com/beam-cloud/beta9/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=beam-cloud/beta9" />
</a>
