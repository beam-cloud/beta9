<div align="center">
<p align="center">
<img alt="Logo" src="static/beam-logo-white.png" width="30%">
</p>

## Ultrafast AI Inference

### Scalable Infrastructure for Running Your AI Workloads at Scale

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

</div>

## Installation

```shell
pip install beam-client
```

## Features

- **Extremely Fast**: Launch containers in 200ms using a custom `runc` runtime
- **Parallelization and Concurrency**: Fan out workloads to 100s of containers
- **First-Class Developer Experience**: Hot-reloading, webhooks, and scheduled jobs
- **Scale-to-Zero**: Workloads are serverless by default
- **Volume Storage**: Mount distributed storage volumes
- **GPU Support**: Run on our cloud (4090s, H100s, and more) or bring your own GPUs

## Quickstart

1. Create an account at https://beam.cloud
2. Follow our [Getting Started Guide](https://platform.beam.cloud/onboarding)

## Creating your first inference endpoint

With Beam, everything is Python-native—no YAML, no config files, just code:

```python
from beam import Image, endpoint


@endpoint(
    image=Image(python_version="python3.11"),
    gpu="A10G",
    cpu=1,
    memory=1024,
)
def handler():
    return {"label": "cat", "confidence": 0.97}
```

This snippet deploys your code to a GPU-backed container with an HTTPS endpoint—ready to serve requests immediately (`https://my-model-v1.app.beam.cloud/`)

> ## Self-Hosting vs Cloud
>
> Beta9 is the open-source engine powering [Beam](https://beam.cloud), our fully-managed cloud platform. You can self-host Beta9 for free or choose managed cloud hosting through Beam.

## Contributing

We welcome contributions big or small. These are the most helpful things for us:

- Submit a [feature request](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=) or [bug report](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=bug-report.md&title=)
- Open a PR with a new feature or improvement

## Thanks to Our Contributors

<a href="https://github.com/beam-cloud/beta9/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=beam-cloud/beta9" />
</a>
