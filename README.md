<div align="center">
<p align="center">
<img alt="Logo" src="static/beam-logo.jpeg" width="20%">
</p>

## Run Cloud Containers from Python

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

- **Extremely fast**: launches runc containers in ~800ms
- **Parallelization and concurrency**: fan out workloads to 100s of containers
- **First-class developer experience**: hot-reloading, webhooks, and scheduled jobs
- **Scale-to-zero**: Workloads are serverless by default
- **Volume Storage**: mount distributed storage volumes
- **GPU support**: run on our cloud, or connect your own hardware

## Quickstart

1. Create an account at https://beam.cloud
2. Follow our [Getting Started Guide](https://platform.beam.cloud/onboarding)

## Creating your first app

With Beta9, everything is Python-native—no YAML, no config files, just code:

```python
from beta9 import Pod, Image


pod = Pod(
    image=Image(python_version="python3.11"),
    gpu="A10G",
    ports=[8000],
    cpu=1,
    memory=1024,
    entrypoint=["python3", "-m", "http.server", "8000"],
)

instance = pod.create()

print("✨ Container hosted at:", instance.url)
```

This single Python snippet launches a container, automatically load-balanced and exposed via HTTPS.

> ## Relationship to Beam
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
