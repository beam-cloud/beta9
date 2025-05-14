<div align="center">
<p align="center">
<img alt="Logo" src="static/beam-logo.jpeg" width="20%">
</p>

---

# Beta9

### Run Cloud Containers from Python

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

Beta9 is a Python-focused cloud for developers—we let you deploy Python functions and scripts without managing any infrastructure, simply by adding decorators to your existing code.

For years, we searched for a simpler alternative to Docker—something lightweight to run a container behind a TCP port, with built-in load balancing and centralized logging, but without YAML or manual config. Existing solutions like Heroku or Railway felt too heavy for smaller services or quick experiments.

> ## Relationship to Beam
>
> Beta9 is the open-source engine powering [Beam](https://beam.cloud), our fully-managed cloud platform. You can self-host Beta9 for free or choose managed cloud hosting through Beam.

## Installation (Cloud)

```shell
pip install beam-client
```

Installation for self-hosting can be found [here](https://docs.beam.cloud/v2/self-hosting/overview).

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

## How It Works

With Beta9, everything is Python-native—no YAML, no config files, just code:

```python
from beta9 import Pod, Image

pod = Pod(
    name="python-example",
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

## Contributing

We welcome contributions big or small. These are the most helpful things for us:

- Submit a [feature request](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=) or [bug report](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=bug-report.md&title=)
- Open a PR with a new feature or improvement

## Thanks to Our Contributors

<a href="https://github.com/beam-cloud/beta9/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=beam-cloud/beta9" />
</a>
