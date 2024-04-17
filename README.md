<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="40%">
</p>

---

### **✨ Ultrafast Serverless GPU Runtime ✨**

<p align="center">
  <a href="https://docs.beam.cloud">
    <img alt="Documentation" src="https://img.shields.io/badge/docs-quickstart-blue">
  </a>
  <a href="https://join.slack.com/t/beam-89x5025/shared_invite/zt-1ye1jzgg2-cGpMKuoXZJiT3oSzgPmN8g">
    <img alt="Join Slack" src="https://img.shields.io/badge/Beam-Join%20Slack-blue?logo=slack">
  </a>
    <a href="https://twitter.com/beam_cloud">
    <img alt="Twitter" src="https://img.shields.io/twitter/follow/beam_cloud.svg?style=social&logo=twitter">
  </a>
  <a href="https://github.com/beam-cloud/beta9/actions">
    <img alt="Tests Passing" src="https://github.com/beam-cloud/beta9/actions/workflows/test.yml/badge.svg">
  </a>
  <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-ff69b4"/>
</p>

---

[English](https://github.com/beam-cloud/beta9/blob/master/README.md) | [简体中文](https://github.com/beam-cloud/beta9/blob/master/README.md) | [繁體中文](https://github.com/beam-cloud/beta9/blob/master/README.md) | [Türkçe](https://github.com/beam-cloud/beta9/blob/master/README.md) | [हिंदी](https://github.com/beam-cloud/beta9/blob/master/README.md) | [Português (Brasil)](https://github.com/beam-cloud/beta9/blob/master/README.md) | [Italiano](https://github.com/beam-cloud/beta9/blob/master/README.md) | [Español](https://github.com/beam-cloud/beta9/blob/master/README.md) | [한국어](https://github.com/beam-cloud/beta9/blob/master/README.md)

---

</div>

# Beta9

Beta9 is an open-source platform for running remote containers directly from Python. It supports GPU/CUDA acceleration, allows you to scale out arbitrary Python code to hundreds of machines, easily deploy functions and task queues, and distribute workloads across various cloud providers (including bare metal providers).

Features:

- Scale out workloads to hundreds of machines (with GPU support!)
- Instantly run remote containers, right from your Python interpreter
- Distribute workloads across multiple cloud providers
- Easily deploy task queues and functions using simple Python abstractions

We use beta9 internally at [Beam](https://beam.cloud) to run AI applications for users at scale.

# Get started

### Beam Cloud (Recommended)

The fastest way and most reliable way to get started with Beam is by signing up for free to [Beam Cloud](https://beam.cloud). Your first 10 hours of usage are free, and afterwards you pay based on usage.

### Open-source deploy (Advanced)

#### Setting up the server

k3d is used for local development. You'll need Docker and Make to get started.

To use our fully automated setup, run the `setup` make target.

> [!NOTE]
> This will overwrite some of the tools you may already have installed. Review the [setup.sh](bin/setup.sh) to learn more.

```
make setup
```

#### Setting up the SDK

The SDK is written in Python. You'll need Python 3.8 or higher. Use the `setup-sdk` make target to get started.

> [!NOTE]
> This will install the Poetry package manager.

```
make setup-sdk
```

#### Using the SDK

After you've setup the server and SDK, check out the SDK readme [here](sdk/README.md).

# Example App

```python
from beta9 import function


@function(cpu=8, gpu="A100-40")
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

    # Run a remote container for every item in list
    for result in square.map(numbers):
        squared.append(result)
```

## How it works

Beta9 is designed for launching remote serverless containers very quickly. There are a few things that make this possible:

- A custom, lazy loading image format (CLIP) backed by S3/FUSE
- A fast, redis-based container scheduling engine
- Content-addressed storage for caching images and files
- A custom runc container runtime

![demo gif](sdk/docs/demo.gif)

## Contributing

We welcome contributions, big or small! These are the most helpful things for us:

- Rank features in our roadmap
- Open a PR
- Submit a [feature request](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=issue--feature-request.md&title=) or [bug report](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=issue--bug-report.md&title=)

## Philosophy

Our mission is to simplify the complexity of the cloud. To do this, we've built a Python-first abstraction for launching serverless containers on GPUs.

In our view, the existing cloud providers provide tools that are too bloated and complicated for developers to iterate quickly.

Beam is the alternative to setting up a Kubernetes cluster or spinning up a cloud VM.

Beam gives you all the tools you need to run code on cloud GPUs, expose that code behind an API, and iterate quickly on your app.

## Open-source vs. paid

This repo is available under the Apache license. If you'd like to use the cloud hosted version, you can visit our [pricing page](https://beam.cloud/pricing).

## Community & Support

If you need support, you can reach out through any of these channels:

- [Slack](https://join.slack.com/t/beam-89x5025/shared_invite/zt-1ye1jzgg2-cGpMKuoXZJiT3oSzgPmN8g) \(Chat live with our engineering team\)
- [GitHub issues](https://github.com/beam-cloud/beta9/issues) \(Bug reports, feature requests, and anything roadmap related)
- [Twitter](https://twitter.com/beam_cloud) \(Updates on releases)

### Thanks to our contributors

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
