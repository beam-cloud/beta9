<div align="center">
<p align="center">
<img alt="Logo" src="static/beam-logo.jpeg" width="20%">
</p>

---

### **✨ The Open-Source Serverless GPU Container Runtime ✨**

<p align="center">
  <a href="https://docs.beta9.beam.cloud">
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
</p>

---

[English](https://github.com/beam-cloud/beta9/blob/master/README.md) | [简体中文](https://github.com/beam-cloud/beta9/blob/master/docs/zh/zh_cn/README.md) | [繁體中文](https://github.com/beam-cloud/beta9/blob/master/docs/zh/zh_cw/README.md) | [Türkçe](https://github.com/beam-cloud/beta9/blob/master/docs/tr/README.md) | [हिंदी](https://github.com/beam-cloud/beta9/blob/master/docs/in/README.md) | [Português (Brasil)](https://github.com/beam-cloud/beta9/blob/master/docs/pt/README.md) | [Italiano](https://github.com/beam-cloud/beta9/blob/master/docs/it/README.md) | [Español](https://github.com/beam-cloud/beta9/blob/master/docs/es/README.md) | [한국어](https://github.com/beam-cloud/beta9/blob/master/docs/kr/README.md)

---

</div>

# Beta9

Beta9 is an open-source platform for running scalable serverless GPU workloads across cloud providers.

Features:

- Scale out workloads to thousands of GPU (or CPU) containers
- Ultrafast cold-start for custom ML models
- Instantly run remote containers, right from your Python interpreter
- Distribute workloads across multiple cloud providers
- Easily deploy task queues and functions using simple Python abstractions

We use beta9 internally at [Beam](https://beam.cloud) to run AI applications for users at scale.

## Use-Cases

### Serverless Inference Endpoints

```python
from beta9 import Image, endpoint


@endpoint(
    cpu=1,
    memory="16Gi",
    gpu="T4",
    image=Image(
        python_packages=[
            "vllm==0.4.1",
        ],  # These dependencies will be installed in your remote container
    ),
)
def predict():
    from vllm import LLM

    prompts = ["The future of AI is"]
    llm = LLM(model="facebook/opt-125m")
    output = llm.generate(prompts)[0]

    return {"prediction": output.outputs[0].text}
```

### Fan-Out Workloads to Hundreds of Containers

```python
from beta9 import function


@function()
def square(i: int):
    return i**2


def main():
    numbers = list(range(100))
    squared = []

    # Run a remote container for every item in list
    for result in square.map(numbers):
        squared.append(result)
```

### Enqueue Async Jobs

```python
from beta9 import task_queue, Image


@task_queue(
    cpu=1.0,
    memory=128,
    gpu="T4",
    image=Image(python_packages=["torch"]),
    keep_warm_seconds=1000,
)
def multiply(x):
    result = x * 2
    return {"result": result}

# Manually insert task into the queue
multiply.put(x=10)
```

## How it works

Beta9 is designed for launching remote serverless containers quickly. There are a few things that make this possible:

- A custom, lazy loading image format (CLIP) backed by S3/FUSE
- A fast, redis-based container scheduling engine
- Content-addressed storage for caching images and files
- A custom runc container runtime

![demo gif](sdk/docs/demo.gif)

# Get started

## Beam Cloud (Recommended)

The fastest and most reliable way to get started is by signing up for our managed service, [Beam Cloud](https://beam.cloud). Your first 10 hours of usage are free, and afterwards you pay based on usage.

## Open-source deploy (Advanced)

You can run Beta9 locally, or in an existing Kubernetes cluster using our [Helm chart](https://github.com/beam-cloud/beta9/tree/main/deploy/charts/beta9).

### Local Development

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

## Contributing

We welcome contributions, big or small! These are the most helpful things for us:

- Rank features in our roadmap
- Open a PR
- Submit a [feature request](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=) or [bug report](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=bug-report.md&title=)

## Community & Support

If you need support, you can reach out through any of these channels:

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(Chat live with maintainers and community members\)
- [GitHub issues](https://github.com/beam-cloud/issues) \(Bug reports, feature requests, and anything roadmap related)
- [Twitter](https://twitter.com/beam_cloud) \(Updates on releases and stuff)

## Thanks to our contributors

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
