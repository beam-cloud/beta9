<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beam/assets/10925686/860052a1-2a96-4bad-a991-dd2b24e3b524"/ width="40%">
</p>

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
  <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-ff69b4"/>
</p>


<h3 align="center">
    The distributed container runtime
</h3>

---

Beam is an open-source platform for running remote containers directly from Python. It supports GPU/CUDA acceleration, and allows you to scale out arbitrary Python code to hundreds of machines, easily deploy functions and task queues, and distribute workloads across various cloud providers (including bare metal providers).

## Features

- Scale out workloads to hundreds of machines (with GPU support!)
- Instantly run remote containers, right from your Python interpreter
- Distribute GPU workloads across multiple cloud providers
- Easily deploy task queues and functions using simple Python abstractions

## How it works

Beam is designed for launching remote serverless containers very quickly. There are a few things that make this possible:

- A custom, lazy loading image format (CLIP) backed by S3/FUSE
- A fast, redis-based scheduling engine
- Content-addressed storage for caching images and files
- A custom runc container runtime

## Local development

Beam can run on your laptop! To get started developing

## Community & Support

Beam is powered by the community. If you need support, you can join any of these channels:

- [Slack](https://join.slack.com/t/beam-89x5025/shared_invite/zt-1ye1jzgg2-cGpMKuoXZJiT3oSzgPmN8g) \(Chat live with our engineering team\)
- [GitHub issues](https://github.com/slai-labs/beam/issues) \(Bug reports, feature requests, and anything roadmap related)
- [Twitter](https://twitter.com/beam_cloud) \(Updates on releases)
