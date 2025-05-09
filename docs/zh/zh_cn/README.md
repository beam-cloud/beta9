<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ 超快无服务器 GPU 运行时 ✨**

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
</p>

---

[English](https://github.com/beam-cloud/beta9/blob/master/README.md) | [简体中文](https://github.com/beam-cloud/beta9/blob/master/docs/zh/zh_cn/README.md) | [繁體中文](https://github.com/beam-cloud/beta9/blob/master/docs/zh/zh_cw/README.md) | [Türkçe](https://github.com/beam-cloud/beta9/blob/master/docs/tr/README.md) | [हिंदी](https://github.com/beam-cloud/beta9/blob/master/docs/in/README.md) | [Português (Brasil)](https://github.com/beam-cloud/beta9/blob/master/docs/pt/README.md) | [Italiano](https://github.com/beam-cloud/beta9/blob/master/docs/it/README.md) | [Español](https://github.com/beam-cloud/beta9/blob/master/docs/es/README.md) | [한국어](https://github.com/beam-cloud/beta9/blob/master/docs/kr/README.md)

---

</div>

# Beta9

Beta9 是一个开源平台，用于直接从 Python 运行远程容器。 它支持 GPU/CUDA 加速，允许您将任意 Python 代码扩展到数百台机器，轻松部署功能和任务队列，以及跨各种云提供商（包括裸机提供商）分配工作负载。

特征：

- 将工作负载扩展到数百台机器（有 GPU 支持！）
- 直接从 Python 解释器即时运行远程容器
- 在多个云提供商之间分配工作负载
- 使用简单的 Python 抽象轻松部署任务队列和函数

我们在 [Beam](https://beam.cloud) 内部使用 beta9 为用户大规模运行 AI 应用程序。

# 开始吧

## 束云（推荐）

开始使用 Beam 最快、最可靠的方法是免费注册 [Beam Cloud](https://beam.cloud)。 前 10 小时免费使用，之后根据使用情况付费。

## 开源部署（高级）

#### 设置服务器

k3d 用于本地开发。 您需要 Docker 和 Make 才能开始。

要使用我们的全自动设置，请运行“setup”make 目标。

> [!NOTE]
> 这将覆盖您可能已经安装的一些工具。 查看 [setup.sh](bin/setup.sh) 以了解更多信息。

```
make setup
```

#### 设置 SDK

SDK 是用 Python 编写的。 您需要 Python 3.8 或更高版本。 使用 `setup-sdk` make 目标开始。

> [!NOTE]
> 这将安装 Poetry 包管理器。

```
make setup-sdk
```

#### 使用 SDK

设置服务器和 SDK 后，请查看 SDK 自述文件 [此处](sdk/README.md)。

# 示例应用程序

```python
from beta9 import function


@function(cpu=8)
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

     # 为列表中的每个项目运行远程容器
    for result in square.map(numbers):
        squared.append(result)
```

## 怎么运行的

Beta9 旨在快速启动远程无服务器容器。 有几件事使这成为可能：

- 由 S3/FUSE 支持的自定义延迟加载图像格式 (CLIP)
- 快速的、基于 redis 的容器调度引擎
- 用于缓存图像和文件的内容寻址存储
- 自定义 runc 容器运行时

![演示 gif](sdk/docs/demo.gif)

## 贡献

我们欢迎贡献，无论大小！ 这些是对我们最有帮助的事情：

- 在我们的路线图中对功能进行排名
- 开启 PR
- 提交[功能请求](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=)

## 哲学

我们的使命是简化云的复杂性。 为此，我们构建了一个 Python-first 抽象，用于在 GPU 上启动无服务器容器。

我们认为，现有的云提供商提供的工具过于臃肿和复杂，开发人员无法快速迭代。

Beam 是设置 Kubernetes 集群或启动云虚拟机的替代方案。

Beam 为您提供了在云 GPU 上运行代码、在 API 后面公开该代码以及在应用程序上快速迭代所需的所有工具。

## 社区与支持

如果您需要支持，可以通过以下任一渠道联系：

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(与我们的工程团队实时聊天\)
- [GitHub 问题](https://github.com/beam-cloud/issues) \（错误报告、功能请求和任何与路线图相关的内容）
- [Twitter](https://twitter.com/beam_cloud) \（版本更新）

## 感谢我们的贡献者

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
   <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
