<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ 超快無伺服器 GPU 執行時間 ✨**

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

Beta9 是一個開源平台，用於直接從 Python 運行遠端容器。 它支援 GPU/CUDA 加速，讓您可以將任意 Python 程式碼擴展到數百台機器，輕鬆部署功能和任務佇列，以及跨各種雲端提供者（包括裸機提供者）分配工作負載。

特徵：

- 將工作負載擴展到數百台機器（有 GPU 支援！）
- 直接從 Python 解釋器即時運行遠端容器
- 在多個雲端提供者之間分配工作負載
- 使用簡單的 Python 抽象輕鬆部署任務佇列和函數

我們在 [Beam](https://beam.cloud) 內部使用 beta9 為使用者大規模運行 AI 應用程式。

# 開始吧

## 束雲（建議）

開始使用 Beam 最快、最可靠的方法是免費註冊 [Beam Cloud](https://beam.cloud)。 前 10 小時免費使用，之後依使用情況付費。

## 開源部署（進階）

#### 設定伺服器

k3d 用於本地開發。 您需要 Docker 和 Make 才能開始。

要使用我們的全自動設置，請執行“setup”make 目標。

> [!NOTE]
> 這將覆蓋您可能已經安裝的一些工具。 查看 [setup.sh](bin/setup.sh) 以了解更多資訊。

```
make setup
```

#### 設定 SDK

SDK 是用 Python 寫的。 您需要 Python 3.8 或更高版本。 使用 `setup-sdk` make 目標開始。

> [!NOTE]
> 這將安裝 Poetry 套件管理器。

```
make setup-sdk
```

#### 使用 SDK

設定伺服器和 SDK 後，請查看 SDK 自述文件 [此處](sdk/README.md)。

# 範例應用程式

```python
from beta9 import function


@function(cpu=8)
def square(i: int):
     return i**2


def main():
     numbers = list(range(10))
     squared = []

      # 為清單中的每個項目執行遠端容器
     for result in square.map(numbers):
         squared.append(result)
```

## 怎麼運作的

Beta9 旨在快速啟動遠端無伺服器容器。 有幾件事使這一切成為可能：

- 由 S3/FUSE 支援的自訂延遲載入影像格式 (CLIP)
- 快速的、基於 redis 的容器調度引擎
- 用於快取映像和檔案的內容尋址存儲
- 自訂 runc 容器運行時

![示範 gif](sdk/docs/demo.gif)

## 貢獻

我們歡迎貢獻，無論大小！ 這些是對我們最有幫助的事：

- 在我們的路線圖中對功能進行排名
- 開啟 PR
- 提交[功能請求](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=)

## 哲學

我們的使命是簡化雲端的複雜性。 為此，我們建立了一個 Python-first 抽象，用於在 GPU 上啟動無伺服器容器。

我們認為，現有的雲端供應商提供的工具過於臃腫和複雜，開發人員無法快速迭代。

Beam 是設定 Kubernetes 叢集或啟動雲端虛擬機器的替代方案。

Beam 為您提供了在雲端 GPU 上運行程式碼、在 API 後面公開程式碼以及在應用程式上快速迭代所需的所有工具。

## 社區與支持

如果您需要支持，可以透過以下任一管道聯繫：

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(與我們的工程團隊即時聊天\)
- [GitHub 問題](https://github.com/beam-cloud/issues) \（錯誤回報、功能請求和任何與路線圖相關的內容）
- [Twitter](https://twitter.com/beam_cloud) \（版本更新）

## 感謝我們的貢獻者

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
    <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
