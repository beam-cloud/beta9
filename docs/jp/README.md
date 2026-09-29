<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ オープンソースのサーバーレス GPU コンテナランタイム ✨**

<p align="center">
  <a href="https://docs.beam.cloud">
    <img alt="Documentation" src="https://img.shields.io/badge/docs-quickstart-purple">
  </a>
  <a href="https://join.slack.com/t/beam-cloud/shared_invite/zt-4bf7jalhy-KI0ebGXk4vKpcDofwDjCLA">
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

# Beta9

Beta9 は、クラウドプロバイダー間でスケーラブルなサーバーレス GPU ワークロードを実行するためのオープンソースプラットフォームです。

特徴:

- GPU（または CPU）コンテナを数千にスケールアウト
- カスタム ML モデルの超高速コールドスタート
- ゼロへの自動スケールで使用した分だけ支払い
- モデルと関数の出力を保存するための柔軟な分散ストレージ
- 複数のクラウドプロバイダー間でワークロードを分散
- シンプルな Python の抽象化を使用してタスクキューと関数を簡単にデプロイ

私たちは、[Beam](https://beam.cloud)でユーザー向けに AI アプリケーションを大規模に実行するために、社内で beta9 を使用しています。

## 使用例

### サーバーレス推論エンドポイント

#### 任意の Python 関数をデコレート

```python
from beta9 import Image, endpoint


@endpoint(
    cpu=1,
    memory="16Gi",
    gpu="T4",
    image=Image(
        python_packages=[
            "vllm==0.4.1",
        ],  # これらの依存関係はリモートコンテナにインストールされます
    ),
)
def predict():
    from vllm import LLM

    prompts = ["AIの未来は"]
    llm = LLM(model="facebook/opt-125m")
    output = llm.generate(prompts)[0]

    return {"prediction": output.outputs[0].text}
```

#### クラウドにデプロイ

```bash
$ beta9 deploy app.py:predict --name llm-inference

=> Building image
=> Using cached image
=> Deploying endpoint
=> Deployed 🎉
=> Invocation details

curl -X POST 'https://app.beam.cloud/endpoint/llm-inference/v1' \
-H 'Authorization: Bearer [YOUR_AUTH_TOKEN]' \
-d '{}'
```

### 数百のコンテナにワークロードをファンアウト

```python
from beta9 import function

# このデコレータを使用すると、この関数を複数のリモートコンテナに並列化できます
@function(cpu=1, memory=128)
def square(i: int):
    return i**2


def main():
    numbers = list(range(100))
    squared = []

    # リスト内の各アイテムに対してリモートコンテナを実行
    for result in square.map(numbers):
        squared.append(result)
```

### 非同期ジョブをキューに入れる

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

# タスクを手動でキューに挿入
multiply.put(x=10)
```

## 仕組み

Beta9 は、リモートのサーバーレスコンテナを迅速に起動するように設計されています。これを可能にするいくつかの要素があります：

- S3/FUSE によってバックアップされたカスタムの遅延ローディングイメージフォーマット（[CLIP](https://github.com/beam-cloud/clip)）
- 高速な Redis ベースのコンテナスケジューリングエンジン
- イメージとファイルをキャッシュするためのコンテンツアドレス指定ストレージ
- カスタムの runc コンテナランタイム

![demo gif](sdk/docs/demo.gif)

# はじめに

## Beam Cloud（推奨）

最も迅速かつ信頼性の高い方法は、[Beam Cloud](https://beam.cloud)に無料でサインアップすることです。最初の 10 時間は無料で、その後は使用量に基づいて支払います。

## オープンソースデプロイ（上級者向け）

ローカルで Beta9 を実行するか、[Helm チャート](https://github.com/beam-cloud/beta9/tree/main/deploy/charts/beta9)を使用して既存の Kubernetes クラスターにデプロイすることができます。

### ローカル開発

#### サーバーの設定

k3d はローカル開発に使用されます。開始するには Docker と Make が必要です。

完全自動化されたセットアップを使用するには、`setup` make ターゲットを実行します。

> [!NOTE]
> これにより、既にインストールされている可能性のある一部のツールが上書きされます。詳細については、[setup.sh](bin/setup.sh)を確認してください。

```bash
make setup
```

#### SDK の設定

SDK は Python で書かれています。Python 3.8 以上が必要です。開始するには、`setup-sdk` make ターゲットを使用します。

> [!NOTE]
> これにより、Poetry パッケージマネージャーがインストールされます。

```bash
make setup-sdk
```

#### SDK の使用

サーバーと SDK の設定が完了したら、[こちら](sdk/README.md)の SDK readme を確認してください。

## 貢献

大きな貢献でも小さな貢献でも歓迎します！これらは私たちにとって最も役立つことです：

- ロードマップの機能をランク付けする
- PR を開く
- [機能リクエスト](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=)または[バグレポート](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=bug-report.md&title=)を提出する

## コミュニティ＆サポート

サポートが必要な場合は、これらのチャネルのいずれかを通じてお問い合わせください：

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-4bf7jalhy-KI0ebGXk4vKpcDofwDjCLA) \(メンテナーやコミュニティメンバーとライブチャット\)
- [GitHub issues](https://github.com/beam-cloud/issues) \(バグレポート、機能リクエスト、ロードマップに関連するものなど\)
- [Twitter](https://twitter.com/beam_cloud) \(リリースやその他の更新情報\)

## 貢献者に感謝

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
