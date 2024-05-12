<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ 초고속 서버리스 GPU 런타임 ✨**

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

Beta9은 Python에서 직접 원격 컨테이너를 실행하기 위한 오픈 소스 플랫폼입니다. GPU/CUDA 가속을 지원하고, 임의의 Python 코드를 수백 대의 머신으로 확장하고, 함수와 작업 대기열을 쉽게 배포하고, 다양한 클라우드 공급자(베어메탈 공급자 포함)에 워크로드를 분산할 수 있습니다.

특징:

- 워크로드를 수백 대의 머신으로 확장합니다(GPU 지원 포함!)
- Python 인터프리터에서 바로 원격 컨테이너를 즉시 실행합니다.
- 여러 클라우드 제공업체에 워크로드 분산
- 간단한 Python 추상화를 사용하여 작업 대기열 및 기능을 쉽게 배포합니다.

우리는 [Beam](https://beam.cloud) 내부적으로 beta9를 사용하여 대규모 사용자를 위한 AI 애플리케이션을 실행합니다.

# 시작하다

## 빔 클라우드(권장)

Beam을 시작하는 가장 빠르고 안정적인 방법은 [Beam Cloud](https://beam.cloud)에 무료로 가입하는 것입니다. 처음 10시간은 무료로 사용할 수 있으며 이후에는 사용량에 따라 비용을 지불합니다.

## 오픈소스 배포(고급)

#### 서버 설정

k3d는 로컬 개발에 사용됩니다. 시작하려면 Docker와 Make가 필요합니다.

완전히 자동화된 설정을 사용하려면 'setup' make 타겟을 실행하세요.

> [!NOTE]
> 이렇게 하면 이미 설치되어 있는 일부 도구를 덮어쓰게 됩니다. 자세한 내용은 [setup.sh](bin/setup.sh)를 검토하세요.

```
make setup
```

#### SDK 설정

SDK는 Python으로 작성되었습니다. Python 3.8 이상이 필요합니다. 시작하려면 'setup-sdk' make 타겟을 사용하세요.

> [!NOTE]
> Poetry 패키지 관리자가 설치됩니다.

```
make setup-sdk
```

#### SDK 사용

서버와 SDK를 설정한 후 [여기](sdk/README.md)에서 SDK Readme를 확인하세요.

# 예제 앱

```python
from beta9 import function


@function(cpu=8)
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

    # 목록의 모든 항목에 대해 원격 컨테이너를 실행합니다.
    for result in square.map(numbers):
        squared.append(result)
```

## 작동 방식

Beta9은 원격 서버리스 컨테이너를 매우 빠르게 시작하도록 설계되었습니다. 이를 가능하게 하는 몇 가지 사항은 다음과 같습니다.

- S3/FUSE가 지원하는 맞춤형 지연 로딩 이미지 형식(CLIP)
- 빠른 Redis 기반 컨테이너 스케줄링 엔진
- 이미지 및 파일 캐싱을 위한 컨텐츠 주소 지정 스토리지
- 사용자 정의 실행 컨테이너 런타임

![데모 gif](sdk/docs/demo.gif)

## 기여

우리는 크든 작든 기여를 환영합니다! 다음은 우리에게 가장 도움이 되는 사항입니다.

- 로드맵의 순위 기능
- PR을 시작하세요
- [기능 요청](https://github.com/beam-cloud/beta9/issues/new?signees=&labels=&projects=&template=feature-request.md&title=)

## 철학

우리의 임무는 클라우드의 복잡성을 단순화하는 것입니다. 이를 위해 우리는 GPU에서 서버리스 컨테이너를 시작하기 위한 Python 우선 추상화를 구축했습니다.

우리가 보기에 기존 클라우드 제공업체는 개발자가 빠르게 반복하기에는 너무 비대하고 복잡한 도구를 제공합니다.

Beam은 Kubernetes 클러스터를 설정하거나 클라우드 VM을 가동하는 대신 사용할 수 있습니다.

Beam은 클라우드 GPU에서 코드를 실행하고, API 뒤에 해당 코드를 노출하고, 앱에서 빠르게 반복하는 데 필요한 모든 도구를 제공합니다.

## 커뮤니티 및 지원

지원이 필요한 경우 다음 채널 중 하나를 통해 문의할 수 있습니다.

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(엔지니어링 팀과 실시간 채팅\)
- [GitHub 문제](https://github.com/beam-cloud/issues) \(버그 보고서, 기능 요청 및 로드맵과 관련된 모든 것)
- [Twitter](https://twitter.com/beam_cloud) \(릴리스 업데이트)

## 기여자들에게 감사드립니다

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
   <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
