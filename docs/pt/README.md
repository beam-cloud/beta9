<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ Tempo de execução de GPU sem servidor ultrarrápido ✨**

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

Beta9 é uma plataforma de código aberto para executar contêineres remotos diretamente do Python. Ele oferece suporte à aceleração GPU/CUDA, permite dimensionar código Python arbitrário para centenas de máquinas, implantar facilmente funções e filas de tarefas e distribuir cargas de trabalho entre vários provedores de nuvem (incluindo provedores bare metal).

Características:

- Expanda cargas de trabalho para centenas de máquinas (com suporte de GPU!)
- Execute contêineres remotos instantaneamente, diretamente do seu interpretador Python
- Distribua cargas de trabalho entre vários provedores de nuvem
- Implante facilmente filas de tarefas e funções usando abstrações simples de Python

Usamos o beta9 internamente no [Beam](https://beam.cloud) para executar aplicativos de IA para usuários em grande escala.

# Iniciar

## Beam Cloud (recomendado)

A maneira mais rápida e confiável de começar a usar o Beam é inscrevendo-se gratuitamente no [Beam Cloud](https://beam.cloud). Suas primeiras 10 horas de uso são gratuitas e depois você paga com base no uso.

## Implantação de código aberto (avançado)

#### Configurando o servidor

k3d é usado para desenvolvimento local. Você precisará do Docker e do Make para começar.

Para usar nossa configuração totalmente automatizada, execute o make target `setup`.

> [!NOTE]
> Isso substituirá algumas das ferramentas que você já pode ter instaladas. Revise o [setup.sh](bin/setup.sh) para saber mais.

```
make setup
```

#### Configurando o SDK

O SDK é escrito em Python. Você precisará do Python 3.8 ou superior. Use o alvo `setup-sdk` para começar.

> [!NOTE]
> Isso instalará o gerenciador de pacotes Poetry.

```
make setup-sdk
```

#### Usando o SDK

Depois de configurar o servidor e o SDK, confira o leia-me do SDK [aqui](sdk/README.md).

# Exemplo de aplicativo

```python
from beta9 import function


@function(cpu=8)
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

    # Executa um container remoto para cada item da lista
    for result in square.map(numbers):
        squared.append(result)
```

## Como funciona

Beta9 foi projetado para lançar contêineres remotos sem servidor muito rapidamente. Existem algumas coisas que tornam isso possível:

- Um formato de imagem de carregamento lento (CLIP) personalizado, apoiado por S3/FUSE
- Um mecanismo rápido de agendamento de contêineres baseado em redis
- Armazenamento endereçado a conteúdo para armazenar imagens e arquivos em cache
- Um tempo de execução de contêiner runc personalizado

![gif de demonstração](sdk/docs/demo.gif)

## Contribuindo

Aceitamos contribuições, grandes ou pequenas! Estas são as coisas mais úteis para nós:

- Classifique os recursos em nosso roteiro
- Abra um PR
- Envie uma [solicitação de recurso](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=)

## Filosofia

Nossa missão é simplificar a complexidade da nuvem. Para fazer isso, construímos uma abstração baseada em Python para lançar contêineres sem servidor em GPUs.

Em nossa opinião, os provedores de nuvem existentes fornecem ferramentas que são muito inchadas e complicadas para que os desenvolvedores possam iterar rapidamente.

O Beam é a alternativa para configurar um cluster Kubernetes ou ativar uma VM na nuvem.

O Beam oferece todas as ferramentas necessárias para executar código em GPUs em nuvem, expor esse código por trás de uma API e iterar rapidamente em seu aplicativo.

## Suporte da comunidade

Se precisar de suporte, você pode entrar em contato por meio de qualquer um destes canais:

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(Converse ao vivo com nossa equipe de engenharia\)
- [Problemas do GitHub](https://github.com/beam-cloud/issues) \(Relatórios de bugs, solicitações de recursos e qualquer coisa relacionada ao roteiro)
- [Twitter](https://twitter.com/beam_cloud) \(Atualizações sobre lançamentos)

## Obrigado aos nossos colaboradores

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
   <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
