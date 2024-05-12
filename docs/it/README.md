<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ Runtime GPU serverless ultraveloce ✨**

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

Beta9 è una piattaforma open source per l'esecuzione di contenitori remoti direttamente da Python. Supporta l'accelerazione GPU/CUDA, consente di scalare il codice Python arbitrario su centinaia di macchine, distribuire facilmente funzioni e code di attività e distribuire carichi di lavoro tra vari provider cloud (inclusi i provider bare metal).

Caratteristiche:

- Scalabilità dei carichi di lavoro su centinaia di macchine (con supporto GPU!)
- Esegui istantaneamente contenitori remoti, direttamente dal tuo interprete Python
- Distribuire i carichi di lavoro tra più provider cloud
- Distribuisci facilmente code e funzioni di attività utilizzando semplici astrazioni Python

Utilizziamo beta9 internamente a [Beam](https://beam.cloud) per eseguire applicazioni IA per gli utenti su larga scala.

# Iniziare

## Beam Cloud (consigliato)

Il modo più rapido e affidabile per iniziare con Beam è registrarsi gratuitamente a [Beam Cloud](https://beam.cloud). Le prime 10 ore di utilizzo sono gratuite, successivamente pagherai in base all'utilizzo.

## Distribuzione open source (avanzata)

#### Configurazione del server

k3d è utilizzato per lo sviluppo locale. Avrai bisogno di Docker e Make per iniziare.

Per utilizzare la nostra configurazione completamente automatizzata, esegui il make target `setup`.

> [!NOTE]
> Ciò sovrascriverà alcuni degli strumenti che potresti aver già installato. Rivedi [setup.sh](bin/setup.sh) per saperne di più.

```
make setup
```

#### Configurazione dell'SDK

L'SDK è scritto in Python. Avrai bisogno di Python 3.8 o versione successiva. Utilizza il target make `setup-sdk` per iniziare.

> [!NOTE]
> Questo installerà il gestore pacchetti Poetry.

```
make setup-sdk
```

#### Utilizzo dell'SDK

Dopo aver configurato il server e l'SDK, consulta il file Leggimi dell'SDK [qui](sdk/README.md).

# App di esempio

```python
from beta9 import function


@function(cpu=8)
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

    # Esegui un contenitore remoto per ogni elemento nell'elenco
    for result in square.map(numbers):
        squared.append(result)
```

## Come funziona

Beta9 è progettata per avviare molto rapidamente contenitori serverless remoti. Ci sono alcune cose che lo rendono possibile:

- Un formato immagine personalizzato a caricamento lento (CLIP) supportato da S3/FUSE
- Un motore di pianificazione dei contenitori veloce e basato su Redis
- Archiviazione indirizzata al contenuto per la memorizzazione nella cache di immagini e file
- Un runtime del contenitore con esecuzione personalizzata

![gif demo](sdk/docs/demo.gif)

## Contribuire

Diamo il benvenuto ai contributi, grandi o piccoli! Queste sono le cose più utili per noi:

- Classifica le funzionalità nella nostra tabella di marcia
- Apri un PR
- Invia una [richiesta di funzionalità](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=)

## Filosofia

La nostra missione è semplificare la complessità del cloud. Per fare ciò, abbiamo creato un'astrazione basata su Python per il lancio di contenitori serverless su GPU.

A nostro avviso, i fornitori di servizi cloud esistenti forniscono strumenti troppo voluminosi e complicati perché gli sviluppatori possano iterarli rapidamente.

Beam è l'alternativa alla configurazione di un cluster Kubernetes o all'avvio di una VM cloud.

Beam ti offre tutti gli strumenti necessari per eseguire codice su GPU cloud, esporre il codice dietro un'API ed eseguire rapidamente l'iterazione sulla tua app.

## Comunità e supporto

Se hai bisogno di supporto, puoi contattarci attraverso uno di questi canali:

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(Chatta dal vivo con il nostro team di ingegneri\)
- [Problemi di GitHub](https://github.com/beam-cloud/issues) \(Segnalazioni di bug, richieste di funzionalità e qualsiasi cosa relativa alla roadmap)
- [Twitter](https://twitter.com/beam_cloud) \(Aggiornamenti sulle versioni)

## Grazie ai nostri contributori

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
   <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
