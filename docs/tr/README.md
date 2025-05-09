<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ Ultra Hızlı Sunucusuz GPU Çalışma Zamanı ✨**

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

Beta9, uzak konteynerleri doğrudan Python'dan çalıştırmak için kullanılan açık kaynaklı bir platformdur. GPU/CUDA hızlandırmayı destekler, isteğe bağlı Python kodunu yüzlerce makineye ölçeklendirmenize, işlevleri ve görev sıralarını kolayca dağıtmanıza ve iş yüklerini çeşitli bulut sağlayıcıları (çıplak donanım sağlayıcıları dahil) arasında dağıtmanıza olanak tanır.

Özellikler:

- İş yüklerini yüzlerce makineye ölçeklendirin (GPU desteğiyle!)
- Uzak konteynerleri doğrudan Python yorumlayıcınızdan anında çalıştırın
- İş yüklerini birden fazla bulut sağlayıcıya dağıtın
- Basit Python soyutlamalarını kullanarak görev kuyruklarını ve işlevlerini kolayca dağıtın

Kullanıcılara yönelik yapay zeka uygulamalarını geniş ölçekte çalıştırmak için beta9'u dahili olarak [Beam](https://beam.cloud)'da kullanıyoruz.

# Başlamak

## Işın Bulutu (Önerilen)

Beam'i kullanmaya başlamanın en hızlı ve en güvenilir yolu, [Beam Cloud](https://beam.cloud)'a ücretsiz kaydolmaktır. İlk 10 saatlik kullanımınız ücretsiz, sonrasında kullanımınıza göre ödeme yaparsınız.

## Açık kaynak dağıtımı (Gelişmiş)

#### Sunucuyu kurma

k3d yerel kalkınma için kullanılır. Başlamak için Docker ve Make'e ihtiyacınız olacak.

Tam otomatik kurulumumuzu kullanmak için 'kurulum' make hedefini çalıştırın.

> [!NOTE]
> Bu, önceden yüklemiş olabileceğiniz bazı araçların üzerine yazacaktır. Daha fazla bilgi edinmek için [setup.sh](bin/setup.sh) dosyasını inceleyin.

```
make setup
```

#### SDK'yı ayarlama

SDK Python'da yazılmıştır. Python 3.8 veya daha yüksek bir sürüme ihtiyacınız olacak. Başlamak için `setup-sdk` make hedefini kullanın.

> [!NOTE]
> Bu, Şiir paket yöneticisini yükleyecektir.

```
make setup-sdk
```

#### SDK'yı kullanma

Sunucuyu ve SDK'yı kurduktan sonra, SDK benioku dosyasına [buradan](sdk/README.md) göz atın.

# Örnek Uygulama

```python
from beta9 import function


@function(cpu=8)
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

    # Listedeki her öğe için uzak bir kapsayıcı çalıştır
    for result in square.map(numbers):
        squared.append(result)
```

## Nasıl çalışır

Beta9, uzak sunucusuz konteynerlerin çok hızlı bir şekilde başlatılması için tasarlanmıştır. Bunu mümkün kılan birkaç şey var:

- S3/FUSE tarafından desteklenen özel, yavaş yükleme görüntü formatı (CLIP)
- Hızlı, redis tabanlı bir konteyner planlama motoru
- Görüntüleri ve dosyaları önbelleğe almak için içerik adresli depolama
- Özel bir runc konteyner çalışma zamanı

![demo gif](sdk/docs/demo.gif)

## Katkı

Büyük veya küçük katkılarınızı bekliyoruz! Bunlar bizim için en yararlı şeyler:

- Yol haritamızdaki özellikleri sıralayın
- Bir PR açın
- Bir [özellik isteği](https://github.com/beam-cloud/beta9/issues/new?signees=&labels=&projects=&template=feature-request.md&title=)

## Felsefe

Misyonumuz bulutun karmaşıklığını basitleştirmektir. Bunu yapmak için, GPU'larda sunucusuz kapsayıcıları başlatmak için Python öncelikli bir soyutlama oluşturduk.

Bizim görüşümüze göre mevcut bulut sağlayıcıları, geliştiricilerin hızlı bir şekilde yineleyemeyeceği kadar şişirilmiş ve karmaşık araçlar sağlıyor.

Beam, bir Kubernetes kümesi kurmanın veya bir bulut sanal makinesini çalıştırmanın alternatifidir.

Beam, bulut GPU'larda kod çalıştırmak, bu kodu bir API'nin arkasında kullanıma sunmak ve uygulamanızda hızlı bir şekilde yineleme yapmak için ihtiyacınız olan tüm araçları sağlar.

## Topluluk ve Destek

Desteğe ihtiyacınız varsa aşağıdaki kanallardan herhangi biri aracılığıyla bize ulaşabilirsiniz:

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(Mühendislik ekibimizle canlı sohbet edin\)
- [GitHub sorunları](https://github.com/beam-cloud/issues) \(Hata raporları, özellik istekleri ve yol haritasıyla ilgili her şey)
- [Twitter](https://twitter.com/beam_cloud) \(Yayınlarla ilgili güncellemeler)

## Katkıda bulunanlarımıza teşekkürler

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
   <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
