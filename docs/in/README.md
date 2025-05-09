<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ अल्ट्राफास्ट सर्वर रहित जीपीयू रनटाइम ✨**

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

Beta9 सीधे Python से दूरस्थ कंटेनर चलाने के लिए एक ओपन-सोर्स प्लेटफ़ॉर्म है। यह GPU/CUDA त्वरण का समर्थन करता है, आपको सैकड़ों मशीनों के लिए मनमाने पायथन कोड को स्केल करने, आसानी से फ़ंक्शन और कार्य कतारों को तैनात करने और विभिन्न क्लाउड प्रदाताओं (बेयर मेटल प्रदाताओं सहित) में वर्कलोड वितरित करने की अनुमति देता है।

विशेषताएँ:

- वर्कलोड को सैकड़ों मशीनों तक बढ़ाएं (जीपीयू समर्थन के साथ!)
- तुरंत अपने पायथन दुभाषिया से दूरस्थ कंटेनर चलाएं
- कई क्लाउड प्रदाताओं में कार्यभार वितरित करें
- सरल पायथन एब्स्ट्रैक्शन का उपयोग करके कार्य कतारों और कार्यों को आसानी से तैनात करें

हम बड़े पैमाने पर उपयोगकर्ताओं के लिए AI एप्लिकेशन चलाने के लिए [बीम](https://beam.cloud) पर आंतरिक रूप से बीटा9 का उपयोग करते हैं।

# शुरू हो जाओ

## बीम क्लाउड (अनुशंसित)

बीम के साथ शुरुआत करने का सबसे तेज़ और सबसे विश्वसनीय तरीका [बीम क्लाउड](https://beam.cloud) पर निःशुल्क साइन अप करना है। आपके उपयोग के पहले 10 घंटे मुफ़्त हैं, और बाद में आप उपयोग के आधार पर भुगतान करते हैं।

## ओपन-सोर्स परिनियोजन (उन्नत)

#### सर्वर सेट करना

k3d का उपयोग स्थानीय विकास के लिए किया जाता है। आरंभ करने के लिए आपको डॉकर और मेक की आवश्यकता होगी।

हमारे पूर्णतः स्वचालित सेटअप का उपयोग करने के लिए, `सेटअप` मेक लक्ष्य चलाएँ।

> [!NOTE]
> यह आपके द्वारा पहले से इंस्टॉल किए गए कुछ टूल को ओवरराइट कर देगा। अधिक जानने के लिए [setup.sh](bin/setup.sh) की समीक्षा करें।

```
make setup
```

#### एसडीके की स्थापना

एसडीके पायथन में लिखा गया है। आपको Python 3.8 या उच्चतर की आवश्यकता होगी। आरंभ करने के लिए `setup-sdk` लक्ष्य बनाएं का उपयोग करें।

> [!NOTE]
> यह पोएट्री पैकेज मैनेजर इंस्टॉल करेगा।

```
make setup-sdk
```

#### एसडीके का उपयोग करना

सर्वर और एसडीके सेटअप करने के बाद, एसडीके रीडमी [यहां] (sdk/README.md) देखें।

# उदाहरण ऐप

```python
from beta9 import function


@function(cpu=8)
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

    # सूची में प्रत्येक आइटम के लिए एक दूरस्थ कंटेनर चलाएँ
    for result in square.map(numbers):
        squared.append(result)
```

## यह काम किस प्रकार करता है

बीटा9 को दूरस्थ सर्वर रहित कंटेनरों को बहुत तेज़ी से लॉन्च करने के लिए डिज़ाइन किया गया है। ऐसी कुछ चीज़ें हैं जो इसे संभव बनाती हैं:

- S3/FUSE द्वारा समर्थित एक कस्टम, आलसी लोडिंग छवि प्रारूप (CLIP)।
- एक तेज़, रेडिस-आधारित कंटेनर शेड्यूलिंग इंजन
- छवियों और फ़ाइलों को कैश करने के लिए सामग्री-संबोधित भंडारण
- एक कस्टम रन कंटेनर रनटाइम

![डेमो gif](sdk/docs/demo.gif)

## योगदान देना

हम योगदान का स्वागत करते हैं, चाहे बड़ा हो या छोटा! ये हमारे लिए सबसे उपयोगी चीजें हैं:

- हमारे रोडमैप में सुविधाओं को रैंक करें
- एक पीआर खोलें
- एक [फ़ीचर अनुरोध](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=)

हमारा मिशन क्लाउड की जटिलता को सरल बनाना है। ऐसा करने के लिए, हमने GPU पर सर्वर रहित कंटेनर लॉन्च करने के लिए पायथन-पहला एब्स्ट्रैक्शन बनाया है।

हमारे विचार में, मौजूदा क्लाउड प्रदाता ऐसे उपकरण प्रदान करते हैं जो डेवलपर्स के लिए जल्दी से पुनरावृत्त करने के लिए बहुत फूले हुए और जटिल हैं।

बीम कुबेरनेट्स क्लस्टर स्थापित करने या क्लाउड वीएम को स्पिन करने का विकल्प है।

बीम आपको क्लाउड जीपीयू पर कोड चलाने, एपीआई के पीछे उस कोड को उजागर करने और आपके ऐप पर तेज़ी से पुनरावृत्त करने के लिए आवश्यक सभी टूल देता है।

## ओपन-सोर्स बनाम भुगतान

यह रेपो अपाचे लाइसेंस के तहत उपलब्ध है। यदि आप क्लाउड होस्टेड संस्करण का उपयोग करना चाहते हैं, तो आप हमारे [मूल्य निर्धारण पृष्ठ](https://beam.cloud/pricing) पर जा सकते हैं।

## समुदाय का समर्थन

यदि आपको सहायता की आवश्यकता है, तो आप इनमें से किसी भी चैनल के माध्यम से संपर्क कर सकते हैं:

- [स्लैक](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(हमारी इंजीनियरिंग टीम के साथ लाइव चैट करें\)
- [गिटहब मुद्दे](https://github.com/beam-cloud/issues) \(बग रिपोर्ट, फीचर अनुरोध और रोडमैप से संबंधित कुछ भी)
- [ट्विटर](https://twitter.com/beam_cloud) \(रिलीज़ पर अपडेट)

## हमारे योगदानकर्ताओं को धन्यवाद

<a href='https://github.com/ssai-labs/get-beam/graphs/contributors'>
   <img src='https://contrib.rocks/image?repo=slai-labs/get-beam' />
</a>
