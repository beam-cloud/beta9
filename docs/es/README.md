<div align="center">
<p align="center">
<img alt="Logo" src="https://github.com/beam-cloud/beta9/assets/10925686/a23019e2-3a34-4efa-9ac7-033c83f528cf"/ width="20%">
</p>

---

### **✨ Tiempo de ejecución de GPU serverless ultrarrápido ✨**

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

Beta9 es una plataforma de código abierto para ejecutar contenedores remotos directamente desde Python. Admite aceleración GPU/CUDA, le permite escalar código Python arbitrario a cientos de máquinas, implementar fácilmente funciones y colas de tareas y distribuir cargas de trabajo entre varios proveedores de nube (incluidos proveedores bare metal).

Características:

- Amplíe las cargas de trabajo a cientos de máquinas (¡con soporte para GPU!)
- Ejecute contenedores remotos instantáneamente, directamente desde su intérprete de Python
- Distribuir cargas de trabajo entre múltiples proveedores de nube.
- Implemente fácilmente colas de tareas y funciones utilizando abstracciones simples de Python

Usamos beta9 internamente en [Beam](https://beam.cloud) para ejecutar aplicaciones de IA para usuarios a escala.

# Empezar

## Nube de haz (recomendado)

La forma más rápida y confiable de comenzar a utilizar Beam es registrándose de forma gratuita en [Beam Cloud](https://beam.cloud). Tus primeras 10 horas de uso son gratuitas y luego pagas según el uso.

## Implementación de código abierto (avanzado)

#### Configurando el servidor

k3d se utiliza para el desarrollo local. Necesitará Docker y Make para comenzar.

Para utilizar nuestra configuración totalmente automatizada, ejecute `setup` make target.

> [!NOTE]
> Esto sobrescribirá algunas de las herramientas que quizás ya tengas instaladas. Revise [setup.sh](bin/setup.sh) para obtener más información.

```
make setup
```

#### Configurar el SDK

El SDK está escrito en Python. Necesitará Python 3.8 o superior. Utilice el objetivo `setup-sdk` para comenzar.

> [!NOTE]
> Esto instalará el administrador de paquetes de Poetry.

```
make setup-sdk
```

#### Usando el SDK

Después de haber configurado el servidor y el SDK, consulte el archivo Léame del SDK [aquí] (sdk/README.md).

# Aplicación de ejemplo

```python
from beta9 import function


@function(cpu=8)
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

    # Run a remote container for every item in list
    for result in square.map(numbers):
        squared.append(result)
```

## Cómo funciona

Beta9 está diseñado para lanzar contenedores remotos sin servidor muy rápidamente. Hay algunas cosas que hacen esto posible:

- Un formato de imagen de carga diferida (CLIP) personalizado respaldado por S3/FUSE
- Un motor de programación de contenedores rápido basado en Redis
- Almacenamiento dirigido a contenido para almacenar en caché imágenes y archivos
- Un tiempo de ejecución de contenedor runc personalizado

![gif de demostración](sdk/docs/demo.gif)

## Contribuyendo

¡Agradecemos las contribuciones, grandes o pequeñas! Estas son las cosas más útiles para nosotros:

- Clasificar funciones en nuestra hoja de ruta
- Abrir un PR
- Envíe una [solicitud de función](https://github.com/beam-cloud/beta9/issues/new?assignes=&labels=&projects=&template=feature-request.md&title=)

## Filosofía

Nuestra misión es simplificar la complejidad de la nube. Para hacer esto, hemos creado una abstracción basada en Python para lanzar contenedores sin servidor en GPU.

En nuestra opinión, los proveedores de nube existentes proporcionan herramientas que están demasiado infladas y complicadas para que los desarrolladores puedan iterar rápidamente.

Beam es la alternativa a configurar un clúster de Kubernetes o poner en marcha una máquina virtual en la nube.

Beam le brinda todas las herramientas que necesita para ejecutar código en GPU en la nube, exponer ese código detrás de una API e iterar rápidamente en su aplicación.

## Soporte comunitario

Si necesita ayuda, puede comunicarse con cualquiera de estos canales:

- [Slack](https://join.slack.com/t/beam-cloud/shared_invite/zt-2f16bwiiq-oP8weCLWNrf_9lJZIDf0Fg) \(Chatea en vivo con nuestro equipo de ingeniería\)
- [Problemas de GitHub](https://github.com/beam-cloud/issues) \(Informes de errores, solicitudes de funciones y cualquier hoja de ruta relacionada)
- [Twitter](https://twitter.com/beam_cloud) \(Actualizaciones sobre lanzamientos)

## Gracias a nuestros colaboradores

<a href="https://github.com/slai-labs/get-beam/graphs/contributors">
   <img src="https://contrib.rocks/image?repo=slai-labs/get-beam" />
</a>
