from enum import Enum


class TriggerType(str, Enum):
    Webhook = "webhook"
    RestAPI = "rest_api"
    Schedule = "cron_job"
    ASGI = "asgi"


class PythonVersion(str, Enum):
    """
    An enum that defines versions of Python.

    Example:
        ```python
        from beam import Image, PythonVersion

        # with an enum
        image = Image(python_version=PythonVersion.Python310)

        # with a string
        image = Image(python_version="python3.10")
        ```
    """

    Python37 = "python3.7"
    Python38 = "python3.8"
    Python39 = "python3.9"
    Python310 = "python3.10"


class GpuType(str, Enum):
    """
    An enum that defines types of GPUs.

    <Info>
        GPUs L4 and A100 are coming soon. Email us at founders@beam.cloud to learn more.
    </Info>

    Example:
        ```python
        from beam import Runtime, GpuType

        r = Runtime(gpu=GpuType.T4)
        ```
    """

    NoGPU = ""
    Any = "any"
    T4 = "T4"
    L4 = "L4"
    A10G = "A10G"
    A100_40 = "A100-40"
    A100_80 = "A100-80"


class VolumeType(str, Enum):
    """
    An enum that defines types of volumes.

    Example:
        ```python
        from beam import Volume, VolumeType

        pv = Volume(
            name='my-persistent-data',
            path='./my-persistent-volume'
            volume_type=VolumeType.Persistent,
        )
        ```
    """

    Persistent = "persistent"
    Shared = "shared"


class AutoscalingType(str, Enum):
    """
    An enum that defines types of autoscaling.

    <Warning>
        This is deprecated. Please see the [RequestLatencyAutoscaler](#requestlatencyautoscaler).
    </Warning>

    Example:
        ```python
        from beam import Autoscaling, AutoscalingType

        a = Autoscaling(autoscaling_type=AutoscalingType.MaxRequestLatency)
        ```
    """

    MaxRequestLatency = "max_request_latency"


class BeamSerializeMode:
    Deploy = "deploy"
    Start = "start"
    Run = "run"
    Stop = "stop"
    Serve = "serve"
