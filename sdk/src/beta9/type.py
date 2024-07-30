from dataclasses import dataclass
from enum import Enum
from typing import Dict, Literal, Type, Union


class LifeCycleMethod(str, Enum):
    OnStart = "on_start"


class TaskStatus(str, Enum):
    Complete = "COMPLETE"
    Error = "ERROR"
    Pending = "PENDING"
    Running = "RUNNING"
    Cancelled = "CANCELLED"
    Retry = "RETRY"
    Timeout = "TIMEOUT"

    def __str__(self) -> str:
        return self.value


class TaskExitCode:
    SigTerm = -15
    SigKill = -9
    Success = 0
    Error = 1
    ErrorLoadingApp = 2
    Cancelled = 3
    Timeout = 4
    Disconnect = 5


class PythonVersion(str, Enum):
    """
    An enum that defines versions of Python.

    Example:
        ```python
        from beta9 import Image, PythonVersion

        # with an enum
        image = Image(python_version=PythonVersion.Python310)

        # with a string
        image = Image(python_version="python3.10")
        ```
    """

    Python38 = "python3.8"
    Python39 = "python3.9"
    Python310 = "python3.10"
    Python311 = "python3.11"
    Python312 = "python3.12"


class GpuType(str, Enum):
    """
    An enum that defines types of GPUs.

    Example:
        ```python
        from beta9 import GpuType, function

        @function(gpu=GpuType.T4)
        def some_func()
            print("I will run on a T4 gpu!")

        # This is equivalent to the above ^
        @function(gpu="T4")
        def some_other_func()
            print("I will run on a T4 gpu!")
        ```
    """

    NoGPU = ""
    Any = "any"
    T4 = "T4"
    L4 = "L4"
    A10G = "A10G"
    A100_40 = "A100-40"
    A100_80 = "A100-80"
    H100 = "H100"
    A6000 = "A6000"
    RTX4090 = "RTX4090"


# Add GpuType str literals. Must copy/paste for now.
# https://github.com/python/typing/issues/781
GpuTypeLiteral = Literal[
    "", "any", "T4", "L4", "A10G", "A100_40", "A100_80", "H100", "A6000", "RTX4090"
]

GpuTypeAlias = Union[GpuType, GpuTypeLiteral]


QUEUE_DEPTH_AUTOSCALER_TYPE = "queue_depth"
DEFAULT_AUTOSCALER_MAX_CONTAINERS = 1
DEFAULT_AUTOSCALER_TASKS_PER_CONTAINER = 1


@dataclass
class Autoscaler:
    max_containers: int = DEFAULT_AUTOSCALER_MAX_CONTAINERS
    tasks_per_container: int = DEFAULT_AUTOSCALER_TASKS_PER_CONTAINER


@dataclass
class QueueDepthAutoscaler(Autoscaler):
    pass


_AUTOSCALER_TYPES: Dict[Type[Autoscaler], str] = {
    QueueDepthAutoscaler: QUEUE_DEPTH_AUTOSCALER_TYPE,
}
