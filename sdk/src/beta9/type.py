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

    def is_complete(self) -> bool:
        return self.value in [self.Complete, self.Error, self.Cancelled, self.Timeout]


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

    The default version is python3. This defaults to python3 already in the image. If python3 does not exist in the image, then the default version of Python will be determined by the server (e.g. Python 3.10).

    Example:
        ```python
        from beta9 import Image, PythonVersion

        # with an enum
        image = Image(python_version=PythonVersion.Python310)

        # with a string
        image = Image(python_version="python3.10")
        ```
    """

    Python3 = "python3"
    Python38 = "python3.8"
    Python39 = "python3.9"
    Python310 = "python3.10"
    Python311 = "python3.11"
    Python312 = "python3.12"


PythonVersionLiteral = Literal[
    "python3.8",
    "python3.9",
    "python3.10",
    "python3.11",
    "python3.12",
]

PythonVersionAlias = Union[PythonVersion, PythonVersionLiteral]


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
    L40S = "L40S"


# Add GpuType str literals. Must copy/paste for now.
# https://github.com/python/typing/issues/781
GpuTypeLiteral = Literal[
    "",
    "any",
    "T4",
    "L4",
    "A10G",
    "A100-40",
    "A100-80",
    "H100",
    "A6000",
    "RTX4090",
    "L40S",
]

GpuTypeAlias = Union[GpuType, GpuTypeLiteral]


QUEUE_DEPTH_AUTOSCALER_TYPE = "queue_depth"
DEFAULT_AUTOSCALER_MAX_CONTAINERS = 1
DEFAULT_AUTOSCALER_TASKS_PER_CONTAINER = 1
DEFAULT_AUTOSCALER_MIN_CONTAINERS = 0


@dataclass
class Autoscaler:
    max_containers: int = DEFAULT_AUTOSCALER_MAX_CONTAINERS
    tasks_per_container: int = DEFAULT_AUTOSCALER_TASKS_PER_CONTAINER
    min_containers: int = DEFAULT_AUTOSCALER_MIN_CONTAINERS


@dataclass
class QueueDepthAutoscaler(Autoscaler):
    pass


@dataclass
class TaskPolicy:
    """
    Task policy for a function. This helps manages lifecycle of an individual task.

    Parameters:
        max_retries (int):
            The maximum number of times a task will be retried if the container crashes. Default is 3.
        timeout (int):
            The maximum number of seconds a task can run before it times out.
            Default depends on the abstraction that you are using.
            Set it to -1 to disable the timeout (this does not disable timeout for endpoints).
        ttl (int):
            The expiration time for a task in seconds. Must be greater than 0 and less than 24 hours (86400 seconds).
    """

    max_retries: int = 0
    timeout: int = 0
    ttl: int = 0


class PricingPolicyCostModel(str, Enum):
    Task = "task"
    Duration = "duration"


@dataclass
class PricingPolicy:
    max_in_flight: int = 10
    cost_model: PricingPolicyCostModel = PricingPolicyCostModel.Task
    cost_per_task: float = 0.000000000000000000
    cost_per_task_duration_ms: float = 0.000000000000000000


_AUTOSCALER_TYPES: Dict[Type[Autoscaler], str] = {
    QueueDepthAutoscaler: QUEUE_DEPTH_AUTOSCALER_TYPE,
}
