from enum import Enum


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
