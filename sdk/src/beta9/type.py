from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Literal, Optional, Type, Union


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
    L40 = "L40"
    A10 = "A10"
    A10G = "A10G"
    A100 = "A100"
    A100_40 = "A100-40"
    A100_80 = "A100-80"
    A16 = "A16"
    A30 = "A30"
    A40 = "A40"
    H100 = "H100"
    H200 = "H200"
    GH200 = "GH200"
    B200 = "B200"
    B300 = "B300"
    GAUDI2 = "GAUDI2"
    A4000 = "A4000"
    A5000 = "A5000"
    A6000 = "A6000"
    RTX4000Ada = "RTX4000Ada"
    RTX4090 = "RTX4090"
    RTX5090 = "RTX5090"
    RTX6000 = "RTX6000"
    RTX6000Ada = "RTX6000Ada"
    RTXPro6000 = "RTXPro6000"
    L40S = "L40S"
    V100 = "V100"
    V100_32 = "V100-32"


# Add GpuType str literals. Must copy/paste for now.
# https://github.com/python/typing/issues/781
GpuTypeLiteral = Literal[
    "",
    "any",
    "T4",
    "L4",
    "L40",
    "A10",
    "A10G",
    "A100",
    "A100-40",
    "A100-80",
    "A16",
    "A30",
    "A40",
    "H100",
    "H200",
    "GH200",
    "B200",
    "B300",
    "GAUDI2",
    "A4000",
    "A5000",
    "A6000",
    "RTX4000Ada",
    "RTX4090",
    "RTX5090",
    "RTX6000",
    "RTX6000Ada",
    "RTXPro6000",
    "L40S",
    "V100",
    "V100-32",
]

GpuTypeAlias = Union[GpuType, GpuTypeLiteral]


def normalize_gpu_type(gpu: GpuTypeAlias) -> str:
    if isinstance(gpu, GpuType):
        return gpu.value

    value = str(gpu).strip()
    key = "".join(ch for ch in value.upper() if ch.isalnum())
    for word in ["NVIDIA", "GEFORCE", "TESLA", "QUADRO"]:
        key = key.replace(word, "")

    if key in ["", "0"]:
        return ""
    if key == "ANY":
        return GpuType.Any.value
    if "A100" in key and "80G" in key:
        return GpuType.A100_80.value
    if "A100" in key and "40G" in key:
        return GpuType.A100_40.value
    for alias, canonical in _GPU_ALIASES:
        if alias in key:
            return canonical
    return value


_GPU_ALIASES = [
    ("RTXPRO6000", GpuType.RTXPro6000.value),
    ("RTX6000ADA", GpuType.RTX6000Ada.value),
    ("RTX4000ADA", GpuType.RTX4000Ada.value),
    ("RTX6000", GpuType.RTX6000.value),
    ("RTX5090", GpuType.RTX5090.value),
    ("RTX4090", GpuType.RTX4090.value),
    ("V10032G", GpuType.V100_32.value),
    ("V10032", GpuType.V100_32.value),
    ("V100", GpuType.V100.value),
    ("GAUDI2", GpuType.GAUDI2.value),
    ("GH200", GpuType.GH200.value),
    ("B300", GpuType.B300.value),
    ("B200", GpuType.B200.value),
    ("H200", GpuType.H200.value),
    ("H100", GpuType.H100.value),
    ("L40S", GpuType.L40S.value),
    ("L40", GpuType.L40.value),
    ("L4", GpuType.L4.value),
    ("T4", GpuType.T4.value),
    ("A10080G", GpuType.A100_80.value),
    ("A10080", GpuType.A100_80.value),
    ("A10040G", GpuType.A100_40.value),
    ("A10040", GpuType.A100_40.value),
    ("A100", GpuType.A100.value),
    ("A6000", GpuType.A6000.value),
    ("A5000", GpuType.A5000.value),
    ("A4000", GpuType.A4000.value),
    ("A40", GpuType.A40.value),
    ("A30", GpuType.A30.value),
    ("A16", GpuType.A16.value),
    ("A10G", GpuType.A10G.value),
    ("A10", GpuType.A10.value),
]


@dataclass
class Pool:
    name: Optional[str] = None
    gpu: Optional[Union[GpuTypeAlias, List[GpuTypeAlias]]] = None
    nodes: Optional[int] = None
    ttl: Optional[str] = None
    max_spend: Optional[float] = None
    providers: Optional[List[str]] = None
    regions: Optional[List[str]] = None
    min_reliability: Optional[float] = None

    def gpu_values(self) -> List[str]:
        if self.gpu is None:
            return []
        if isinstance(self.gpu, list):
            return [GpuType(normalize_gpu_type(g)).value for g in self.gpu]
        if self.gpu == "":
            return []
        return [GpuType(normalize_gpu_type(self.gpu)).value]

    def _requires_reservation(self) -> bool:
        return any(
            [
                self.nodes is not None,
                self.ttl is not None,
                self.max_spend is not None,
                bool(self.providers),
                bool(self.regions),
                self.min_reliability is not None,
            ]
        )

    def validate(self) -> None:
        if self.nodes is not None and self.nodes <= 0:
            raise ValueError("Pool.nodes must be greater than 0")
        if self._requires_reservation():
            if not self.nodes:
                raise ValueError("Reserved pools require nodes")
            if not self.ttl:
                raise ValueError("Reserved pools require ttl")
            if not self.max_spend or self.max_spend <= 0:
                raise ValueError("Reserved pools require max_spend")
        if self.min_reliability is not None and not 0 <= self.min_reliability <= 1:
            raise ValueError("Pool.min_reliability must be between 0 and 1")

    def export(self, selector: str = ""):
        from .clients.gateway import PoolConfig

        self.validate()
        name = self.name or ""
        return PoolConfig(
            name=name,
            gpu=self.gpu_values(),
            nodes=self.nodes or 0,
            ttl=self.ttl or "",
            max_spend=float(self.max_spend or 0),
            providers=self.providers or [],
            regions=self.regions or [],
            min_reliability=float(self.min_reliability or 0),
            selector=selector or name,
        )


QUEUE_DEPTH_AUTOSCALER_TYPE = "queue_depth"
LLM_TOKEN_PRESSURE_AUTOSCALER_TYPE = "llm_token_pressure"
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
class LLMTokenPressureAutoscaler(Autoscaler):
    pass


@dataclass
class LLMConfig:
    model_id: str = ""
    engine: str = ""
    served_model_name: str = ""
    context_length: int = 0
    tokenizer: str = ""
    metrics_path: str = ""
    slo_tier: str = ""


@dataclass
class ServingConfig:
    app_kind: str = ""
    serving_protocol: str = ""
    llm: Optional[LLMConfig] = None


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
    LLMTokenPressureAutoscaler: LLM_TOKEN_PRESSURE_AUTOSCALER_TYPE,
}
