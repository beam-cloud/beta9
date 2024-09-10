from typing import Any, Callable, List, Optional, Union

from ....abstractions.base.runner import BOT_DEPLOYMENT_STUB_TYPE, BOT_STUB_TYPE, RunnerAbstraction
from ....abstractions.image import Image
from ....abstractions.volume import Volume
from ....channel import with_grpc_error_handling
from ....clients.bot import (
    BotServiceStub,
)
from ....env import is_local
from ....sync import FileSyncer
from ....type import GpuType, GpuTypeAlias, TaskPolicy


class BotTransition:
    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        timeout: int = 180,
        keep_warm_seconds: int = 180,
        max_pending_tasks: int = 100,
        on_start: Optional[Callable] = None,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
        callback_url: Optional[str] = None,
        task_policy: TaskPolicy = TaskPolicy(),
    ):
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            timeout=timeout,
            retries=0,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            on_start=on_start,
            volumes=volumes,
            secrets=secrets,
            name=name,
            callback_url=callback_url,
            task_policy=task_policy,
        )


class _CallableWrapper:
    deployment_stub_type = BOT_DEPLOYMENT_STUB_TYPE
    base_stub_type = BOT_STUB_TYPE

    def __init__(self, func: Callable, parent: BotTransition):
        self.func: Callable = func
        self.parent: BotTransition = parent

    def __call__(self, *args, **kwargs) -> Any:
        if not is_local():
            return self.local(*args, **kwargs)

        raise NotImplementedError("Direct calls to Bots are not supported.")

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    @with_grpc_error_handling
    def serve(self, timeout: int = 0):
        pass


class Bot(RunnerAbstraction):
    """
    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (GpuTypeAlias):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        image (Union[Image, dict]):
            The container image used for the task execution. Default is [Image](#image).
        volumes (Optional[List[Volume]]):
            A list of volumes to be mounted to the container. Default is None.
        secrets (Optional[List[str]):
            A list of secrets that are injected into the container as environment variables. Default is [].
        name (Optional[str]):
            A name for the bot. Default is None.
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        callback_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            volumes=volumes,
            secrets=secrets,
            callback_url=callback_url,
        )

        self.task_id = ""
        self._bot_stub: Optional[BotServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)

    @property
    def stub(self) -> BotServiceStub:
        if not self._bot_stub:
            self._bot_stub = BotServiceStub(self.channel)
        return self._bot_stub

    @stub.setter
    def stub(self, value: BotServiceStub) -> None:
        self._bot_stub = value

    def transition(self, *args, **kwargs) -> BotTransition:
        """
        Decorator which allows you to create a network of related actions, for
        Tasks are invoked asynchronously.

        Parameters:
            cpu (Union[int, float, str]):
                The number of CPU cores allocated to the container. Default is 1.0.
            memory (Union[int, str]):
                The amount of memory allocated to the container. It should be specified in
                MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
            gpu (Union[GpuType, str]):
                The type or name of the GPU device to be used for GPU-accelerated tasks. If not
                applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
            image (Union[Image, dict]):
                The container image used for the task execution. Default is [Image](#image).
            volumes (Optional[List[Volume]]):
                A list of volumes to be mounted to the endpoint. Default is None.
            timeout (Optional[int]):
                The maximum number of seconds a task can run before it times out.
                Default is 3600. Set it to -1 to disable the timeout.
            retries (Optional[int]):
                The maximum number of times a task will be retried if the container crashes. Default is 3.
            keep_warm_seconds (int):
                The duration in seconds to keep the task queue warm even if there are no pending
                tasks. Keeping the queue warm helps to reduce the latency when new tasks arrive.
                Default is 10s.
            max_pending_tasks (int):
                The maximum number of tasks that can be pending in the queue. If the number of
                pending tasks exceeds this value, the task queue will stop accepting new tasks.
                Default is 100.
            secrets (Optional[List[str]):
                A list of secrets that are injected into the container as environment variables. Default is [].
            callback_url (Optional[str]):
                An optional URL to send a callback to when a task is completed, timed out, or cancelled.
            task_policy (TaskPolicy):
                The task policy for the function. This helps manage the lifecycle of an individual task.
                Setting values here will override timeout and retries.
        """
        return BotTransition(*args, **kwargs)
