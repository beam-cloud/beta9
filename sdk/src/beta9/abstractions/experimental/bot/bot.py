import os
import threading
from typing import Callable, List, Optional, Union

from .... import terminal
from ....abstractions.base.runner import (
    BOT_DEPLOYMENT_STUB_TYPE,
    BOT_SERVE_STUB_TYPE,
    BOT_STUB_TYPE,
    RunnerAbstraction,
)
from ....abstractions.image import Image
from ....abstractions.volume import Volume
from ....channel import with_grpc_error_handling
from ....clients.bot import (
    BotServeKeepAliveRequest,
    BotServiceStub,
    StartBotServeRequest,
    StartBotServeResponse,
    StopBotServeRequest,
)
from ....sync import FileSyncer
from ....type import GpuType, GpuTypeAlias, TaskPolicy
from ...mixins import DeployableMixin


class BotTransition:
    # TODO convert to dataclass
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
        pass


class Bot(RunnerAbstraction, DeployableMixin):
    deployment_stub_type = BOT_DEPLOYMENT_STUB_TYPE
    base_stub_type = BOT_STUB_TYPE

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
        places: List[str],
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

        self._bot_stub: Optional[BotServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)
        self.places: List[str] = places

    @property
    def bot_stub(self) -> BotServiceStub:
        if not self._bot_stub:
            self._bot_stub = BotServiceStub(self.channel)
        return self._bot_stub

    @bot_stub.setter
    def bot_stub(self, value: BotServiceStub) -> None:
        self._bot_stub = value

    def transition(self, *args, **kwargs) -> BotTransition:
        pass

    @with_grpc_error_handling
    def serve(self, timeout: int = 0):
        if not self.prepare_runtime(
            func=None, stub_type=BOT_SERVE_STUB_TYPE, force_create_stub=True
        ):
            return False

        try:
            with terminal.progress("Serving endpoint..."):
                base_url = self.settings.api_host
                if not base_url.startswith(("http://", "https://")):
                    base_url = f"http://{base_url}"

                invocation_url = f"{base_url}/{self.base_stub_type}/id/{self.stub_id}"
                self.print_invocation_snippet(invocation_url=invocation_url)

                return self._serve(dir=os.getcwd(), object_id=self.object_id, timeout=timeout)

        except KeyboardInterrupt:
            self._handle_serve_interrupt()

    def _serve(self, *, dir: str, object_id: str, timeout: int = 0):
        def notify(*_, **__):
            self.bot_stub.bot_serve_keep_alive(
                BotServeKeepAliveRequest(
                    stub_id=self.stub_id,
                    timeout=timeout,
                )
            )

        threading.Thread(
            target=self.sync_dir_to_workspace,
            kwargs={"dir": dir, "object_id": object_id, "on_event": notify},
            daemon=True,
        ).start()

        r: Optional[StartBotServeResponse] = None
        for r in self.bot_stub.start_bot_serve(
            StartBotServeRequest(
                stub_id=self.stub_id,
                timeout=timeout,
            )
        ):
            if r.output != "":
                terminal.detail(r.output, end="")

            if r.done or r.exit_code != 0:
                break

        if r is None or not r.done or r.exit_code != 0:
            terminal.error("Serve container failed ❌")

        terminal.warn("Bot serve timed out. All container have been stopped.")

    def _handle_serve_interrupt(self) -> None:
        terminal.header("Stopping all bot containers")
        self.bot_stub.stop_bot_serve(StopBotServeRequest(stub_id=self.stub_id))
        terminal.print("Goodbye 👋")
        os._exit(0)
