import asyncio
import inspect
import json
import os
from typing import Callable, Dict, List, Optional, Union

from .... import terminal
from ....abstractions.base.runner import (
    BOT_DEPLOYMENT_STUB_TYPE,
    BOT_SERVE_STUB_TYPE,
    BOT_STUB_TYPE,
    RunnerAbstraction,
)
from ....abstractions.image import Image, ImageBuildResult
from ....abstractions.volume import Volume
from ....channel import with_grpc_error_handling
from ....clients.bot import (
    BotServeKeepAliveRequest,
    BotServiceStub,
    StartBotServeRequest,
    StartBotServeResponse,
)
from ....sync import FileSyncer
from ....type import GpuType, GpuTypeAlias
from ...mixins import DeployableMixin
from .marker import BotLocation


class BotTransition:
    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        timeout: int = 180,
        keep_warm: int = 180,
        max_pending: int = 100,
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
        callback_url: Optional[str] = None,
        task_policy: Optional[str] = None,
        handler: Optional[str] = None,
        inputs: dict = {},
        outputs: dict = {},
        description: Optional[str] = None,
        bot_instance: Optional["Bot"] = None,  # Reference to parent Bot instance
    ):
        self.handler: str = handler
        self.image: Image = image
        self.image_available: bool = False
        self.image_id: str = ""

        self.config = {
            "cpu": cpu,
            "memory": memory,
            "gpu": gpu,
            "timeout": timeout,
            "keep_warm": keep_warm,
            "max_pending": max_pending,
            "volumes": volumes or [],
            "secrets": secrets or [],
            "name": name or "",
            "callback_url": callback_url or "",
            "task_policy": task_policy or "",
            "handler": handler or "",
            "inputs": {k.__name__ if isinstance(k, type) else k: v for k, v in inputs.items()},
            "outputs": {k.__name__ if isinstance(k, type) else k: v for k, v in outputs.items()},
            "description": description or "",
        }

        self.bot_instance: Optional["Bot"] = bot_instance

        if self.bot_instance.image != self.image:
            if not self._build_image_for_transition():
                return

        self.config["image_id"] = self.image_id

    def _build_image_for_transition(
        self,
    ) -> bool:
        if not self.image_available:
            terminal.detail(f"Building image for transition: {self.config}")

            image_build_result: ImageBuildResult = self.image.build()
            if image_build_result and image_build_result.success:
                self.image_available = True
                self.image_id = image_build_result.image_id
                return True
            else:
                terminal.error("Image build failed", exit=False)
                return False

    def _map_callable_to_attr(self, *, attr: str, func: Callable):
        """
        Determine the module and function name of a callable function, and cache on the class.
        This is used for passing callables to stub config.
        """
        if getattr(self, attr):
            return

        module = inspect.getmodule(func)  # Determine module / function name
        if module:
            module_file = os.path.relpath(module.__file__, start=os.getcwd()).replace("/", ".")
            module_name = os.path.splitext(module_file)[0]
        else:
            module_name = "__main__"

        function_name = func.__name__
        setattr(self, attr, f"{module_name}:{function_name}")

    def __call__(self, func: Callable) -> None:
        self._map_callable_to_attr(attr="handler", func=func)
        self.config["handler"] = getattr(self, "handler")

        transition_data = self.config.copy()
        transition_data["name"] = func.__name__

        if not hasattr(self.bot_instance, "extra"):
            self.bot_instance.extra = {}

        if "transitions" not in self.bot_instance.extra:
            self.bot_instance.extra["transitions"] = {}

        self.bot_instance.extra["transitions"][func.__name__] = transition_data


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
        model: str,
        locations: List[BotLocation],
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        volumes: Optional[List[Volume]] = None,
        secrets: Optional[List[str]] = None,
        callback_url: Optional[str] = None,
        on_start: Optional[Callable] = None,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            volumes=volumes,
            secrets=secrets,
            callback_url=callback_url,
            on_start=on_start,
        )

        self._bot_stub: Optional[BotServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)
        self.locations: List[BotLocation] = locations

        self.extra: Dict[str, Dict[str, dict]] = {}
        self.extra["model"] = model
        self.extra["locations"] = {}
        self.extra["description"] = description

        for location in self.locations:
            location_config = location.to_dict()
            print(location_config)
            self.extra["locations"][location.name] = location_config

    @property
    def bot_stub(self) -> BotServiceStub:
        if not self._bot_stub:
            self._bot_stub = BotServiceStub(self.channel)
        return self._bot_stub

    @bot_stub.setter
    def bot_stub(self, value: BotServiceStub) -> None:
        self._bot_stub = value

    def transition(self, *args, **kwargs) -> BotTransition:
        return BotTransition(*args, **kwargs, bot_instance=self)

    @with_grpc_error_handling
    def serve(self, timeout: int = 0, url_type: str = ""):
        if not self.prepare_runtime(
            func=None, stub_type=BOT_SERVE_STUB_TYPE, force_create_stub=True
        ):
            return False

        try:
            res = self.print_invocation_snippet(url_type=url_type)
            return self._serve(url=res.url, timeout=timeout)
        except KeyboardInterrupt:
            self._handle_serve_interrupt()

    def _serve(self, *, url: str, timeout: int = 0):
        ws_url = url.replace("http://", "ws://").replace("https://", "wss://")

        import websockets

        async def handle_websocket_connection():
            async with websockets.connect(
                ws_url,
                extra_headers={"Authorization": f"Bearer {self.config_context.token}"},
            ) as websocket:

                async def send_keep_alive():
                    while True:
                        self.bot_stub.bot_serve_keep_alive(
                            BotServeKeepAliveRequest(
                                stub_id=self.stub_id,
                                timeout=timeout,
                            )
                        )
                        await asyncio.sleep(timeout or 30)

                keep_alive_task = asyncio.create_task(send_keep_alive())

                async def recv_messages():
                    try:
                        while True:
                            message = await websocket.recv()
                            terminal.detail(f"Received: {message}")
                    except websockets.exceptions.ConnectionClosed as e:
                        terminal.error(f"WebSocket connection closed: {e}")
                    except asyncio.CancelledError:
                        pass

                async def send_user_input():
                    loop = asyncio.get_event_loop()

                    try:
                        while True:
                            msg = await loop.run_in_executor(
                                None, lambda: terminal.prompt(text="#")
                            )
                            if msg:
                                user_request = json.dumps({"msg": msg})
                                await websocket.send(user_request)
                    except asyncio.CancelledError:
                        pass

                recv_task = asyncio.create_task(recv_messages())
                input_task = asyncio.create_task(send_user_input())

                await asyncio.gather(recv_task, input_task)

                keep_alive_task.cancel()

        try:
            asyncio.run(handle_websocket_connection())
        except KeyboardInterrupt:
            self._handle_serve_interrupt()

        r: Optional[StartBotServeResponse] = None
        for r in self.bot_stub.start_bot_serve(
            StartBotServeRequest(
                stub_id=self.stub_id,
                timeout=timeout,
            )
        ):
            if r.output != "":
                terminal.detail(r.output, end="")

            if r.done:
                break

        if r is None or not r.done:
            terminal.error("Serve failed ❌")

        terminal.warn("Bot serve timed out. All containers have been stopped.")

    def _handle_serve_interrupt(self) -> None:
        terminal.header("Stopping all bot containers")
        terminal.print("Goodbye 👋")
        os._exit(0)
