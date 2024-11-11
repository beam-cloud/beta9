import inspect
import json
import os
import sys
import threading
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from pydantic import BaseModel

from .... import env, terminal
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
    BotServiceStub,
)
from ....sync import FileSyncer
from ....type import GpuType, GpuTypeAlias, PythonVersion
from ...mixins import DeployableMixin
from .marker import BotLocation


class BotEventType(str, Enum):
    AGENT_MESSAGE = "agent_message"
    USER_MESSAGE = "user_message"
    MEMORY_MESSAGE = "memory_message"
    SESSION_CREATED = "session_created"
    TRANSITION_FIRED = "transition_fired"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"


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
        secrets: Optional[List[str]] = None,
        name: Optional[str] = None,
        callback_url: Optional[str] = None,
        task_policy: Optional[str] = None,
        handler: Optional[str] = None,
        inputs: dict = {},
        outputs: dict = {},
        description: Optional[str] = None,
        expose: bool = True,
        bot_instance: Optional["Bot"] = None,  # Reference to parent Bot instance
    ):
        self.handler: str = handler
        self.image: Image = image
        self.image_available: bool = False
        self.image_id: str = ""

        if not isinstance(inputs, dict):
            raise ValueError("Inputs must be a dictionary.")

        if not inputs:
            raise ValueError("Inputs cannot be empty.")

        for key, value in inputs.items():
            if not (isinstance(key, type) and issubclass(key, BaseModel)):
                raise ValueError(
                    f"Invalid key in inputs: {key}. All keys must be classes inherited from a Pydantic BaseModel."
                )

            if not isinstance(value, int):
                raise ValueError(
                    f"Invalid value in inputs for key {key}: {value}. All values must be integers."
                )

        self.config = {
            "cpu": cpu,
            "memory": memory,
            "gpu": gpu,
            "timeout": timeout,
            "keep_warm": keep_warm,
            "max_pending": max_pending,
            "secrets": secrets or [],
            "name": name or "",
            "callback_url": callback_url or "",
            "task_policy": task_policy or "",
            "handler": handler or "",
            "python_version": self.image.python_version or PythonVersion.Python310,
            "inputs": {
                k.__name__ if isinstance(k, type) and env.is_local() else k: v
                for k, v in inputs.items()
            },
            "outputs": {
                k.__name__ if isinstance(k, type) and env.is_local() else k: v
                for k, v in outputs.items()
            },
            "description": description or "",
            "expose": expose,
        }

        self.bot_instance: Optional["Bot"] = bot_instance

        if self.bot_instance.image != self.image or self.bot_instance.image_id == "":
            if env.is_remote():
                return

            if not self._build_image_for_transition():
                return

        self.config["image_id"] = self.image_id

    def _build_image_for_transition(
        self,
    ) -> bool:
        if not self.image_available:
            terminal.detail(f"Building image for transition: {self.image}")

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

    def local(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def __call__(self, *args, **kwargs) -> None:
        if env.called_on_import():
            self.func = args[0]
            return self

        if not env.is_local():
            return self.local(*args, **kwargs)

        self.func = args[0]
        self._map_callable_to_attr(attr="handler", func=self.func)
        self.config["handler"] = getattr(self, "handler")

        transition_data = self.config.copy()
        transition_data["name"] = self.func.__name__

        if not hasattr(self.bot_instance, "extra"):
            self.bot_instance.extra = {}

        if "transitions" not in self.bot_instance.extra:
            self.bot_instance.extra["transitions"] = {}

        self.bot_instance.extra["transitions"][self.func.__name__] = transition_data
        return self


class Bot(RunnerAbstraction, DeployableMixin):
    """
    Parameters:
        model (Optional[str]):
            Which model to use for the bot. Default is "gpt-4o".
        locations (Optional[List[BotLocation]]):
            A list of locations where the bot can store markers. Default is [].
        description (Optional[str]):
            A description of the bot. Default is None.
        volumes (Optional[List[Volume]]):
            A list of volumes to mount in bot transitions. Default is None.
    """

    deployment_stub_type = BOT_DEPLOYMENT_STUB_TYPE
    base_stub_type = BOT_STUB_TYPE

    VALID_MODELS = [
        "gpt-4o",
        "gpt-4o-mini",
        "gpt-4",
        "gpt-3.5-turbo",
        "gpt-3.5-turbo-instruct",
        "gpt-3.5-turbo-16k",
        "gpt-3.5-turbo-0613",
        "gpt-4-0613",
    ]

    def __init__(
        self,
        model: str = "gpt-4o",
        locations: List[BotLocation] = [],
        description: Optional[str] = None,
        volumes: Optional[List[Volume]] = None,
    ) -> None:
        super().__init__(volumes=volumes)

        if model not in self.VALID_MODELS:
            raise ValueError(
                f"Invalid model name: {model}. We currently only support: {', '.join(self.VALID_MODELS)}"
            )

        self.is_websocket = True
        self._bot_stub: Optional[BotServiceStub] = None
        self.syncer: FileSyncer = FileSyncer(self.gateway_stub)
        self.locations: List[BotLocation] = locations
        self.parent: "Bot" = self

        self.extra: Dict[str, Dict[str, dict]] = {}
        self.extra["model"] = model
        self.extra["locations"] = {}
        self.extra["description"] = description

        for location in self.locations:
            location_config = location.to_dict()
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

    def set_handler(self, handler: str):
        self.handler = handler

    def func(self, *args: Any, **kwargs: Any):
        pass

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
        def _connect_to_session():
            session_event = threading.Event()
            msg_event = threading.Event()

            import websocket

            def on_message(ws, message):
                event = json.loads(message)
                event_type = event.get("type")
                event_value = event.get("value")

                def _print_bot_event(header_text=None, detail_text=None):
                    if header_text:
                        sys.stdout.write("\r\033")
                        terminal.header(header_text)
                        sys.stdout.write("#: ")
                        sys.stdout.flush()

                    if detail_text:
                        msg_event.set()
                        terminal.detail(detail_text)

                if event_type == BotEventType.SESSION_CREATED:
                    session_id = event_value
                    terminal.header(f"Session started: {session_id}")
                    terminal.header("💬 Chat with your bot below...")
                    session_event.set()  # Signal that session_id is received

                elif event_type in [
                    BotEventType.AGENT_MESSAGE,
                    BotEventType.TASK_STARTED,
                    BotEventType.TASK_COMPLETED,
                    BotEventType.TASK_FAILED,
                    BotEventType.TRANSITION_FIRED,
                ]:
                    header_map = {
                        BotEventType.AGENT_MESSAGE: None,
                        BotEventType.TASK_STARTED: f"Task started: {event_value}",
                        BotEventType.TASK_COMPLETED: f"Task completed: {event_value}",
                        BotEventType.TASK_FAILED: f"Task failed: {event_value}",
                        BotEventType.TRANSITION_FIRED: f"Transition fired: {event_value}",
                    }

                    _print_bot_event(
                        header_text=header_map[event_type],
                        detail_text=event_value
                        if event_type == BotEventType.AGENT_MESSAGE
                        else None,
                    )
                else:
                    terminal.detail(f"{message}")

            def on_error(ws, error):
                pass

            def on_close(ws, close_status_code, close_msg):
                pass

            def on_open(ws):
                def _send_user_input():
                    with terminal.progress("Waiting for session to start..."):
                        session_event.wait()  # Wait until a session_id is received

                    while True:
                        msg = terminal.prompt(text="#")
                        if msg:
                            user_request = json.dumps({"msg": msg})
                            ws.send(user_request)

                            with terminal.progress(""):
                                msg_event.wait()
                                msg_event.clear()

                threading.Thread(target=_send_user_input, daemon=True).start()

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                header={"Authorization": f"Bearer {self.config_context.token}"},
            )
            ws.run_forever()

        try:
            _connect_to_session()
        except KeyboardInterrupt:
            self._handle_serve_interrupt()

        terminal.warn("Bot serve session exited. All containers have been stopped.")

    def _handle_serve_interrupt(self) -> None:
        terminal.header("Stopping all bot containers")
        terminal.print("Goodbye 👋")
        os._exit(0)
