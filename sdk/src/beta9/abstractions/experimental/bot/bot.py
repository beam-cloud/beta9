import inspect
import json
import os
import threading
import uuid
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
    TRANSITION_MESSAGE = "transition_message"
    MEMORY_MESSAGE = "memory_message"
    MEMORY_UPDATED = "memory_updated"
    SESSION_CREATED = "session_created"
    TRANSITION_FIRED = "transition_fired"
    TRANSITION_STARTED = "transition_started"
    TRANSITION_COMPLETED = "transition_completed"
    TRANSITION_FAILED = "transition_failed"
    NETWORK_STATE = "network_state"
    CONFIRM_TRANSITION = "confirm_transition"
    ACCEPT_TRANSITION = "accept_transition"
    REJECT_TRANSITION = "reject_transition"
    OUTPUT_FILE = "output_file"
    INPUT_FILE_REQUEST = "input_file_request"
    INPUT_FILE_RESPONSE = "input_file_response"
    CONFIRM_REQUEST = "confirm_request"
    CONFIRM_RESPONSE = "confirm_response"


class BotEvent(BaseModel):
    type: BotEventType
    value: str
    metadata: dict = {}


class BotTransition:
    """
    Parameters:
        cpu (Union[int, float, str]):
            The number of CPUs to allocate to the transition. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory to allocate to the transition. Default is 128.
        gpu (GpuTypeAlias):
            The type of GPU to allocate to the transition. Default is GpuType.NoGPU.
        image (Image):
            The image to use for the transition. Default is an empty Image.
        secrets (Optional[List[str]]):
            A list of secrets to pass to the transition. Default is None.
        callback_url (Optional[str]):
            The URL to send callback events to. Default is None.
        inputs (dict):
            A dictionary of inputs to pass to the transition. Default is {}.
        outputs (list):
            A list of outputs the transition can return. Default is [].
        description (Optional[str]):
            A description of the transition. Default is None.
        expose (bool):
            Whether or not to give the model awareness of this transition. Default is True.
        confirm (bool):
            Whether or not to ask the user for confirmation before running the transition. Default is False.
        task_policy (Optional[str]):
            The task policy to use for the transition. Default is None.
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: GpuTypeAlias = GpuType.NoGPU,
        image: Image = Image(),
        secrets: Optional[List[str]] = None,
        callback_url: Optional[str] = None,
        inputs: dict = {},
        outputs: list = [],
        description: Optional[str] = None,
        expose: bool = True,
        confirm: bool = False,
        bot_instance: Optional["Bot"] = None,  # Reference to parent Bot instance
        task_policy: Optional[str] = None,
        handler: Optional[str] = None,
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
        for v in outputs:
            if not (isinstance(v, type) and issubclass(v, BaseModel)):
                raise ValueError(
                    f"Invalid value in outputs: {v}. All values must be classes inherited from a Pydantic BaseModel."
                )

        self.config = {
            "cpu": cpu,
            "memory": memory,
            "gpu": gpu,
            "secrets": secrets or [],
            "callback_url": callback_url or "",
            "task_policy": task_policy or "",
            "handler": handler or "",
            "python_version": self.image.python_version or PythonVersion.Python310,
            "inputs": {
                k.__name__ if isinstance(k, type) and env.is_local() else k: v
                for k, v in inputs.items()
            },
            "outputs": [
                k.__name__ if isinstance(k, type) and env.is_local() else k for k in outputs
            ],
            "description": description or "",
            "expose": expose,
            "confirm": confirm,
        }

        self.bot_instance: Optional["Bot"] = bot_instance

        if self.bot_instance.image != self.image or self.bot_instance.image_id == "":
            if env.is_remote():
                return

            if not self._build_image_for_transition():
                return

        self.config["image_id"] = self.image_id

    def _build_image_for_transition(self) -> Optional[bool]:
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
        api_key (str):
            OpenAI API key to use for the bot. In the future this will support other LLM providers.
        locations (Optional[List[BotLocation]]):
            A list of locations where the bot can store markers. Default is [].
        description (Optional[str]):
            A description of the bot. Default is None.
        volumes (Optional[List[Volume]]):
            A list of volumes to mount in bot transitions. Default is None.
        authorized (bool):
            If false, allows the bot to be invoked without an auth token.
            Default is True.
        welcome_message (Optional[str]):
            A welcome message to display to a user when a new session with the bot is started. Default is None.
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
        api_key: str = "",
        locations: List[BotLocation] = [],
        description: Optional[str] = None,
        volumes: Optional[List[Volume]] = None,
        authorized: bool = True,
        welcome_message: Optional[str] = None,
    ) -> None:
        super().__init__(volumes=volumes)

        if not api_key or api_key == "":
            raise ValueError("API key is required")

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
        self.extra["api_key"] = api_key
        self.extra["authorized"] = authorized
        self.extra["welcome_message"] = welcome_message

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

            import websocket
            from prompt_toolkit import PromptSession

            def on_message(ws, message):
                event = BotEvent(**json.loads(message))
                event_type = event.type
                event_value = event.value

                if event_type == BotEventType.SESSION_CREATED:
                    session_id = event_value
                    terminal.header(f"Session started: {session_id}")
                    terminal.header("💬 Chat with your bot below...")
                    session_event.set()  # Signal that session is ready
                elif event_type == BotEventType.NETWORK_STATE:
                    pass
                else:
                    terminal.print(f"\n{json.dumps(event.model_dump(), indent=2)}")

            def on_error(ws, error):
                terminal.error(f"Error: {error}")

            def on_close(ws, close_status_code, close_msg):
                pass

            def on_open(ws):
                def _send_user_input():
                    with terminal.progress("Waiting for session to start..."):
                        session_event.wait()  # Wait until session is ready

                    session = PromptSession()
                    while True:
                        try:
                            msg = session.prompt("# ")
                            if msg:
                                ws.send(
                                    BotEvent(
                                        type=BotEventType.USER_MESSAGE,
                                        value=msg,
                                        metadata={"request_id": str(uuid.uuid4())},
                                    ).model_dump_json()
                                )
                        except KeyboardInterrupt:
                            confirm = session.prompt("# Exit chat session (y/n) ")
                            if confirm.strip().lower() == "y":
                                ws.close()
                                break
                            else:
                                continue  # Return to the prompt
                        except BaseException as e:
                            terminal.error(f"An error occurred: {e}")
                            continue

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

        _connect_to_session()

        terminal.warn("Bot serve session exited. All containers have been stopped.")

    def _handle_serve_interrupt(self) -> None:
        terminal.header("Stopping all bot containers")
        terminal.print("Goodbye 👋")
        os._exit(0)
