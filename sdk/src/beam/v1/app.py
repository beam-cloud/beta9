import copy
import json
import os
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

from marshmallow import ValidationError
from typing_extensions import TypedDict

from beam.utils.parse import compose_cpu, compose_memory, load_requirements_file
from beam.v1.serializer import AppConfiguration
from beam.v1.type import (
    AutoscalingType,
    BeamSerializeMode,
    GpuType,
    PythonVersion,
    TriggerType,
    VolumeType,
)

try:
    from importlib.metadata import version  # type: ignore
except ImportError:
    from importlib_metadata import version


workspace_root = os.path.abspath(os.curdir)
BEAM_SERIALIZE_MODE = os.getenv("BEAM_SERIALIZE_MODE", None)
sdk_version = version("beam-sdk")


def build_config(obj: Any, cls: Type) -> Union[Dict, None]:
    if isinstance(obj, cls):
        return obj.data
    elif isinstance(obj, dict):
        return cls(**obj).data
    return None


class Image:
    """
    Defines a custom container image that your code will run in.
    """

    def __init__(
        self,
        python_version: Union[PythonVersion, str] = PythonVersion.Python38,
        python_packages: Union[List[str], str] = [],
        commands: List[str] = [],
        base_image: Optional[str] = None,
    ):
        """
        Creates an Image instance.

        An Image object encapsulates the configuration of a custom container image
        that will be used as the runtime environment for executing tasks.

        Parameters:
            python_version (Union[PythonVersion, str]):
                The Python version to be used in the image. Default is
                [PythonVersion.Python38](#pythonversion).
            python_packages (Union[List[str], str]):
                A list of Python packages to install in the container image. Alternatively, a string
                containing a path to a requirements.txt can be provided. Default is [].
            commands (List[str]):
                A list of shell commands to run when building your container image. These commands
                can be used for setting up the environment, installing dependencies, etc.
                Default is [].
            base_image (Optional[str]):
                A custom base image to replace the default ubuntu20.04 image used in your container.
                For example: docker.io/library/ubuntu:20.04
                This image must contain a valid python executable that matches the version specified
                in python_version (i.e. python3.8, python3.9, etc)
                Default is None.

        Example:
            ```python
            from beam import App, Runtime, Image, PythonVersion

            image = Image(
                python_version=PythonVersion.Python38,
                python_packages=["numpy", "pandas"],
                commands=["apt update", "apt install -y libgomp1"]
            )

            app = App(name="my-app", runtime=Runtime(image=image))

            @app.task_queue()
            def my_queue():
                ...
            ```
        """
        self.python_version = python_version
        self.python_packages = python_packages
        self.commands = commands
        self.base_image = base_image
        self.base_image_creds = None

    @property
    def data(self):
        python_packages = copy.deepcopy(self.python_packages)
        if isinstance(python_packages, str):
            python_packages = load_requirements_file(python_packages)

        # We inject the current version of beam into here if does not exist
        if len([pkg for pkg in python_packages if "beam-sdk" in pkg]) == 0:
            python_packages.append(f"beam-sdk=={sdk_version}")

        return {
            "python_version": self.python_version,
            "python_packages": python_packages,
            "commands": self.commands,
            "base_image": self.base_image,
        }


class TaskPolicy:
    def __init__(
        self,
        max_retries: int = 3,
        timeout: int = 3600,
    ):
        """
        Creates a TaskPolicy instance. A task policy modifies the default retry
        and timeout behavior of any tasks created by your App.

        Parameters:
            max_retries (int):
                The maximum number of times a task can be retried. Default is 3.
            timeout (int):
                The maximum number of seconds a task can run before it times out.
                Default is 3600. Set it to -1 to disable the timeout.
        """
        self.max_retries = max_retries
        self.timeout = timeout

    @property
    def data(self) -> Dict[str, int]:
        return {
            "max_retries": self.max_retries,
            "timeout": self.timeout,
        }


class Runtime:
    """
    Defines the environment a task will be executed in.
    """

    def __init__(
        self,
        cpu: Union[int, str] = 1,
        memory: str = "500Mi",
        gpu: Union[GpuType, str] = GpuType.NoGPU,
        image: Union[Image, dict] = Image(),
    ):
        """
        Creates a Runtime instance.

        It is used to define the CPU, memory, GPU (if applicable), and the container image used for
        the task execution.

        Parameters:
            cpu (Union[int, str]):
                The number of CPU cores allocated to the container. Default is 1.
            memory (str):
                The amount of memory allocated to the container. It should be specified in
                Kubernetes resource format (e.g., 500Mi for 500 megabytes). Default is 500Mi.
            gpu (Union[GpuType, str]):
                The type or name of the GPU device to be used for GPU-accelerated tasks. If not
                applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
            image (Union[Image, dict]):
                The container image used for the task execution. Default is [Image](#image).

        Example:
            ```python
            from beam import App, Runtime, Image

            runtime = Runtime(
                cpu=4,
                memory="8Gi",
                gpu="T4",
                image=Image(python_version="python3.9"),
            )

            app = App(name="my-app", runtime=runtime)

            @app.task_queue()
            def my_queue():
                ...
            ```
        """
        self.cpu = compose_cpu(cpu)
        self.memory = compose_memory(memory)
        self.gpu = gpu
        self.image = image

    @property
    def data(self):
        return {
            "cpu": self.cpu,
            "memory": self.memory,
            "gpu": self.gpu,
            "image": build_config(self.image, Image),
        }


class Output:
    """
    Defines which file or directory to save with a task.
    """

    def __init__(self, path: str) -> None:
        """
        Creates an Output instance.

        Saves the file or directory and associates it with a task which can be downloaded at a later
        date.

        Parameters:
            path (str):
                The path of the file or directory produced during task execution.

        Example:
            ```python
            from beam import App, Runtime, Output

            app = App(name="my-app", runtime=Runtime())

            @app.task_queue(
                outputs=[Output(path="my_file.txt")]
            )
            def my_func():
                ...
            ```
        """
        self.path = path

    @property
    def data(self) -> Dict[str, str]:
        return {
            "path": self.path,
        }


class Autoscaling:
    """
    Configures autoscaling for your functions.

    <Warning>
        This is deprecated. Please see the [RequestLatencyAutoscaler](#requestlatencyautoscaler).
    </Warning>
    """

    def __init__(
        self,
        max_replicas: int = 1,
        desired_latency: float = 100,
        autoscaling_type: Union[AutoscalingType, str] = AutoscalingType.MaxRequestLatency,
    ):
        """
        Creates an Autoscaling instance.

        Use this to define an autoscaling strategy for RESTful APIs or task queues.

        Parameters:
            max_replicas (int):
                The maximum number of replicas that the autoscaler can create. It defines an upper
                limit to avoid excessive resource consumption. Default is 1.
            desired_latency (float):
                The maximum number of seconds to wait before a task is processed. Beam will add or
                remove replicas to ensure tasks are processed within this window. Default is 100.
            autoscaling_type (Union[AutoscalingType, str]):
                The type of autoscaling strategy to apply. Default is
                [AutoscalingType.MaxRequestLatency](#autoscalingtype).

        Example:
            ```python
            from beam import App, Runtime, Autoscaling

            autoscaling=Autoscaling(
                max_replicas=2,
                desired_latency=30,
            )

            app = App(name="my-app", runtime=Runtime())

            @app.task_queue(autoscaling=autoscaling)
            def my_func():
                ...
            ```
        """
        self.max_replicas = max_replicas
        self.desired_latency = desired_latency
        self.autoscaling_type = autoscaling_type

    @property
    def data(self):
        return {
            "max_replicas": self.max_replicas,
            "desired_latency": self.desired_latency,
            "autoscaling_type": self.autoscaling_type,
        }


class RequestLatencyAutoscalerDict(TypedDict, total=False):
    desired_latency: float
    max_replicas: int


class QueueDepthAutoscalerDict(TypedDict, total=False):
    max_tasks_per_replica: int
    max_replicas: int


class AutoscalerDict(TypedDict, total=False):
    request_latency: Union["RequestLatencyAutoscaler", RequestLatencyAutoscalerDict]
    queue_depth: Union["QueueDepthAutoscaler", QueueDepthAutoscalerDict]


AutoscalerType = TypeVar("AutoscalerType", "RequestLatencyAutoscaler", "QueueDepthAutoscaler")


class Autoscaler:
    @property
    def data(self) -> Dict:
        ...

    @classmethod
    def build_config(cls, instance) -> Union[Dict, None]:
        try:
            return build_config(instance, cls)
        except TypeError:
            pass
        return instance


class RequestLatencyAutoscaler(Autoscaler):
    """
    Defines a Request Latency based autoscaling strategy.
    """

    def __init__(
        self,
        *,
        desired_latency: float = 100,
        max_replicas: int = 1,
    ):
        """
        Creates a RequestLatencyAutoscaler instance.

        Parameters:
            desired_latency (float):
                The maximum number of seconds to wait before a task is processed. Beam will add or
                remove replicas to ensure tasks are processed within this window. Default is 100.
            max_replicas (int):
                The maximum number of replicas that the autoscaler can create. It defines an upper
                limit to avoid excessive resource consumption. Default is 1.

        Example:
            ```python
            from beam import App, Runtime, RequestLatencyAutoscaler

            app = App(name="my-app", runtime=Runtime())

            @app.task_queue(autoscaler=RequestLatencyAutoscaler(desired_latency=300))
            def tq1():
                pass

            @app.task_queue(autoscaler={"request_latency": {"desired_latency": 300}})
            def tq2():
                pass
            ```
        """
        self.desired_latency = desired_latency
        self.max_replicas = max_replicas

    @property
    def data(self) -> Dict[str, Dict[str, Any]]:
        return {
            "request_latency": {
                "desired_latency": self.desired_latency,
                "max_replicas": self.max_replicas,
            }
        }


class QueueDepthAutoscaler(Autoscaler):
    """
    Defines a Queue Depth based autoscaling strategy.
    """

    def __init__(
        self,
        *,
        max_tasks_per_replica: int = 0,
        max_replicas: int = 1,
    ):
        """
        Creates a QueueDepthAutoscaler instance.

        Parameters:
            max_tasks_per_replica (int):
                The max number of tasks that can be queued up to a single replica. This can help
                manage throughput and cost of compute. When `max_tasks_per_replica` is 0, a
                replica can process any number of tasks. Default is 0.
            max_replicas (int):
                The maximum number of replicas that the autoscaler can create. It defines an upper
                limit to avoid excessive resource consumption. Default is 1.

        Example:
            ```python
            from beam import App, Runtime, QueueDepthAutoscaler

            app = App(name="my-app", runtime=Runtime())

            @app.task_queue(
                autoscaler=QueueDepthAutoscaler(max_tasks_per_replica=5, max_replicas=3)
            )
            def tq1():
                pass

            @app.task_queue(
                autoscaler={"queue_depth": {"max_tasks_per_replica": 5, "max_replicas": 3}}
            )
            def tq2():
                pass
            ```
        """
        self.max_tasks_per_replica = max_tasks_per_replica
        self.max_replicas = max_replicas

    @property
    def data(self) -> Dict[str, Dict[str, Any]]:
        return {
            "queue_depth": {
                "max_tasks_per_replica": self.max_tasks_per_replica,
                "max_replicas": self.max_replicas,
            }
        }


class FunctionTrigger:
    def __init__(
        self,
        trigger_type: str,
        handler: str,
        runtime: Optional[Union[Runtime, dict]] = None,
        outputs: List[Union[Output, dict]] = [],
        autoscaler: Optional[Union[AutoscalerType, AutoscalerDict]] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        task_policy: Union[TaskPolicy, dict] = TaskPolicy(),
        workers: int = 1,
        authorized: bool = True,
        **kwargs,
    ):
        self.trigger_type = trigger_type
        self.kwargs = kwargs
        self.runtime = runtime
        self.handler = handler
        self.outputs = outputs
        self.autoscaling = kwargs.get("autoscaling", None)
        self.autoscaler = autoscaler
        self.task_policy = task_policy
        self.timeout = timeout
        self.max_retries = max_retries
        self.workers = workers
        self.authorized = authorized

    @property
    def data(self):
        task_policy = build_config(self.task_policy, TaskPolicy)

        if self.timeout is not None:
            task_policy["timeout"] = self.timeout

        if self.max_retries is not None:
            task_policy["max_retries"] = self.max_retries

        return {
            **self.kwargs,
            "handler": self.handler,
            "runtime": build_config(self.runtime, Runtime),
            "trigger_type": self.trigger_type,
            "outputs": [build_config(output, Output) for output in self.outputs],
            "autoscaling": build_config(self.autoscaling, Autoscaling),
            "autoscaler": Autoscaler.build_config(self.autoscaler),
            "task_policy": task_policy,
            "workers": self.workers,
            "authorized": self.authorized,
        }


class Run:
    """
    Defines an ephemeral task.
    """

    def __init__(
        self,
        handler: str,
        runtime: Union[Runtime, dict],
        outputs: List[Union[Output, dict]] = [],
        timeout: Optional[int] = None,
        task_policy: Union[TaskPolicy, dict] = None,
        **kwargs,
    ):
        """
        Creates a Run instance.

        See [App.run()](#app-run) for a description.

        Parameters:
            handler (str):
                The handler function or entry point of the task to be executed. It should be a
                reference to the function that will process the task.
            runtime (Union[Runtime, dict]):
                The runtime environment for the task execution. Default is None.
            outputs (List[Union[Output, dict]]):
                A list of of artifacts created during the task execution. Default is [].
            **kwargs:
                Additional keyword arguments to pass to the handler.

        Example:
            **app.py**
            ```python
            from beam import Run, Runtime

            run = Run(handler="my_func", runtime=Runtime())
            ```

            **main.py**
            ```python
            def my_fun():
                ...
            ```
        """
        self.kwargs = kwargs
        self.runtime = runtime
        self.handler = handler
        self.outputs = outputs
        self.task_policy = task_policy
        self.timeout = timeout

    @property
    def data(self):
        task_policy = build_config(self.task_policy, TaskPolicy)
        if self.timeout is not None:
            task_policy["timeout"] = self.timeout

        return {
            **self.kwargs,
            "handler": self.handler,
            "runtime": build_config(self.runtime, Runtime),
            "outputs": [build_config(output, Output) for output in self.outputs],
            "task_policy": task_policy,
        }


class Volume:
    """
    A data store that can be attached to an app.
    """

    def __init__(
        self,
        name: str,
        path: str,
        volume_type: Union[VolumeType, str] = VolumeType.Shared,
    ):
        """
        Creates a Volume instance.

        When your app runs, your volume will be available at `./{name}` and `/volumes/{name}`.

        Parameters:
            name (str):
                The name of the volume, a descriptive identifier for the data volume.
            path (str):
                The path where the volume is mounted within the app environment.
            volume_type (Union[VolumeType, str]):
                The type of volume. Default is [VolumeType.Shared](#volumetype).

        Example:
            ```python
            from beam import Volume, VolumeType

            # Shared Volume
            shared_volume = Volume(
                name='some_shared_data',
                path='./my-shared-volume'
            )

            # Persistent Volume
            persistent_volume = Volume(
                name='persistent_data',
                path='./my-persistent-volume'
                volume_type=VolumeType.Persistent,
            )
            ```
        """
        self.name = name
        self.app_path = path
        self.volume_type = volume_type

    @property
    def data(self):
        return {
            "name": self.name,
            "app_path": self.app_path,
            "mount_type": self.volume_type,
        }


class App:
    """
    Defines a namespace for your functions.
    """

    def __init__(
        self,
        name: str,
        volumes: List[Union[Volume, dict]] = [],
        runtime: Optional[Union[Runtime, dict]] = None,
    ):
        """
        Creates an App instance.

        Parameters:
            name (str):
                The unique identifier for the app.
            volumes (List[Union[Volume, dict]]):
                A list of storage volumes to be associated with the app. Default is [].
            runtime (Optional[Union[Runtime, dict]]):
                The runtime environment for the app execution. Defines the container environment and
                hardware configuration the app will run on. If not specified, a runtime will need to
                be passed inline to any function triggers attached to this app. Default is None.

        Example:
            **Running functions in an app**

            First, instantiate an `App()` object. You can then decorate any function with the app
            object and a trigger type, for example `@app.run()`:

            ```python
            from beam import App, Runtime, Image, Output, Volume

            app = App(
                name="my_app",
                runtime=Runtime(
                    cpu=1,
                    gpu="A10G",
                    image=Image(
                        python_version="python3.8",
                        python_packages=["torch"],
                        commands=["apt-get install ffmpeg"],
                    ),
                ),
                volumes=[Volume(name="my_models", path="models")],
            )

            @app.run(outputs=[Output(path="./test.txt")])
            def some_function():
                return
            ```
        """
        self.name = name
        self.volumes = []
        self.triggers = []
        self.runtime = runtime
        self.volumes = volumes

    def _function_metadata(self, func):
        f_dir = func.__code__.co_filename.replace(workspace_root, "").strip("/")
        f_name = func.__name__

        return f_dir, f_name

    def _parse_path(self, path: Union[str, None], handler: Union[str, None] = ""):
        parsed_path = path
        if parsed_path is None:
            parsed_path = handler.split(":")[1]

        if not parsed_path.startswith("/"):
            parsed_path = "/" + parsed_path

        return parsed_path

    def build_config(
        self,
        triggers: List[Union[FunctionTrigger, dict]] = [],
        run: Optional[dict] = None,
    ):
        """
        Serializes the app before running it on Beam.

        This validates that the required `App` and `Runtime` objects are registered to your
        functions correctly.
        """
        if (len(triggers) == 0) == bool(run is None):
            raise ValidationError("Provide either triggers or a run, but not both")

        runtime = self.runtime

        serialized_triggers = []
        for trigger in triggers:
            serialized_trigger = build_config(trigger, FunctionTrigger)
            if serialized_trigger is None:
                raise ValidationError("Trigger did not serialize properly.")
            if serialized_trigger["runtime"] is None and self.runtime is None:
                raise ValidationError(
                    "Runtime must be specified for all triggers if not specified at the app level"
                )
            serialized_triggers.append(serialized_trigger)

        if len(serialized_triggers) > 0 and serialized_triggers[0].get("runtime") is not None:
            runtime = serialized_triggers[0]["runtime"]

        serialized_run = None
        if run is not None:
            serialized_run = build_config(run, Run)
            if serialized_run is None:
                raise ValidationError("Run did not serialize properly.")
            if serialized_run["runtime"] is None and self.runtime is None:
                raise ValidationError(
                    "Runtime must be specified for the run if not specified at the app level"
                )

            if serialized_run.get("runtime") is not None:
                runtime = serialized_run["runtime"]

        config = {
            "app_spec_version": "v3",
            "sdk_version": sdk_version,
            "name": self.name,
            "mounts": [build_config(volume, Volume) for volume in self.volumes],
            "runtime": build_config(runtime, Runtime),
            "triggers": serialized_triggers,
            "run": serialized_run,
        }

        serializer = AppConfiguration()
        # convert orderdict to a dict that's still ordered
        return json.loads(json.dumps(serializer.load(config)))

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if BEAM_SERIALIZE_MODE in [BeamSerializeMode.Start, BeamSerializeMode.Deploy]:
            return self.build_config(triggers=self.triggers)

        elif BEAM_SERIALIZE_MODE == BeamSerializeMode.Run:
            raise ValidationError("Cannot run an app. Please run a function instead.")

    def _get_function_path(self, func: Optional[Union[Callable, str]] = None):
        if isinstance(func, str):
            return func

        if func is None:
            return None

        try:
            f_dir, f_name = self._function_metadata(func)
        except AttributeError:
            raise ValidationError(
                "Could not find function. Please make sure that the function exists"
            )
        return f"{f_dir}:{f_name}"

    def task_queue(
        self,
        runtime: Optional[Union[Runtime, dict]] = None,
        outputs: List[Union[Output, dict]] = [],
        autoscaling: Optional[Union[dict, Autoscaling]] = None,
        autoscaler: Optional[Union[AutoscalerType, AutoscalerDict]] = None,
        loader: Optional[Callable] = None,
        callback_url: Optional[str] = None,
        max_pending_tasks: int = 100,
        keep_warm_seconds: int = 10,
        max_retries: Optional[int] = None,
        timeout: Optional[int] = None,
        task_policy: Union[TaskPolicy, dict] = TaskPolicy(),
        workers: int = 1,
        authorized: bool = True,
    ):
        """
        Decorator used for deploying a task queue.

        This method allows you to add tasks to a task queue for processing. The tasks are executed
        asynchronously, and the results can be obtained later, through firing a callback using
        `callback_url`, or retrieving the results manually by calling
        `api.beam.cloud/v1/task/{task_id}/status/`.

        Parameters:
            runtime (Optional[Union[Runtime, dict]]):
                The runtime environment to execute the tasks. It can be a dictionary containing
                runtime configurations or an instance of the Runtime class. If not specified, the
                default runtime will be used. Default is None.
            outputs (List[Union[Output, dict]]):
                A list of outputs or output configurations for handling task results. Each element
                can be an Output instance or a dictionary with output configurations. Default is [].
            autoscaling (Optional[Union[dict, Autoscaling]]):
                [DEPRECATED] Autoscaling configurations for the task queue. It can be a dictionary
                containing autoscaling settings or an instance of the Autoscaling class. If not
                provided, autoscaling will not be applied. Default is None.
            autoscaler (Optional[Union[AutoscalerType, AutoscalerDict]]):
                Defines an autoscaling strategy for your workload. See
                [RequestLatencyAutoscaler](#requestlatencyautoscaler).
            loader (Optional[Callable]):
                A function that runs exactly once when the app is first started. Useful for loading
                models or performing initialization of the app. Default is None.
            callback_url (Optional[str]):
                A URL where task execution status updates will be sent. If provided, the system
                will make a single POST request to this URL with status updates for each task.
                Default is None.
            max_pending_tasks (int):
                The maximum number of tasks that can be pending in the queue. If the number of
                pending tasks exceeds this value, the task queue will stop accepting new tasks.
                Default is 100.
            keep_warm_seconds (int):
                The duration in seconds to keep the task queue warm even if there are no pending
                tasks. Keeping the queue warm helps to reduce the latency when new tasks arrive.
                Default is 10s.
            task_policy (Union[TaskPolicy, dict]):
                The task policy object where you can specify max retries and timeout
            timeout (Optional[int]):
                The maximum number of seconds a task can run before it times out.
                Default is 3600. Set it to -1 to disable the timeout.
                (Overrides task_policy.timeout)
            max_retries (Optional[int]):
                The maximum number of times a task can be retried. Default is 3.
                (Overrides task_policy.max_retries)
            workers (int):
                The number of workers to launch per container.
                Modifying this parameter can improve throughput for certain workloads.
                Workers will share the CPU, Memory, and GPU defined for your App.
                You may need to increase these values to utilize more workers.
                Default is 1.
            authorized (bool):
                Enable basic authentication for this function.
                Get your API keys here: https://docs.beam.cloud/account/api-keys
                Default is True.
        Example:
            ```python
            from beam import App, Runtime, Image, Volume

            app = App(
                name="vocalize",
                runtime=Runtime(
                    cpu=1,
                    gpu="A10G",
                    image=Image(
                        python_version="python3.8",
                        python_packages=["torch"],
                        commands=["apt-get install ffmpeg"],
                    ),
                ),
                volumes=[Volume(name="my_models", path="models")],
            )

            @app.task_queue(keep_warm_seconds=1000)
            def transcribe():
                return
            ```
        """

        def decorator(func):
            loader_path = self._get_function_path(loader)
            handler_path = self._get_function_path(func)
            endpoint_path = self._parse_path(None, handler_path)

            config_data = {
                "trigger_type": TriggerType.Webhook,
                "handler": handler_path,
                "method": "POST",
                "runtime": runtime,
                "outputs": outputs,
                "autoscaling": autoscaling,
                "autoscaler": autoscaler,
                "path": endpoint_path,
                "loader": loader_path,
                "callback_url": callback_url,
                "max_pending_tasks": max_pending_tasks,
                "keep_warm_seconds": keep_warm_seconds,
                "task_policy": task_policy,
                "max_retries": max_retries,
                "timeout": timeout,
                "workers": workers,
                "authorized": authorized,
            }

            task_queue = FunctionTrigger(**config_data)
            self.triggers.append(task_queue)

            def wrapper(*args, **kwargs):
                if BEAM_SERIALIZE_MODE in [
                    BeamSerializeMode.Start,
                    BeamSerializeMode.Deploy,
                ]:
                    return self.build_config(triggers=[task_queue])

                elif BEAM_SERIALIZE_MODE == BeamSerializeMode.Run:
                    return self.build_config(run=Run(**config_data))

                return func(*args, **kwargs)

            return wrapper

        return decorator

    def rest_api(
        self,
        runtime: Optional[Union[dict, Runtime]] = None,
        outputs: List[Union[Output, dict]] = [],
        autoscaling: Optional[Union[dict, Autoscaling]] = None,
        autoscaler: Optional[Union[AutoscalerType, AutoscalerDict]] = None,
        loader: Optional[Callable] = None,
        callback_url: Optional[str] = None,
        max_pending_tasks: int = 100,
        keep_warm_seconds: int = 90,
        max_retries: Optional[int] = None,
        timeout: Optional[int] = None,
        task_policy: Union[TaskPolicy, dict] = TaskPolicy(),
        workers: int = 1,
        authorized: bool = True,
    ):
        """
        Decorator used for deploying a RESTful API.

        This method sets up a RESTful API endpoint that can receive task submissions for processing.
        When tasks are submitted to this endpoint, they are added to a task queue and processed
        synchronously. If the task takes longer than 90 seconds to complete, it will continue to run
        asynchronously and the results can be retrieved by firing a callback using `callback_url`
        or retrieving the results manually calling `api.beam.cloud/v1/task/{task_id}/status/`.

        Parameters:
            runtime (Optional[Union[dict, Runtime]]):
                The runtime environment to execute the tasks submitted via the API. It can be a
                dictionary containing runtime configurations or an instance of the Runtime class.
                Default is None.
            outputs (List[Union[Output, dict]]):
                A list of outputs or output configurations for handling task results. Default is [].
            autoscaling (Optional[Union[dict, Autoscaling]]):
                [DEPRECATED] Autoscaling configurations for the task queue. It can be a dictionary
                containing autoscaling settings or an instance of the Autoscaling class. If not
                provided, autoscaling will not be applied. Default is None.
            autoscaler (Optional[Union[AutoscalerType, AutoscalerDict]]):
                Defines an autoscaling strategy for your workload. See
                [RequestLatencyAutoscaler](#requestlatencyautoscaler).
            loader (Optional[Callable]):
                A function that runs exactly once when the app is first started. Useful for loading
                models or performing initialization of the app. Default is None.
            callback_url (Optional[str]):
                A URL where task execution status updates will be sent. If provided, the system will
                make a single POST request to this URL with status updates for each task. Default is
                None.
            max_pending_tasks (int):
                The maximum number of tasks that can be pending in the queue before rejecting new
                submissions to the API. Default is 100.
            keep_warm_seconds (int):
                The duration in seconds to keep the container warm even if there are no pending
                tasks. Keeping the queue warm helps to reduce the latency when new tasks arrive via
                the API. Default is 90s.
            task_policy (Union[TaskPolicy, dict]):
                The task policy object where you can specify max retries and timeout
            timeout (Optional[int]):
                The maximum number of seconds a task can run before it times out.
                Default is 3600. Set it to -1 to disable the timeout.
                (Overrides task_policy.timeout)
            max_retries (Optional[int]):
                The maximum number of times a task can be retried. Default is 3.
                (Overrides task_policy.max_retries)
            workers (int):
                The number of workers to launch per container.
                Modifying this parameter can improve throughput for certain workloads.
                Workers will share the CPU, Memory, and GPU defined for your App.
                You may need to increase these values to utilize more workers.
                Default is 1.
            authorized (bool):
                Enable basic authentication for this function.
                Get your API keys here: https://docs.beam.cloud/account/api-keys
                Default is True.
        Note:
            This endpoint is secured using basic auth. See
            [our docs](https://docs.beam.cloud/deployment/authentication) for more information.

        Example:
            ```python
            from beam import App, Runtime, Image, Volume

            app = App(
                name="vocalize",
                runtime=Runtime(
                    cpu=1,
                    gpu="A10G",
                    image=Image(
                        python_version="python3.8",
                        python_packages=["torch"],
                        commands=["apt-get install ffmpeg"],
                    ),
                ),
                volumes=[Volume(name="my_models", path="models")],
            )

            @app.rest_api(keep_warm_seconds=1000)
            def transcribe():
                return
            ```
        """

        def decorator(func):
            loader_path = self._get_function_path(loader)
            handler_path = self._get_function_path(func)
            endpoint_path = self._parse_path(None, handler_path)

            config_data = {
                "trigger_type": TriggerType.RestAPI,
                "handler": handler_path,
                "method": "POST",
                "runtime": runtime,
                "outputs": outputs,
                "autoscaling": autoscaling,
                "autoscaler": autoscaler,
                "path": endpoint_path,
                "loader": loader_path,
                "callback_url": callback_url,
                "max_pending_tasks": max_pending_tasks,
                "keep_warm_seconds": keep_warm_seconds,
                "task_policy": task_policy,
                "max_retries": max_retries,
                "timeout": timeout,
                "workers": workers,
                "authorized": authorized,
            }

            rest_api = FunctionTrigger(**config_data)
            self.triggers.append(rest_api)

            def wrapper(*args, **kwargs):
                if BEAM_SERIALIZE_MODE in [
                    BeamSerializeMode.Start,
                    BeamSerializeMode.Deploy,
                ]:
                    return self.build_config(triggers=[rest_api])

                elif BEAM_SERIALIZE_MODE == BeamSerializeMode.Run:
                    return self.build_config(run=Run(**config_data))

                return func(*args, **kwargs)

            return wrapper

        return decorator

    def schedule(
        self,
        when: str,
        runtime: Optional[Union[Runtime, dict]] = None,
        outputs: List[Union[Output, dict]] = [],
        callback_url: Optional[str] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[int] = None,
        task_policy: Union[TaskPolicy, dict] = TaskPolicy(),
    ):
        """
        Decorator used for scheduling a task.

        This method is used to add configuration for scheduling a task for future execution at a
        specified time or interval. The task will be added to a scheduler, and when the
        scheduled time or interval is reached, the task will be executed asynchronously.

        Parameters:
            when (str):
                The scheduling time or interval for the task execution. It can be a cron or string
                expressions. You can use [Crontab](https://crontab.guru/) to help generate
                expressions.
            runtime (Optional[Union[Runtime, dict]]):
                The runtime environment to execute the scheduled task. Default is None.
            outputs (List[Union[Output, dict]]):
                A list of outputs for handling the results of the scheduled task. Default is [].
            callback_url (Optional[str]):
                A URL where task execution status updates will be sent. If provided, the system will
                make a single POST request to this URL with status updates for each task. Default is
                None.
            task_policy (Union[TaskPolicy, dict]):
                The task policy object where you can specify max retries and timeout
            timeout (Optional[int]):
                The maximum number of seconds a task can run before it times out.
                Default is 3600. Set it to -1 to disable the timeout.
                (Overrides task_policy.timeout)
            max_retries (Optional[int]):
                The maximum number of times a task can be retried. Default is 3.
                (Overrides task_policy.max_retries)

        Example:
            ```python
            # Runs every 5 minutes
            @app.schedule(when="every 5m")

            # Runs every 1 hour
            @app.schedule(when="every 1h")

            # Runs every day at midnight
            @app.schedule(when="0 0 * * *")
            ```
        """

        def decorator(func):
            handler_path = self._get_function_path(func)

            config_data = {
                "when": when,
                "trigger_type": TriggerType.Schedule,
                "handler": handler_path,
                "runtime": runtime,
                "outputs": outputs,
                "callback_url": callback_url,
                "task_policy": task_policy,
                "max_retries": max_retries,
                "timeout": timeout,
            }

            schedule = FunctionTrigger(
                **config_data,
            )
            self.triggers.append(schedule)

            def wrapper(*args, **kwargs):
                if BEAM_SERIALIZE_MODE in [
                    BeamSerializeMode.Start,
                    BeamSerializeMode.Deploy,
                ]:
                    return self.build_config(triggers=[schedule])

                elif BEAM_SERIALIZE_MODE == BeamSerializeMode.Run:
                    return self.build_config(run=Run(**config_data))

                return func(*args, **kwargs)

            return wrapper

        return decorator

    def asgi(
        self,
        runtime: Optional[Union[Runtime, dict]] = None,
        autoscaler: Optional[Union[AutoscalerType, AutoscalerDict]] = None,
        loader: Optional[Callable] = None,
        max_pending_tasks: int = 100,
        keep_warm_seconds: int = 90,
        timeout: Optional[int] = None,
        task_policy: Union[TaskPolicy, dict] = TaskPolicy(),
        workers: int = 1,
        authorized: bool = True,
    ):
        """
        Decorator used for deploying an arbitrary ASGI-compatible application.

        <Info>
            ASGI deployments are coming soon. Email us at founders@beam.cloud to learn more.
        </Info>

        Parameters:
            runtime (Optional[Union[Runtime, dict]]):
                The runtime environment to execute the tasks. It can be a dictionary containing
                runtime configurations or an instance of the Runtime class. If not specified, the
                default runtime will be used. Default is None.
            autoscaler (Optional[Union[AutoscalerType, AutoscalerDict]]):
                Defines an autoscaling strategy for your workload. See
                [RequestLatencyAutoscaler](#requestlatencyautoscaler).
            loader (Optional[Callable]):
                A function that runs exactly once when the app is first started. Useful for loading
                models or performing initialization of the app. Default is None.
            max_pending_tasks (int):
                The maximum number of tasks that can be pending in the queue. If the number of
                pending tasks exceeds this value, the task queue will stop accepting new tasks.
                Default is 100.
            keep_warm_seconds (int):
                The duration in seconds to keep the task queue warm even if there are no pending
                tasks. Keeping the queue warm helps to reduce the latency when new tasks arrive.
                Default is 90s.
            task_policy (Union[TaskPolicy, dict]):
                The task policy object where you can specify max retries and timeout
            timeout (Optional[int]):
                The maximum number of seconds a task can run before it times out.
                Default is 3600. Set it to -1 to disable the timeout.
                (Overrides task_policy.timeout)
            workers (int):
                The number of workers to launch per container.
                Modifying this parameter can improve throughput for certain workloads.
                Workers will share the CPU, Memory, and GPU defined for your App.
                You may need to increase these values to utilize more workers.
                Default is 1.
            authorized (bool):
                Enable global basic authentication for all endpoints in your app.
                Default is True.
        Example:
            ```python
            from beam import App, Runtime, Image, Volume
            from fastapi import FastAPI

            app = App(
                name="vocalize",
                runtime=Runtime(
                    cpu=1,
                    gpu="A10G",
                    image=Image(
                        python_version="python3.8",
                        python_packages=["torch"],
                        commands=["apt-get install ffmpeg"],
                    ),
                ),
                volumes=[Volume(name="my_models", path="models")],
            )

            @app.asgi(keep_warm_seconds=1000, workers=2)
            def transcribe():
                my_app = FastAPI()

                @my_app.get("/something")
                def func():
                    return {}

                @my_app.post("/something-else")
                def func():
                    return {}

                return my_app
            ```
        """

        def decorator(func):
            loader_path = self._get_function_path(loader)
            handler_path = self._get_function_path(func)
            endpoint_path = self._parse_path(None, handler_path)

            config_data = {
                "trigger_type": TriggerType.ASGI,
                "handler": handler_path,
                "method": "POST",
                "runtime": runtime,
                "autoscaler": autoscaler,
                "loader": loader_path,
                "path": endpoint_path,
                "max_pending_tasks": max_pending_tasks,
                "keep_warm_seconds": keep_warm_seconds,
                "task_policy": task_policy,
                "timeout": timeout,
                "workers": workers,
                "authorized": authorized,
            }

            asgi_trigger = FunctionTrigger(**config_data)
            self.triggers.append(asgi_trigger)

            def wrapper(*args, **kwargs):
                if BEAM_SERIALIZE_MODE in [
                    BeamSerializeMode.Start,
                    BeamSerializeMode.Deploy,
                ]:
                    return self.build_config(triggers=[asgi_trigger])

                elif BEAM_SERIALIZE_MODE == BeamSerializeMode.Run:
                    raise ValidationError("Cannot run an ASGI function. Use [beam serve] instead.")

                return func(*args, **kwargs)

            return wrapper

        return decorator

    def run(
        self,
        runtime: Optional[Union[dict, Runtime]] = None,
        outputs: List[Union[Output, dict]] = [],
        callback_url: Optional[str] = None,
        timeout: Optional[int] = None,
        task_policy: Union[TaskPolicy, dict] = TaskPolicy(),
    ):
        """
        Decorator used for running code immediately.

        This method is used to define the configuration for executing your code on Beam,
        without scheduling it or deploying it as an API.

        Parameters:
            runtime (Optional[Union[dict, Runtime]]):
                The runtime environment for the task execution. Default is None.
            outputs (List[Union[Output, dict]]):
                A list of of artifacts created during the task execution. Default is [].
            callback_url (Optional[str]):
                A URL where task execution status updates will be sent. If provided, the system will
                make a single POST request to this URL with status updates for each task. Default is
                None.
            task_policy (Union[TaskPolicy, dict]):
                The task policy object where you can specify max retries and timeout
            timeout (Optional[int]):
                The maximum number of seconds a task can run before it times out.
                Default is 3600. Set it to -1 to disable the timeout.
                (Overrides task_policy.timeout)

        Example:
            **Defining a run in your project**

            ```python
            from beam import App, Runtime

            app = App(name="my-app", runtime=Runtime())

            @app.run()
            def my_func():
                ...
            ```

            **Starting a run with the CLI**

            When you run the `beam run` command, this decorator function will be run asynchronously,
            and the results can be obtained through the `/task` API or web dashboard.

            When starting a run, you can pass data to your function using the `-d` argument:

                ```sh
                beam run my-file:func -d '{"text": "some-text"}'
                ```
        """

        def decorator(func):
            handler_path = self._get_function_path(func)

            config_data = {
                "handler": handler_path,
                "runtime": runtime,
                "outputs": outputs,
                "callback_url": callback_url,
                "task_policy": task_policy,
                "timeout": timeout,
            }

            def wrapper(*args, **kwargs):
                if BEAM_SERIALIZE_MODE == BeamSerializeMode.Deploy:
                    raise ValidationError("Cannot deploy a run function. Use [beam run] instead.")
                elif BEAM_SERIALIZE_MODE in [
                    BeamSerializeMode.Start,
                    BeamSerializeMode.Run,
                ]:
                    return self.build_config(run=Run(**config_data))

                return func(*args, **kwargs)

            return wrapper

        return decorator
