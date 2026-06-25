import os
import shlex
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..abstractions.image import Image
from ..abstractions.pod import Pod
from ..abstractions.service import (
    Service,
    command_from_dockerfile,
    dockerfile_instructions,
    resolve_service_ports,
)
from ..channel import ServiceClient
from ..cli import extraclick
from ..clients.gateway import (
    DeleteDeploymentRequest,
    DeleteDeploymentResponse,
    ListDeploymentsRequest,
    ListDeploymentsResponse,
    ScaleDeploymentRequest,
    ScaleDeploymentResponse,
    StartDeploymentRequest,
    StartDeploymentResponse,
    StopDeploymentRequest,
    StopDeploymentResponse,
    StringList,
)
from ..logging import StoredStdoutInterceptor
from ..type import GpuType, LLMConfig, LLMTokenPressureAutoscaler
from ..utils import load_module_spec
from .extraclick import (
    ClickCommonGroup,
    ClickManagementGroup,
    env_vars_to_dict,
    handle_config_override,
    image_from_dockerfile_option,
    override_config_options,
)

LLM_APP_KIND = "llm_model"
LLM_SERVING_PROTOCOL = "openai"
LLM_DEFAULT_METRICS_PATH = "/metrics"

LLM_MODEL_ENV_KEYS = (
    "MODEL_ID",
    "MODEL",
    "HF_MODEL_ID",
    "HUGGINGFACE_MODEL_ID",
    "HUGGING_FACE_HUB_MODEL_ID",
    "VLLM_MODEL",
    "SGLANG_MODEL",
)
LLM_TOKENIZER_ENV_KEYS = ("TOKENIZER", "TOKENIZER_ID", "VLLM_TOKENIZER")
LLM_CONTEXT_ENV_KEYS = ("CONTEXT_LENGTH", "MAX_MODEL_LEN", "MAX_MODEL_LENGTH", "MAX_SEQ_LEN")
LLM_MODEL_FLAGS = ("--model", "--model-id", "--model_id", "--model-path", "--model_path")
LLM_SERVED_MODEL_FLAGS = ("--served-model-name", "--served_model_name")
LLM_CONTEXT_FLAGS = (
    "--max-model-len",
    "--max_model_len",
    "--context-length",
    "--context_length",
    "--max-seq-len",
    "--max_seq_len",
)
LLM_TOKENIZER_FLAGS = ("--tokenizer",)
LLM_ENGINE_MARKERS = (
    ("vllm", ("vllm", "vllm-openai")),
    ("sglang", ("sglang", "sgl-project")),
    ("tgi", ("text-generation-inference", "huggingface/tgi")),
    ("tensorrt-llm", ("tensorrt-llm", "trtllm")),
    ("llama.cpp", ("llama.cpp", "llama-server")),
)


@dataclass(frozen=True)
class LLMMetadataHints:
    engine: str = ""
    model_id: str = ""
    served_model_name: str = ""
    context_length: int = 0
    tokenizer: str = ""


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="deploy",
    help="""
    Deploy a function, web service, or container.

    HANDLER is in the format of "file:function". If no handler is supplied,
    beta9 deploys a service from --dockerfile, --image, --command, or a local Dockerfile.
    """,
    epilog="""
      Examples:

        {cli_name} deploy --name my-app app.py:my_func

        {cli_name} deploy --name web --dockerfile Dockerfile

        {cli_name} deploy --name api --dockerfile Dockerfile --pool web-cpu

        {cli_name} deploy --name api --dockerfile Dockerfile --always-on

        {cli_name} deploy --name worker --image ghcr.io/acme/worker:latest --command "npm start"

        {cli_name} deploy --name qwen --dockerfile Dockerfile --port 8000 --llm
        \b
    """,
)
@click.option(
    "--name",
    "-n",
    type=click.STRING,
    help="The name the deployment.",
    required=False,
)
@click.argument(
    "handler",
    nargs=1,
    required=False,
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@click.option(
    "--format",
    type=click.Choice(["json"]),
    default=None,
    help="The format of the output after a successful deployment.",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Output deployment details as JSON.",
)
@override_config_options
@extraclick.config_context_option
@click.pass_context
def deploy(
    ctx: click.Context,
    name: str,
    handler: str,
    url_type: str,
    format: str,
    json_output: bool,
    context: str = None,
    **kwargs,
):
    ctx.invoke(
        create_deployment,
        name=name,
        handler=handler,
        url_type=url_type,
        format="json" if json_output else format,
        json_output=False,
        context=context,
        **kwargs,
    )


@click.group(
    name="deployment",
    help="Manage deployments.",
    cls=ClickManagementGroup,
)
def management():
    pass


def _merge_port_options(kwargs: Dict) -> None:
    ports = list(kwargs.get("ports") or [])
    for port in kwargs.pop("port", ()) or ():
        if port not in ports:
            ports.append(port)
    kwargs["ports"] = ports


def _autodetect_dockerfile(kwargs: Dict) -> None:
    if kwargs.get("dockerfile") is not None or kwargs.get("image") is not None:
        return

    dockerfile = "Dockerfile"
    if not os.path.exists(dockerfile):
        return

    kwargs["dockerfile"] = dockerfile


def _materialize_dockerfile_image(kwargs: Dict) -> Optional[Image]:
    dockerfile = kwargs.get("dockerfile")
    if dockerfile is None:
        return None

    image = image_from_dockerfile_option(dockerfile)
    kwargs["dockerfile"] = image
    return image


def _service_image_option(kwargs: Dict) -> Optional[Image]:
    if kwargs.get("dockerfile") is not None and kwargs.get("image") is not None:
        raise ValueError("Specify either --dockerfile or --image, not both.")

    dockerfile_image = _materialize_dockerfile_image(kwargs)
    if dockerfile_image is not None:
        return dockerfile_image
    return kwargs.get("image")


def _llm_requested(kwargs: Dict) -> bool:
    if kwargs.get("llm_enabled"):
        return True

    return any(
        kwargs.get(key) not in (None, "", 0)
        for key in (
            "llm_model_id",
            "llm_engine",
            "llm_served_model_name",
            "llm_context_length",
            "llm_tokenizer",
            "llm_metrics_path",
            "llm_slo_tier",
        )
    )


def _dockerfile_env(image: Optional[Image]) -> Dict[str, str]:
    dockerfile = getattr(image, "dockerfile", "") or ""
    env: Dict[str, str] = {}

    for instruction, value in dockerfile_instructions(dockerfile):
        if instruction not in ("ENV", "ARG"):
            continue

        try:
            tokens = shlex.split(value)
        except ValueError:
            tokens = value.split()

        if len(tokens) >= 2 and "=" not in tokens[0]:
            env[tokens[0]] = tokens[1]
            continue

        for token in tokens:
            key, sep, raw_value = token.partition("=")
            if sep and key:
                env[key] = raw_value

    return env


def _first_env(env: Dict[str, str], keys: Tuple[str, ...]) -> str:
    for key in keys:
        value = env.get(key)
        if value:
            return value
    return ""


def _command_tokens(*commands: Optional[List[str]]) -> List[str]:
    tokens: List[str] = []
    for command in commands:
        for token in command or []:
            token = str(token)
            tokens.append(token)
            if any(char.isspace() for char in token):
                try:
                    tokens.extend(shlex.split(token))
                except ValueError:
                    continue
    return tokens


def _command_flag_value(tokens: List[str], flags: Tuple[str, ...]) -> str:
    for index, token in enumerate(tokens):
        for flag in flags:
            if token == flag and index + 1 < len(tokens):
                value = tokens[index + 1]
                if not value.startswith("-"):
                    return value
            if token.startswith(f"{flag}="):
                return token.split("=", 1)[1]
    return ""


def _positive_int(value) -> int:
    if value in (None, ""):
        return 0

    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0

    return parsed if parsed > 0 else 0


def _image_engine_sources(image: Optional[Image]) -> List[str]:
    sources = [
        getattr(image, "base_image", "") or "",
        getattr(image, "dockerfile_path", "") or "",
    ]

    dockerfile = getattr(image, "dockerfile", "") or ""
    for instruction, value in dockerfile_instructions(dockerfile):
        if instruction == "FROM":
            sources.append(value)

    return sources


def _infer_llm_engine(image: Optional[Image], command_tokens: List[str]) -> str:
    source = " ".join([*_image_engine_sources(image), *command_tokens]).lower()

    for engine, markers in LLM_ENGINE_MARKERS:
        if any(marker in source for marker in markers):
            return engine
    return ""


def _llm_metadata_hints(
    kwargs: Dict,
    image: Optional[Image],
    entrypoint: Optional[List[str]],
) -> LLMMetadataHints:
    env = _dockerfile_env(image)
    env.update(env_vars_to_dict(kwargs.get("env")))

    tokens = _command_tokens(entrypoint, command_from_dockerfile(image))
    return LLMMetadataHints(
        engine=kwargs.get("llm_engine") or _infer_llm_engine(image, tokens),
        model_id=kwargs.get("llm_model_id")
        or _first_env(env, LLM_MODEL_ENV_KEYS)
        or _command_flag_value(tokens, LLM_MODEL_FLAGS),
        served_model_name=kwargs.get("llm_served_model_name")
        or _command_flag_value(tokens, LLM_SERVED_MODEL_FLAGS),
        context_length=_positive_int(kwargs.get("llm_context_length"))
        or _positive_int(_first_env(env, LLM_CONTEXT_ENV_KEYS))
        or _positive_int(_command_flag_value(tokens, LLM_CONTEXT_FLAGS)),
        tokenizer=kwargs.get("llm_tokenizer")
        or _first_env(env, LLM_TOKENIZER_ENV_KEYS)
        or _command_flag_value(tokens, LLM_TOKENIZER_FLAGS),
    )


def _llm_config_from_options(
    kwargs: Dict,
    image: Optional[Image] = None,
    entrypoint: Optional[List[str]] = None,
) -> Optional[LLMConfig]:
    if not _llm_requested(kwargs):
        return None

    hints = _llm_metadata_hints(kwargs, image, entrypoint)
    metrics_path = kwargs.get("llm_metrics_path") or ""
    if not metrics_path and hints.engine in {"vllm", "sglang", "tgi"}:
        metrics_path = LLM_DEFAULT_METRICS_PATH

    return LLMConfig(
        model_id=hints.model_id or "",
        engine=hints.engine or "",
        served_model_name=hints.served_model_name or "",
        context_length=hints.context_length,
        tokenizer=hints.tokenizer or "",
        metrics_path=metrics_path,
        slo_tier=kwargs.get("llm_slo_tier") or "",
    )


def _metadata_target(user_obj):
    return getattr(user_obj, "parent", user_obj)


def _enable_llm_autoscaler(target) -> None:
    autoscaler = getattr(target, "autoscaler", None)
    if autoscaler is None or isinstance(autoscaler, LLMTokenPressureAutoscaler):
        return

    target.autoscaler = LLMTokenPressureAutoscaler(
        min_containers=getattr(autoscaler, "min_containers", 0),
        max_containers=getattr(autoscaler, "max_containers", 1),
        tasks_per_container=getattr(autoscaler, "tasks_per_container", 1),
    )


def _ensure_llm_pool_gpu(target) -> None:
    if not getattr(target, "llm", None) or not getattr(target, "pool_config", None):
        return
    if getattr(target, "gpu", "") or getattr(target, "gpu_count", 0):
        return

    target.gpu = GpuType.Any
    target.gpu_count = 1


def _apply_llm_metadata(user_obj, kwargs: Dict) -> bool:
    target = _metadata_target(user_obj)
    llm_config = _llm_config_from_options(
        kwargs,
        image=getattr(target, "image", None),
        entrypoint=getattr(target, "entrypoint", None),
    )
    if llm_config is None:
        return True

    if not isinstance(target, (Pod, Service)):
        terminal.error("--llm is only supported for Pod and Service deployments.", exit=False)
        return False

    target.app_kind = LLM_APP_KIND
    target.serving_protocol = LLM_SERVING_PROTOCOL
    target.llm = llm_config
    _enable_llm_autoscaler(target)
    _ensure_llm_pool_gpu(target)
    return True


def _generate_service_module(name: Optional[str], kwargs: Dict) -> Service:
    service_image = _service_image_option(kwargs)
    ports = resolve_service_ports(
        ports=kwargs.get("ports"),
        image=service_image,
        default=service_image is not None,
    )
    keep_warm_seconds = kwargs.get("keep_warm_seconds")
    service_kwargs = {
        "name": name or os.path.basename(os.getcwd()),
        "entrypoint": kwargs.get("entrypoint"),
        "ports": ports,
        "image": service_image or Image(),
        "env": env_vars_to_dict(kwargs.get("env")),
        "keep_warm_seconds": 0 if keep_warm_seconds is None else keep_warm_seconds,
        "min_replicas": kwargs.get("min_replicas") or 0,
        "max_replicas": kwargs.get("max_replicas"),
        "always_on": bool(kwargs.get("always_on")),
        "pool": kwargs.get("pool"),
        "tcp": bool(kwargs.get("tcp")),
    }
    llm_config = _llm_config_from_options(
        kwargs,
        image=service_image,
        entrypoint=service_kwargs["entrypoint"],
    )
    if llm_config is not None:
        service_kwargs.update(
            {
                "app_kind": LLM_APP_KIND,
                "serving_protocol": LLM_SERVING_PROTOCOL,
                "llm": llm_config,
            }
        )

    for key in ("cpu", "memory", "gpu", "gpu_count", "secrets"):
        value = kwargs.get(key)
        if value is not None:
            service_kwargs[key] = value

    return Service(**service_kwargs)


def _release_deployment_grpc_refs(user_obj) -> None:
    seen = set()

    def clear(obj) -> None:
        if obj is None or id(obj) in seen:
            return

        seen.add(id(obj))
        attrs = getattr(obj, "__dict__", {})
        for attr in ("syncer", "_gateway_stub", "_shell_stub", "_stub", "_pod_stub"):
            if attr in attrs:
                setattr(obj, attr, None)

        clear(attrs.get("parent"))
        clear(attrs.get("image"))

    clear(user_obj)


@management.command(
    name="create",
    help="Create a new deployment.",
    epilog="""
      Examples:

        {cli_name} deploy --name my-app --entrypoint app.py:handler

        {cli_name} deploy --name web --dockerfile Dockerfile

        {cli_name} deploy --name web --dockerfile Dockerfile --pool web-cpu

        {cli_name} deploy --name web --dockerfile Dockerfile --min-replicas 2 --max-replicas 4
        \b
    """,
)
@click.option(
    "--name",
    "-n",
    help="The name of the deployment.",
    required=False,
)
@click.option(
    "--handler",
    help='The name the handler e.g. "file:function" or script to run.',
)
@click.option(
    "--url-type",
    help="The type of URL to get back. [default is determined by the server] ",
    type=click.Choice(["host", "path"]),
)
@click.option(
    "--format",
    type=click.Choice(["json"]),
    default=None,
    help="The format of the output after a successful deployment.",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Output deployment details as JSON.",
)
@override_config_options
@extraclick.pass_service_client
def create_deployment(
    service: ServiceClient,
    name: str,
    handler: str,
    url_type: str,
    format: str,
    json_output: bool,
    **kwargs,
):
    if json_output:
        format = "json"
    _merge_port_options(kwargs)
    entrypoint = kwargs["entrypoint"]

    logs = []
    user_obj = None

    with StoredStdoutInterceptor(capture_logs=format == "json") as capture_logs:
        try:
            if handler:
                user_obj, module_name, obj_name = load_module_spec(handler, "deploy")

                if hasattr(user_obj, "set_handler"):
                    user_obj.set_handler(f"{module_name}:{obj_name}")

            else:
                try:
                    _autodetect_dockerfile(kwargs)
                    if (
                        entrypoint
                        or kwargs.get("dockerfile") is not None
                        or kwargs.get("image") is not None
                    ):
                        user_obj = _generate_service_module(name, kwargs)
                    else:
                        terminal.error("No handler, entrypoint, image, or Dockerfile specified")
                        return
                except (OSError, ValueError) as exc:
                    terminal.error(f"Invalid service configuration: {exc}")
                    return

            if not handle_config_override(user_obj, kwargs):
                return

            if not _apply_llm_metadata(user_obj, kwargs):
                return

            if hasattr(user_obj, "generate_deployment_artifacts"):
                user_obj.generate_deployment_artifacts(**kwargs)

            response, ok = user_obj.deploy(
                name=name,
                context=service._config,
                url_type=url_type,
            )
            if not ok:
                terminal.error("Deployment failed ☠️")
                return

            if hasattr(user_obj, "cleanup_deployment_artifacts"):
                user_obj.cleanup_deployment_artifacts()

            if capture_logs.capture_logs:
                logs.extend(capture_logs.logs)
        finally:
            _release_deployment_grpc_refs(user_obj)

    if format == "json":
        terminal.print_json(
            {
                "logs": logs,
                **response,
            }
        )


@management.command(
    name="list",
    help="List all deployments.",
    epilog="""
    Examples:

      # List the first 10 deployments
      {cli_name} deployment list --limit 10

      # List deployments that are at version 9
      {cli_name} deployment list --filter version=9

      # List deployments that are not active
      {cli_name} deployment list --filter active=false
      {cli_name} deployment list --filter active=no

      # List deployments and output in JSON format
      {cli_name} deployment list --format json
      \b
    """,
)
@click.option(
    "--limit",
    type=click.IntRange(1, 100),
    default=20,
    help="The number of deployments to fetch.",
)
@click.option(
    "--format",
    type=click.Choice(["json", "none", "table"]),
    default="table",
    show_default=True,
    help="Change the format of the output.",
)
@click.option(
    "--filter",
    multiple=True,
    callback=extraclick.filter_values_callback,
    help="Filters deployments. Add this option for each field you want to filter on.",
)
@extraclick.pass_service_client
def list_deployments(
    service: ServiceClient,
    limit: int,
    format: str,
    filter: Dict[str, StringList],
):
    res: ListDeploymentsResponse
    res = service.gateway.list_deployments(ListDeploymentsRequest(filter, limit))

    if not res.ok:
        terminal.error(res.err_msg)

    if format == "json":
        deployments = [d.to_dict(casing=Casing.SNAKE) for d in res.deployments]  # type:ignore
        terminal.print_json(deployments)
        return

    table = Table(
        Column("ID"),
        Column("Name"),
        Column("Active"),
        Column("Version", justify="right"),
        Column("Created At"),
        Column("Updated At"),
        Column("Stub Name"),
        Column("Workspace Name"),
        box=box.SIMPLE,
    )

    for deployment in res.deployments:
        table.add_row(
            deployment.id,
            deployment.name,
            "Yes" if deployment.active else "No",
            str(deployment.version),
            terminal.humanize_date(deployment.created_at),
            terminal.humanize_date(deployment.updated_at),
            deployment.stub_name,
            deployment.workspace_name,
        )

    table.add_section()
    table.add_row(f"[bold]{len(res.deployments)} items")
    terminal.print(table)


@management.command(
    name="stop",
    help="Stop deployments.",
    epilog="""
    Examples:

      # Stop a deployment
      {cli_name} deployment stop 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572

      # Stop multiple deployments
      {cli_name} deployment stop 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572 7b968ad5-c001-4df3-ba05-e99895aa9596
      \b
    """,
)
@click.argument(
    "deployment_ids",
    nargs=-1,
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def stop_deployments(service: ServiceClient, deployment_ids: List[str]):
    for id in deployment_ids:
        res: StopDeploymentResponse
        res = service.gateway.stop_deployment(StopDeploymentRequest(id))

        if not res.ok:
            terminal.error(res.err_msg, exit=False)
            continue

        terminal.success(f"Stopped deployment: {id}")


@management.command(
    name="start",
    help="Start an inactive deployment.",
    epilog="""
    Examples:

        # Start a deployment
        {cli_name} deployment start 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572
        """,
)
@click.argument(
    "deployment_id",
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def start_deployment(service: ServiceClient, deployment_id: str):
    res: StartDeploymentResponse
    res = service.gateway.start_deployment(StartDeploymentRequest(id=deployment_id))

    if not res.ok:
        terminal.error(res.err_msg)

    terminal.print(f"Starting deployment: {deployment_id}")


@management.command(
    name="delete",
    help="Delete a deployment.",
    epilog="""
    Examples:

        # Delete a deployment
        {cli_name} deployment delete 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572
        \b
     """,
)
@click.argument(
    "deployment_id",
    nargs=1,
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def delete_deployment(service: ServiceClient, deployment_id: str):
    res: DeleteDeploymentResponse
    res = service.gateway.delete_deployment(DeleteDeploymentRequest(deployment_id))

    if not res.ok:
        terminal.error(res.err_msg)

    terminal.print(f"Deleted {deployment_id}")


@management.command(
    name="scale",
    help="Scale an active deployment.",
    epilog="""
    Examples:

        # Keep two replicas running
        {cli_name} deployment scale 5bd2e248-6d7c-417b-ac7b-0b92aa0a5572 --replicas 2
        """,
)
@click.argument(
    "deployment_id",
    type=click.STRING,
    required=True,
)
@click.option(
    "--containers",
    "--replicas",
    "containers",
    type=click.IntRange(min=0),
    required=True,
    help="The fixed replica count to run.",
)
@extraclick.pass_service_client
def scale_deployment(service: ServiceClient, deployment_id: str, containers: int):
    res: ScaleDeploymentResponse
    res = service.gateway.scale_deployment(
        ScaleDeploymentRequest(id=deployment_id, containers=containers)
    )

    if not res.ok:
        terminal.error(res.err_msg)

    terminal.print(f"Scaled deployment {deployment_id} to {containers} replicas")
