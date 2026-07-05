import os
from typing import Dict, List, Optional

import click
from betterproto import Casing
from rich.table import Column, Table, box

from .. import terminal
from ..abstractions.image import Image
from ..abstractions.service import (
    DEFAULT_SERVICE_KEEP_WARM_SECONDS,
    Service,
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
from ..utils import load_module_spec
from .extraclick import (
    ClickCommonGroup,
    ClickManagementGroup,
    env_vars_to_dict,
    handle_config_override,
    image_from_dockerfile_option,
    override_config_options,
)

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


def _llm_options_present(kwargs: Dict) -> bool:
    return any(
        key.startswith("llm_") and value not in (None, "", 0, False)
        for key, value in kwargs.items()
    )


def _service_llm_metadata(kwargs: Dict, image: Optional[Image], entrypoint: Optional[List[str]]):
    if not _llm_options_present(kwargs):
        return None

    from .llm import service_llm_metadata

    return service_llm_metadata(kwargs, image=image, entrypoint=entrypoint)


def _service_checkpoint_options(kwargs: Dict) -> Dict:
    options = {
        "checkpoint_enabled": bool(kwargs.get("checkpoint_enabled")),
    }
    if kwargs.get("checkpoint_readiness_path"):
        options.update(
            {
                "checkpoint_readiness_path": kwargs.get("checkpoint_readiness_path"),
                "checkpoint_readiness_port": kwargs.get("checkpoint_readiness_port"),
                "checkpoint_readiness_timeout": kwargs.get("checkpoint_readiness_timeout") or 600,
                "checkpoint_readiness_interval": kwargs.get("checkpoint_readiness_interval") or 1,
            }
        )
    return options


def _apply_llm_metadata_if_requested(user_obj, kwargs: Dict) -> bool:
    if not _llm_options_present(kwargs):
        return True

    from .llm import apply_llm_metadata

    return apply_llm_metadata(user_obj, kwargs)


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
        "keep_warm_seconds": (
            DEFAULT_SERVICE_KEEP_WARM_SECONDS
            if keep_warm_seconds is None
            else keep_warm_seconds
        ),
        "min_replicas": kwargs.get("min_replicas") or 0,
        "max_replicas": kwargs.get("max_replicas"),
        "always_on": bool(kwargs.get("always_on")),
        "pool": kwargs.get("pool"),
        "tcp": bool(kwargs.get("tcp")),
    }
    service_kwargs.update(_service_checkpoint_options(kwargs))
    llm_metadata = _service_llm_metadata(
        kwargs,
        image=service_image,
        entrypoint=service_kwargs["entrypoint"],
    )
    if llm_metadata is not None:
        service_kwargs.update(llm_metadata)

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

            if not _apply_llm_metadata_if_requested(user_obj, kwargs):
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
