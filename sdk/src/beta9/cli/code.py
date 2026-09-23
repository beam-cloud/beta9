"""
`beam code`: a persistent development VM on a Pod, with coding agents
preinstalled, that you attach to over `beam shell`.

    beam code                    # create-or-wake "dev", attach a shell
    beam code --agent claude     # image ships Claude Code (also: codex, all)
    beam code --sync .           # keep this directory synced into /workspace
    beam code sleep|wake|status|destroy [--name dev]

A pod deployment with a durable /workspace disk; `--idle` seconds of inactivity
sleeps it, attaching wakes it. Agent keys come from ANTHROPIC_API_KEY /
OPENAI_API_KEY workspace secrets.
"""

import time
from typing import Any, Dict, List, Optional

import click
from betterproto import Casing

from .. import terminal
from ..abstractions.image import Image
from ..abstractions.pod import Pod
from ..channel import ServiceClient
from ..clients.gateway import (
    DeleteDeploymentRequest,
    ListContainersRequest,
    ListDeploymentsRequest,
    ScaleDeploymentRequest,
    StringList,
)
from ..clients.secret import ListSecretsRequest
from ..type import DurableDisk
from . import extraclick
from .extraclick import ClickCommonGroup

AGENTS = ("claude", "codex", "all", "none")
AGENT_PACKAGES = {
    "claude": ["@anthropic-ai/claude-code"],
    "codex": ["@openai/codex"],
}
AGENT_SECRETS = {"claude": "ANTHROPIC_API_KEY", "codex": "OPENAI_API_KEY"}
DEFAULT_NAME = "dev"
WAKE_TIMEOUT_S = 600


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


def _vm_options(func):
    for option in reversed(
        [
            click.option(
                "--name",
                default=DEFAULT_NAME,
                show_default=True,
                help="VM name (one pod deployment per name).",
            ),
            click.option(
                "--agent",
                type=click.Choice(AGENTS),
                default="all",
                show_default=True,
                help="Agents to preinstall.",
            ),
            click.option("--cpu", type=click.FLOAT, default=2.0, show_default=True),
            click.option("--memory", default="4Gi", show_default=True),
            click.option("--gpu", default="", help="GPU type for the VM (e.g. A10G)."),
            click.option(
                "--disk", default="20Gi", show_default=True, help="Durable /workspace size."
            ),
            click.option(
                "--idle",
                type=click.INT,
                default=1800,
                show_default=True,
                help="Seconds idle before the VM sleeps.",
            ),
            click.option(
                "--sync",
                "sync_dir",
                default=None,
                help="Local directory to keep synced into the VM.",
            ),
            click.option("--no-attach", is_flag=True, help="Create/wake without opening a shell."),
        ]
    ):
        func = option(func)
    return func


@common.group(
    name="code",
    invoke_without_command=True,
    help="Persistent dev VM with coding agents; attaches a shell.",
)
@_vm_options
@click.pass_context
def code(ctx: click.Context, **kwargs):
    if ctx.invoked_subcommand is None:
        ctx.invoke(up, **kwargs)


def _find_vm(service: ServiceClient, name: str) -> Optional[Dict[str, Any]]:
    res = service.gateway.list_deployments(
        ListDeploymentsRequest(filters={"name": StringList([name])}, limit=50)
    )
    vms = [
        d.to_dict(casing=Casing.SNAKE)
        for d in res.deployments
        if d.name == name and d.stub_type == "pod/deployment"
    ]  # type: ignore[attr-defined]
    if not vms:
        return None
    return max(vms, key=lambda d: (bool(d.get("active")), d["version"]))


def _containers(service: ServiceClient, stub_id: str) -> List[Any]:
    res = service.gateway.list_containers(ListContainersRequest())
    return (
        [c for c in res.containers if c.stub_id == stub_id and c.status in ("RUNNING", "PENDING")]
        if res.ok
        else []
    )


def _scale(service: ServiceClient, deployment_id: str, containers: int) -> None:
    res = service.gateway.scale_deployment(
        ScaleDeploymentRequest(id=deployment_id, containers=containers)
    )
    if not res.ok:
        terminal.error(res.err_msg or "Failed to scale the dev VM", code="ERROR")


def _agent_secrets(service: ServiceClient, agent: str) -> List[str]:
    wanted = [AGENT_SECRETS[a] for a in AGENT_PACKAGES if agent in (a, "all")]
    listed = service.secret.list_secrets(ListSecretsRequest())
    have = {s.name for s in listed.secrets} if listed.ok else set()
    return [s for s in wanted if s in have]


def _image(agent: str) -> Image:
    packages = [p for a, pkgs in AGENT_PACKAGES.items() if agent in (a, "all") for p in pkgs]
    commands = [
        "apt-get update && apt-get install -y --no-install-recommends git curl openssh-client ripgrep jq && rm -rf /var/lib/apt/lists/*",
        "pip install --no-cache-dir beam-client uv",
    ]
    if packages:
        commands.append("npm install -g " + " ".join(packages))
    return Image(
        base_image="docker.io/library/node:20-bookworm",
        python_version="python3.11",
        commands=commands,
    )


def _create_vm(
    service: ServiceClient,
    name: str,
    agent: str,
    cpu: float,
    memory: str,
    gpu: str,
    disk: str,
    idle: int,
) -> Dict[str, Any]:
    terminal.header(f"Creating dev VM {name}", "pod deployment with a durable /workspace")
    secrets = _agent_secrets(service, agent)
    pod = Pod(
        name=name,
        cpu=cpu,
        memory=memory,
        gpu=gpu or "",
        image=_image(agent),
        entrypoint=["sh", "-lc", "mkdir -p /workspace && cd /workspace && exec sleep infinity"],
        keep_warm_seconds=idle,
        secrets=secrets,
        env={"BEAM_DEV_VM": "1", "BEAM_DEV_AGENT": agent, "HOME": "/workspace/home"},
        disks=[DurableDisk(name=f"{name}-workspace", size=disk, mount_path="/workspace")],
    )
    result, ok = pod.deploy(name=name, context=service._config)
    if not ok:
        terminal.error("Failed to create the dev VM", code="ERROR")
    vm = _find_vm(service, name)
    if vm is None:
        terminal.error("Dev VM was created but could not be found", code="ERROR")
    return vm


def _wake(service: ServiceClient, vm: Dict[str, Any]) -> Any:
    containers = _containers(service, vm["stub_id"])
    if not containers:
        terminal.detail("Waking the VM…")
        _scale(service, vm["id"], 1)
    deadline = time.monotonic() + WAKE_TIMEOUT_S
    while time.monotonic() < deadline:
        running = [c for c in _containers(service, vm["stub_id"]) if c.status == "RUNNING"]
        if running:
            return running[0]
        time.sleep(2)
    terminal.error("The dev VM did not start in time", code="TIMEOUT")


@code.command(name="up", help="Create the VM if needed, wake it and attach a shell (default).")
@_vm_options
@extraclick.pass_service_client
def up(
    service: ServiceClient,
    name: str,
    agent: str,
    cpu: float,
    memory: str,
    gpu: str,
    disk: str,
    idle: int,
    sync_dir: Optional[str],
    no_attach: bool,
):
    vm = _find_vm(service, name) or _create_vm(service, name, agent, cpu, memory, gpu, disk, idle)
    container = _wake(service, vm)
    info = {
        "name": name,
        "deployment_id": vm["id"],
        "stub_id": vm["stub_id"],
        "container_id": container.container_id,
        "agent": agent,
    }
    if terminal.json_output() or no_attach:
        terminal.print_json(info) if terminal.json_output() else terminal.resource("Dev VM", info)
        return
    terminal.detail(
        "Attaching. Inside: `claude`, `codex`, `beam --help`. Files under /workspace persist."
    )
    Pod(entrypoint=[]).shell(container_id=container.container_id, sync_dir=sync_dir)


@code.command(name="status", help="Show the VM's deployment and running container.")
@click.option("--name", default=DEFAULT_NAME, show_default=True)
@extraclick.pass_service_client
def status(service: ServiceClient, name: str):
    vm = _find_vm(service, name)
    if vm is None:
        terminal.error(
            f"No dev VM named {name!r}. Run `beam code` to create one.", code="NOT_FOUND"
        )
    containers = _containers(service, vm["stub_id"])
    info = {
        "name": name,
        "deployment_id": vm["id"],
        "stub_id": vm["stub_id"],
        "state": "awake" if containers else "asleep",
        "containers": [c.container_id for c in containers],
    }
    terminal.print_json(info) if terminal.json_output() else terminal.resource("Dev VM", info)


@code.command(name="sleep", help="Scale the VM to zero; /workspace is kept.")
@click.option("--name", default=DEFAULT_NAME, show_default=True)
@extraclick.pass_service_client
def sleep(service: ServiceClient, name: str):
    vm = _find_vm(service, name)
    if vm is None:
        terminal.error(f"No dev VM named {name!r}", code="NOT_FOUND")
    _scale(service, vm["id"], 0)
    terminal.success(f"{name} is going to sleep")


@code.command(name="wake", help="Start the VM without attaching.")
@click.option("--name", default=DEFAULT_NAME, show_default=True)
@extraclick.pass_service_client
def wake(service: ServiceClient, name: str):
    vm = _find_vm(service, name)
    if vm is None:
        terminal.error(f"No dev VM named {name!r}", code="NOT_FOUND")
    container = _wake(service, vm)
    terminal.success(f"{name} is awake ({container.container_id})")


@code.command(
    name="destroy", help="Delete the VM deployment. The durable disk is kept until you delete it."
)
@click.option("--name", default=DEFAULT_NAME, show_default=True)
@click.option("--yes", "-y", is_flag=True)
@extraclick.pass_service_client
def destroy(service: ServiceClient, name: str, yes: bool):
    vm = _find_vm(service, name)
    if vm is None:
        terminal.error(f"No dev VM named {name!r}", code="NOT_FOUND")
    if not yes and not terminal.confirm(
        f"Destroy dev VM {name}? Files on its disk are kept.", default=False
    ):
        terminal.error("Cancelled.", code="CANCELLED")
    res = service.gateway.list_deployments(
        ListDeploymentsRequest(filters={"name": StringList([name])}, limit=100)
    )
    for d in res.deployments:
        if d.name == name and d.stub_type == "pod/deployment":
            service.gateway.delete_deployment(DeleteDeploymentRequest(id=d.id))
    terminal.success(f"Destroyed {name}")
