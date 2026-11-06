"""Build a VM template from one of these recipes, then check a VM started from it.

    python sdk/examples/vm/templates/publish.py openclaw
    python sdk/examples/vm/templates/publish.py openclaw --public --context staging

The build runs the recipe on a fresh VM and saves its root disk as a template.
The check starts a second VM from the template and waits for the recipe's check
to pass, the way someone starting the template would find it. Both VMs are
removed afterwards unless --keep is given.

A public template shares the build VM's whole root disk and its snapshot history
with every workspace, permanently. Build it in a workspace that has never held
personal data, and keep accounts and secrets out of the recipe.
"""

import argparse
import io
import sys
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from beta9 import VM, Image, Kiosk, MCPServer
from beta9.abstractions.vm import default_base_image

HERE = Path(__file__).resolve().parent
KIOSK_RECIPES = HERE.parent.parent / "kiosk"


@dataclass(frozen=True)
class Recipe:
    description: str
    # A shell command, run as root, that succeeds once a VM from the template is ready.
    check: str
    desktop: bool = False
    docker: bool = False
    # A kiosk template's app command; its image is sdk/examples/kiosk/<slug>/Dockerfile.
    kiosk: Optional[List[str]] = None
    # MCP servers for a kiosk's app. Registered on the build VM, they're on the
    # root disk the template copies.
    mcp: Optional[Dict[str, MCPServer]] = None
    # A GPU type, such as RTX4090. VMs from the template get one, and a kiosk's
    # Dockerfile builds on Beam's NVIDIA image as BASE_IMAGE.
    gpu: Optional[str] = None
    # Seconds a VM from the template may take to pass its check after it boots.
    timeout: int = 600


RECIPES = {
    "openclaw": Recipe(
        "OpenClaw, a personal agent you message on WhatsApp, Telegram, Slack and more",
        check="openclaw --version && loginctl show-user ubuntu --property=Linger | grep -qx Linger=yes",
    ),
    "hermes-agent": Recipe(
        "Hermes Agent from Nous Research, a personal agent with a messaging gateway",
        check="hermes --version && loginctl show-user ubuntu --property=Linger | grep -qx Linger=yes",
    ),
    "agent-devbox": Recipe(
        "Claude Code, Codex and the Cursor CLI on a desktop VM with Docker and Remote Control",
        desktop=True,
        docker=True,
        check="claude --version && codex --version && cursor-agent --version && command -v claude-remote"
        " && docker info",
    ),
    "computer-use": Recipe(
        "A desktop that Claude Code and Codex operate with agent-desktop and agent-browser",
        desktop=True,
        check="on-desktop agent-desktop list-apps && agent-browser --version",
    ),
    "video-studio": Recipe(
        "HyperFrames and Remotion for Claude Code and Codex on a desktop VM",
        desktop=True,
        check="hyperframes --version && test -d /workspace/hyperframes && test -d /workspace/remotion/node_modules",
    ),
    "blender": Recipe(
        "Blender full screen on an RTX 4090 kiosk desktop, with MCP for agents",
        kiosk=["blender", "/workspace/project.blend"],
        mcp={
            "blender": MCPServer(
                "/opt/blender-mcp/bin/mcp-for-blender", env={"DISABLE_TELEMETRY": "1"}
            )
        },
        gpu="RTX4090",
        check="""set -e
nvidia-smi -L
pgrep -x Xorg
pgrep -x blender
python3 - <<'PYTHON'
import json, urllib.request
health = json.load(urllib.request.urlopen("http://127.0.0.1:8082/health"))
assert health["servers"]["blender"]["state"] == "ready", health
PYTHON
BLENDER_USER_SCRIPTS=/opt/blender-scripts blender --background /workspace/project.blend \\
    --python-exit-code 1 --python-expr '
import bpy
cycles = bpy.context.preferences.addons["cycles"].preferences
assert cycles.compute_device_type == "OPTIX", cycles.compute_device_type
assert any(d.use and d.type == "OPTIX" for d in cycles.devices), list(cycles.devices)
scene = bpy.context.scene
assert scene.cycles.device == "GPU", scene.cycles.device
scene.render.engine = "CYCLES"
scene.cycles.samples = 4
scene.render.resolution_x = scene.render.resolution_y = 64
scene.render.filepath = "/tmp/gpu-check.png"
bpy.ops.render.render(write_still=True)
'""",
    ),
}

# Leaves nothing from the build that a new VM doesn't need: the recipe, caches,
# package downloads and logs.
CLEAN = """
set -e
rm -rf /root/template /root/.cache/uv /root/.npm /home/*/.npm /var/tmp/*
apt-get clean
journalctl --rotate >/dev/null 2>&1 || true
journalctl --vacuum-time=1s >/dev/null 2>&1 || true
find /var/log -path /var/log/journal -prune -o -type f \\( -name '*.gz' -o -name '*.[0-9]' \\) -exec rm -f {} +
find /var/log -path /var/log/journal -prune -o -type f -exec truncate -s 0 {} +
sync
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("slug", choices=sorted(RECIPES))
    parser.add_argument("--name", help="Template name in your workspace. Default: the slug.")
    parser.add_argument(
        "--public",
        action="store_true",
        help="Let any workspace start the template by its ID. This can't be undone.",
    )
    parser.add_argument("--context", help="CLI context to publish from. Default: the default.")
    parser.add_argument("--keep", action="store_true", help="Keep the build and check VMs.")
    args = parser.parse_args()
    recipe = RECIPES[args.slug]

    build = build_vm(args.slug, recipe, args.context)
    step(f"Saving the template {args.name or args.slug}")
    template = build.create_template(args.name or args.slug, recipe.description, public=args.public)
    print(f"Template ID: {template['id']}", flush=True)
    if not args.keep:
        build.remove()

    check_template(template["id"], args.slug, recipe, args.context, args.keep)
    print(f"\n{args.slug}: {'public' if args.public else 'private'} template {template['id']}")


def build_vm(slug: str, recipe: Recipe, context: Optional[str]) -> VM:
    options = dict(
        # A kiosk brings its own display.
        desktop=None if recipe.kiosk else recipe.desktop,
        docker_enabled=True if recipe.docker else None,
        # The template shouldn't carry the publisher's SSH key.
        ssh=False,
        gpu=recipe.gpu,
        context=context,
    )
    if recipe.kiosk:
        dockerfile = KIOSK_RECIPES / slug / "Dockerfile"
        build_args = {"BASE_IMAGE": default_base_image(recipe.gpu)} if recipe.gpu else None
        options.update(
            image=Image.from_dockerfile(str(dockerfile), build_args=build_args),
            kiosk=Kiosk(recipe.kiosk, transport="webrtc"),
        )

    step(f"Creating the build VM build-{slug}")
    vm = VM(f"build-{slug}", **options).create()
    vm.wait(timeout=900, services=True)
    try:
        if not recipe.kiosk:
            step(f"Running {slug}/setup.sh")
            vm.process.exec(
                "tar", "-xzf", "-", "-C", "/root", stdin=recipe_archive(slug), cwd="/"
            ).wait()
            run(vm, f"sh /root/template/{slug}/setup.sh")
        for name, server in (recipe.mcp or {}).items():
            step(f"Registering the MCP server {name}")
            vm.mcp.install(name, server)
        step("Cleaning up the build")
        run(vm, CLEAN)
    except BaseException:
        print(
            f"\nThe build VM {vm.name} is still running; remove it when you're done.",
            file=sys.stderr,
        )
        raise
    return vm


def recipe_archive(slug: str) -> bytes:
    """common.sh and the recipe's directory, under template/."""
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        archive.add(HERE / "common.sh", "template/common.sh")
        archive.add(HERE / slug, f"template/{slug}")
    return output.getvalue()


def check_template(
    template_id: str, slug: str, recipe: Recipe, context: Optional[str], keep: bool
) -> None:
    step(f"Starting check-{slug} from the template")
    vm = VM(f"check-{slug}", template=template_id, context=context).create()
    try:
        vm.wait(timeout=900, services=True)
        step(f"Waiting for: {recipe.check}")
        deadline = time.monotonic() + recipe.timeout
        while vm.process.exec("sh", "-c", recipe.check, cwd="/").wait() != 0:
            if time.monotonic() > deadline:
                raise SystemExit(f"error: the check still fails after {recipe.timeout}s")
            time.sleep(5)
        print(f"check-{slug}: ok", flush=True)
    except BaseException:
        print(
            f"\nThe check VM {vm.name} is still running; remove it when you're done.",
            file=sys.stderr,
        )
        raise
    if not keep:
        vm.remove()


def run(vm: VM, script: str) -> None:
    process = vm.process.exec("sh", "-c", script, cwd="/root")
    for line in process.logs:
        print(line, end="", flush=True)
    status = process.wait()
    if status != 0:
        raise SystemExit(f"error: the script exited with {status} on {vm.name}")


def step(message: str) -> None:
    print(f"\n--> {message}", flush=True)


if __name__ == "__main__":
    main()
