"""VM operations that need files or sockets on the MCP client's machine."""

from pathlib import Path
from typing import Any, Dict, List

from .tools import LocalTools, Tool, _cli_command, error_result, home_or_above

STRING = {"type": "string", "minLength": 1}
BOOLEAN = {"type": "boolean"}
PORT = {"type": "integer", "minimum": 1, "maximum": 65535}


def definitions(local: LocalTools) -> List[Tool]:
    def run(kind: str, args: Dict[str, Any]):
        if args.get("confirm") is not True:
            return error_result(
                "Confirm this local file or network operation with confirm=true", "CONFIRM_REQUIRED"
            )
        command = _cli_command() + ["--context", local.context_name, "vm"]
        directory = local.cwd
        name = args.get("name", kind)
        if kind == "build":
            path = Path(args["directory"]).expanduser().resolve()
            if home_or_above(str(path)):
                return error_result(
                    "Choose a project directory below your home folder", "INVALID_ARGS"
                )
            if not path.is_dir():
                return error_result("Build directory does not exist", "INVALID_ARGS")
            directory = str(path)
            command += ["image", "build", str(path), "--json"]
            if args.get("dockerfile"):
                dockerfile = (path / args["dockerfile"]).resolve()
                if path not in dockerfile.parents or not dockerfile.is_file():
                    return error_result(
                        "Dockerfile must be a file inside the build directory", "INVALID_ARGS"
                    )
                command += ["--file", str(dockerfile)]
            if args.get("desktop"):
                command += ["--desktop"]
            if args.get("kiosk"):
                command += ["--kiosk"]
            for secret in args.get("build_secrets", []):
                command += ["--build-secret", secret]
        elif kind in ("upload", "download"):
            path = Path(args["local_path"]).expanduser().resolve()
            if home_or_above(str(path)):
                return error_result(
                    "Choose a file or directory below your home folder", "INVALID_ARGS"
                )
            remote = f"{name}:{args['remote_path']}"
            command += ["scp"] + (["-r"] if args.get("recursive") else [])
            command += [str(path), remote] if kind == "upload" else [remote, str(path)]
        elif kind == "sync":
            path = Path(args["directory"]).expanduser().resolve()
            if home_or_above(str(path)) or not path.is_dir():
                return error_result(
                    "Choose an existing project directory below your home folder", "INVALID_ARGS"
                )
            command += ["sync", name, str(path)]
            if args.get("watch"):
                command += ["--watch"]
        elif kind == "ssh":
            command += ["ssh", name, "--", *args["command"]]
        else:
            command += ["port-forward", name, f"{args['local_port']}:{args['remote_port']}"]
        return local.start_command(
            name,
            directory,
            command,
            key=args["request_key"],
            wait_seconds=args.get("wait_seconds", 0),
            operation="command",
        )

    tools = []
    for name, kind, description, properties, required in [
        (
            "vm_build_image",
            "build",
            (
                "Build a VM image from a local project Dockerfile (e.g. FROM ubuntu:24.04); "
                "no default base is supplied. Desktop is opt-in and supports Ubuntu 24.04 or 22.04. "
                "Returns image_id when the job completes."
            ),
            {
                "directory": STRING,
                "dockerfile": STRING,
                "desktop": BOOLEAN,
                "kiosk": BOOLEAN,
                "build_secrets": {"type": "array", "items": STRING},
            },
            ["directory"],
        ),
        (
            "vm_upload_file",
            "upload",
            "Upload a local file or directory to a VM over authenticated SSH.",
            {"name": STRING, "local_path": STRING, "remote_path": STRING, "recursive": BOOLEAN},
            ["name", "local_path", "remote_path"],
        ),
        (
            "vm_download_file",
            "download",
            "Download a VM file or directory to this machine over authenticated SSH.",
            {"name": STRING, "local_path": STRING, "remote_path": STRING, "recursive": BOOLEAN},
            ["name", "local_path", "remote_path"],
        ),
        (
            "vm_sync",
            "sync",
            "Sync a local project to /workspace. watch=true keeps a background sync job running.",
            {"name": STRING, "directory": STRING, "watch": BOOLEAN},
            ["name", "directory"],
        ),
        (
            "vm_ssh",
            "ssh",
            "Run an explicit command over SSH. Use vm_exec for the managed process transport.",
            {"name": STRING, "command": {"type": "array", "items": STRING, "minItems": 1}},
            ["name", "command"],
        ),
        (
            "vm_port_forward",
            "forward",
            "Forward a loopback port to a VM port in a background job. Cancel the job to close it.",
            {"name": STRING, "local_port": PORT, "remote_port": PORT},
            ["name", "local_port", "remote_port"],
        ),
    ]:
        tools.append(
            (
                {
                    "name": name,
                    "description": description
                    + " Poll deploy_status. Reuse request_key to recover the same job; use a new key after a failure. Requires confirm=true.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [*required, "request_key", "confirm"],
                        "properties": {
                            **properties,
                            "request_key": STRING,
                            "confirm": BOOLEAN,
                            "wait_seconds": {"type": "integer", "minimum": 0, "maximum": 55},
                        },
                    },
                    "annotations": {
                        "readOnlyHint": False,
                        "destructiveHint": kind in ("upload", "download", "sync"),
                        "idempotentHint": False,
                        "openWorldHint": True,
                    },
                    "_meta": {"beam/category": "local_vm"},
                },
                lambda args, operation=kind: run(operation, args),
            )
        )
    return tools
