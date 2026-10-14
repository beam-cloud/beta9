"""MCP adapter for the shared Compose deployment planner."""

from pathlib import Path
from typing import Any, Callable, Dict

from ..deployment.compose import translate


def definition() -> Dict[str, Any]:
    return {
        "name": "stack_from_compose",
        "description": (
            "Translate a docker-compose project into a draft stack spec for stack_plan; "
            "provisions nothing. Compose service names become prefixed app names; postgres "
            "and redis images become managed databases; build: and image: services become "
            "applications (one-shot services that others wait on become jobs); named volumes "
            "become durable disks; bind-mounted files are copied into a derived image; "
            "addresses such as http://minio:9000, redis:6379 or localhost:<published port> "
            "become references; placeholder credentials shared between services become "
            "generated stack secrets. Review every warning before stack_plan: they mark what "
            "compose expresses that Beam does not (private networking, TLS to managed "
            "databases, user, ulimits)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the compose file or the directory holding it.",
                },
                "prefix": {
                    "type": "string",
                    "description": "App name prefix; app names are workspace-wide. Default: the compose project name.",
                },
                "env": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                    "description": "Interpolation variables, overriding the project's .env.",
                },
                "profiles": {"type": "array", "items": {"type": "string"}},
                "files": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "More compose files merged over it in order, relative to its directory, "
                        "as with `docker compose -f a.yml -f b.yml`; use the files the project's "
                        "README deploys with."
                    ),
                },
            },
            "required": ["path"],
            "additionalProperties": False,
        },
        "annotations": {"title": "Translate a compose project", "readOnlyHint": True},
    }


def handler(build_root: Callable[[], Path]) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    from .tools import text_result

    def run(args: Dict[str, Any]) -> Dict[str, Any]:
        result = translate(
            str(args["path"]),
            prefix=args.get("prefix"),
            env=args.get("env"),
            profiles=args.get("profiles"),
            build_root=build_root(),
            files=args.get("files"),
        )
        return text_result(
            "Draft stack spec. Resolve the warnings, adjust the spec, then call stack_plan "
            "with a name and this spec.",
            **result,
        )

    return run
