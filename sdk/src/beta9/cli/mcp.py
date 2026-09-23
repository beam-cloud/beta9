"""
`beam mcp`: an MCP server over stdio plus installers for agent clients. Tools
run the equivalent `beam --json` command and return its JSON.
"""

import datetime
import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import click

from .. import terminal
from ..channel import GatewayHTTP, GatewayHTTPError, ServiceClient, _sdk_version
from ..config import DEFAULT_CONTEXT_NAME, get_config_context
from ..references import complete, validate_env
from . import extraclick
from .extraclick import ClickCommonGroup, cli_command, parse_last_json
from .stubconfig import (
    connect_apps,
    redeploy_with_config,
    set_env,
    stub_config,
    stub_request_from_config,
)

PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "beam"


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="mcp", help="Run or install the Beam MCP server for coding agents.")
def mcp():
    pass


# --- tool definitions -------------------------------------------------------


def _obj(properties: Dict[str, Any], required: Optional[List[str]] = None) -> Dict[str, Any]:
    schema: Dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _cli_args(args: Dict[str, Any], mapping: Dict[str, str]) -> List[str]:
    """Turn tool arguments into CLI flags via mapping {arg: --flag}."""
    out: List[str] = []
    for key, flag in mapping.items():
        value = args.get(key)
        if value is None or value == "" or value is False:
            continue
        if value is True:
            out.append(flag)
        elif isinstance(value, list):
            for item in value:
                out.extend([flag, str(item)])
        else:
            out.extend([flag, str(value)])
    return out


class Tool:
    """CLI-backed (`build` returns argv) or in-process (`run`). `confirm` gates irreversible actions."""

    def __init__(
        self,
        name: str,
        description: str,
        schema: Dict[str, Any],
        build: Optional[Callable[[Dict[str, Any]], List[str]]] = None,
        run: Optional[Callable[[Dict[str, Any], Optional[str]], Any]] = None,
        destructive: bool = False,
        confirm: Optional[str] = None,
        cwd_from: Optional[str] = None,
        per_line: bool = False,
        redact: Tuple[str, ...] = (),
    ):
        self.name = name
        self.description = description
        self.schema = schema
        self.build = build
        self.run = run
        self.destructive = destructive or confirm is not None
        self.confirm = confirm
        self.cwd_from = cwd_from
        self.per_line = per_line
        self.redact = redact  # result keys withheld from the agent (credentials)

    def describe(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.schema,
            "annotations": {
                "destructiveHint": self.destructive,
                "readOnlyHint": not self.destructive,
            },
        }


TOOLS: List[Tool] = [
    Tool(
        "whoami",
        "Active context, workspace id and gateway URLs.",
        _obj({}),
        lambda a: ["whoami", "--format", "json"],
    ),
    Tool(
        "status",
        "Snapshot of the workspace: latest deployments and running containers.",
        _obj({"limit": {"type": "integer", "minimum": 1, "default": 10}}),
        lambda a: ["status", "--format", "json"] + _cli_args(a, {"limit": "--limit"}),
    ),
    Tool(
        "list_deployments",
        "List deployments. Filter with name=..., stub_type=..., active=true.",
        _obj(
            {
                "limit": {"type": "integer", "minimum": 1, "default": 20},
                "filter": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "key=value filters, e.g. name=api",
                },
            }
        ),
        lambda a: (
            ["deployment", "list", "--format", "json"]
            + _cli_args(a, {"limit": "--limit", "filter": "--filter"})
        ),
    ),
    Tool(
        "deploy",
        "Deploy an app from a local directory (runs `beam deploy` there). Returns deployment_id, version and invoke_url. "
        "invoke_url is pinned to this version; wire other services with ${{app.<name>.URL}} instead of copying it.",
        _obj(
            {
                "directory": {
                    "type": "string",
                    "description": "Project directory containing the app.",
                },
                "entrypoint": {
                    "type": "string",
                    "description": "module:function, e.g. app:handler (omit for Dockerfile/entrypoint apps).",
                },
                "name": {"type": "string", "description": "Deployment name."},
                "rollout": {
                    "type": "string",
                    "enum": ["auto", "immediate", "manual"],
                    "default": "auto",
                },
            },
            ["directory", "name"],
        ),
        lambda a: (
            ["deploy", "--json", "--name", a["name"]]
            + ([a["entrypoint"]] if a.get("entrypoint") else [])
            + _cli_args(a, {"rollout": "--rollout"})
        ),
        destructive=True,
        cwd_from="directory",
    ),
    Tool(
        "wait_deployment",
        "Block until a deployment is serving (health check for endpoints, active for others).",
        _obj(
            {"deployment_id": {"type": "string"}, "timeout": {"type": "number", "default": 300}},
            ["deployment_id"],
        ),
        lambda a: (
            ["deployment", "wait", a["deployment_id"]] + _cli_args(a, {"timeout": "--timeout"})
        ),
    ),
    Tool(
        "stop_deployment",
        "Stop a deployment (drains containers; requests fail until started).",
        _obj({"deployment_id": {"type": "string"}}, ["deployment_id"]),
        lambda a: ["deployment", "stop", a["deployment_id"]],
        destructive=True,
    ),
    Tool(
        "start_deployment",
        "Start a stopped deployment.",
        _obj({"deployment_id": {"type": "string"}}, ["deployment_id"]),
        lambda a: ["deployment", "start", a["deployment_id"]],
        destructive=True,
    ),
    Tool(
        "delete_deployment",
        "Delete a deployment. Requires confirm=true.",
        _obj(
            {"deployment_id": {"type": "string"}, "confirm": {"type": "boolean"}}, ["deployment_id"]
        ),
        lambda a: ["deployment", "delete", a["deployment_id"], "--yes"],
        confirm="Deleting a deployment is irreversible.",
    ),
    Tool(
        "scale_deployment",
        "Set the replica count of a pod deployment.",
        _obj(
            {"deployment_id": {"type": "string"}, "containers": {"type": "integer", "minimum": 0}},
            ["deployment_id", "containers"],
        ),
        lambda a: ["deployment", "scale", a["deployment_id"], "--containers", str(a["containers"])],
        destructive=True,
    ),
    Tool(
        "list_tasks",
        "List recent tasks (invocations), newest first.",
        _obj(
            {
                "limit": {"type": "integer", "minimum": 1, "default": 20},
                "stub_id": {"type": "string", "description": "Only tasks of this stub."},
                "status": {
                    "type": "string",
                    "description": "Comma-separated: pending, running, complete, error, cancelled, timeout",
                },
            }
        ),
        lambda a: (
            ["task", "list", "--format", "json"]
            + _cli_args(a, {"limit": "--limit"})
            + (["--filter", f"stub-id={a['stub_id']}"] if a.get("stub_id") else [])
            + (["--filter", f"status={a['status']}"] if a.get("status") else [])
        ),
    ),
    Tool(
        "logs",
        "Fetch recent logs for a deployment, container, task, stub or app (one JSON record per line).",
        _obj(
            {
                "deployment_id": {"type": "string"},
                "container_id": {"type": "string"},
                "task_id": {"type": "string"},
                "stub_id": {"type": "string"},
                "app_id": {"type": "string"},
                "tail": {"type": "integer", "default": 100},
                "search": {"type": "string"},
            }
        ),
        lambda a: (
            ["logs", "--json"]
            + _cli_args(
                a,
                {
                    "deployment_id": "--deployment-id",
                    "container_id": "--container-id",
                    "task_id": "--task-id",
                    "stub_id": "--stub-id",
                    "app_id": "--app-id",
                    "tail": "--tail",
                    "search": "--search",
                },
            )
        ),
        per_line=True,
    ),
    Tool(
        "list_secrets",
        "List workspace secret names.",
        _obj({}),
        lambda a: ["secret", "list", "--format", "json"],
    ),
    Tool(
        "create_secret",
        "Create a workspace secret. Reference it from apps with ${{secret.NAME}} or secrets=[NAME].",
        _obj({"name": {"type": "string"}, "value": {"type": "string"}}, ["name", "value"]),
        lambda a: ["secret", "create", a["name"], a["value"]],
        destructive=True,
    ),
    Tool(
        "list_databases",
        "List managed database services (postgres, redis).",
        _obj({}),
        lambda a: ["db", "list", "--format", "json"],
    ),
    Tool(
        "create_database",
        "Create a managed Postgres, Redis, MySQL or MongoDB service. Credentials are stored as secrets; reference with ${{db.<name>.DATABASE_URL}}.",
        _obj(
            {
                "kind": {"type": "string", "enum": ["postgres", "redis", "mysql", "mongo"]},
                "name": {"type": "string", "description": "lowercase letters, digits, dashes"},
                "always_on": {"type": "boolean", "default": False},
            },
            ["kind", "name"],
        ),
        lambda a: (
            ["db", a["kind"], "create", a["name"], "--format", "json"]
            + (["--min-replicas", "1"] if a.get("always_on") else [])
        ),
        destructive=True,
        redact=("connection_string",),
    ),
    Tool(
        "connect_services",
        "Wire one app to another: adds an env var on `target` referencing `source` (a database's URL or an app's public URL) and redeploys `target`. Default env name is DATABASE_URL / REDIS_URL / <SOURCE>_URL.",
        _obj(
            {
                "source": {"type": "string", "description": "App name to reference"},
                "target": {"type": "string", "description": "App name that receives the env var"},
                "env_name": {"type": "string"},
            },
            ["source", "target"],
        ),
        run=lambda a, c: connect_apps(
            _service(c),
            a["source"],
            a["target"],
            a.get("env_name", ""),
        ),
        destructive=True,
    ),
    Tool(
        "database_credentials",
        "Connection details for a database service.",
        _obj(
            {
                "kind": {"type": "string", "enum": ["postgres", "redis", "mysql", "mongo"]},
                "name": {"type": "string"},
            },
            ["kind", "name"],
        ),
        lambda a: ["db", a["kind"], "credentials", a["name"], "--format", "json"],
    ),
    Tool(
        "rotate_database_credentials",
        "Rotate a database service's password. The database and every deployment bound to its secrets restart with the new credentials.",
        _obj(
            {
                "kind": {"type": "string", "enum": ["postgres", "redis", "mysql", "mongo"]},
                "name": {"type": "string"},
            },
            ["kind", "name"],
        ),
        lambda a: ["db", a["kind"], "rotate", a["name"], "--format", "json"],
        destructive=True,
        redact=("connection_string",),
    ),
    Tool(
        "delete_database",
        "Delete a database service and its credential secrets. Requires confirm=true.",
        _obj(
            {
                "kind": {"type": "string", "enum": ["postgres", "redis", "mysql", "mongo"]},
                "name": {"type": "string"},
                "confirm": {"type": "boolean"},
            },
            ["kind", "name"],
        ),
        lambda a: ["db", a["kind"], "delete", a["name"]],
        confirm="Deleting a database removes its credential secrets.",
    ),
    Tool(
        "api_search",
        "Find gateway REST routes by keyword (discovery for api_call).",
        _obj({"term": {"type": "string"}}),
        lambda a: ["api", "search", a.get("term", ""), "--format", "json"],
    ),
    Tool(
        "api_call",
        "Call a gateway REST route. `{ws}` in path is replaced with the workspace id. Body is a JSON string.",
        _obj(
            {
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"]},
                "path": {"type": "string"},
                "data": {"type": "string", "description": "JSON request body"},
                "query": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "KEY=VALUE query params",
                },
            },
            ["method", "path"],
        ),
        lambda a: (
            ["api", "call", a["method"], a["path"]]
            + _cli_args(a, {"data": "--data", "query": "--query"})
        ),
        destructive=True,
    ),
]

# --- in-process tools: staged changes, request metrics, references ------------

# Paths the gateway applies live; mirrors LIVE_PATHS in the dashboard.
LIVE_PATHS = {
    "keep_warm_seconds",
    "autoscaler.min_containers",
    "autoscaler.max_containers",
    "autoscaler.tasks_per_container",
    "autoscaler.type",
    "checkpoint_enabled",
    "max_pending_tasks",
    "workers",
    "concurrent_requests",
    "task_policy.timeout",
    "task_policy.max_retries",
    "task_policy.ttl",
    "authorized",
}

# The session's staged changes: stub id -> {dotted path: value}.
STAGED: Dict[str, Dict[str, Any]] = {}
STAGED_MESSAGE = {"text": ""}


def _service(context: Optional[str]) -> ServiceClient:
    return ServiceClient(get_config_context(context or DEFAULT_CONTEXT_NAME))


def _http(context: Optional[str]) -> GatewayHTTP:
    return _service(context).http


def _cli_env() -> Dict[str, str]:
    env = {**os.environ, "BETA9_NO_INPUT": "1"}
    env.setdefault("BEAM_CALLER", f"mcp/{_sdk_version()}")
    return env


def _set_path(obj: Dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cursor = obj
    for part in parts[:-1]:
        cursor = cursor.setdefault(part, {})
        if not isinstance(cursor, dict):
            raise ValueError(f"{path}: {part} is not an object")
    cursor[parts[-1]] = value


def _describe_staged() -> Dict[str, Any]:
    return {
        "message": STAGED_MESSAGE["text"],
        "stubs": [
            {
                "stub_id": stub_id,
                "fields": fields,
                "apply": "redeploy" if any(p not in LIVE_PATHS for p in fields) else "patch",
            }
            for stub_id, fields in STAGED.items()
        ],
    }


def stage_config(args: Dict[str, Any], _: Optional[str]) -> Any:
    fields = args.get("fields") or {}
    if not isinstance(fields, dict) or not fields:
        return {
            "error": "fields must be a non-empty object of dotted paths",
            "code": "INVALID_CONFIG",
        }
    STAGED.setdefault(args["stub_id"], {}).update(fields)
    return _describe_staged()


def discard_staged(args: Dict[str, Any], _: Optional[str]) -> Any:
    if args.get("stub_id"):
        STAGED.pop(args["stub_id"], None)
    else:
        STAGED.clear()
        STAGED_MESSAGE["text"] = ""
    return _describe_staged()


def accept_deploy(args: Dict[str, Any], context: Optional[str]) -> Any:
    """Live paths are patched in place; anything else becomes a new stub + version."""
    if not STAGED:
        return {"error": "Nothing is staged.", "code": "NOT_FOUND"}
    STAGED_MESSAGE["text"] = args.get("message") or STAGED_MESSAGE["text"]
    http = _http(context)
    results = []
    for stub_id, fields in list(STAGED.items()):
        mode = "patch" if all(p in LIVE_PATHS for p in fields) else "redeploy"
        try:
            if mode == "patch":
                http.json("PATCH", f"/api/v1/stub/{{ws}}/{stub_id}/config", json={"fields": fields})
                results.append(
                    {"stub_id": stub_id, "mode": mode, "ok": True, "fields": sorted(fields)}
                )
            else:
                stub = http.json("GET", f"/api/v1/stub/{{ws}}/{stub_id}")
                config = stub_config(stub)
                for path, value in fields.items():
                    _set_path(config, path, value)
                created = http.json(
                    "POST", "/api/v1/gateway/stubs", json=stub_request_from_config(stub, config)
                )
                if not created.get("ok"):
                    raise RuntimeError(created.get("errMsg") or "stub creation failed")
                name = (stub.get("app") or {}).get("name") or stub["name"]
                deployed = http.json(
                    "POST",
                    "/api/v1/gateway/stubs/deploy",
                    json={"stub_id": created["stubId"], "name": name},
                )
                if not deployed.get("ok"):
                    raise RuntimeError(deployed.get("errMsg") or "deploy failed")
                results.append(
                    {
                        "stub_id": stub_id,
                        "mode": mode,
                        "ok": True,
                        "new_stub_id": created["stubId"],
                        "deployment_id": deployed.get("deploymentId"),
                        "version": deployed.get("version"),
                    }
                )
            STAGED.pop(stub_id, None)
        except Exception as exc:  # report per stub, keep the rest staged
            results.append({"stub_id": stub_id, "mode": mode, "ok": False, "error": str(exc)})
    if not STAGED:
        STAGED_MESSAGE["text"] = ""
    return {"results": results, "remaining": _describe_staged()}


def _request_stats(args: Dict[str, Any], context: Optional[str]) -> Dict[str, Any]:
    """Sum the window's endpoint.request_stats records into one aggregate."""
    minutes = int(args.get("window_minutes") or 60)
    end = datetime.datetime.now(datetime.timezone.utc)
    start = end - datetime.timedelta(minutes=minutes)
    history = _http(context).json(
        "GET",
        "/api/v1/events/{ws}/history",
        params={
            "stub_id": args["stub_id"],
            "event_types": "endpoint.request_stats",
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
            "limit": 5000,
        },
    )
    total = {"requests": 0, "status_5xx": 0, "duration_sum_ms": 0, "duration_max_ms": 0}
    buckets: List[int] = []
    bounds: List[int] = []
    for e in (history or {}).get("events", []):
        d = e.get("cloud_event", {}).get("data", {})
        for k in ("requests", "status_5xx", "duration_sum_ms"):
            total[k] += int(d.get(k) or 0)
        total["duration_max_ms"] = max(total["duration_max_ms"], int(d.get("duration_max_ms") or 0))
        counts = d.get("latency_buckets") or []
        if len(counts) > len(buckets):
            buckets += [0] * (len(counts) - len(buckets))
            bounds = d.get("latency_bounds_ms") or bounds
        for i, c in enumerate(counts):
            buckets[i] += int(c)

    def percentile(p: float) -> int:
        target, seen = total["requests"] * p / 100, 0
        for i, c in enumerate(buckets):
            seen += c
            if seen >= target:
                bound = int(bounds[i]) if i < len(bounds) else total["duration_max_ms"]
                return min(bound, total["duration_max_ms"])
        return 0

    return {
        "stub_id": args["stub_id"],
        "window_minutes": minutes,
        **total,
        "percentile": percentile,
    }


def http_requests(args: Dict[str, Any], context: Optional[str]) -> Any:
    stats = _request_stats(args, context)
    return {
        "stub_id": stats["stub_id"],
        "window_minutes": stats["window_minutes"],
        "requests": stats["requests"],
        "per_minute": round(stats["requests"] / stats["window_minutes"], 3),
    }


def http_error_rate(args: Dict[str, Any], context: Optional[str]) -> Any:
    stats = _request_stats(args, context)
    requests, errors = stats["requests"], stats["status_5xx"]
    return {
        "stub_id": stats["stub_id"],
        "window_minutes": stats["window_minutes"],
        "requests": requests,
        "errors_5xx": errors,
        "error_rate": round(errors / requests, 4) if requests else 0.0,
    }


def http_response_time(args: Dict[str, Any], context: Optional[str]) -> Any:
    stats = _request_stats(args, context)
    return {
        "stub_id": stats["stub_id"],
        "window_minutes": stats["window_minutes"],
        "requests": stats["requests"],
        "p50_ms": stats["percentile"](50),
        "p95_ms": stats["percentile"](95),
        "p99_ms": stats["percentile"](99),
        "max_ms": stats["duration_max_ms"],
    }


def validate_references_tool(args: Dict[str, Any], _: Optional[str]) -> Any:
    out: Dict[str, Any] = {"problems": validate_env(args.get("env") or [])}
    if args.get("complete") is not None:
        out["completions"] = complete(args["complete"])
    out["ok"] = not out["problems"]
    return out


# --- apps, invoke, env, stacks -------------------------------------------------

_INVOCABLE = ("endpoint", "asgi", "taskqueue", "function")

# Config keys an agent acts on; the rest is runner plumbing.
_CONFIG_KEYS = (
    "runtime",
    "autoscaler",
    "keep_warm_seconds",
    "workers",
    "concurrent_requests",
    "max_pending_tasks",
    "task_policy",
    "env",
    "ports",
    "tcp",
    "authorized",
    "entry_point",
    "volumes",
    "disks",
    "pool",
)


def _apps(http: GatewayHTTP) -> List[Dict[str, Any]]:
    return http.json("GET", "/api/v1/app/{ws}/latest", params={"limit": 200}).get("data") or []


def _app(http: GatewayHTTP, name: str) -> Dict[str, Any]:
    for app in _apps(http):
        if app["name"] == name:
            return app
    raise RuntimeError(f"no app named {name}")


def _latest_url(url: str) -> str:
    """Version-pinned deployment URL -> its `latest` alias (path or subdomain form)."""
    return re.sub(r"-v\d+\.", "-latest.", re.sub(r"/v\d+$", "/latest", url))


def _app_summary(app: Dict[str, Any]) -> Dict[str, Any]:
    deployment = app.get("deployment") or {}
    stub = app.get("stub") or {}
    return {
        "name": app["name"],
        "app_id": app["id"],
        "stub_type": deployment.get("stub_type") or stub.get("type"),
        "stub_id": deployment.get("stub_id") or stub.get("id"),
        "deployment_id": deployment.get("id"),
        "version": deployment.get("version"),
        "active": deployment.get("active"),
        "running_containers": app.get("running_containers", 0),
        "url": app.get("url"),
        "latest_url": _latest_url(app["url"]) if deployment and app.get("url") else None,
    }


def list_apps(_: Dict[str, Any], context: Optional[str]) -> Any:
    return [_app_summary(app) for app in _apps(_http(context))]


def get_app(args: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    summary = _app_summary(_app(http, args["name"]))
    config: Dict[str, Any] = {}
    if summary["stub_id"]:
        config = stub_config(http.json("GET", f"/api/v1/stub/{{ws}}/{summary['stub_id']}"))
    view = {k: config[k] for k in _CONFIG_KEYS if k in config}
    view["secrets"] = [
        {"name": s["name"], "env_name": s.get("env_name") or s["name"]}
        for s in config.get("secrets") or []
    ]
    return {**summary, "config": view}


def invoke(args: Dict[str, Any], context: Optional[str]) -> Any:
    import requests

    summary = _app_summary(_app(_http(context), args["name"]))
    kind = (summary["stub_type"] or "").split("/")[0]
    if kind not in _INVOCABLE or not summary["latest_url"]:
        raise RuntimeError(f"{args['name']} is not an invocable deployment")
    url = summary["latest_url"]
    if path := (args.get("path") or "").lstrip("/"):
        url += "/" + path
    response = requests.request(
        args.get("method") or "POST",
        url,
        headers=_http(context).headers,
        json=args.get("body"),
        timeout=float(args.get("timeout") or 180),
    )
    try:
        body: Any = response.json()
    except ValueError:
        body = response.text[-4000:]
    return {"status": response.status_code, "url": url, "body": body}


def set_env_tool(args: Dict[str, Any], context: Optional[str]) -> Any:
    env: Dict[str, str] = args.get("env") or {}
    unset: List[str] = args.get("unset") or []
    if not env and not unset:
        return {"error": "env or unset is required", "code": "INVALID_CONFIG"}
    if problems := validate_env([f"{k}={v}" for k, v in env.items()]):
        return {"error": "; ".join(problems), "code": "INVALID_REFERENCE"}
    service = _service(context)
    summary = _app_summary(_app(service.http, args["name"]))
    if not summary["deployment_id"]:
        raise RuntimeError(f"{args['name']} has nothing deployed")

    def mutate(config: Dict[str, Any]) -> None:
        for key, value in env.items():
            set_env(config, key, value)
        config["env"] = [e for e in config.get("env") or [] if e.split("=", 1)[0] not in unset]
        config["secrets"] = [
            s for s in config.get("secrets") or [] if (s.get("env_name") or s["name"]) not in unset
        ]

    return redeploy_with_config(service, args["name"], summary["stub_id"], mutate)


def delete_app(args: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    app = _app(http, args["name"])
    http.json("DELETE", f"/api/v1/app/{{ws}}/{app['id']}")
    return {"deleted": args["name"], "app_id": app["id"]}


def _stacks(http: GatewayHTTP) -> List[Dict[str, Any]]:
    return http.json("GET", "/api/v1/stack/{ws}") or []


def _stack(http: GatewayHTTP, name: str) -> Dict[str, Any]:
    for stack in _stacks(http):
        if stack["name"] == name:
            return stack
    raise RuntimeError(f"no stack named {name}")


def _stack_view(http: GatewayHTTP, stack: Dict[str, Any]) -> Dict[str, Any]:
    names = {a["id"]: a["name"] for a in _apps(http)}
    ids = (stack.get("spec") or {}).get("appIds") or []
    return {"name": stack["name"], "id": stack["id"], "apps": [names.get(i, i) for i in ids]}


def list_stacks(_: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    return [_stack_view(http, s) for s in _stacks(http)]


def create_stack(args: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    ids = [_app(http, n)["id"] for n in args.get("apps") or []]
    body = {"name": args["name"], "spec": {"appIds": ids}}
    return _stack_view(http, http.json("POST", "/api/v1/stack/{ws}", json=body))


def update_stack(args: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    stack = _stack(http, args["name"])
    ids = list((stack.get("spec") or {}).get("appIds") or [])
    ids += [i for i in (_app(http, n)["id"] for n in args.get("add") or []) if i not in ids]
    drop = {_app(http, n)["id"] for n in args.get("remove") or []}
    ids = [i for i in ids if i not in drop]
    spec = {**(stack.get("spec") or {}), "appIds": ids}
    spec["positions"] = {k: v for k, v in (spec.get("positions") or {}).items() if k in ids}
    body = {"name": stack["name"], "spec": spec}
    return _stack_view(http, http.json("PUT", f"/api/v1/stack/{{ws}}/{stack['id']}", json=body))


def delete_stack(args: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    stack = _stack(http, args["name"])
    http.json("DELETE", f"/api/v1/stack/{{ws}}/{stack['id']}")
    return {"deleted": stack["name"]}


_RUN_FUNCTION = """
import importlib, json, sys
from beta9.abstractions.base import set_channel
from beta9.config import get_config_context
set_channel(context=get_config_context(sys.argv[1]))
module, name = sys.argv[2].split(":")
fn = getattr(importlib.import_module(module), name)
result = fn.remote(**json.loads(sys.argv[3]))
print("__RESULT__" + json.dumps(result, default=str))
"""


def run_function(args: Dict[str, Any], context: Optional[str]) -> Any:
    entrypoint = args["entrypoint"]
    if ":" not in entrypoint:
        return {"error": "entrypoint must be module:function", "code": "INVALID_ARGS"}
    command = [
        sys.executable,
        "-c",
        _RUN_FUNCTION,
        context or DEFAULT_CONTEXT_NAME,
        entrypoint.replace(".py:", ":"),
        json.dumps(args.get("args") or {}),
    ]
    try:
        proc = subprocess.run(
            command,
            cwd=args["directory"],
            env=_cli_env(),
            capture_output=True,
            text=True,
            timeout=float(args.get("timeout") or 900),
        )
    except subprocess.TimeoutExpired:
        return {"error": "run_function timed out", "code": "TIMEOUT"}
    output, marker, result = proc.stdout.rpartition("__RESULT__")
    if not marker:
        return {
            "error": (proc.stderr or output).strip()[-4000:] or "function did not return",
            "code": "ERROR",
        }
    return {"result": json.loads(result), "logs": output.strip()[-4000:]}


def _http_stats_tool(
    name: str, description: str, run: Callable[[Dict[str, Any], Optional[str]], Any]
) -> Tool:
    return Tool(
        name,
        description,
        _obj(
            {
                "stub_id": {"type": "string", "description": "Deployment stub id (endpoint/asgi)."},
                "window_minutes": {
                    "type": "integer",
                    "default": 60,
                    "minimum": 1,
                    "maximum": 10080,
                },
            },
            ["stub_id"],
        ),
        run=run,
    )


TOOLS += [
    Tool(
        "stage_config",
        "Stage config edits for a stub without applying them. Dotted paths (keep_warm_seconds, autoscaler.max_containers, runtime.cpu, runtime.memory, env). Live paths patch in place on accept_deploy; others create a new version.",
        _obj(
            {
                "stub_id": {"type": "string"},
                "fields": {"type": "object", "additionalProperties": True},
            },
            ["stub_id", "fields"],
        ),
        run=stage_config,
    ),
    Tool(
        "staged_changes",
        "Show the session's staged changes and how each would apply.",
        _obj({}),
        run=lambda a, c: _describe_staged(),
    ),
    Tool(
        "discard_staged",
        "Drop staged changes (all, or one stub's).",
        _obj({"stub_id": {"type": "string"}}),
        run=discard_staged,
        destructive=True,
    ),
    Tool(
        "accept_deploy",
        "Apply every staged change: PATCH live paths, redeploy stubs whose staged paths need a new version. Requires confirm=true.",
        _obj({"confirm": {"type": "boolean"}, "message": {"type": "string"}}),
        run=accept_deploy,
        confirm="accept_deploy applies staged changes to the workspace.",
    ),
    _http_stats_tool(
        "http_requests",
        "Request count for an endpoint over a window.",
        http_requests,
    ),
    _http_stats_tool(
        "http_error_rate", "Share of 5xx responses for an endpoint over a window.", http_error_rate
    ),
    _http_stats_tool(
        "http_response_time",
        "p50/p95/p99 latency (ms) for an endpoint over a window; percentiles come from a fixed histogram, so they are upper bounds.",
        http_response_time,
    ),
    Tool(
        "validate_references",
        "Check the syntax of ${{...}} references in KEY=VALUE env entries; whether the named database or app exists is checked by the gateway at deploy time.",
        _obj(
            {
                "env": {"type": "array", "items": {"type": "string"}},
                "complete": {"type": "string", "description": "partial expression after ${{"},
            }
        ),
        run=validate_references_tool,
    ),
    Tool(
        "list_webhooks",
        "Workspace webhooks (URL, event types, enabled).",
        _obj({}),
        lambda a: ["api", "call", "GET", "/api/v1/webhook/{ws}"],
    ),
    Tool(
        "create_webhook",
        "Register a signed HTTP webhook for workspace events (e.g. stub.*, task.*, endpoint.request_stats). Returns the signing secret once.",
        _obj(
            {
                "url": {"type": "string"},
                "event_types": {"type": "array", "items": {"type": "string"}},
                "description": {"type": "string"},
            },
            ["url"],
        ),
        lambda a: [
            "api",
            "call",
            "POST",
            "/api/v1/webhook/{ws}",
            "--data",
            json.dumps(
                {
                    "url": a["url"],
                    "event_types": a.get("event_types") or ["stub.*", "task.*"],
                    "description": a.get("description", ""),
                }
            ),
        ],
        destructive=True,
    ),
    Tool(
        "template_plan",
        "Show the ordered steps of a template manifest (path, URL, or name from beam-cloud/beam-skills/templates).",
        _obj(
            {"source": {"type": "string"}, "prefix": {"type": "string", "default": ""}}, ["source"]
        ),
        lambda a: ["template", "plan", a["source"]] + _cli_args(a, {"prefix": "--prefix"}),
    ),
    Tool(
        "deploy_template",
        "Deploy every service in a template manifest in dependency order (databases first). Requires confirm=true.",
        _obj(
            {
                "source": {"type": "string"},
                "prefix": {"type": "string", "default": ""},
                "only": {"type": "array", "items": {"type": "string"}},
                "confirm": {"type": "boolean"},
            },
            ["source"],
        ),
        lambda a: (
            ["template", "deploy", a["source"], "--yes"]
            + _cli_args(a, {"prefix": "--prefix", "only": "--only"})
        ),
        confirm="deploy_template creates databases and deployments; use template_plan first.",
    ),
    Tool(
        "export_template",
        "Save existing apps as a template manifest (secret values are never included).",
        _obj(
            {
                "apps": {"type": "array", "items": {"type": "string"}},
                "name": {"type": "string", "default": "exported"},
            },
            ["apps"],
        ),
        lambda a: ["template", "export"] + list(a["apps"]) + _cli_args(a, {"name": "--name"}),
    ),
    Tool(
        "list_apps",
        "Apps in the workspace with their latest deployment and URLs.",
        _obj({}),
        run=list_apps,
    ),
    Tool(
        "get_app",
        "One app: its config (resources, scaling, env, secret bindings, ports) and URLs.",
        _obj({"name": {"type": "string"}}, ["name"]),
        run=get_app,
    ),
    Tool(
        "delete_app",
        "Delete an app and all of its deployments and versions. Requires confirm=true.",
        _obj({"name": {"type": "string"}, "confirm": {"type": "boolean"}}, ["name"]),
        run=delete_app,
        confirm="delete_app removes every deployment of the app.",
    ),
    Tool(
        "invoke",
        "Call a deployed endpoint, ASGI app, task queue or function by name (latest version) with the workspace token. Task queues and functions return a task_id; follow it with list_tasks or get_task.",
        _obj(
            {
                "name": {"type": "string", "description": "App name"},
                "method": {"type": "string", "default": "POST"},
                "path": {"type": "string", "description": "Sub-path for ASGI apps, e.g. /notes"},
                "body": {"description": "JSON body"},
                "timeout": {"type": "number", "default": 180},
            },
            ["name"],
        ),
        run=invoke,
        destructive=True,
    ),
    Tool(
        "get_task",
        "Status, timing and result of one task.",
        _obj({"task_id": {"type": "string"}}, ["task_id"]),
        run=lambda a, c: _http(c).json("GET", f"/api/v1/task/{{ws}}/{a['task_id']}"),
    ),
    Tool(
        "stop_task",
        "Stop a running or pending task.",
        _obj({"task_id": {"type": "string"}}, ["task_id"]),
        lambda a: ["task", "stop", a["task_id"]],
        destructive=True,
    ),
    Tool(
        "run_function",
        "Run a @function from a local directory on the control plane and return its result: the image is built if needed, then fn.remote(**args) is called. Blocks until the function finishes.",
        _obj(
            {
                "directory": {"type": "string"},
                "entrypoint": {"type": "string", "description": "module:function, e.g. app:square"},
                "args": {"type": "object", "description": "Keyword arguments"},
                "timeout": {"type": "number", "default": 900},
            },
            ["directory", "entrypoint"],
        ),
        run=run_function,
        destructive=True,
    ),
    Tool(
        "run_pod",
        "Start a one-off container from a local directory: a Pod handler (app.py:pod) or a command on an image. Returns container_id; read output with logs and stop it with stop_task.",
        _obj(
            {
                "directory": {"type": "string"},
                "entrypoint": {"type": "string", "description": "module:pod, omit for a command"},
                "command": {
                    "type": "string",
                    "description": "argv, no shell: use 'sh -c \"...\"' for pipes",
                },
                "image": {"type": "string", "description": "e.g. python:3.12"},
                "cpu": {"type": "number"},
                "memory": {"type": "string"},
                "gpu": {"type": "string"},
                "env": {"type": "array", "items": {"type": "string"}, "description": "KEY=VALUE"},
            },
            ["directory"],
        ),
        lambda a: (
            ["run", "--detach", "--json"]
            + ([a["entrypoint"]] if a.get("entrypoint") else [])
            + _cli_args(
                a,
                {
                    "command": "--command",
                    "image": "--image",
                    "cpu": "--cpu",
                    "memory": "--memory",
                    "gpu": "--gpu",
                    "env": "--env",
                },
            )
        ),
        destructive=True,
        cwd_from="directory",
    ),
    Tool(
        "set_env",
        "Set or remove environment variables on a deployed app and redeploy it as a new version. Values may be ${{secret.NAME}}, ${{db.NAME.DATABASE_URL}} or ${{app.NAME.URL}} references.",
        _obj(
            {
                "name": {"type": "string"},
                "env": {"type": "object", "additionalProperties": {"type": "string"}},
                "unset": {"type": "array", "items": {"type": "string"}},
            },
            ["name"],
        ),
        run=set_env_tool,
        destructive=True,
    ),
    Tool(
        "update_secret",
        "Change a workspace secret's value. Deployments pick it up on their next container start.",
        _obj({"name": {"type": "string"}, "value": {"type": "string"}}, ["name", "value"]),
        lambda a: ["secret", "modify", a["name"], a["value"]],
        destructive=True,
    ),
    Tool(
        "delete_secret",
        "Delete a workspace secret. Requires confirm=true.",
        _obj({"name": {"type": "string"}, "confirm": {"type": "boolean"}}, ["name"]),
        lambda a: ["secret", "delete", a["name"]],
        confirm="delete_secret breaks deployments still bound to it.",
    ),
    Tool(
        "list_stacks",
        "Stacks: named groups of apps shown together on the dashboard board.",
        _obj({}),
        run=list_stacks,
    ),
    Tool(
        "create_stack",
        "Create a stack, optionally with apps (by name).",
        _obj(
            {"name": {"type": "string"}, "apps": {"type": "array", "items": {"type": "string"}}},
            ["name"],
        ),
        run=create_stack,
        destructive=True,
    ),
    Tool(
        "update_stack",
        "Add or remove apps (by name) on a stack. Apps are untouched; only membership changes.",
        _obj(
            {
                "name": {"type": "string"},
                "add": {"type": "array", "items": {"type": "string"}},
                "remove": {"type": "array", "items": {"type": "string"}},
            },
            ["name"],
        ),
        run=update_stack,
        destructive=True,
    ),
    Tool(
        "delete_stack",
        "Delete a stack. Its apps are untouched.",
        _obj({"name": {"type": "string"}}, ["name"]),
        run=delete_stack,
        destructive=True,
    ),
    Tool(
        "list_volumes",
        "Persistent volumes in the workspace (mount with Volume(name, mount_path) in app code).",
        _obj({}),
        lambda a: ["volume", "list", "--format", "json"],
    ),
    Tool(
        "create_volume",
        "Create a persistent volume.",
        _obj({"name": {"type": "string"}}, ["name"]),
        lambda a: ["volume", "create", a["name"], "--format", "json"],
        destructive=True,
    ),
]

TOOLS_BY_NAME = {tool.name: tool for tool in TOOLS}


# --- server ------------------------------------------------------------------


def _run_tool(tool: Tool, args: Dict[str, Any], context: Optional[str]) -> Any:
    if tool.confirm and not args.get("confirm"):
        return {
            "error": f"{tool.confirm} Call again with confirm=true.",
            "code": "NEEDS_CONFIRMATION",
        }
    if tool.run is not None:
        try:
            return tool.run(args, context)
        except GatewayHTTPError as exc:
            return {
                "error": exc.message,
                "code": "NOT_AUTHENTICATED" if exc.status == 401 else "ERROR",
            }
        except Exception as exc:  # surface as a tool error; keep the server alive
            return {"error": str(exc), "code": "ERROR"}

    command = cli_command() + ["--json"]
    if context:
        command += ["--context", context]
    command += tool.build(args)  # type: ignore[misc]

    cwd = args.get(tool.cwd_from) if tool.cwd_from else None
    try:
        proc = subprocess.run(
            command,
            cwd=cwd,
            env={**_cli_env(), "BETA9_JSON": "1"},
            capture_output=True,
            text=True,
            timeout=900,
        )
    except subprocess.TimeoutExpired:
        return {"error": f"{tool.name} timed out", "code": "TIMEOUT"}
    except FileNotFoundError as exc:
        return {"error": str(exc), "code": "ERROR"}

    stdout = proc.stdout.strip()
    parsed: Any = None
    if stdout and tool.per_line:
        parsed = [
            record for record in map(parse_last_json, stdout.splitlines()) if record is not None
        ]
    elif stdout:
        parsed = parse_last_json(stdout)
    if stdout and parsed is None:
        parsed = {"output": stdout}

    if isinstance(parsed, dict):
        for key in tool.redact:
            parsed.pop(key, None)
    if proc.returncode != 0:
        if isinstance(parsed, dict) and "error" in parsed:
            return parsed
        return {
            "error": (proc.stderr or stdout or f"{tool.name} failed").strip()[-2000:],
            "code": "ERROR",
        }
    return parsed if parsed is not None else {"ok": True}


class StdioServer:
    def __init__(self, context: Optional[str]):
        self.context = context

    def _send(self, message: Dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(message) + "\n")
        sys.stdout.flush()

    def _result(self, request_id: Any, result: Any) -> None:
        self._send({"jsonrpc": "2.0", "id": request_id, "result": result})

    def _error(self, request_id: Any, code: int, message: str) -> None:
        self._send(
            {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}
        )

    def handle(self, message: Dict[str, Any]) -> None:
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params") or {}

        if method == "initialize":
            self._result(
                request_id,
                {
                    "protocolVersion": params.get("protocolVersion") or PROTOCOL_VERSION,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": SERVER_NAME, "version": _sdk_version()},
                    "instructions": (
                        "Beam runs serverless GPU/CPU apps, one-off containers and managed databases. "
                        "Start with whoami and list_apps. Ship code with deploy (from a project directory), "
                        "then wait_deployment on the returned deployment_id, invoke it by app name, and read "
                        "logs. Provision databases with create_database and wire them with connect_services "
                        "or set_env using ${{db.NAME.DATABASE_URL}}, ${{secret.NAME}} and ${{app.NAME.URL}}; "
                        "get_app shows the resulting config. Group related apps with create_stack. "
                        "run_function and run_pod execute local code without deploying. Tools marked "
                        "destructive change infrastructure; those that say so need confirm=true."
                    ),
                },
            )
        elif method in ("notifications/initialized", "notifications/cancelled"):
            return
        elif method == "ping":
            self._result(request_id, {})
        elif method == "tools/list":
            self._result(request_id, {"tools": [tool.describe() for tool in TOOLS]})
        elif method == "tools/call":
            name = params.get("name")
            tool = TOOLS_BY_NAME.get(name)
            if tool is None:
                self._error(request_id, -32602, f"Unknown tool {name!r}")
                return
            result = _run_tool(tool, params.get("arguments") or {}, self.context)
            is_error = isinstance(result, dict) and "error" in result
            self._result(
                request_id,
                {
                    "content": [
                        {"type": "text", "text": json.dumps(result, indent=2, default=str)}
                    ],
                    "structuredContent": result if isinstance(result, dict) else {"items": result},
                    "isError": is_error,
                },
            )
        elif request_id is not None:
            self._error(request_id, -32601, f"Method not found: {method}")

    def serve_forever(self) -> None:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                self._error(None, -32700, "Parse error")
                continue
            try:
                self.handle(message)
            except Exception as exc:  # keep the server alive on tool bugs
                self._error(message.get("id"), -32603, str(exc))


@mcp.command(name="serve", help="Serve MCP over stdio (what agent clients launch).")
@extraclick.config_context_option
def serve(context: Optional[str]):
    StdioServer(context).serve_forever()


@mcp.command(name="tools", help="List the tools the server exposes.")
def tools():
    terminal.print_json(
        [
            {"name": t.name, "description": t.description, "destructive": t.destructive}
            for t in TOOLS
        ]
    )


# --- installers ----------------------------------------------------------------


def _server_entry(context: Optional[str]) -> Dict[str, Any]:
    # Pin non-default contexts only.
    context = context or extraclick.selected_context()
    args = ["mcp", "serve"]
    if context and context != DEFAULT_CONTEXT_NAME:
        args += ["--context", context]
    return {"command": terminal.cli_name(), "args": args}


def _merge_json(path: Path, key: str, entry: Dict[str, Any]) -> None:
    data: Dict[str, Any] = {}
    if path.exists():
        try:
            data = json.loads(path.read_text() or "{}")
        except json.JSONDecodeError:
            raise click.ClickException(
                f"{path} is not valid JSON; fix it or pass --print to install by hand."
            )
    servers = data.setdefault(key, {})
    servers[SERVER_NAME] = entry
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def _codex_toml(path: Path, entry: Dict[str, Any]) -> None:
    block = f'\n[mcp_servers.{SERVER_NAME}]\ncommand = "{entry["command"]}"\nargs = {json.dumps(entry["args"])}\n'
    existing = path.read_text() if path.exists() else ""
    if f"[mcp_servers.{SERVER_NAME}]" in existing:
        terminal.detail(
            f"{path} already has a [mcp_servers.{SERVER_NAME}] block; leaving it unchanged."
        )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(existing.rstrip("\n") + block)


CLIENTS = ("cursor", "claude-code", "claude-desktop", "codex", "windsurf")


def _client_path(client: str, project: bool) -> Optional[Path]:
    home = Path.home()
    if client == "cursor":
        return Path.cwd() / ".cursor" / "mcp.json" if project else home / ".cursor" / "mcp.json"
    if client == "claude-code":
        return Path.cwd() / ".mcp.json" if project else home / ".claude.json"
    if client == "claude-desktop":
        if platform.system() == "Darwin":
            return (
                home / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
            )
        return home / ".config" / "Claude" / "claude_desktop_config.json"
    if client == "codex":
        return home / ".codex" / "config.toml"
    if client == "windsurf":
        return home / ".codeium" / "windsurf" / "mcp_config.json"
    return None


@mcp.command(name="install", help="Register the server with an agent client.")
@click.option("--client", type=click.Choice(CLIENTS), required=True)
@click.option(
    "--project",
    is_flag=True,
    help="Write project-scoped config in the current directory when the client supports it.",
)
@click.option(
    "--print", "print_only", is_flag=True, help="Print the config snippet instead of writing it."
)
@extraclick.config_context_option
def install(client: str, project: bool, print_only: bool, context: Optional[str]):
    entry = _server_entry(context)
    path = _client_path(client, project)
    if print_only or path is None:
        terminal.print_json({"mcpServers": {SERVER_NAME: entry}})
        return

    if client == "codex":
        _codex_toml(path, entry)
    else:
        _merge_json(path, "mcpServers", entry)

    if terminal.json_output():
        terminal.print_json({"client": client, "path": str(path), "server": entry})
    else:
        terminal.success(f"Registered the {SERVER_NAME} MCP server for {client}")
        terminal.detail(f"{path}")
        terminal.detail("Restart the client to pick it up.")
