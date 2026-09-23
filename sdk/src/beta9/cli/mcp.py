"""
`mcp`: an MCP server over stdio plus installers for agent clients. Tools call
the SDK in-process; `deploy` and `run_pod` run the CLI in the project directory
because they sync files and stream builds.
"""

import datetime
import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import click
from betterproto import Casing

from .. import terminal
from ..channel import GatewayHTTP, GatewayHTTPError, ServiceClient, _sdk_version
from ..clients.gateway import (
    DeleteDeploymentRequest,
    ListContainersRequest,
    ListDeploymentsRequest,
    ListTasksRequest,
    ScaleDeploymentRequest,
    StartDeploymentRequest,
    StopDeploymentRequest,
    StopTasksRequest,
    StringList,
)
from ..clients.secret import (
    CreateSecretRequest,
    DeleteSecretRequest,
    GetSecretRequest,
    ListSecretsRequest,
    UpdateSecretRequest,
)
from ..clients.volume import GetOrCreateVolumeRequest, ListVolumesRequest
from ..config import DEFAULT_CONTEXT_NAME, get_config_context, get_settings
from ..references import complete, validate_env
from . import extraclick
from .api import ECHO_ROUTES, _load_specs, _spec_routes
from .database import _redis_fields, _result as database_result
from .deployment import DeploymentNotReady, wait_for_deployment
from .extraclick import ClickCommonGroup, cli_command, parse_last_json
from .stubconfig import (
    connect_apps,
    redeploy_with_config,
    set_env,
    stub_config,
    stub_request_from_config,
)
from .template import (
    deploy_template as run_template,
    load_manifest,
    manifest_service,
    missing_secrets,
    plan_steps,
)

PROTOCOL_VERSION = "2024-11-05"
DATABASE_KINDS = ["postgres", "redis", "mysql", "mongo"]


def _server_name() -> str:
    return get_settings().name.lower()


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="mcp", help="Run or install the MCP server for coding agents.")
def mcp():
    pass


# --- tool plumbing -------------------------------------------------------------


def _obj(properties: Dict[str, Any], required: Optional[List[str]] = None) -> Dict[str, Any]:
    schema: Dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


ToolFn = Callable[[Dict[str, Any], Optional[str]], Any]


class Tool:
    """`run(args, context)` returns JSON. `confirm` gates irreversible actions."""

    def __init__(
        self,
        name: str,
        description: str,
        schema: Dict[str, Any],
        run: ToolFn,
        destructive: bool = False,
        confirm: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.schema = schema
        self.run = run
        self.destructive = destructive or confirm is not None
        self.confirm = confirm

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


def _proto(message: Any) -> Any:
    return message.to_dict(casing=Casing.SNAKE)


def _grpc(response: Any, ok_value: Any = None) -> Any:
    """Turn a gateway response into tool output; failures become errors."""
    if not response.ok:
        raise RuntimeError(response.err_msg or "request failed")
    return ok_value if ok_value is not None else {"ok": True}


def _cli(
    args: List[str], context: Optional[str], cwd: Optional[str] = None, timeout: int = 900
) -> Any:
    """Run the CLI for the few operations that are process-shaped (deploy, run)."""
    command = cli_command() + ["--json"] + (["--context", context] if context else []) + args
    try:
        proc = subprocess.run(
            command,
            cwd=cwd,
            env={**_cli_env(), "BETA9_JSON": "1"},
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"error": f"{args[0]} timed out", "code": "TIMEOUT"}
    stdout = proc.stdout.strip()
    parsed = parse_last_json(stdout) if stdout else None
    if proc.returncode != 0:
        if isinstance(parsed, dict) and "error" in parsed:
            return parsed
        return {
            "error": (proc.stderr or stdout or f"{args[0]} failed").strip()[-2000:],
            "code": "ERROR",
        }
    return parsed if parsed is not None else {"output": stdout}


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
    env.setdefault("BETA9_CALLER", f"mcp/{_sdk_version()}")
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
    http = _http(context)
    headers = dict(http.headers)
    target = url
    # Subdomains of .localhost resolve in browsers but not in Python; dial the gateway and keep Host.
    parsed = urlparse(url)
    if parsed.hostname and parsed.hostname.endswith(".localhost"):
        headers["Host"] = parsed.netloc
        target = urlunparse(parsed._replace(netloc=urlparse(http.base_url).netloc))
    response = requests.request(
        args.get("method") or "POST",
        target,
        headers=headers,
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


# --- workspace, deployments, tasks, logs --------------------------------------


def _summary(service: ServiceClient, context: Optional[str]) -> Dict[str, Any]:
    cfg = service._config
    return {
        "context": context or DEFAULT_CONTEXT_NAME,
        "workspace_id": service.http.workspace_id,
        "gateway_grpc": f"{cfg.gateway_host}:{cfg.gateway_port}",
        "gateway_http": service.http.base_url,
        "token": f"{(cfg.token or '')[:6]}…" if cfg.token else "",
    }


def whoami(_: Dict[str, Any], context: Optional[str]) -> Any:
    return _summary(_service(context), context)


def status(args: Dict[str, Any], context: Optional[str]) -> Any:
    service = _service(context)
    deployments = service.gateway.list_deployments(
        ListDeploymentsRequest(limit=int(args.get("limit") or 10))
    )
    containers = service.gateway.list_containers(ListContainersRequest())
    return {
        **_summary(service, context),
        "deployments": [_proto(d) for d in _grpc(deployments, deployments.deployments)],
        "containers": [_proto(c) for c in containers.containers] if containers.ok else [],
    }


def list_deployments(args: Dict[str, Any], context: Optional[str]) -> Any:
    filters = {
        k: StringList([str(args[k])])
        for k in ("name", "stub_type", "active")
        if args.get(k) is not None
    }
    res = _service(context).gateway.list_deployments(
        ListDeploymentsRequest(filters=filters, limit=int(args.get("limit") or 50))
    )
    return [_proto(d) for d in _grpc(res, res.deployments)]


def deploy(args: Dict[str, Any], context: Optional[str]) -> Any:
    argv = ["deploy", "--json"] + ([args["entrypoint"]] if args.get("entrypoint") else [])
    if args.get("name"):
        argv += ["--name", args["name"]]
    if args.get("rollout"):
        argv += ["--rollout", args["rollout"]]
    return _cli(argv, context, cwd=args["directory"])


def wait_deployment(args: Dict[str, Any], context: Optional[str]) -> Any:
    try:
        return wait_for_deployment(
            _service(context), args["deployment_id"], float(args.get("timeout") or 300)
        )
    except DeploymentNotReady as exc:
        return {"error": str(exc), "code": exc.code}


def stop_deployment(args: Dict[str, Any], context: Optional[str]) -> Any:
    return _grpc(
        _service(context).gateway.stop_deployment(StopDeploymentRequest(id=args["deployment_id"]))
    )


def start_deployment(args: Dict[str, Any], context: Optional[str]) -> Any:
    return _grpc(
        _service(context).gateway.start_deployment(StartDeploymentRequest(id=args["deployment_id"]))
    )


def delete_deployment(args: Dict[str, Any], context: Optional[str]) -> Any:
    return _grpc(
        _service(context).gateway.delete_deployment(
            DeleteDeploymentRequest(id=args["deployment_id"])
        )
    )


def scale_deployment(args: Dict[str, Any], context: Optional[str]) -> Any:
    res = _service(context).gateway.scale_deployment(
        ScaleDeploymentRequest(id=args["deployment_id"], containers=int(args["containers"]))
    )
    return _grpc(
        res, {"deployment_id": args["deployment_id"], "containers": int(args["containers"])}
    )


def list_tasks(args: Dict[str, Any], context: Optional[str]) -> Any:
    filters: Dict[str, StringList] = {}
    if args.get("stub_id"):
        filters["stub-id"] = StringList([args["stub_id"]])
    if args.get("status"):
        filters["status"] = StringList([s.strip().upper() for s in str(args["status"]).split(",")])
    res = _service(context).gateway.list_tasks(
        ListTasksRequest(filters=filters, limit=int(args.get("limit") or 20))
    )
    return [_proto(t) for t in _grpc(res, res.tasks)]


def get_task(args: Dict[str, Any], context: Optional[str]) -> Any:
    return _http(context).json("GET", f"/api/v1/task/{{ws}}/{args['task_id']}")


def stop_task(args: Dict[str, Any], context: Optional[str]) -> Any:
    return _grpc(_service(context).gateway.stop_tasks(StopTasksRequest(task_ids=[args["task_id"]])))


LOG_TARGETS = ("deployment_id", "container_id", "task_id", "stub_id", "app_id")


def logs(args: Dict[str, Any], context: Optional[str]) -> Any:
    chosen = [k for k in LOG_TARGETS if args.get(k)]
    if len(chosen) != 1:
        return {"error": f"pass exactly one of {', '.join(LOG_TARGETS)}", "code": "INVALID_ARGS"}
    kind = chosen[0][: -len("_id")]
    params: Dict[str, Any] = {
        "object_id": args[chosen[0]],
        "object_type": f"BETA9_{kind.upper()}",
        "limit": int(args.get("tail") or 100),
    }
    if args.get("since"):
        params["start_time"] = args["since"]
    if args.get("search"):
        params["query"] = args["search"]
    return (_http(context).json("GET", "/api/v1/logs/{ws}", params=params) or {}).get("logs", [])


# --- secrets, databases, volumes, webhooks --------------------------------------


def list_secrets(_: Dict[str, Any], context: Optional[str]) -> Any:
    res = _service(context).secret.list_secrets(ListSecretsRequest())
    return [
        {"name": s.name, "updated_at": _proto(s).get("updated_at")} for s in _grpc(res, res.secrets)
    ]


def create_secret(args: Dict[str, Any], context: Optional[str]) -> Any:
    res = _service(context).secret.create_secret(
        CreateSecretRequest(name=args["name"], value=args["value"])
    )
    return _grpc(res, {"name": args["name"], "created": True})


def update_secret(args: Dict[str, Any], context: Optional[str]) -> Any:
    res = _service(context).secret.update_secret(
        UpdateSecretRequest(name=args["name"], value=args["value"])
    )
    return _grpc(res, {"name": args["name"], "updated": True})


def delete_secret(args: Dict[str, Any], context: Optional[str]) -> Any:
    res = _service(context).secret.delete_secret(DeleteSecretRequest(name=args["name"]))
    return _grpc(res, {"name": args["name"], "deleted": True})


def _databases(http: GatewayHTTP) -> List[Dict[str, Any]]:
    return http.json("GET", "/api/v1/database/{ws}") or []


def _database(http: GatewayHTTP, kind: str, name: str) -> Dict[str, Any]:
    for info in _databases(http):
        if info["name"] == name and info["kind"] == kind:
            return info
    raise RuntimeError(f"no {kind} database named {name}")


def _public(info: Dict[str, Any]) -> Dict[str, Any]:
    """Database service info without credential values."""
    out = database_result(info)
    out.pop("connection_string", None)
    return out


def list_databases(_: Dict[str, Any], context: Optional[str]) -> Any:
    return [_public(d) for d in _databases(_http(context))]


def create_database(args: Dict[str, Any], context: Optional[str]) -> Any:
    body = {"kind": args["kind"], "name": args["name"], "always_on": bool(args.get("always_on"))}
    return _public(_http(context).json("POST", "/api/v1/database/{ws}", json=body, timeout=660))


def database_credentials(args: Dict[str, Any], context: Optional[str]) -> Any:
    service = _service(context)
    info = _database(service.http, args["kind"], args["name"])

    def secret(name: str) -> str:
        res = service.secret.get_secret(GetSecretRequest(name=name))
        return _grpc(res, res.secret.value)

    payload = {
        "name": args["name"],
        "kind": args["kind"],
        "username": secret(info["username_secret"]),
        "connection_string": secret(info["connection_string_secret"]),
        "connection_string_secret": info["connection_string_secret"],
    }
    if info.get("database_secret"):
        payload["database"] = secret(info["database_secret"])
    if args["kind"] == "redis":
        payload.update(
            {
                k: v
                for k, v in _redis_fields(payload["connection_string"]).items()
                if k != "password"
            }
        )
    return payload


def rotate_database_credentials(args: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    _database(http, args["kind"], args["name"])
    return _public(http.json("POST", f"/api/v1/database/{{ws}}/{args['name']}/rotate", timeout=660))


def delete_database(args: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    _database(http, args["kind"], args["name"])
    http.json("DELETE", f"/api/v1/database/{{ws}}/{args['name']}")
    return {"deleted": args["name"], "kind": args["kind"]}


def list_volumes(_: Dict[str, Any], context: Optional[str]) -> Any:
    res = _service(context).volume.list_volumes(ListVolumesRequest())
    return [_proto(v) for v in _grpc(res, res.volumes)]


def create_volume(args: Dict[str, Any], context: Optional[str]) -> Any:
    res = _service(context).volume.get_or_create_volume(GetOrCreateVolumeRequest(name=args["name"]))
    return _grpc(res, _proto(res.volume))


def list_webhooks(_: Dict[str, Any], context: Optional[str]) -> Any:
    return _http(context).json("GET", "/api/v1/webhook/{ws}") or []


def create_webhook(args: Dict[str, Any], context: Optional[str]) -> Any:
    body = {
        "url": args["url"],
        "event_types": args.get("event_types") or ["stub.*", "task.*"],
        "description": args.get("description", ""),
    }
    return _http(context).json("POST", "/api/v1/webhook/{ws}", json=body)


# --- REST escape hatch ------------------------------------------------------------


def api_search(args: Dict[str, Any], _: Optional[str]) -> Any:
    needle = (args.get("term") or "").lower()
    return [
        r
        for r in ECHO_ROUTES + _spec_routes(_load_specs())
        if not needle
        or needle in r["path"].lower()
        or needle in r.get("summary", "").lower()
        or needle in r.get("operation", "").lower()
    ]


def api_call(args: Dict[str, Any], context: Optional[str]) -> Any:
    body = (
        json.loads(args["body"])
        if isinstance(args.get("body"), str) and args["body"]
        else args.get("body")
    )
    response = _http(context).request(
        args["method"].upper(), args["path"], params=args.get("query"), json=body
    )
    try:
        payload: Any = response.json()
    except ValueError:
        payload = response.text
    return {"status": response.status_code, "body": payload}


# --- templates ---------------------------------------------------------------------


def template_plan(args: Dict[str, Any], _: Optional[str]) -> Any:
    manifest = load_manifest(args["source"])
    return {"name": manifest.get("name"), "steps": plan_steps(manifest, args.get("prefix") or "")}


def deploy_template(args: Dict[str, Any], context: Optional[str]) -> Any:
    service = _service(context)
    manifest = load_manifest(args["source"])
    if missing := missing_secrets(service, manifest):
        return {
            "error": f"Create these secrets first: {', '.join(missing)}",
            "code": "MISSING_SECRETS",
        }
    results = run_template(
        service,
        manifest,
        context or DEFAULT_CONTEXT_NAME,
        args.get("prefix") or "",
        args.get("only") or None,
    )
    return {"name": manifest.get("name"), "results": results}


def export_template(args: Dict[str, Any], context: Optional[str]) -> Any:
    http = _http(context)
    services: Dict[str, Any] = {}
    for name in args["apps"]:
        summary = _app_summary(_app(http, name))
        if not summary["stub_id"]:
            raise RuntimeError(f"{name} has nothing deployed")
        services[name] = manifest_service(
            stub_config(http.json("GET", f"/api/v1/stub/{{ws}}/{summary['stub_id']}"))
        )
    return {"name": args.get("name") or "exported", "services": services}


def run_pod(args: Dict[str, Any], context: Optional[str]) -> Any:
    argv = ["run", "--detach", "--json"] + ([args["entrypoint"]] if args.get("entrypoint") else [])
    for key, flag in (
        ("command", "--command"),
        ("image", "--image"),
        ("cpu", "--cpu"),
        ("memory", "--memory"),
        ("gpu", "--gpu"),
    ):
        if args.get(key) is not None:
            argv += [flag, str(args[key])]
    for kv in args.get("env") or []:
        argv += ["--env", kv]
    return _cli(argv, context, cwd=args["directory"])


# --- tool catalog -------------------------------------------------------------------

_KIND = {"type": "string", "enum": DATABASE_KINDS}
_NAME = _obj({"name": {"type": "string"}}, ["name"])
_DB = _obj({"kind": _KIND, "name": {"type": "string"}}, ["kind", "name"])
_DB_CONFIRM = _obj(
    {"kind": _KIND, "name": {"type": "string"}, "confirm": {"type": "boolean"}}, ["kind", "name"]
)
_DEPLOYMENT = _obj({"deployment_id": {"type": "string"}}, ["deployment_id"])
_WINDOW = _obj(
    {
        "stub_id": {"type": "string", "description": "Deployment stub id (endpoint/asgi)."},
        "window_minutes": {"type": "integer", "default": 60, "minimum": 1, "maximum": 10080},
    },
    ["stub_id"],
)

TOOLS: List[Tool] = [
    # workspace
    Tool("whoami", "Active context, workspace id and gateway URLs.", _obj({}), whoami),
    Tool(
        "status",
        "Snapshot of the workspace: latest deployments and running containers.",
        _obj({"limit": {"type": "integer", "default": 10}}),
        status,
    ),
    Tool(
        "list_apps",
        "Apps in the workspace with their latest deployment and URLs.",
        _obj({}),
        list_apps,
    ),
    Tool(
        "get_app",
        "One app: its config (resources, scaling, env, secret bindings, ports) and URLs.",
        _NAME,
        get_app,
    ),
    Tool(
        "delete_app",
        "Delete an app and all of its deployments and versions. Requires confirm=true.",
        _obj({"name": {"type": "string"}, "confirm": {"type": "boolean"}}, ["name"]),
        delete_app,
        confirm="delete_app removes every deployment of the app.",
    ),
    # ship code
    Tool(
        "deploy",
        "Deploy an app from a local directory: a python entrypoint (app.py:handler) or a Dockerfile/image app. Returns deployment_id, version and invoke_url (pinned to this version; wire other services with ${{app.<name>.URL}}).",
        _obj(
            {
                "directory": {
                    "type": "string",
                    "description": "Project root; files sync from here.",
                },
                "entrypoint": {"type": "string", "description": "module:function for python apps."},
                "name": {"type": "string"},
                "rollout": {"type": "string", "enum": ["auto", "blue-green", "replace"]},
            },
            ["directory"],
        ),
        deploy,
        destructive=True,
    ),
    Tool(
        "wait_deployment",
        "Block until a deployment is serving (health check for endpoints, active for others).",
        _obj(
            {"deployment_id": {"type": "string"}, "timeout": {"type": "number", "default": 300}},
            ["deployment_id"],
        ),
        wait_deployment,
    ),
    Tool(
        "invoke",
        "Call a deployed endpoint, ASGI app, task queue or function by name (latest version) with the workspace token. Task queues and functions return a task_id; follow it with get_task.",
        _obj(
            {
                "name": {"type": "string"},
                "method": {"type": "string", "default": "POST"},
                "path": {"type": "string", "description": "Sub-path for ASGI apps, e.g. /notes"},
                "body": {"description": "JSON body"},
                "timeout": {"type": "number", "default": 180},
            },
            ["name"],
        ),
        invoke,
        destructive=True,
    ),
    Tool(
        "run_function",
        "Run a @function from a local directory on the control plane and return its result: builds the image if needed, then fn.remote(**args).",
        _obj(
            {
                "directory": {"type": "string"},
                "entrypoint": {"type": "string", "description": "module:function, e.g. app:square"},
                "args": {"type": "object"},
                "timeout": {"type": "number", "default": 900},
            },
            ["directory", "entrypoint"],
        ),
        run_function,
        destructive=True,
    ),
    Tool(
        "run_pod",
        "Start a one-off container from a local directory: a Pod handler or a command on an image. Returns container_id; read output with logs, stop it with stop_task.",
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
        run_pod,
        destructive=True,
    ),
    # deployments
    Tool(
        "list_deployments",
        "List deployments (versions). Filter by name, stub_type, active.",
        _obj(
            {
                "name": {"type": "string"},
                "stub_type": {"type": "string"},
                "active": {"type": "boolean"},
                "limit": {"type": "integer", "default": 50},
            }
        ),
        list_deployments,
    ),
    Tool(
        "stop_deployment",
        "Stop a deployment (drains containers; requests fail until started).",
        _DEPLOYMENT,
        stop_deployment,
        destructive=True,
    ),
    Tool(
        "start_deployment",
        "Start a stopped deployment.",
        _DEPLOYMENT,
        start_deployment,
        destructive=True,
    ),
    Tool(
        "delete_deployment",
        "Delete one deployment version. Requires confirm=true.",
        _obj(
            {"deployment_id": {"type": "string"}, "confirm": {"type": "boolean"}}, ["deployment_id"]
        ),
        delete_deployment,
        confirm="delete_deployment is irreversible.",
    ),
    Tool(
        "scale_deployment",
        "Set the replica count of a pod deployment.",
        _obj(
            {"deployment_id": {"type": "string"}, "containers": {"type": "integer", "minimum": 0}},
            ["deployment_id", "containers"],
        ),
        scale_deployment,
        destructive=True,
    ),
    # settings
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
        set_env_tool,
        destructive=True,
    ),
    Tool(
        "connect_services",
        "Wire one app to another: adds env on `target` referencing `source` (a database's URL and parts, or an app's URL) and redeploys target.",
        _obj(
            {
                "source": {"type": "string", "description": "App or database name to reference"},
                "target": {"type": "string", "description": "App that receives the env vars"},
                "env_name": {
                    "type": "string",
                    "description": "Override the variable name (single URL)",
                },
            },
            ["source", "target"],
        ),
        lambda a, c: connect_apps(_service(c), a["source"], a["target"], a.get("env_name", "")),
        destructive=True,
    ),
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
        stage_config,
    ),
    Tool(
        "staged_changes",
        "Show the session's staged changes and how each would apply.",
        _obj({}),
        lambda a, c: _describe_staged(),
    ),
    Tool(
        "discard_staged",
        "Drop staged changes (all, or one stub's).",
        _obj({"stub_id": {"type": "string"}}),
        discard_staged,
        destructive=True,
    ),
    Tool(
        "accept_deploy",
        "Apply every staged change: PATCH live paths, redeploy stubs whose staged paths need a new version. Requires confirm=true.",
        _obj({"confirm": {"type": "boolean"}, "message": {"type": "string"}}),
        accept_deploy,
        confirm="accept_deploy applies staged changes to the workspace.",
    ),
    Tool(
        "validate_references",
        "Check the syntax of ${{...}} references in KEY=VALUE env entries; existence of the named database or app is checked at deploy time.",
        _obj(
            {
                "env": {"type": "array", "items": {"type": "string"}},
                "complete": {"type": "string", "description": "partial expression after ${{"},
            }
        ),
        validate_references_tool,
    ),
    # secrets
    Tool("list_secrets", "Workspace secret names.", _obj({}), list_secrets),
    Tool(
        "create_secret",
        "Create a workspace secret. Reference it from apps with ${{secret.NAME}} or secrets=[NAME].",
        _obj({"name": {"type": "string"}, "value": {"type": "string"}}, ["name", "value"]),
        create_secret,
        destructive=True,
    ),
    Tool(
        "update_secret",
        "Change a secret's value. Deployments pick it up on their next container start.",
        _obj({"name": {"type": "string"}, "value": {"type": "string"}}, ["name", "value"]),
        update_secret,
        destructive=True,
    ),
    Tool(
        "delete_secret",
        "Delete a workspace secret. Requires confirm=true.",
        _obj({"name": {"type": "string"}, "confirm": {"type": "boolean"}}, ["name"]),
        delete_secret,
        confirm="delete_secret breaks deployments still bound to it.",
    ),
    # databases
    Tool("list_databases", "Managed database services and their state.", _obj({}), list_databases),
    Tool(
        "create_database",
        "Create a managed Postgres, Redis, MySQL or MongoDB service. Credentials are stored as secrets; reference them with ${{db.<name>.DATABASE_URL}} (or REDIS_URL, HOST, PORT, USERNAME, PASSWORD, DATABASE).",
        _obj(
            {
                "kind": _KIND,
                "name": {"type": "string"},
                "always_on": {"type": "boolean", "default": False},
            },
            ["kind", "name"],
        ),
        create_database,
        destructive=True,
    ),
    Tool(
        "database_credentials",
        "Connection details for a database service, including the connection string.",
        _DB,
        database_credentials,
    ),
    Tool(
        "rotate_database_credentials",
        "Rotate a database's password. The database and every deployment bound to its secrets restart with the new credentials.",
        _DB,
        rotate_database_credentials,
        destructive=True,
    ),
    Tool(
        "delete_database",
        "Delete a database service and its credential secrets. Requires confirm=true.",
        _DB_CONFIRM,
        delete_database,
        confirm="delete_database removes the service and its data.",
    ),
    # storage
    Tool(
        "list_volumes",
        "Persistent volumes (mount with Volume(name, mount_path) in app code).",
        _obj({}),
        list_volumes,
    ),
    Tool("create_volume", "Create a persistent volume.", _NAME, create_volume, destructive=True),
    # stacks
    Tool(
        "list_stacks",
        "Stacks: named groups of apps shown together on the dashboard board.",
        _obj({}),
        list_stacks,
    ),
    Tool(
        "create_stack",
        "Create a stack, optionally with apps (by name).",
        _obj(
            {"name": {"type": "string"}, "apps": {"type": "array", "items": {"type": "string"}}},
            ["name"],
        ),
        create_stack,
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
        update_stack,
        destructive=True,
    ),
    Tool(
        "delete_stack",
        "Delete a stack. Its apps are untouched.",
        _NAME,
        delete_stack,
        destructive=True,
    ),
    # templates
    Tool(
        "template_plan",
        "Show the ordered steps of a template manifest (path, URL, or a catalog name).",
        _obj(
            {"source": {"type": "string"}, "prefix": {"type": "string", "default": ""}}, ["source"]
        ),
        template_plan,
    ),
    Tool(
        "deploy_template",
        "Deploy every service in a template manifest in dependency order (databases first) and group them into a stack. Requires confirm=true.",
        _obj(
            {
                "source": {"type": "string"},
                "prefix": {"type": "string", "default": ""},
                "only": {"type": "array", "items": {"type": "string"}},
                "confirm": {"type": "boolean"},
            },
            ["source"],
        ),
        deploy_template,
        confirm="deploy_template creates databases and deployments; use template_plan first.",
    ),
    Tool(
        "export_template",
        "Describe existing apps as a template manifest (secret values are never included).",
        _obj(
            {
                "apps": {"type": "array", "items": {"type": "string"}},
                "name": {"type": "string", "default": "exported"},
            },
            ["apps"],
        ),
        export_template,
    ),
    # observe
    Tool(
        "logs",
        "Recent logs for one target: deployment_id, container_id, task_id, stub_id or app_id.",
        _obj(
            {
                "deployment_id": {"type": "string"},
                "container_id": {"type": "string"},
                "task_id": {"type": "string"},
                "stub_id": {"type": "string"},
                "app_id": {"type": "string"},
                "tail": {"type": "integer", "default": 100},
                "since": {"type": "string", "description": "RFC3339 start time"},
                "search": {"type": "string"},
            }
        ),
        logs,
    ),
    Tool(
        "list_tasks",
        "Recent tasks (invocations), newest first.",
        _obj(
            {
                "limit": {"type": "integer", "minimum": 1, "default": 20},
                "stub_id": {"type": "string"},
                "status": {
                    "type": "string",
                    "description": "Comma-separated: pending, running, complete, error, cancelled, timeout",
                },
            }
        ),
        list_tasks,
    ),
    Tool(
        "get_task",
        "Status, timing and result of one task.",
        _obj({"task_id": {"type": "string"}}, ["task_id"]),
        get_task,
    ),
    Tool(
        "stop_task",
        "Stop a running or pending task.",
        _obj({"task_id": {"type": "string"}}, ["task_id"]),
        stop_task,
        destructive=True,
    ),
    Tool("http_requests", "Request count for an endpoint over a window.", _WINDOW, http_requests),
    Tool(
        "http_error_rate",
        "Share of 5xx responses for an endpoint over a window.",
        _WINDOW,
        http_error_rate,
    ),
    Tool(
        "http_response_time",
        "p50/p95/p99 latency (ms) for an endpoint over a window; percentiles come from a fixed histogram, so they are upper bounds.",
        _WINDOW,
        http_response_time,
    ),
    Tool(
        "list_webhooks", "Workspace webhooks (URL, event types, enabled).", _obj({}), list_webhooks
    ),
    Tool(
        "create_webhook",
        "Register a signed HTTP webhook for workspace events (stub.*, task.*, endpoint.request_stats). Returns the signing secret once.",
        _obj(
            {
                "url": {"type": "string"},
                "event_types": {"type": "array", "items": {"type": "string"}},
                "description": {"type": "string"},
            },
            ["url"],
        ),
        create_webhook,
        destructive=True,
    ),
    # escape hatch
    Tool(
        "api_search",
        "Find gateway REST routes by keyword (discovery for api_call).",
        _obj({"term": {"type": "string"}}),
        api_search,
    ),
    Tool(
        "api_call",
        "Call a gateway REST route. `{ws}` in path is replaced with the workspace id.",
        _obj(
            {
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"]},
                "path": {"type": "string"},
                "body": {"description": "JSON body (object or JSON string)"},
                "query": {"type": "object", "additionalProperties": {"type": "string"}},
            },
            ["method", "path"],
        ),
        api_call,
        destructive=True,
    ),
]

TOOLS_BY_NAME = {tool.name: tool for tool in TOOLS}


def _run_tool(tool: Tool, args: Dict[str, Any], context: Optional[str]) -> Any:
    if tool.confirm and not args.get("confirm"):
        return {
            "error": f"{tool.confirm} Call again with confirm=true.",
            "code": "NEEDS_CONFIRMATION",
        }
    try:
        return tool.run(args, context)
    except GatewayHTTPError as exc:
        return {"error": exc.message, "code": "NOT_AUTHENTICATED" if exc.status == 401 else "ERROR"}
    except Exception as exc:  # surface as a tool error; keep the server alive
        return {"error": str(exc), "code": "ERROR"}


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
                    "serverInfo": {"name": get_settings().name.lower(), "version": _sdk_version()},
                    "instructions": (
                        f"{get_settings().name} runs serverless GPU/CPU apps, one-off containers and managed databases. "
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
    servers[_server_name()] = entry
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def _codex_toml(path: Path, entry: Dict[str, Any]) -> None:
    block = f'\n[mcp_servers.{_server_name()}]\ncommand = "{entry["command"]}"\nargs = {json.dumps(entry["args"])}\n'
    existing = path.read_text() if path.exists() else ""
    if f"[mcp_servers.{_server_name()}]" in existing:
        terminal.detail(
            f"{path} already has a [mcp_servers.{_server_name()}] block; leaving it unchanged."
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
        terminal.print_json({"mcpServers": {_server_name(): entry}})
        return

    if client == "codex":
        _codex_toml(path, entry)
    else:
        _merge_json(path, "mcpServers", entry)

    if terminal.json_output():
        terminal.print_json({"client": client, "path": str(path), "server": entry})
    else:
        terminal.success(f"Registered the {_server_name()} MCP server for {client}")
        terminal.detail(f"{path}")
        terminal.detail("Restart the client to pick it up.")
