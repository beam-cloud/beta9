import copy
from pathlib import Path
from typing import Any, Dict, List

import pytest

from beta9.mcp import stacks
from beta9.mcp.tools import RemoteToolError


class FakeStackTools:
    """The gateway and job surface a stack apply uses, for one stack."""

    def __init__(self, root: Path):
        self.cwd = str(root)
        self.job_dir = root / "jobs"
        self.job_dir.mkdir()
        self.stack: Dict[str, Any] = {}
        self.submitted: List[str] = []
        self.failures = 0
        self.deployed: List[str] = []
        self.deploy_options: List[Dict[str, Any]] = []
        self.deploy_outcomes: List[str] = []
        self.ready = True
        self.health_checks: List[Dict[str, Any]] = []
        self.health_error_code = ""
        self.secrets: Dict[str, str] = {}
        self.outside: List[str] = []  # workspace apps no stack owns

    def remote(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if name == "list_stacks":
            return {"items": [copy.deepcopy(self.stack)] if self.stack else []}
        if name == "list_apps":
            return {"items": [{"name": app} for app in self.stack.get("apps", []) + self.outside]}
        if name == "create_stack":
            self.stack = {"name": args["name"], "revision": "0", "spec": {}, "apps": []}
            return copy.deepcopy(self.stack)
        if name == "update_stack":
            assert args["expected_revision"] == self.stack["revision"]
            self.stack["spec"].update(args.get("spec", {}))
            self.stack["apps"] += [
                app for app in args.get("add", []) if app not in self.stack["apps"]
            ]
            self.stack["revision"] = str(int(self.stack["revision"]) + 1)
            return copy.deepcopy(self.stack)
        if name == "database_readiness":
            return {"ready": True}
        if name == "wait_deployment":
            self.health_checks.append(args)
            if self.health_error_code:
                raise RemoteToolError({"code": self.health_error_code, "error": "cannot check"})
            if not self.ready:
                raise RuntimeError("no healthy replica")
            return {"status": 200}
        if name == "list_secrets":
            return {"items": [{"name": secret} for secret in self.secrets]}
        if name == "create_secret":
            assert args["name"] not in self.secrets
            self.secrets[args["name"]] = args["value"]
            return {"name": args["name"], "created": True}
        raise AssertionError(f"unexpected remote call {name}")

    def database_job(self, deploy: Dict[str, Any], key: str) -> Dict[str, Any]:
        self.submitted.append(key)
        if self.failures:
            self.failures -= 1
            return failed(key, "Database job failed")
        return {"structuredContent": {"job_id": key, "status": "accepted", "deployment_id": "d1"}}

    def deploy(self, options: Dict[str, Any], operation: str) -> Dict[str, Any]:
        key = options["idempotency_key"]
        self.deployed.append(key)
        self.deploy_options.append(options)
        outcome = self.deploy_outcomes.pop(0) if self.deploy_outcomes else "accepted"
        if outcome == "failed":
            return failed(key, f"Deploy of {options['name']} failed: build exited with code 1")
        job = {"job_id": key, "status": outcome}
        if outcome == "accepted":
            job["deployment_id"] = f"web-{len(self.deployed)}"
        return {"structuredContent": job}


def failed(key: str, message: str) -> Dict[str, Any]:
    return {
        "isError": True,
        "content": [{"type": "text", "text": message}],
        "structuredContent": {"job_id": key, "status": "failed"},
    }


def database_stack(tools: FakeStackTools) -> str:
    spec = {"version": 1, "services": {"db": {"type": "database", "deploy": {"kind": "postgres"}}}}
    return stacks.plan(tools, {"name": "app", "spec": spec})["structuredContent"]["plan_id"]


def web_stack(tools: FakeStackTools, image: str) -> str:
    services = {
        "db": {"type": "database", "deploy": {"kind": "postgres"}},
        "web": {
            "health_path": "/health",
            "deploy": {"image": image, "env": {"DATABASE_URL": "${{db.db.DATABASE_URL}}"}},
        },
    }
    spec = {"version": 1, "services": services}
    return stacks.plan(tools, {"name": "app", "spec": spec})["structuredContent"]["plan_id"]


def text(result: Dict[str, Any]) -> str:
    return result["content"][0]["text"]


# A service whose submission failed blocks the stack until it is resolved, so
# every message about it must lead to stack_resolve, whatever its type.
def test_a_failed_database_service_is_resolved_and_retried(tmp_path):
    tools = FakeStackTools(tmp_path)
    tools.failures = 1
    plan_id = database_stack(tools)

    assert text(stacks.apply(tools, {"plan_id": plan_id})) == (
        "db is uncertain (Database job failed); inspect it, then call stack_resolve or apply "
        "a corrected plan"
    )
    with pytest.raises(ValueError, match="requires stack_resolve .*: Database job failed$"):
        stacks.apply(tools, {"plan_id": plan_id})

    resolution = {"plan_id": plan_id, "service": "db", "evidence": "list_databases shows no db"}
    stacks.resolve(tools, {**resolution, "resolution": "retry"})
    assert text(stacks.apply(tools, {"plan_id": plan_id})) == "Stack applied"
    assert tools.submitted == [f"stack:{plan_id}:db", f"stack:{plan_id}:db:attempt:1"]
    assert tools.stack["apps"] == ["db"]


# A create that failed after making the database leaves it outside the stack.
# A corrected plan must send the caller to stack_resolve, never create it again.
def test_a_database_left_by_an_unresolved_create_must_be_resolved(tmp_path):
    tools = FakeStackTools(tmp_path)
    tools.failures = 1
    plan_id = database_stack(tools)
    stacks.apply(tools, {"plan_id": plan_id})
    tools.outside.append("db")

    with pytest.raises(ValueError, match=f"db exists but plan {plan_id} left it uncertain; call"):
        database_stack(tools)
    assert tools.submitted == [f"stack:{plan_id}:db"]


def test_an_accepted_resolution_adds_the_service_to_the_stack(tmp_path):
    tools = FakeStackTools(tmp_path)
    tools.failures = 1
    plan_id = database_stack(tools)
    stacks.apply(tools, {"plan_id": plan_id})

    resolution = {"plan_id": plan_id, "service": "db", "evidence": "database_readiness is ready"}
    stacks.resolve(tools, {**resolution, "resolution": "complete"})

    assert text(stacks.apply(tools, {"plan_id": plan_id})) == "Stack applied"
    assert tools.stack["apps"] == ["db"]
    assert tools.submitted == [f"stack:{plan_id}:db"]


# Retrying a failed build reruns the reviewed source, so the fix for a
# deterministic failure has to arrive as a new plan.
def test_a_corrected_plan_replaces_one_stopped_on_a_failed_service(tmp_path):
    tools = FakeStackTools(tmp_path)
    tools.deploy_outcomes = ["failed"]
    broken = web_stack(tools, "web:broken")
    stacks.apply(tools, {"plan_id": broken})
    assert text(stacks.apply(tools, {"plan_id": broken})).startswith("web is uncertain")

    fixed = web_stack(tools, "web:fixed")
    assert text(stacks.apply(tools, {"plan_id": fixed})) == "Stack applied"

    assert tools.submitted == [f"stack:{broken}:db"]  # the database is never recreated
    assert tools.deployed == [f"stack:{broken}:web", f"stack:{fixed}:web"]
    assert tools.stack["apps"] == ["db", "web"]
    with pytest.raises(ValueError, match="create a new plan"):
        stacks.apply(tools, {"plan_id": broken})


def test_a_corrected_plan_replaces_one_waiting_on_an_unready_service(tmp_path):
    tools = FakeStackTools(tmp_path)
    tools.ready = False
    crashing = web_stack(tools, "web:crashing")
    stacks.apply(tools, {"plan_id": crashing})
    assert text(stacks.apply(tools, {"plan_id": crashing})).startswith("web is starting")
    assert tools.stack["apps"] == ["db", "web"]  # deployed, so a new plan may redeploy it

    tools.ready = True
    fixed = web_stack(tools, "web:fixed")
    assert text(stacks.apply(tools, {"plan_id": fixed})) == "Stack applied"
    assert tools.deployed == [f"stack:{crashing}:web", f"stack:{fixed}:web"]


# Only a database carries over between plans; everything else is redeployed or
# rerun, so the plan must not call an unchanged application reusable.
def test_only_unchanged_databases_are_reusable(tmp_path):
    tools = FakeStackTools(tmp_path)
    first = web_stack(tools, "web:same")
    stacks.apply(tools, {"plan_id": first})
    assert text(stacks.apply(tools, {"plan_id": first})) == "Stack applied"

    second = web_stack(tools, "web:same")
    assert stacks._load_plan(tools, second)["reusable"] == ["db"]
    assert text(stacks.apply(tools, {"plan_id": second})) == "Stack applied"
    assert tools.submitted == [f"stack:{first}:db"]
    assert tools.deployed == [f"stack:{first}:web", f"stack:{second}:web"]


def plan_app(tools: FakeStackTools, service: Dict[str, Any], **spec: Any) -> Dict[str, Any]:
    spec = {"version": 1, "services": {"web": service}, **spec}
    return stacks.plan(tools, {"name": "app", "spec": spec})["structuredContent"]


# A multi-port app (ClickHouse serves HTTP on 8123 and its native protocol on
# 9000) is only checkable on its HTTP port; the gateway rejects a check that
# does not name one.
def test_a_multi_port_app_is_checked_on_its_first_port(tmp_path):
    tools = FakeStackTools(tmp_path)
    service = {"health_path": "/ping", "deploy": {"image": "ch", "ports": [8123, 9000]}}
    plan_id = plan_app(tools, service)["plan_id"]

    assert text(stacks.apply(tools, {"plan_id": plan_id})) == "Stack applied"
    assert tools.health_checks[0]["port"] == 8123

    with pytest.raises(ValueError, match=r"health_port must be one of its ports \[8123, 9000\]"):
        plan_app(tools, {**service, "health_port": 9001})


def test_an_http_check_on_a_tcp_app_is_rejected_with_the_alternative(tmp_path):
    tools = FakeStackTools(tmp_path)
    service = {"health_path": "/ping", "deploy": {"image": "ch", "tcp": True, "ports": [9000]}}
    with pytest.raises(ValueError, match=r"keep tcp false .*\$\{\{app\.NAME\.TCP\.<port>\}\}"):
        plan_app(tools, service)


# Waiting cannot fix a check the gateway refuses, so the service fails at once
# instead of sitting in "starting".
def test_a_health_check_the_gateway_refuses_fails_the_service(tmp_path):
    tools = FakeStackTools(tmp_path)
    tools.health_error_code = "UNSUPPORTED_PROTOCOL"
    plan_id = plan_app(tools, {"health_path": "/", "deploy": {"image": "web"}})["plan_id"]

    assert text(stacks.apply(tools, {"plan_id": plan_id})).startswith(
        "web is failed (health check cannot pass: cannot check); inspect it"
    )


# Compose services run continuously; scaling to zero is opt-in.
def test_applications_run_continuously_unless_they_choose_scaling(tmp_path):
    tools = FakeStackTools(tmp_path)
    plan_id = plan_app(tools, {"health_path": "/", "deploy": {"image": "web"}})["plan_id"]
    stacks.apply(tools, {"plan_id": plan_id})
    assert tools.deploy_options[-1]["min_replicas"] == 1

    service = {"health_path": "/", "deploy": {"image": "web", "keep_warm_seconds": 60}}
    plan_id = plan_app(tools, service)["plan_id"]
    stacks.apply(tools, {"plan_id": plan_id})
    assert "min_replicas" not in tools.deploy_options[-1]


# The gateway resolves an app's references to itself while creating it, so only
# references between apps order the stack.
def test_self_references_are_allowed_and_a_cycle_names_its_path(tmp_path):
    tools = FakeStackTools(tmp_path)
    own_url = {"health_path": "/", "deploy": {"image": "web", "env": {"URL": "${{app.web.URL}}"}}}
    assert plan_app(tools, own_url)["plan_id"]

    spec = {
        "version": 1,
        "services": {
            "a": {"health_path": "/", "deploy": {"image": "a", "env": {"B": "${{app.b.URL}}"}}},
            "b": {"health_path": "/", "deploy": {"image": "b", "env": {"A": "${{app.a.URL}}"}}},
        },
    }
    with pytest.raises(ValueError, match=r"dependency cycle: a -> b -> a\. .*add it with set_env"):
        stacks.plan(tools, {"name": "app", "spec": spec})


def test_an_app_without_a_health_path_is_planned_with_a_warning(tmp_path):
    planned = plan_app(FakeStackTools(tmp_path), {"deploy": {"image": "web"}})
    assert planned["warnings"] == [
        "web: no health_path, so readiness only checks that its container runs; "
        "set a side-effect-free GET path that answers 2xx once it serves"
    ]


# Secrets shared between services are generated once, kept across applies, and
# never echoed back.
def test_declared_secrets_are_generated_once_and_never_returned(tmp_path):
    tools = FakeStackTools(tmp_path)
    tools.secrets["KEPT"] = "existing"
    service = {"health_path": "/", "deploy": {"image": "web", "env": {"K": "${{secret.KEY}}"}}}
    secrets = {"KEY": {"length": 64, "alphabet": "0123456789abcdef"}, "KEPT": {}}
    plan_id = plan_app(tools, service, secrets=secrets)["plan_id"]

    result = stacks.apply(tools, {"plan_id": plan_id})
    assert text(result) == "Stack applied"
    assert tools.secrets["KEPT"] == "existing"
    assert len(tools.secrets["KEY"]) == 64 and set(tools.secrets["KEY"]) <= set("0123456789abcdef")
    assert tools.secrets["KEY"] not in repr(result)

    with pytest.raises(ValueError, match="length must be an integer from 8 to 512"):
        plan_app(tools, service, secrets={"KEY": {"length": 4}})


def test_a_plan_with_a_deploy_in_flight_is_not_replaced(tmp_path):
    tools = FakeStackTools(tmp_path)
    tools.deploy_outcomes = ["running"]
    first = web_stack(tools, "web:first")
    stacks.apply(tools, {"plan_id": first})
    assert text(stacks.apply(tools, {"plan_id": first})).startswith("web is running")

    second = web_stack(tools, "web:second")
    with pytest.raises(ValueError, match=f"applying web \\(running\\).* plan {first}"):
        stacks.apply(tools, {"plan_id": second})
    assert tools.deployed == [f"stack:{first}:web"]
