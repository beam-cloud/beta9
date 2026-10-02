import copy
from pathlib import Path
from typing import Any, Dict, List

import pytest

from beta9.mcp import stacks


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
        self.deploy_outcomes: List[str] = []
        self.ready = True
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
            if not self.ready:
                raise RuntimeError("no healthy replica")
            return {"status": 200}
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
        "db is uncertain; inspect it, then call stack_resolve or apply a corrected plan"
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
