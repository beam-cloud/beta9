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

    def remote(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if name == "list_stacks":
            return {"items": [copy.deepcopy(self.stack)] if self.stack else []}
        if name == "list_apps":
            return {"items": [{"name": app} for app in self.stack.get("apps", [])]}
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
        raise AssertionError(f"unexpected remote call {name}")

    def database_job(self, deploy: Dict[str, Any], key: str) -> Dict[str, Any]:
        self.submitted.append(key)
        if self.failures:
            self.failures -= 1
            return {"isError": True, "structuredContent": {"job_id": key, "status": "failed"}}
        return {"structuredContent": {"job_id": key, "status": "accepted", "deployment_id": "d1"}}


def database_stack(tools: FakeStackTools) -> str:
    spec = {"version": 1, "services": {"db": {"type": "database", "deploy": {"kind": "postgres"}}}}
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
        "db is uncertain; inspect it, then call stack_resolve"
    )
    with pytest.raises(ValueError, match="requires reconciliation with stack_resolve"):
        stacks.apply(tools, {"plan_id": plan_id})

    resolution = {"plan_id": plan_id, "service": "db", "evidence": "list_databases shows no db"}
    stacks.resolve(tools, {**resolution, "resolution": "retry"})
    assert text(stacks.apply(tools, {"plan_id": plan_id})) == "Stack applied"
    assert tools.submitted == [f"stack:{plan_id}:db", f"stack:{plan_id}:db:attempt:1"]
    assert tools.stack["apps"] == ["db"]


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
