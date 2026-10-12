"""Recoverable first-VM setup, shared by the CLI and onboarding integrations."""

import fcntl
import hashlib
import json
import os
import uuid
from pathlib import Path

from ..abstractions.vm import VM
from ..channel import GatewayHTTPError, ServiceClient


def prepare_vm(service: ServiceClient, state_directory: Path, name: str, *, desktop=False):
    """Resume this setup's VM, preserving the original creation request across retries.

    A pre-existing resource is reused only if its metadata identifies this setup.
    Success means a managed command has exited successfully inside the guest.
    """
    workspace = service.http.workspace_id
    directory = state_directory / hashlib.sha256(workspace.encode()).hexdigest()[:24]
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / (hashlib.sha256(name.encode()).hexdigest()[:24] + ".json")
    with open(path.with_suffix(".lock"), "a", opener=_private_file) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = json.loads(path.read_text()) if path.exists() else {"request_id": str(uuid.uuid4())}
        marker = {"beam.onboarding": state["request_id"]}
        try:
            vm = VM.get(name, _service=service)
        except GatewayHTTPError as exc:
            if exc.status != 404:
                raise
            vm = VM(
                name,
                cpu=1,
                memory=2048,
                disk_size="10Gi",
                desktop=desktop,
                docker_enabled=True,
                ttl=900,
                auto_resume=True,
                metadata=marker,
                request_id=state["request_id"],
                _service=service,
            )
            if state.get("creation_body"):
                vm._creation_body = state["creation_body"]
            else:
                vm.prepare()
                state["creation_body"] = vm._creation_body
                temporary = path.with_suffix(".tmp")
                with open(temporary, "w", opener=_private_file) as output:
                    json.dump(state, output)
                    output.flush()
                    os.fsync(output.fileno())
                temporary.replace(path)
            vm.create(wait=False)
        else:
            if (vm.info.get("metadata") or {}).get("beam.onboarding") != marker["beam.onboarding"]:
                raise ValueError(f"VM {name!r} already exists; choose a different --name")
            if vm.info.get("desired_state") != "running":
                vm.start(wait=False)
        vm.wait(timeout=240, services=True)
        probe = vm.process.exec("sh", "-c", "test -d /workspace && uname -s", cwd="/")
        if probe.wait(30) != 0 or probe.stdout.read().strip() != "Linux":
            raise RuntimeError(
                "VM guest readiness check failed; inspect vm get/logs and retry setup"
            )
        return vm.refresh().info


def _private_file(path, flags):
    return os.open(path, flags, 0o600)
