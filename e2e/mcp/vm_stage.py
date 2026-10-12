"""Disposable staging VM acceptance through MCP. See docs/mcp/README.md.

Runs real lifecycle, file/process, desktop and artifact operations. Cleans up
only this run's uniquely named VMs and artifacts, even after a failed check.
"""

import argparse
import base64
import json
import time
import uuid
from pathlib import Path

from beta9.abstractions.vm import VM, identity
from beta9.channel import ServiceClient
from beta9.config import get_config_context
from beta9.mcp.server import RemoteMCP


class Acceptance:
    def __init__(self, profile, pool):
        self.context = get_config_context(profile)
        if self.context.gateway_host != "gateway.stage.beam.cloud":
            raise ValueError("This acceptance suite requires a staging profile")
        self.service = ServiceClient(self.context)
        self.remote = RemoteMCP(self.context)
        self.name = "mcp-audit-" + uuid.uuid4().hex[:8]
        self.pool = pool
        self.vms = []
        self.artifacts = []
        self.passed = set()
        self.public_key = identity().with_suffix(".pub").read_text().strip()
        self.suffix = self.name.rsplit("-", 1)[1]

    def call(self, tool, **arguments):
        status, body = self.remote.call(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": tool, "arguments": arguments},
            }
        )
        result = body.get("result", {})
        if status != 200 or body.get("error") or result.get("isError"):
            raise RuntimeError(f"{tool}: HTTP {status}, {result or body.get('error')}")
        self.passed.add(tool)
        print("PASS", tool, flush=True)
        return result.get("structuredContent") or result

    def wait(self, name):
        VM.get(name, _service=self.service).wait(timeout=240, services=True)

    def run(self, screenshot):
        name = self.name
        vm = VM(
            name,
            cpu=1,
            memory=2048,
            disk_size="10Gi",
            pool=self.pool,
            desktop=True,
            docker_enabled=True,
            ttl=900,
            auto_resume=True,
            metadata={"test": "mcp-acceptance"},
            _service=self.service,
        )
        vm.prepare()
        self.vms.append(name)
        self.call("create_vm", **vm._creation_body)
        self.wait(name)
        self.call("get_vm", name=name)
        self.call("list_vms")
        self.call("vm_create_directory", name=name, path="/workspace/mcp-audit")
        path = "/workspace/mcp-audit/message.txt"
        self.call(
            "vm_write_file", name=name, path=path, text="hello Beam α\n", confirm=True
        )
        result = self.call("vm_read_file", name=name, path=path)
        assert base64.b64decode(result["data"]).decode() == "hello Beam α\n"
        self.call("vm_stat_file", name=name, path=path)
        self.call("vm_list_files", name=name, path="/workspace/mcp-audit")
        self.call(
            "vm_find_in_files", name=name, path="/workspace/mcp-audit", pattern="hello"
        )
        self.call(
            "vm_replace_in_files",
            name=name,
            path=path,
            pattern="hello",
            replacement="goodbye",
            confirm=True,
        )
        result = self.call(
            "vm_exec",
            name=name,
            command=["printf", "%s", "two words; $(literal)"],
            wait=True,
        )
        assert (
            result["done"]
            and result["exit_code"] == 0
            and result["stdout"] == "two words; $(literal)"
        )
        process = self.call("vm_exec", name=name, command=["sleep", "60"])
        pinned = {
            "name": name,
            "pid": process["pid"],
            "container_id": process["container_id"],
        }
        self.call("vm_process_status", **pinned)
        self.call("vm_list_processes", name=name)
        self.call("vm_kill_process", **pinned, confirm=True)
        self.call("vm_process_output", **pinned)
        self.call("vm_metrics", name=name)
        self.call("vm_logs", name=name, unit="beam-terminal.service", tail=5)
        self.call("vm_keep_alive", name=name)
        for tool, arguments in [
            ("vm_desktop_size", {}),
            ("vm_desktop_resize", {"width": 1024, "height": 768}),
            ("vm_desktop_move_mouse", {"x": 20, "y": 20}),
            ("vm_desktop_click", {"x": 20, "y": 20}),
            ("vm_desktop_scroll", {"direction": "down", "amount": 1}),
            ("vm_desktop_press", {"keys": ["Escape"]}),
            (
                "vm_desktop_drag",
                {"start_x": 20, "start_y": 20, "end_x": 40, "end_y": 40},
            ),
            ("vm_desktop_write", {"text": "Beam staging α\n"}),
        ]:
            self.call(tool, name=name, **arguments)
        result = self.call(
            "vm_exec",
            name=name,
            command=["xclip", "-selection", "clipboard", "-out"],
            wait=True,
        )
        assert result["stdout"] == "Beam staging α\n"
        image = self.call("vm_desktop_screenshot", name=name)
        raw = base64.b64decode(image["content"][0]["data"])
        assert raw.startswith(b"\x89PNG")
        screenshot.write_bytes(raw)
        gui = self.call("vm_desktop_launch", name=name, command=["xterm"])
        self.call(
            "vm_kill_process",
            name=name,
            pid=gui["pid"],
            container_id=gui["container_id"],
            confirm=True,
        )
        recording = self.call(
            "vm_desktop_record", name=name, path="/workspace/mcp-test.mp4", fps=5
        )
        time.sleep(3)
        self.call(
            "vm_desktop_stop_recording",
            name=name,
            pid=recording["pid"],
            container_id=recording["container_id"],
        )
        time.sleep(1)
        video = self.call("vm_read_file", name=name, path="/workspace/mcp-test.mp4")
        assert base64.b64decode(video["data"])[4:8] == b"ftyp"
        self.call(
            "vm_delete_file", name=name, path="/workspace/mcp-test.mp4", confirm=True
        )
        docker = self.call(
            "vm_docker",
            name=name,
            args=["version", "--format", "{{.Server.Version}}"],
            wait=True,
        )
        assert docker["done"] and docker["exit_code"] == 0
        self.call("vm_ports", name=name)
        self.call("vm_bind_port", name=name, port=9001)
        self.call("vm_expose_port", name=name, port=9000, protected=True)
        self.call("vm_access_token", name=name)
        self.call("vm_access_url", name=name, port=9000, ttl=60)
        self.call("vm_rotate_access_token", name=name, confirm=True)
        self.call("vm_unexpose_port", name=name, port=9000, confirm=True)
        self.call("update_vm", name=name, idle_timeout=900, auto_resume=True)
        for kind in ("snapshot", "template"):
            artifact = f"{kind}-{self.suffix}"
            self.artifacts.append((kind, artifact))
            self.call(f"create_vm_{kind}", name=name, **{f"{kind}_name": artifact})
            self.call(f"list_vm_{kind}s")
        fork = "fork-" + self.suffix
        self.vms.append(fork)
        self.call("fork_vm", name=name, new_name=fork, ssh_public_key=self.public_key)
        self.wait(fork)
        result = self.call("vm_read_file", name=fork, path=path)
        assert b"goodbye" in base64.b64decode(result["data"])
        self.call("delete_vm", name=fork, confirm=True)
        self.vms.remove(fork)
        clone = "tmpl-" + self.suffix
        self.vms.append(clone)
        self.call(
            "create_vm",
            name=clone,
            template=f"template-{self.suffix}",
            spec={"ssh_public_key": self.public_key},
            request_id=str(uuid.uuid4()),
        )
        self.wait(clone)
        self.call("vm_exec", name=clone, command=["uname", "-s"], wait=True)
        self.call("delete_vm", name=clone, confirm=True)
        self.vms.remove(clone)
        for action in ("pause", "stop"):
            self.call(f"{action}_vm", name=name, confirm=True)
            if action == "pause":
                time.sleep(2)
            self.call("start_vm", name=name)
            self.wait(name)
            result = self.call("vm_read_file", name=name, path=path)
            assert b"goodbye" in base64.b64decode(result["data"])
        self.call("vm_delete_file", name=name, path=path, confirm=True)
        self.call(
            "vm_delete_directory", name=name, path="/workspace/mcp-audit", confirm=True
        )

    def cleanup(self):
        failures = []
        for name in reversed(self.vms):
            try:
                self.call("delete_vm", name=name, confirm=True)
            except RuntimeError as exc:
                failures.append(str(exc))
        for kind, artifact in self.artifacts:
            try:
                self.call(f"delete_vm_{kind}", artifact=artifact, confirm=True)
            except RuntimeError as exc:
                failures.append(str(exc))
        self.service.close()
        if failures:
            raise RuntimeError("Acceptance cleanup failed: " + "; ".join(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="staging")
    parser.add_argument("--pool", default="microvm-cpu")
    parser.add_argument(
        "--output", type=Path, default=Path("/tmp/beam-mcp-vm-result.json")
    )
    parser.add_argument(
        "--screenshot", type=Path, default=Path("/tmp/beam-mcp-vm-desktop.png")
    )
    args = parser.parse_args()
    acceptance = Acceptance(args.profile, args.pool)
    try:
        acceptance.run(args.screenshot)
    finally:
        acceptance.cleanup()
    args.output.write_text(
        json.dumps(
            {"passed_tools": sorted(acceptance.passed), "cleanup": True}, indent=2
        )
    )
    print(
        "VM acceptance complete:", len(acceptance.passed), "distinct tools", flush=True
    )


if __name__ == "__main__":
    main()
