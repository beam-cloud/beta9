#!/usr/bin/env python3
"""Verify persistent VMs against an explicitly selected development profile.

Requires an amd64 KVM worker and a matching VM image/build worker. The local ARM
k3d cluster cannot execute this runtime. Never substitutes a plain container.
"""

import argparse
import json
import tempfile
import uuid
from pathlib import Path

import requests

from beta9 import Image, VM


def execute(vm, *argv):
    process = vm.process.exec(*argv)
    code = process.wait(60)
    output = process.stdout.read()
    assert code == 0, (argv, code, process.stderr.read())
    return output.strip()


def write(vm, filename, contents):
    with tempfile.TemporaryDirectory() as directory:
        local = Path(directory) / "upload"
        local.write_text(contents)
        vm.fs.upload_file(str(local), filename)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--pool", required=True)
    parser.add_argument(
        "--image-id", help="Existing amd64 Beam VM image (including desktop)."
    )
    parser.add_argument("--no-desktop", action="store_true")
    args = parser.parse_args()
    prefix = "vm-smoke-" + uuid.uuid4().hex[:8]
    vms = []
    artifacts = []
    service = None
    report = {"context": args.context, "pool": args.pool}
    try:
        vm = VM(
            prefix,
            context=args.context,
            image=Image.from_id(args.image_id) if args.image_id else None,
            pool=args.pool,
            cpu=2,
            memory=2048,
            disk_size="8GiB",
            desktop=not args.no_desktop,
            docker_enabled=True,
            ssh=False,
            env={"VM_SMOKE_ENV": "inherited by systemd % literally"},
        )
        vms.append(vm)
        vm.create()
        service = vm._service
        identity = execute(vm, "cat", "/etc/machine-id")
        assert execute(vm, "cat", "/proc/1/comm") == "systemd"
        assert "x86_64" in execute(vm, "docker", "run", "--rm", "busybox:1.36.1", "uname", "-m")
        report["nested_docker"] = "passed"
        urls = vm.refresh().info["urls"].copy()
        runtime = vm.info["container_id"]
        write(vm, "/root/persistent-marker", prefix)
        write(
            vm,
            "/root/vm-smoke-unit.py",
            "import os, signal, time\n"
            "with open('/root/unit-boots', 'a') as f: f.write(os.environ['VM_SMOKE_ENV']+'\\n')\n"
            "def stop(*args):\n"
            " with open('/root/shutdown-marker', 'w') as f: f.write('committed-at-shutdown')\n"
            " raise SystemExit(0)\n"
            "signal.signal(signal.SIGTERM, stop)\n"
            "while True: time.sleep(1)\n",
        )
        write(
            vm,
            "/etc/systemd/system/vm-smoke.service",
            "[Unit]\nAfter=network.target\n[Service]\nType=exec\n"
            "ExecStart=/usr/bin/python3 /root/vm-smoke-unit.py\n"
            "TimeoutStopSec=5\n[Install]\nWantedBy=multi-user.target\n",
        )
        execute(vm, "systemctl", "daemon-reload")
        execute(vm, "systemctl", "enable", "--now", "vm-smoke.service")
        vm.stop(no_snapshot=True)
        vm.start()
        assert vm.info["container_id"] != runtime
        assert vm.info["urls"] == urls
        assert execute(vm, "cat", "/etc/machine-id") == identity
        assert execute(vm, "cat", "/root/persistent-marker") == prefix
        assert execute(vm, "cat", "/root/shutdown-marker") == "committed-at-shutdown"
        assert execute(vm, "systemctl", "is-active", "vm-smoke.service") == "active"
        assert (
            execute(vm, "cat", "/root/unit-boots").count(
                "inherited by systemd % literally"
            )
            >= 2
        )
        report["root_systemd_shutdown_identity_urls"] = "passed"
        if vm.desktop_url:
            # localhost's wildcard DNS is browser-specific. Send the stable
            # desktop Host explicitly through this profile's local gateway.
            from urllib.parse import urlsplit

            parsed = urlsplit(vm.desktop_url)
            response = requests.get(
                service.http.base_url + parsed.path,
                headers={"Host": parsed.netloc},
                timeout=30,
            )
            response.raise_for_status()
            assert "KasmVNC" in response.text
            report["desktop_http"] = "passed"
        template = vm.create_template(prefix + "-tpl")
        artifacts.append(("template", template["id"]))
        child = vm.fork(prefix + "-fork")
        vms.append(child)
        assert execute(child, "cat", "/root/persistent-marker") == prefix
        assert execute(child, "cat", "/etc/machine-id") != identity
        assert child.refresh().info["urls"] != urls
        execute(child, "sh", "-c", "echo child > /root/persistent-marker")
        assert execute(vm, "cat", "/root/persistent-marker") == prefix
        vm.stop(no_snapshot=True)
        vm.remove()
        templated = VM(
            prefix + "-tpl-vm", template=template["id"], ssh=False, _service=service
        )
        vms.append(templated)
        templated.create()
        assert execute(templated, "cat", "/root/persistent-marker") == prefix
        assert execute(templated, "cat", "/etc/machine-id") not in (
            identity,
            execute(child, "cat", "/etc/machine-id"),
        )
        report["fork_and_template_after_source_removal"] = "passed"
        print(json.dumps(report, indent=2))
    finally:
        failures = []
        for vm in reversed(vms):
            if vm.info.get("id"):
                try:
                    vm.remove()
                except Exception as error:
                    failures.append(f"remove {vm.info['id']}: {error}")
        if service:
            for kind, artifact in artifacts:
                try:
                    vm._api("DELETE", f"/artifacts/{kind}/{artifact}")
                except Exception as error:
                    failures.append(f"remove artifact {artifact}: {error}")
        for vm in vms:
            vm.close()
        if failures:
            raise RuntimeError("Cleanup incomplete: " + "; ".join(failures))


if __name__ == "__main__":
    main()
