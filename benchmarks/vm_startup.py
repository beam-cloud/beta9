#!/usr/bin/env python3
"""Measure fresh CPU runtime launches through the public SDK and verify first exec.

Image/key/stub preparation is reported separately, as in sandbox_parallel.py.
Every measured VM has a fresh durable root; no warm VM or RAM restore is reused.
The budget applies to API submission through the first accepted command, including
network transport. Use a client near the gateway and compare remote-client results.
"""
import argparse
import json
import math
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from beta9 import Image, Sandbox, VM
from beta9.abstractions.base import set_channel, unset_channel
from beta9.abstractions.base.runner import SANDBOX_STUB_TYPE
from beta9.channel import ServiceClient
from beta9.config import ConfigContext, get_config_context


def arguments():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", default="staging")
    p.add_argument(
        "--context-stdin",
        action="store_true",
        help="Read the context from stdin, keeping credentials out of argv/files",
    )
    p.add_argument("--mode", choices=("vm", "sandbox"), required=True)
    p.add_argument("--pool", required=True)
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--cpu", type=float, default=1)
    p.add_argument("--memory", type=int, default=1024)
    p.add_argument("--disk-size", default="50GiB")
    p.add_argument(
        "--image-id", help="An already prepared VM image, or ordinary sandbox image"
    )
    p.add_argument("--image-uri", default="ubuntu:22.04")
    p.add_argument(
        "--use-vm",
        action="store_true",
        help="Use a microVM for the temporary sandbox mode",
    )
    p.add_argument("--no-ssh", action="store_true")
    p.add_argument("--desktop", action="store_true")
    p.add_argument("--budget-ms", type=float, default=500)
    p.add_argument(
        "--http-url", help="Override transport for a client in the gateway's cluster"
    )
    p.add_argument("--grpc-host")
    p.add_argument("--grpc-port", type=int)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    if a.count < 1 or a.cpu <= 0 or a.memory <= 0:
        p.error("count, CPU and memory must be positive")
    return a


def main():
    args = arguments()
    context = (
        ConfigContext.from_dict(json.load(sys.stdin))
        if args.context_stdin
        else get_config_context(args.context)
    )
    if args.http_url:
        context.api_url = args.http_url
    if args.grpc_host:
        context.gateway_host = args.grpc_host
    if args.grpc_port:
        context.gateway_port = args.grpc_port
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "context": args.context,
        "mode": args.mode,
        "pool": args.pool,
        "cpu": args.cpu,
        "memory": args.memory,
        "disk_size": args.disk_size if args.mode == "vm" else None,
        "budget_ms": args.budget_ms,
        "samples": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, indent=2))

    with ServiceClient(context) as service:
        # Connect transport during preparation, without allocating a runtime.
        _ = service.http.base_url
        set_channel(channel=service.channel)
        try:
            image = (
                Image.from_id(args.image_id)
                if args.image_id
                else Image.from_registry(args.image_uri)
            )
            sandbox = None
            if args.mode == "sandbox":
                sandbox = Sandbox(
                    image=image,
                    cpu=args.cpu,
                    memory=args.memory,
                    pool=args.pool,
                    use_vm=args.use_vm,
                    sync_local_dir=False,
                    keep_warm_seconds=180,
                    name="tti-" + uuid.uuid4().hex[:8],
                )
                begin = time.perf_counter()
                if not sandbox.prepare_runtime(
                    stub_type=SANDBOX_STUB_TYPE,
                    force_create_stub=True,
                    ignore_patterns=["*"],
                ):
                    raise RuntimeError("Sandbox preparation failed")
                report["sandbox_preparation_ms"] = (time.perf_counter() - begin) * 1000
                report["image_id"] = sandbox.image_id
            for index in range(args.count):
                nonce = "tti-" + uuid.uuid4().hex
                runtime = None
                row = {"index": index, "name": nonce[:20]}
                report["samples"].append(row)
                save()  # Ownership is recorded before launch or cleanup.
                try:
                    if args.mode == "vm":
                        runtime = VM(
                            row["name"],
                            image=image,
                            cpu=args.cpu,
                            memory=args.memory,
                            pool=args.pool,
                            disk_size=args.disk_size,
                            ssh=not args.no_ssh,
                            desktop=args.desktop,
                            ttl=0,
                            metadata={"beam-startup-benchmark": nonce},
                            _service=service,
                        )
                        begin = time.perf_counter()
                        runtime.prepare()
                        row["preparation_ms"] = (time.perf_counter() - begin) * 1000
                    begin = time.perf_counter()
                    if args.mode == "vm":
                        runtime.create()
                        row.update(
                            id=runtime.id,
                            container_id=runtime.info["container_id"],
                            exec_ready_ms=(time.perf_counter() - begin) * 1000,
                        )
                    else:
                        runtime = sandbox.create()
                        row.update(
                            container_id=runtime.container_id,
                            submitted_ms=(time.perf_counter() - begin) * 1000,
                        )
                    save()
                    process = runtime.process.exec(
                        "sh", "-c", 'printf "%s\\n" "$1"', "startup", nonce, cwd="/"
                    )
                    row["first_exec_ms"] = (time.perf_counter() - begin) * 1000
                    assert process.wait(30) == 0, "First command failed"
                    output = process.stdout.read().strip()
                    row["verified_output_ms"] = (time.perf_counter() - begin) * 1000
                    assert output == nonce, f"First command output mismatch: {output!r}"
                    row["passed"] = row["first_exec_ms"] < args.budget_ms
                    print(json.dumps(row), flush=True)
                except Exception as exc:
                    row.update(passed=False, error=str(exc))
                    raise
                finally:
                    save()
                    if runtime is not None:
                        if args.mode == "vm" and not runtime.info.get("id"):
                            # A blocking launch can lose its response after
                            # persisting the resource. Resolve only this run's
                            # unique name, and verify the ownership marker.
                            try:
                                recovered = VM.get(row["name"], _service=service)
                            except Exception:
                                recovered = None
                            if (
                                recovered is not None
                                and recovered.info.get("metadata", {}).get(
                                    "beam-startup-benchmark"
                                )
                                == nonce
                            ):
                                runtime = recovered
                                row.update(
                                    id=runtime.id,
                                    container_id=runtime.info.get("container_id"),
                                )
                        if args.mode == "vm" and runtime.info.get("id"):
                            runtime.remove()
                            row["removed"] = True
                        elif args.mode == "sandbox":
                            row["removed"] = runtime.terminate()
                        save()
        finally:
            unset_channel()
    times = sorted(row["first_exec_ms"] for row in report["samples"])
    report["summary"] = {
        "p50_ms": times[math.ceil(len(times) * 0.5) - 1],
        "p95_ms": times[math.ceil(len(times) * 0.95) - 1],
        "max_ms": times[-1],
        "passed": all(row["passed"] for row in report["samples"]),
    }
    save()
    print(json.dumps(report["summary"]), flush=True)
    return 0 if report["summary"]["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
