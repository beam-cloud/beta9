#!/usr/bin/env python3
"""Measure a fresh-VM burst and verify durable pause/resume on the same VMs.

Image preparation and client connections are outside the launch measurement.
Every launch gets a new VM and root disk. Readiness means SDK exec readiness;
verification additionally checks command output, tmpfs RAM, disk, and boot ID.
Only uniquely named resources owned by this run are removed, including after
an interrupted creation response. No workers or shared caches are reset.
"""

import argparse
import json
import math
import resource
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from beta9 import Image, VM
from beta9.channel import ServiceClient
from beta9.clients.gateway import ListContainersRequest, ListWorkersRequest
from beta9.config import get_config_context


def arguments():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", default="staging-admin")
    p.add_argument("--pool", default="microvm-cpu")
    p.add_argument("--worker-id", required=True)
    p.add_argument("--image-id", required=True, help="A prepared persistent-VM image")
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--cpu", type=float, default=0.1)
    p.add_argument("--memory", type=int, default=512)
    p.add_argument("--disk-size", default="1GiB")
    p.add_argument("--cycles", type=int, default=1)
    p.add_argument("--okteto", action="store_true")
    p.add_argument("--trace-startup", action="store_true")
    p.add_argument("--retain-mismatches", action="store_true", help="Retain only VMs with mismatching restored proofs for diagnosis (900-second VM TTL)")
    p.add_argument("--cold-client-connections", action="store_true", help="Include HTTP connection setup in launch")
    p.add_argument("--post-launch-gate", type=Path, help="Hold pause and cleanup after initial state verification until this file exists")
    p.add_argument("--post-startup-gate", type=Path, help="Hold state seeding immediately after timed launch, so startup profiles exclude verification")
    p.add_argument("--post-pause-gate", type=Path, help="Hold restore after pause until this file exists, so pause profiles exclude restore")
    p.add_argument("--launch-gate", type=Path, help="Wait for this file after preparing all clients")
    p.add_argument("--startup-budget-ms", type=float, default=1500)
    p.add_argument("--pause-budget-ms", type=float, default=8000)
    p.add_argument("--restore-budget-ms", type=float, default=5000)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    if a.count < 1 or a.cycles < 0 or a.cpu < 0.1 or a.memory < 256:
        p.error("positive count, nonnegative cycles, and at least 0.1 CPU / 256 MiB are required")
    return a


def command_result(vm, script, *args):
    proc = vm.process.exec("sh", "-c", script, "burst", *args, cwd="/")
    code = proc.wait(60)
    out, err = proc.stdout.read(), proc.stderr.read()
    return {"pid": proc.pid, "exit_code": code, "stdout": out, "stderr": err}


def command(vm, script, *args):
    result = command_result(vm, script, *args)
    code, out, err = result["exit_code"], result["stdout"], result["stderr"]
    if code != 0:
        raise RuntimeError(f"guest command exited {code}: {out} {err}")
    return out


def proof_files(vm):
    # The filesystem service uses vsock independently of process stdout.
    # Preserve evidence that distinguishes output loss from state loss.
    results = {}
    for path in ("/proc/sys/kernel/random/boot_id", "/dev/shm/beam-burst-proof", "/root/beam-burst-proof"):
        try:
            results[path] = {"contents": vm.fs.read_text(path)}
        except Exception as exc:
            results[path] = {"error": str(exc)}
    return results


def distribution(values):
    values = sorted(values)
    return {
        "count": len(values),
        "p50_ms": values[math.ceil(len(values) * 0.50) - 1] if values else None,
        "p95_ms": values[math.ceil(len(values) * 0.95) - 1] if values else None,
        "max_ms": values[-1] if values else None,
    }


def main():
    args = arguments()
    context = get_config_context(args.context)
    if context.gateway_host != "gateway.stage.beam.cloud":
        raise ValueError("This harness requires an explicit staging profile")
    if args.okteto:
        context.gateway_host, context.gateway_port = "127.0.0.1", 11993
        context.api_url = "http://127.0.0.1:11994"
    run_id = "burst-" + uuid.uuid4().hex[:12]
    report = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": vars(args) | {"output": str(args.output), "launch_gate": str(args.launch_gate) if args.launch_gate else None, "post_launch_gate": str(args.post_launch_gate) if args.post_launch_gate else None, "post_startup_gate": str(args.post_startup_gate) if args.post_startup_gate else None, "post_pause_gate": str(args.post_pause_gate) if args.post_pause_gate else None},
        "samples": [],
        "phases": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    save_lock = threading.Lock()

    def save():
        with save_lock:
            temp = args.output.with_suffix(".tmp")
            temp.write_text(json.dumps(report, indent=2))
            temp.replace(args.output)

    control = ServiceClient(context)
    clients, vms = [], []
    worker = next(
        (w for w in control.gateway.list_workers(ListWorkersRequest()).workers
         if w.id == args.worker_id and w.pool_name == args.pool), None
    )
    if worker is None:
        raise RuntimeError("Requested staging test worker is unavailable")
    report["worker"] = worker.to_dict()
    # Record names and ownership before the first launch, even if preparation fails.
    for i in range(args.count):
        report["samples"].append({"name": f"{run_id}-{i}", "proof": uuid.uuid4().hex})
    save()

    def phase(name, fn, budget=None):
        gate = threading.Event()

        def one(i):
            gate.wait()
            begin = time.perf_counter()
            report["samples"][i][name + "_started_ns"] = time.time_ns()
            try:
                fn(i, vms[i])
                return i, (time.perf_counter() - begin) * 1000, None
            except Exception as exc:
                return i, (time.perf_counter() - begin) * 1000, str(exc)

        with ThreadPoolExecutor(max_workers=args.count) as pool:
            futures = [pool.submit(one, i) for i in range(args.count)]
            begin = time.perf_counter()
            started_ns = time.time_ns()
            client_cpu = resource.getrusage(resource.RUSAGE_SELF)
            gate.set()
            results = [f.result() for f in as_completed(futures)]
            elapsed = (time.perf_counter() - begin) * 1000
            finished_ns = time.time_ns()
        errors = []
        for i, duration, error in results:
            row = report["samples"][i]
            row[name + "_ms"] = duration
            if error:
                row[name + "_error"] = error
                errors.append({"name": row["name"], "error": error})
        cpu_end = resource.getrusage(resource.RUSAGE_SELF)
        result = {"phase": name, "wall_ms": elapsed, "started_ns": started_ns, "finished_ns": finished_ns,
                  "client_user_cpu_ms": (cpu_end.ru_utime - client_cpu.ru_utime) * 1000,
                  "client_system_cpu_ms": (cpu_end.ru_stime - client_cpu.ru_stime) * 1000,
                  **distribution([d for _, d, e in results if not e]), "errors": errors}
        if budget is not None:
            result.update(budget_ms=budget, passed=not errors and elapsed <= budget)
        report["phases"].append(result)
        save()
        print(json.dumps(result), flush=True)
        if errors:
            raise RuntimeError(f"{name}: {len(errors)} VM(s) failed; see report")

    try:
        for row in report["samples"]:
            service = ServiceClient(context)
            if args.trace_startup:
                service.http.headers["X-Beam-Startup-Trace"] = "1"
            clients.append(service)
            vm = VM(row["name"], image=Image.from_id(args.image_id), cpu=args.cpu,
                    memory=args.memory, disk_size=args.disk_size, pool=args.pool,
                    ssh=False, ttl=900, auto_resume=False,
                    metadata={"beam-burst-benchmark": run_id}, _service=service)
            vms.append(vm)
            vm.prepare()
            if not args.cold_client_connections:
                service.http.request("GET", "/api/v1/health/live", timeout=15).raise_for_status()
            if args.trace_startup:
                row["http_requests"] = []
                original_send = service.http._session.send
                def traced_send(request, *, _row=row, _send=original_send, **kwargs):
                    timing = {"method": request.method, "started_ns": time.time_ns()}
                    _row["http_requests"].append(timing)
                    try:
                        return _send(request, **kwargs)
                    finally:
                        timing["finished_ns"] = time.time_ns()
                service.http._session.send = traced_send
        report["prepared_at"] = datetime.now(timezone.utc).isoformat()
        save()
        if args.launch_gate:
            while not args.launch_gate.exists():
                time.sleep(0.05)

        def launch(i, vm):
            vm.create()
            report["samples"][i].update(id=vm.id, container_id=vm.info["container_id"])

        phase("launch", launch, args.startup_budget_ms)
        if args.post_startup_gate:
            while not args.post_startup_gate.exists():
                time.sleep(0.05)
        containers = control.gateway.list_containers(ListContainersRequest())
        placement = {c.container_id: c.worker_id for c in containers.containers}
        for row in report["samples"]:
            row["worker_id"] = placement.get(row["container_id"])
            if row["worker_id"] != args.worker_id:
                raise RuntimeError(f"VM escaped the test worker: {row['name']}")

        def seed(i, vm):
            row = report["samples"][i]
            token = row["proof"]
            result = command_result(vm,
                'printf "%s" "$1" > /dev/shm/beam-burst-proof; '
                'printf "%s" "$1" > /root/beam-burst-proof; '
                'cat /proc/sys/kernel/random/boot_id; '
                'cat /dev/shm/beam-burst-proof /root/beam-burst-proof', token)
            row["seed_command"] = result
            proof = result["stdout"]
            row["guest_proof"] = proof
            parts = proof.split("\n")
            boot_valid = False
            if len(parts) == 2:
                try:
                    boot_valid = str(uuid.UUID(parts[0])) == parts[0]
                except ValueError:
                    pass
            if (result["exit_code"] != 0 or len(parts) != 2
                    or parts[1] != token * 2
                    or not boot_valid):
                row["initial_proof_mismatch"] = True
                save()
                row["initial_mismatch_files"] = proof_files(vm)
                diagnostics = row.setdefault("initial_mismatch_diagnostics", [])
                for attempt in range(3):
                    try:
                        diagnostics.append(command_result(vm,
                            'printf "boot="; cat /proc/sys/kernel/random/boot_id; '
                            'printf "ram="; cat /dev/shm/beam-burst-proof; '
                            'printf "\\ndisk="; cat /root/beam-burst-proof; printf "\\n"'))
                    except Exception as exc:
                        diagnostics.append({"error": str(exc)})
                save()
                raise RuntimeError("Initial RAM, disk, or boot proof was incomplete; see seed_command")

        phase("verify_launch", seed)
        boot_ids = {row["guest_proof"].split("\n", 1)[0] for row in report["samples"]}
        if len(boot_ids) != args.count:
            raise RuntimeError("Fresh VM burst reused a kernel boot identity")
        report["fresh_boot_ids_unique"] = True
        save()
        if args.post_launch_gate:
            while not args.post_launch_gate.exists():
                time.sleep(0.05)
        for cycle in range(args.cycles):
            def pause(i, vm):
                vm.pause()
                if vm.info["status"] != "paused" or not vm.info.get("memory_checkpoint_id"):
                    raise RuntimeError("Pause did not publish a usable RAM checkpoint")
                report["samples"][i].setdefault("checkpoint_ids", []).append(
                    vm.info["memory_checkpoint_id"])

            phase(f"pause_{cycle}", pause, args.pause_budget_ms)
            if args.post_pause_gate:
                while not args.post_pause_gate.exists():
                    time.sleep(0.05)
            def restore(i, vm):
                vm.resume()
                report["samples"][i].setdefault("restore_container_ids", []).append(vm.info["container_id"])

            phase(f"restore_{cycle}", restore, args.restore_budget_ms)

            def verify(i, vm):
                row = report["samples"][i]
                result = command_result(vm, 'cat /proc/sys/kernel/random/boot_id; '
                    'cat /dev/shm/beam-burst-proof /root/beam-burst-proof')
                row.setdefault("restore_commands", []).append(result)
                if result["exit_code"] != 0 or result["stdout"] != row["guest_proof"]:
                    # Collect evidence before cleanup. A successful diagnostic
                    # retry never turns the original mismatch into a pass.
                    row["restore_proof_mismatch"] = True
                    save()
                    row.setdefault("restore_mismatch_files", []).append(proof_files(vm))
                    diagnostics = row.setdefault("restore_mismatch_diagnostics", [])
                    for attempt in range(3):
                        try:
                            diagnostics.append(command_result(vm,
                                'printf "boot="; cat /proc/sys/kernel/random/boot_id; '
                                'printf "ram="; cat /dev/shm/beam-burst-proof; '
                                'printf "\\ndisk="; cat /root/beam-burst-proof; printf "\\n"'))
                        except Exception as exc:
                            diagnostics.append({"error": str(exc)})
                    save()
                    raise RuntimeError("RAM, disk, or boot identity changed across resume")

            phase(f"verify_restore_{cycle}", verify)
    except Exception as exc:
        report["error"] = str(exc)
    finally:
        # Resolve only the run's recorded names after an ambiguous launch.
        def cleanup(i):
            row = report["samples"][i]
            if args.retain_mismatches and (row.get("initial_proof_mismatch") or row.get("restore_proof_mismatch")
                                           or row.get("restore_mismatch_diagnostics")
                                           or row.get("initial_mismatch_diagnostics")):
                row["retained_for_diagnosis"] = True
                save()
                return
            service = clients[i] if i < len(clients) else ServiceClient(context)
            try:
                path = "/api/v1/vm/{ws}/" + row["name"]
                deadline = time.monotonic() + 180
                while True:
                    try:
                        response = service.http.request("GET", path, timeout=10)
                        if response.status_code == 404:
                            row["removed"] = True
                            break
                        response.raise_for_status()
                        if response.json().get("metadata", {}).get("beam-burst-benchmark") != run_id:
                            raise ValueError("Resource ownership check failed")
                        response = service.http.request("DELETE", path, timeout=20)
                        response.raise_for_status()
                        row["removed"] = True
                        break
                    except ValueError:
                        raise
                    except Exception:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(2)
            except Exception as exc:
                row["cleanup_error"] = str(exc)
            finally:
                save()
                if i >= len(clients):
                    service.close()

        with ThreadPoolExecutor(max_workers=min(8, args.count)) as pool:
            list(pool.map(cleanup, range(args.count)))
        for client in clients:
            client.close()
        control.close()
    report["passed"] = (not report.get("error")
        and all(p.get("passed", True) for p in report["phases"])
        and all(row.get("removed") for row in report["samples"]))
    save()
    print(json.dumps({"passed": report["passed"], "output": str(args.output)}), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
