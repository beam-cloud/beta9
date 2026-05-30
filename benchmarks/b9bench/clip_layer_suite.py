#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from b9bench.cache_suite import Kube, find_redis_pod, redis_cli, run


REPO_ROOT = Path(__file__).resolve().parents[2]
LAYER_PATH = "/opt/b9bench/layer.bin"
SHA_PATH = "/opt/b9bench/layer.sha256"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prove CLIP OCI layer bytes survive worker churn through the embedded cache."
    )
    parser.add_argument("--namespace", default=os.getenv("BENCH_NAMESPACE", "beta9"))
    parser.add_argument("--gateway-url", default=os.getenv("BENCH_GATEWAY_URL", "http://127.0.0.1:1994"))
    parser.add_argument("--grpc-addr", default=os.getenv("BENCH_GRPC_ADDR", "127.0.0.1:1993"))
    parser.add_argument("--token", default=os.getenv("BENCH_TOKEN", ""))
    parser.add_argument("--profile", default=os.getenv("BETA9_PROFILE", "default"))
    parser.add_argument("--beta9-config", default=os.getenv("BENCH_CONFIG", "~/.beta9/config.ini"))
    parser.add_argument("--output", default="/tmp/beta9-clip-layer-cache.json")
    parser.add_argument("--size-mib", type=int, default=128)
    parser.add_argument("--timeout-seconds", type=float, default=1200)
    parser.add_argument("--read-timeout-seconds", type=float, default=240)
    parser.add_argument("--cache-pool-name", default=os.getenv("BENCH_CACHE_POOL_NAME", "default"))
    parser.add_argument("--cache-locality", default=os.getenv("BENCH_CACHE_LOCALITY", "default"))
    parser.add_argument("--reset-workers-between", dest="reset_workers_between", action="store_true")
    parser.add_argument("--no-reset-workers-between", dest="reset_workers_between", action="store_false")
    parser.set_defaults(reset_workers_between=True)
    parser.add_argument("--clear-clip-disk-cache", dest="clear_clip_disk_cache", action="store_true")
    parser.add_argument("--keep-clip-disk-cache", dest="clear_clip_disk_cache", action="store_false")
    parser.set_defaults(clear_clip_disk_cache=True)
    parser.add_argument("--probe-id", default="")
    return parser.parse_args()


def configure_sdk(args: argparse.Namespace) -> None:
    os.environ["CONFIG_PATH"] = str(Path(args.beta9_config).expanduser())
    os.environ["BETA9_PROFILE"] = args.profile
    if args.token:
        os.environ["BETA9_TOKEN"] = args.token
    if str(REPO_ROOT / "sdk" / "src") not in sys.path:
        sys.path.insert(0, str(REPO_ROOT / "sdk" / "src"))


def deterministic_layer_command(size_mib: int, probe_id: str) -> str:
    size = size_mib * 1024 * 1024
    return f"""mkdir -p /opt/b9bench && python3 - <<'PY'
import hashlib

size = {size}
probe_id = {probe_id!r}
path = {LAYER_PATH!r}
sha_path = {SHA_PATH!r}

def block(counter):
    seed = f"b9bench-clip-layer:{{probe_id}}:{{counter}}".encode()
    chunk = bytearray()
    nonce = 0
    while len(chunk) < 1024 * 1024:
        chunk.extend(hashlib.sha256(seed + b":" + str(nonce).encode()).digest())
        nonce += 1
    return bytes(chunk[:1024 * 1024])

digest = hashlib.sha256()
remaining = size
counter = 0
with open(path, "wb") as handle:
    while remaining:
        data = block(counter)
        data = data[: min(len(data), remaining)]
        handle.write(data)
        digest.update(data)
        remaining -= len(data)
        counter += 1
with open(sha_path, "w") as handle:
    handle.write(digest.hexdigest() + "\\n")
PY"""


READ_CODE = r"""
import hashlib
import json
import os
import time

path = "/opt/b9bench/layer.bin"
sha_path = "/opt/b9bench/layer.sha256"
expected = open(sha_path, "r", encoding="utf-8").read().strip()
digest = hashlib.sha256()
size = os.path.getsize(path)
started = time.perf_counter()
with open(path, "rb", buffering=0) as handle:
    while True:
        chunk = handle.read(8 * 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
duration = time.perf_counter() - started
actual = digest.hexdigest()
print(json.dumps({
    "ok": actual == expected,
    "path": path,
    "size": size,
    "durationSeconds": duration,
    "mbps": (size / 1024 / 1024) / duration if duration else 0,
    "expectedSha256": expected,
    "actualSha256": actual,
}))
"""


def build_probe_image(args: argparse.Namespace, probe_id: str):
    from beta9 import Image

    image = Image(python_version="python3.12")
    image.add_commands([deterministic_layer_command(args.size_mib, probe_id)])
    result = image.build()
    if not result.success:
        raise RuntimeError("failed to build CLIP layer probe image")
    return image, result.image_id


def create_sandbox(image, args: argparse.Namespace):
    from beta9 import Sandbox

    sandbox = Sandbox(
        cpu=1,
        memory=1024,
        image=image,
        keep_warm_seconds=300,
        name="b9bench-clip-layer-cache",
    )
    instance = sandbox.create()
    if not instance.ok:
        raise RuntimeError(instance.error_msg or "Sandbox.create returned ok=false")
    return instance


def wait_instance_running(instance, timeout_seconds: float) -> None:
    from beta9.clients.pod import PodSandboxStatusRequest, PodSandboxStatusResponse

    container_id = instance.container_id
    deadline = time.monotonic() + timeout_seconds
    attempts = 0
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        attempts += 1
        try:
            response = instance.stub._unary_unary(
                "/pod.PodService/SandboxStatus",
                PodSandboxStatusRequest,
                PodSandboxStatusResponse,
            )(
                PodSandboxStatusRequest(container_id=container_id, pid=0),
                timeout=5,
            )
            last = {
                "ok": bool(response.ok),
                "status": response.status,
                "error": response.error_msg,
            }
            if response.ok and str(response.status).lower() == "running":
                return
        except Exception as exc:
            last = {"error": str(exc)}
        if attempts % 30 == 0:
            log(f"waiting for sandbox RUNNING container={container_id} last={last}")
        time.sleep(0.5)
    raise TimeoutError(f"timed out waiting for sandbox RUNNING container={container_id}; last={last}")


def read_layer(instance, timeout_seconds: float) -> dict[str, Any]:
    process = instance.process.exec("python3", "-c", READ_CODE, cwd="/")
    exit_code = process.wait(timeout=timeout_seconds)
    stdout = process.stdout.read()
    stderr = process.stderr.read()
    if exit_code != 0:
        raise RuntimeError(stderr or stdout or f"layer read exited {exit_code}")
    payload = parse_json_from_output(stdout)
    if not payload:
        raise RuntimeError(f"layer read did not emit JSON: stdout={stdout!r} stderr={stderr!r}")
    return payload


def parse_json_from_output(output: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {}


def reset_workers(kube: Kube, timeout: float) -> dict[str, Any]:
    pods = [pod["name"] for pod in kube.running_workers() if pod["name"].startswith("worker-default-")]
    if not pods:
        return {"ok": False, "error": "no running default workers"}
    log(f"Resetting {len(pods)} default worker pod(s): {', '.join(pods)}")
    return kube.restart_worker_pods(pods, timeout=timeout)


def worker_for_container(kube: Kube, container_id: str) -> dict[str, Any]:
    pod = kube.wait_worker_pod_for_container(container_id, timeout=120)
    return pod or {}


def collect_worker_logs(
    kube: Kube,
    since_time: str,
    artifact_path: Path,
    include_pods: list[str],
) -> list[str]:
    pod_names = {pod["name"] for pod in kube.worker_pods() if pod.get("name")}
    pod_names.update(name for name in include_pods if name)
    chunks: list[str] = []
    for pod in sorted(pod_names):
        proc = run(
            [
                "kubectl",
                "-n",
                kube.config.namespace,
                "logs",
                pod,
                "-c",
                "worker",
                "--since-time",
                since_time,
                "--tail=-1",
            ],
            check=False,
            timeout=60,
        )
        if proc.stdout:
            chunks.append("\n".join(f"[pod/{pod}/worker] {line}" for line in proc.stdout.splitlines()))
        if proc.stderr and "not found" not in proc.stderr.lower():
            chunks.append(f"## stderr {pod}\n{proc.stderr}")
    text = "\n".join(chunks)
    artifact_path.write_text(text, encoding="utf-8")
    return text.splitlines()


def summarize_logs(lines: list[str], image_id: str) -> dict[str, Any]:
    relevant = [line for line in lines if image_id in line or "clip image content cache" in line or "content cache" in line or "oci cache" in line or "layer decompressed" in line]
    decompressed_hashes = sorted(set(re.findall(r'"decompressed_hash":"([^"]+)"|decompressed_hash=([0-9a-f]{16,})', "\n".join(relevant))))
    flat_hashes = sorted({item for pair in decompressed_hashes for item in pair if item})
    return {
        "lineCount": len(relevant),
        "ociCacheMisses": count_contains(relevant, "oci cache miss"),
        "layerDecompressed": count_contains(relevant, "layer decompressed and cached"),
        "contentCacheHits": count_contains(relevant, "content cache hit"),
        "contentCacheMisses": count_contains(relevant, "content cache miss"),
        "contentCacheReadErrors": count_contains(relevant, "clip image content cache read result"),
        "contentCacheStoreResults": count_contains(relevant, "clip image content cache store result"),
        "alreadyPresent": count_contains(relevant, "decompressed layer already present in content cache"),
        "diskCacheHits": count_contains(relevant, "disk cache hit"),
        "decompressedHashes": flat_hashes,
        "sample": relevant[-40:],
    }


def count_contains(lines: list[str], needle: str) -> int:
    return sum(1 for line in lines if needle in line)


def embedded_cache_page_proof(kube: Kube, hashes: list[str]) -> dict[str, Any]:
    proof: dict[str, Any] = {"hashes": hashes, "workers": []}
    if not hashes:
        proof["ok"] = False
        proof["error"] = "no decompressed layer hash found in logs"
        return proof
    script_parts = []
    for hash_value in hashes:
        prefix = hash_value[:2]
        script_parts.append(
            f"h={hash_value}; count=$(find /var/lib/beta9/cache/default -path '*/pages/{prefix}/{hash_value}/{hash_value}-*' -type f 2>/dev/null | wc -l); echo \"$h $count\""
        )
    script = "; ".join(script_parts)
    ok = False
    for pod in kube.running_workers():
        proc = kube.worker_shell(pod["name"], script, timeout=30)
        rows = []
        for line in proc.stdout.splitlines():
            parts = line.split()
            if len(parts) == 2:
                rows.append({"hash": parts[0], "pages": int(parts[1])})
                ok = ok or int(parts[1]) > 0
        proof["workers"].append(
            {
                "pod": pod["name"],
                "nodeName": pod.get("nodeName", ""),
                "returnCode": proc.returncode,
                "rows": rows,
                "stderr": proc.stderr.strip()[-300:],
            }
        )
    proof["ok"] = ok
    return proof


def cache_hosts_snapshot(kube: Kube, pool_name: str, locality: str) -> dict[str, Any]:
    redis_pod = find_redis_pod(kube.config.namespace)
    if not redis_pod:
        return cache_hosts_from_worker_logs(kube)

    index_pattern = f"cache:coordinator:host_index:*:{locality}"
    index_keys = [
        line.strip()
        for line in (redis_cli(kube.config.namespace, redis_pod, "KEYS", index_pattern) or "").splitlines()
        if line.strip()
    ]
    if not index_keys:
        index_keys = [f"cache:coordinator:host_index:{pool_name}:{locality}"]

    logical_host_ids: list[str] = []
    seen = set()
    for index_key in index_keys:
        members = redis_cli(kube.config.namespace, redis_pod, "SMEMBERS", index_key) or ""
        for line in members.splitlines():
            logical_host_id = line.strip()
            if logical_host_id and logical_host_id not in seen:
                seen.add(logical_host_id)
                logical_host_ids.append(logical_host_id)

    hosts: list[dict[str, Any]] = []
    for logical_host_id in logical_host_ids:
        for registration_id in registration_ids(kube.config.namespace, redis_pod, logical_host_id):
            raw = redis_cli(
                kube.config.namespace,
                redis_pod,
                "GET",
                f"cache:coordinator:host:{logical_host_id}:registration:{registration_id}",
            )
            if not raw:
                continue
            try:
                registration = json.loads(raw)
            except json.JSONDecodeError:
                continue
            hosts.append(
                {
                    "hostId": registration.get("logical_host_id")
                    or registration.get("logicalHostId")
                    or logical_host_id,
                    "registrationId": registration.get("registration_id")
                    or registration.get("registrationId")
                    or registration_id,
                    "poolName": registration.get("pool_name")
                    or registration.get("poolName")
                    or pool_name,
                    "nodeId": registration.get("node_id") or registration.get("nodeId") or "",
                    "nodeName": registration.get("node_id") or registration.get("nodeId") or "",
                    "cachePathId": registration.get("cache_path_id")
                    or registration.get("cachePathId")
                    or "",
                    "addr": registration.get("addr") or "",
                    "privateAddr": registration.get("private_addr")
                    or registration.get("privateAddr")
                    or "",
                    "source": "coordinator",
                }
            )
            break
    if hosts:
        return {"available": True, "source": "coordinator", "indexKeys": index_keys, "hosts": hosts}
    return cache_hosts_from_worker_logs(kube)


def registration_ids(namespace: str, redis_pod: str, logical_host_id: str) -> list[str]:
    active = redis_cli(
        namespace,
        redis_pod,
        "GET",
        f"cache:coordinator:host:{logical_host_id}:active_registration",
    )
    ids = [active.strip()] if active and active.strip() else []
    ids.extend(
        registration_id.strip()
        for registration_id in (
            redis_cli(namespace, redis_pod, "SMEMBERS", f"cache:coordinator:host:{logical_host_id}:registrations")
            or ""
        ).splitlines()
        if registration_id.strip() and registration_id.strip() not in ids
    )
    return ids


def cache_hosts_from_worker_logs(kube: Kube) -> dict[str, Any]:
    hosts_by_id: dict[str, dict[str, Any]] = {}
    for pod in kube.running_workers():
        proc = run(
            [
                "kubectl",
                "-n",
                kube.config.namespace,
                "logs",
                pod["name"],
                "-c",
                "worker",
                "--since",
                "2h",
                "--tail",
                "10000",
            ],
            check=False,
            timeout=30,
        )
        if proc.returncode != 0:
            continue
        latest: dict[str, Any] | None = None
        for line in proc.stdout.splitlines():
            if "Registered cache host" not in line:
                continue
            start = line.find("{")
            end = line.rfind("}")
            if start < 0 or end <= start:
                continue
            try:
                latest = json.loads(line[start : end + 1])
            except json.JSONDecodeError:
                continue
        if not latest:
            continue
        host_id = latest.get("logical_host_id") or latest.get("logicalHostId")
        registration_id = latest.get("registration_id") or latest.get("registrationId")
        addr = latest.get("addr") or ""
        if not host_id or not registration_id:
            continue
        hosts_by_id[host_id] = {
            "hostId": host_id,
            "registrationId": registration_id,
            "poolName": latest.get("pool_name") or latest.get("poolName") or "",
            "nodeId": latest.get("node_id") or latest.get("nodeId") or pod.get("nodeName", ""),
            "nodeName": pod.get("nodeName", ""),
            "cachePathId": latest.get("cache_path_id") or latest.get("cachePathId") or "",
            "addr": addr,
            "privateAddr": addr,
            "pod": pod.get("name", ""),
            "source": "worker_logs",
        }
    hosts = list(hosts_by_id.values())
    return {"available": bool(hosts), "source": "worker_logs", "hosts": hosts}


def hrw_routing_proof(
    kube: Kube,
    hashes: list[str],
    page_proof: dict[str, Any],
    artifact_dir: Path,
    pool_name: str,
    locality: str,
) -> dict[str, Any]:
    proof: dict[str, Any] = {
        "ok": False,
        "hashes": hashes,
        "hostsSnapshot": cache_hosts_snapshot(kube, pool_name, locality),
        "routes": [],
    }
    hosts = proof["hostsSnapshot"].get("hosts") or []
    if not hashes:
        proof["error"] = "no hashes provided"
        return proof
    if not hosts:
        proof["error"] = "no active cache hosts found"
        return proof

    go = shutil.which("go")
    if not go:
        proof["error"] = "go not found"
        return proof

    env = dict(os.environ)
    bundled_goroot = "/opt/homebrew/Cellar/go/1.24.2/libexec"
    if Path(bundled_goroot).exists():
        env["GOROOT"] = bundled_goroot

    proc = subprocess.run(
        [
            go,
            "run",
            "./benchmarks/b9bench/cache_tools/hrw_routing.go",
            "--hosts-json",
            json.dumps(hosts, sort_keys=True),
            "--keys",
            ",".join(hashes),
            "--n",
            "1",
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=120,
        env=env,
    )
    if proc.returncode != 0:
        proof["error"] = proc.stderr.strip()[-1000:] or f"hrw helper exited {proc.returncode}"
        return proof

    routes = json.loads(proc.stdout)
    pages_by_hash_node: dict[tuple[str, str], int] = {}
    for worker in page_proof.get("workers") or []:
        node = worker.get("nodeName") or ""
        for row in worker.get("rows") or []:
            pages_by_hash_node[(row.get("hash") or "", node)] = int(row.get("pages") or 0)

    checked = []
    ok = True
    for route in routes:
        key = route.get("key") or ""
        selected = (route.get("hosts") or [{}])[0]
        node = selected.get("nodeName") or selected.get("nodeId") or ""
        pages = pages_by_hash_node.get((key, node), 0)
        item = {
            "hash": key,
            "selectedHostId": selected.get("hostId") or "",
            "selectedRegistrationId": selected.get("registrationId") or "",
            "selectedNode": node,
            "selectedPod": selected.get("pod") or "",
            "pagesOnSelectedNode": pages,
            "ok": pages > 0,
        }
        checked.append(item)
        ok = ok and item["ok"]

    proof["routes"] = checked
    proof["ok"] = ok
    if not ok:
        (artifact_dir / "clip-hrw-routing-proof.json").write_text(
            json.dumps(proof, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return proof


def clip_disk_cache_presence(kube: Kube, hashes: list[str]) -> dict[str, Any]:
    proof: dict[str, Any] = {"hashes": hashes, "workers": []}
    if not hashes:
        proof["ok"] = False
        proof["error"] = "no hashes provided"
        return proof
    script = "; ".join(
        f"if [ -e /images/cache/{hash_value} ]; then echo {hash_value} present; else echo {hash_value} absent; fi"
        for hash_value in hashes
    )
    present = False
    for pod in kube.running_workers():
        proc = kube.worker_shell(pod["name"], script, timeout=30)
        rows = []
        for line in proc.stdout.splitlines():
            parts = line.split()
            if len(parts) == 2:
                rows.append({"hash": parts[0], "state": parts[1]})
                present = present or parts[1] == "present"
        proof["workers"].append(
            {
                "pod": pod["name"],
                "returnCode": proc.returncode,
                "rows": rows,
                "stderr": proc.stderr.strip()[-300:],
            }
        )
    proof["ok"] = not present
    proof["present"] = present
    return proof


def clear_clip_decompressed_disk_cache(kube: Kube, hashes: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": True, "workers": []}
    if hashes:
        script = "; ".join(f"rm -f /images/cache/{hash_value}" for hash_value in hashes)
    else:
        script = (
            "find /images/cache -maxdepth 1 -type f "
            "| grep -E '/[0-9a-f]{64}$' "
            "| xargs -r rm -f"
        )
    for pod in kube.running_workers():
        proc = kube.worker_shell(pod["name"], script, timeout=30)
        result["workers"].append(
            {
                "pod": pod["name"],
                "returnCode": proc.returncode,
                "stderr": proc.stderr.strip()[-300:],
            }
        )
        if proc.returncode != 0:
            result["ok"] = False
    return result


def extract_archive_hashes(kube: Kube, image_id: str, artifact_dir: Path) -> list[str]:
    workers = kube.running_workers()
    if not workers:
        return []
    go = shutil.which("go")
    if not go:
        return []
    env = dict(os.environ)
    bundled_goroot = "/opt/homebrew/Cellar/go/1.24.2/libexec"
    if Path(bundled_goroot).exists():
        env["GOROOT"] = bundled_goroot

    for suffix in (".rclip", ".clip"):
        archive = artifact_dir / f"{image_id}{suffix}"
        for pod in workers:
            proc = subprocess.run(
                [
                    "kubectl",
                    "-n",
                    kube.config.namespace,
                    "cp",
                    "-c",
                    "worker",
                    f"{pod['name']}:/images/cache/{image_id}{suffix}",
                    str(archive),
                ],
                cwd=REPO_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=60,
            )
            if proc.returncode != 0 or not archive.exists():
                continue

            parse = subprocess.run(
                [go, "run", "./benchmarks/b9bench/cache_tools/clip_hashes.go", str(archive)],
                cwd=REPO_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=120,
                env=env,
            )
            if parse.returncode == 0:
                return list(json.loads(parse.stdout))
            (artifact_dir / f"{image_id}{suffix}.clip_hashes.err").write_text(
                parse.stderr,
                encoding="utf-8",
            )
    return []


def terminate(instance) -> None:
    try:
        instance.terminate()
    except Exception as exc:
        log(f"warning: failed to terminate sandbox {getattr(instance, 'container_id', '')}: {exc}")


def main() -> int:
    args = parse_args()
    configure_sdk(args)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    artifact_dir = output.parent
    probe_id = args.probe_id or f"{int(time.time())}-{os.getpid()}"
    kube = Kube(SimpleNamespace(namespace=args.namespace))

    validation_failures: list[str] = []
    report: dict[str, Any] = {
        "ok": False,
        "startedAt": utc_now(),
        "probeId": probe_id,
        "sizeMiB": args.size_mib,
        "resetWorkersBetween": args.reset_workers_between,
        "iterations": [],
        "validationFailures": validation_failures,
    }

    try:
        log(f"Building deterministic CLIP layer probe image ({args.size_mib}MiB)")
        image, image_id = build_probe_image(args, probe_id)
        report["imageId"] = image_id
        report["decompressedHashes"] = extract_archive_hashes(kube, image_id, artifact_dir)
        if args.clear_clip_disk_cache:
            report["preColdClipDiskCacheClear"] = clear_clip_decompressed_disk_cache(kube, report["decompressedHashes"])
            report["preColdClipDiskCachePresence"] = clip_disk_cache_presence(kube, report["decompressedHashes"])

        first_since = utc_now()
        first = run_iteration(kube, image, image_id, "cold", args, artifact_dir, first_since)
        report["iterations"].append(first)
        first_hashes = first.get("logSummary", {}).get("decompressedHashes") or report["decompressedHashes"]
        first["embeddedCachePageProof"] = embedded_cache_page_proof(kube, first_hashes)
        first["hrwRoutingProof"] = hrw_routing_proof(
            kube,
            first_hashes,
            first["embeddedCachePageProof"],
            artifact_dir,
            args.cache_pool_name,
            args.cache_locality,
        )
        first["clipDiskCachePresence"] = clip_disk_cache_presence(kube, first_hashes)

        if args.reset_workers_between:
            report["workerReset"] = reset_workers(kube, timeout=max(300, args.timeout_seconds / 2))
            if not report["workerReset"].get("ok"):
                validation_failures.append(f"worker reset failed: {report['workerReset']}")
            kube.wait_running_workers(min_count=1, timeout=max(180, args.timeout_seconds / 4))

        second_hashes = first_hashes or report["decompressedHashes"]
        if args.clear_clip_disk_cache:
            report["preSecondClipDiskCacheClear"] = clear_clip_decompressed_disk_cache(kube, second_hashes)
            report["preSecondClipDiskCachePresence"] = clip_disk_cache_presence(kube, second_hashes)

        second_since = utc_now()
        second_name = "after_worker_kill" if args.reset_workers_between else "stable_second_read"
        second = run_iteration(kube, image, image_id, second_name, args, artifact_dir, second_since)
        report["iterations"].append(second)
        second_hashes = second.get("logSummary", {}).get("decompressedHashes") or second_hashes
        second["embeddedCachePageProof"] = embedded_cache_page_proof(kube, second_hashes)
        second["hrwRoutingProof"] = hrw_routing_proof(
            kube,
            second_hashes,
            second["embeddedCachePageProof"],
            artifact_dir,
            args.cache_pool_name,
            args.cache_locality,
        )
        second["clipDiskCachePresence"] = clip_disk_cache_presence(kube, second_hashes)

        validate_report(report, validation_failures)
        report["ok"] = not validation_failures
        report["finishedAt"] = utc_now()
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return 0 if report["ok"] else 1
    except Exception as exc:
        validation_failures.append(str(exc))
        report["error"] = str(exc)
        report["finishedAt"] = utc_now()
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        raise


def run_iteration(
    kube: Kube,
    image,
    image_id: str,
    name: str,
    args: argparse.Namespace,
    artifact_dir: Path,
    since_time: str,
) -> dict[str, Any]:
    log(f"Starting CLIP layer read iteration: {name}")
    instance = create_sandbox(image, args)
    wait_instance_running(instance, min(args.timeout_seconds, 300))
    worker = worker_for_container(kube, instance.container_id)
    started = time.perf_counter()
    try:
        read = read_layer(instance, args.read_timeout_seconds)
    finally:
        terminate(instance)
    duration_ms = (time.perf_counter() - started) * 1000
    logs_path = artifact_dir / f"clip-layer-{name}.worker.log"
    worker_name = worker.get("name", "") if worker else ""
    lines = collect_worker_logs(kube, since_time, logs_path, [worker_name])
    summary = summarize_logs(lines, image_id)
    return {
        "name": name,
        "containerId": instance.container_id,
        "worker": worker,
        "read": read,
        "durationMs": duration_ms,
        "logPath": str(logs_path),
        "logSummary": summary,
    }


def validate_report(report: dict[str, Any], failures: list[str]) -> None:
    iterations = report.get("iterations") or []
    if len(iterations) < 2:
        failures.append("expected cold and second read iterations")
        return
    cold, after = iterations[0], iterations[1]
    for item in iterations:
        read = item.get("read") or {}
        if not read.get("ok"):
            failures.append(f"{item.get('name')} SHA verification failed")
        if int(read.get("size") or 0) != int(report.get("sizeMiB", 0)) * 1024 * 1024:
            failures.append(f"{item.get('name')} read size mismatch")
    if not cold.get("embeddedCachePageProof", {}).get("ok"):
        failures.append("cold iteration did not materialize embedded cache pages")
    if not after.get("embeddedCachePageProof", {}).get("ok"):
        failures.append(f"{after.get('name')} iteration could not find embedded cache pages")
    if not cold.get("hrwRoutingProof", {}).get("ok"):
        failures.append("cold iteration did not materialize pages on HRW-selected cache hosts")
    if not after.get("hrwRoutingProof", {}).get("ok"):
        failures.append(f"{after.get('name')} iteration did not find pages on HRW-selected cache hosts")

    cold_logs = cold.get("logSummary") or {}
    after_logs = after.get("logSummary") or {}
    if not report.get("decompressedHashes"):
        failures.append("could not extract OCI decompressed layer hashes from clip archive")
    if report.get("preSecondClipDiskCachePresence", {}).get("present"):
        failures.append("CLIP local decompressed disk cache was still present before second iteration")
    if after.get("clipDiskCachePresence", {}).get("present"):
        failures.append(f"{after.get('name')} iteration used/materialized CLIP local decompressed disk cache")
    if after_logs.get("ociCacheMisses", 0) > 0 or after_logs.get("layerDecompressed", 0) > 0:
        failures.append(f"{after.get('name')} iteration fell back to OCI registry/decompression")
    if report.get("resetWorkersBetween", True):
        cold_worker = ((cold.get("worker") or {}).get("name") or "")
        after_worker = ((after.get("worker") or {}).get("name") or "")
        if cold_worker and after_worker and cold_worker == after_worker:
            failures.append(f"worker did not change across reset: {cold_worker}")


if __name__ == "__main__":
    raise SystemExit(main())
