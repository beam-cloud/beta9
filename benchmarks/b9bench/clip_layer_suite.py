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

from b9bench.cache_suite import Kube, run


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
    archive = artifact_dir / f"{image_id}.clip"
    copied = False
    for pod in workers:
        proc = subprocess.run(
            [
                "kubectl",
                "-n",
                kube.config.namespace,
                "cp",
                "-c",
                "worker",
                f"{pod['name']}:/images/cache/{image_id}.clip",
                str(archive),
            ],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60,
        )
        if proc.returncode == 0 and archive.exists():
            copied = True
            break
    if not copied:
        return []
    go = shutil.which("go")
    if not go:
        return []
    env = dict(os.environ)
    bundled_goroot = "/opt/homebrew/Cellar/go/1.24.2/libexec"
    if Path(bundled_goroot).exists():
        env["GOROOT"] = bundled_goroot
    proc = subprocess.run(
        [go, "run", "./benchmarks/b9bench/cache_tools/clip_hashes.go", str(archive)],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=120,
        env=env,
    )
    if proc.returncode != 0:
        return []
    return list(json.loads(proc.stdout))


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
        first["clipDiskCachePresence"] = clip_disk_cache_presence(kube, first_hashes)

        if args.reset_workers_between:
            report["workerReset"] = reset_workers(kube, timeout=max(300, args.timeout_seconds / 2))
            if not report["workerReset"].get("ok"):
                validation_failures.append(f"worker reset failed: {report['workerReset']}")
            kube.wait_running_workers(min_count=1, timeout=max(180, args.timeout_seconds / 4))
            if args.clear_clip_disk_cache:
                report["postResetClipDiskCacheClear"] = clear_clip_decompressed_disk_cache(kube, report["decompressedHashes"])
                report["postResetClipDiskCachePresence"] = clip_disk_cache_presence(kube, report["decompressedHashes"])

        second_since = utc_now()
        second = run_iteration(kube, image, image_id, "after_worker_kill", args, artifact_dir, second_since)
        report["iterations"].append(second)
        second_hashes = second.get("logSummary", {}).get("decompressedHashes") or first_hashes or report["decompressedHashes"]
        second["embeddedCachePageProof"] = embedded_cache_page_proof(kube, second_hashes)
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
        failures.append("expected cold and after-worker-kill iterations")
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
        failures.append("after-worker-kill iteration could not find embedded cache pages")

    cold_logs = cold.get("logSummary") or {}
    after_logs = after.get("logSummary") or {}
    if not report.get("decompressedHashes"):
        failures.append("could not extract OCI decompressed layer hashes from clip archive")
    if cold.get("clipDiskCachePresence", {}).get("present"):
        failures.append("cold iteration left CLIP local decompressed disk cache present")
    if after.get("clipDiskCachePresence", {}).get("present"):
        failures.append("after-worker-kill iteration used/materialized CLIP local decompressed disk cache")
    if after_logs.get("ociCacheMisses", 0) > 0 or after_logs.get("layerDecompressed", 0) > 0:
        failures.append("after-worker-kill iteration fell back to OCI registry/decompression")
    cold_worker = ((cold.get("worker") or {}).get("name") or "")
    after_worker = ((after.get("worker") or {}).get("name") or "")
    if cold_worker and after_worker and cold_worker == after_worker:
        failures.append(f"worker did not change across reset: {cold_worker}")


if __name__ == "__main__":
    raise SystemExit(main())
