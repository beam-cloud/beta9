#!/usr/bin/env python3
import argparse
import atexit
import hashlib
import json
import os
import random
import re
import shlex
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from sandbox_parallel import (
    cleanup_sandbox,
    create_sandbox_with_retries,
    import_sdk,
    parse_sdk_cpu,
    parse_sdk_memory,
    wait_running,
    write_sdk_config,
)
from startup import (
    api_url,
    authorize,
    cluster_snapshot,
    delete_workers,
    env_bool,
    env_float,
    env_int,
    find_redis_pod,
    format_ms,
    gateway_host_is_local,
    grpc_addr,
    http_json,
    install_overlay,
    load_cached_token,
    log,
    redis_cli,
    require_tools,
    run,
    save_cached_token,
    start_grpc_port_forward_if_needed,
    start_http_port_forward_if_needed,
    wait_http,
)


HEALTH_PATH = "/api/v1/health"
DEFAULT_CACHE_MOUNT_PATH = "/var/lib/beta9/cache"
DEFAULT_IMAGE_CACHE_PATH = "/images/cache"
IMAGE_READ_ROOT = "/bench-cache"
VOLUME_CONTAINER_PREFIX = "/volumes"
DETERMINISTIC_BLOCK_SIZE = 4096

READ_HARNESS = r"""
import argparse
import hashlib
import json
import random
import time
from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024
DETERMINISTIC_BLOCK_SIZE = 4096


def deterministic_range(nonce, label, offset, length):
    out = bytearray()
    position = offset
    while len(out) < length:
        block_index = position // DETERMINISTIC_BLOCK_SIZE
        seed = hashlib.sha256(f"{nonce}:{label}:{block_index}".encode()).digest()
        block = (seed * ((DETERMINISTIC_BLOCK_SIZE // len(seed)) + 1))[:DETERMINISTIC_BLOCK_SIZE]
        block_offset = position % DETERMINISTIC_BLOCK_SIZE
        take = min(length - len(out), len(block) - block_offset)
        out.extend(block[block_offset:block_offset + take])
        position += take
    return bytes(out)


def manifest_entry(root, rel_path):
    manifest = json.loads((root / "manifest.json").read_text())
    for entry in manifest["files"]:
        if entry["path"] == rel_path:
            return manifest, entry
    raise SystemExit(f"file {rel_path!r} not found in manifest")


def sequential(path, expected):
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    digest = h.hexdigest()
    if size != expected["size"] or digest != expected["sha256"]:
        raise SystemExit(f"sequential mismatch size={size} sha256={digest}")
    return size, 1, digest


def random_reads(path, manifest, expected, block_size, total_bytes, seed):
    total_bytes = total_bytes or expected["size"]
    rng = random.Random(seed)
    aggregate = hashlib.sha256()
    bytes_read = 0
    ops = 0

    with path.open("rb") as f:
        while bytes_read < total_bytes:
            length = min(block_size, total_bytes - bytes_read, expected["size"])
            max_offset = expected["size"] - length
            if max_offset <= 0:
                offset = 0
            else:
                max_block = max_offset // block_size
                offset = rng.randint(0, max_block) * block_size

            f.seek(offset)
            chunk = f.read(length)
            if len(chunk) != length:
                raise SystemExit(f"short random read offset={offset} expected={length} got={len(chunk)}")
            expected_chunk = deterministic_range(manifest["nonce"], expected["label"], offset, length)
            if chunk != expected_chunk:
                got = hashlib.sha256(chunk).hexdigest()
                want = hashlib.sha256(expected_chunk).hexdigest()
                raise SystemExit(f"random chunk mismatch offset={offset} got={got} want={want}")

            aggregate.update(str(offset).encode())
            aggregate.update(b"\0")
            aggregate.update(chunk)
            bytes_read += length
            ops += 1

    return bytes_read, ops, aggregate.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--pattern", choices=("sequential", "random"), required=True)
    parser.add_argument("--random-block-bytes", type=int, default=4096)
    parser.add_argument("--random-total-bytes", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1337)
    args = parser.parse_args()

    root = Path(args.root)
    manifest, entry = manifest_entry(root, args.file)
    path = root / args.file
    started = time.monotonic_ns()
    if args.pattern == "sequential":
        bytes_read, ops, digest = sequential(path, entry)
    else:
        bytes_read, ops, digest = random_reads(
            path,
            manifest,
            entry,
            args.random_block_bytes,
            args.random_total_bytes,
            args.seed,
        )
    duration_ms = (time.monotonic_ns() - started) / 1_000_000
    mb = bytes_read / (1024 * 1024)
    print(json.dumps({
        "ok": True,
        "path": args.file,
        "pattern": args.pattern,
        "fileSizeBytes": entry["size"],
        "bytes": bytes_read,
        "operations": ops,
        "durationMs": duration_ms,
        "mbps": mb / (duration_ms / 1000) if duration_ms > 0 else 0,
        "opsPerSecond": ops / (duration_ms / 1000) if duration_ms > 0 else 0,
        "digest": digest,
        "expectedSha256": entry["sha256"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
"""

PAYLOAD_GENERATOR = r"""
import hashlib
import json
import os
from pathlib import Path

ROOT = Path("/bench-cache")
SIZES_MB = [int(part) for part in os.environ["CACHE_BENCH_SIZES_MB"].split(",") if part]
ACCESS_TYPES = [part for part in os.environ["CACHE_BENCH_ACCESS_TYPES"].split(",") if part]
PATTERNS = [part for part in os.environ["CACHE_BENCH_PATTERNS"].split(",") if part]
NONCE = os.environ["CACHE_BENCH_NONCE"]
CHUNK_SIZE = 1024 * 1024
DETERMINISTIC_BLOCK_SIZE = 4096


def deterministic_chunk(label, offset, length):
    out = bytearray()
    position = offset
    while len(out) < length:
        block_index = position // DETERMINISTIC_BLOCK_SIZE
        seed = hashlib.sha256(f"{NONCE}:{label}:{block_index}".encode()).digest()
        block = (seed * ((DETERMINISTIC_BLOCK_SIZE // len(seed)) + 1))[:DETERMINISTIC_BLOCK_SIZE]
        block_offset = position % DETERMINISTIC_BLOCK_SIZE
        take = min(length - len(out), len(block) - block_offset)
        out.extend(block[block_offset:block_offset + take])
        position += take
    return bytes(out)


def write_file(path, label, size):
    path.parent.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256()
    remaining = size
    offset = 0
    with path.open("wb") as f:
        while remaining > 0:
            chunk_len = min(CHUNK_SIZE, remaining)
            chunk = deterministic_chunk(label, offset, chunk_len)
            f.write(chunk)
            h.update(chunk)
            remaining -= chunk_len
            offset += chunk_len
    return h.hexdigest()


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    files = []
    for access_type in ACCESS_TYPES:
        for pattern in PATTERNS:
            for size_mb in SIZES_MB:
                size = size_mb * 1024 * 1024
                label = f"{access_type}:{pattern}:size:{size_mb}mb"
                rel = f"files/{access_type}/{pattern}/{size_mb}mb.bin"
                digest = write_file(ROOT / rel, label, size)
                files.append({
                    "path": rel,
                    "accessType": access_type,
                    "pattern": pattern,
                    "size": size,
                    "sizeMiB": size_mb,
                    "sha256": digest,
                    "label": label,
                })

    manifest = {
        "version": 3,
        "nonce": NONCE,
        "files": files,
    }
    (ROOT / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
"""

VOLUME_PREPARE_SCRIPT = r"""
import argparse
import hashlib
import json
import os
import shutil
import time
from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024


def sha256_file(path):
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return size, h.hexdigest()


def verify(root, access_types=None):
    manifest = json.loads((root / "manifest.json").read_text())
    total = 0
    files = 0
    for entry in manifest["files"]:
        if access_types and entry.get("accessType") not in access_types:
            continue
        size, digest = sha256_file(root / entry["path"])
        if size != entry["size"] or digest != entry["sha256"]:
            raise SystemExit(f"mismatch path={entry['path']} size={size} sha256={digest}")
        total += size
        files += 1
    return manifest, total, files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="/bench-cache")
    parser.add_argument("--dest", required=True)
    parser.add_argument("--access-types", default="")
    args = parser.parse_args()

    source = Path(args.source)
    dest = Path(args.dest)
    access_types = {part for part in args.access_types.split(",") if part}
    started = time.monotonic_ns()
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((source / "manifest.json").read_text())
    (dest / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
    for entry in manifest["files"]:
        if access_types and entry.get("accessType") not in access_types:
            continue
        source_path = source / entry["path"]
        dest_path = dest / entry["path"]
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest_path)
    os.sync()
    manifest, total, files = verify(dest, access_types)
    duration_ms = (time.monotonic_ns() - started) / 1_000_000
    mb = total / (1024 * 1024)
    print(json.dumps({
        "ok": True,
        "root": str(dest),
        "bytes": total,
        "files": files,
        "manifest": manifest,
        "durationMs": duration_ms,
        "mbps": mb / (duration_ms / 1000) if duration_ms > 0 else 0,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
"""

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def now_rfc3339():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_csv(value):
    return [part.strip() for part in value.split(",") if part.strip()]


def parse_sizes(value):
    sizes = [int(part) for part in parse_csv(value)]
    if not sizes or any(size <= 0 for size in sizes):
        raise SystemExit("--sizes-mb must contain positive integers")
    return sizes


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark beta9 embedded cache access patterns.")
    parser.add_argument("--namespace", default=os.getenv("BENCH_NAMESPACE", "beta9"))
    parser.add_argument("--gateway-url", default=os.getenv("BENCH_GATEWAY_URL", "http://127.0.0.1:11994"))
    parser.add_argument("--grpc-addr", default=os.getenv("BENCH_GRPC_ADDR", "127.0.0.1:11993"))
    parser.add_argument("--token", default=os.getenv("BENCH_TOKEN") or os.getenv("BETA9_TOKEN") or "")
    parser.add_argument("--token-cache", default=os.getenv("BENCH_TOKEN_CACHE", ""))
    parser.add_argument("--timeout-seconds", type=float, default=env_float("BENCH_TIMEOUT_SECONDS", 300))
    parser.add_argument("--poll-interval-ms", type=int, default=env_int("BENCH_POLL_INTERVAL_MS", 100))
    parser.add_argument("--cleanup-ttl-seconds", type=int, default=env_int("BENCH_CLEANUP_TTL_SECONDS", 5))
    parser.add_argument("--sdk-config", default=os.getenv("BENCH_SDK_CONFIG", ""))

    parser.add_argument("--preset", choices=("default", "smoke"), default="default")
    parser.add_argument("--sizes-mb", default=os.getenv("BENCH_CACHE_SIZES_MB", "32,128"))
    parser.add_argument("--patterns", default=os.getenv("BENCH_CACHE_PATTERNS", "sequential,random"))
    parser.add_argument("--access-types", default=os.getenv("BENCH_CACHE_ACCESS_TYPES", "image_archive,volume_mount,workspace_fuse"))
    parser.add_argument("--iterations", type=int, default=env_int("BENCH_CACHE_ITERATIONS", 1))
    parser.add_argument("--random-block-bytes", type=int, default=env_int("BENCH_CACHE_RANDOM_BLOCK_BYTES", 4096))
    parser.add_argument("--random-seed", type=int, default=env_int("BENCH_CACHE_RANDOM_SEED", 1337))

    parser.add_argument("--image-uri", default=os.getenv("BENCH_CACHE_IMAGE_URI", ""))
    parser.add_argument("--image-platform", default=os.getenv("BENCH_CACHE_IMAGE_PLATFORM", ""))
    parser.add_argument("--source-image", default=os.getenv("BENCH_CACHE_SOURCE_IMAGE", "python:3.10-slim"))
    parser.add_argument("--output", default=os.getenv("BENCH_CACHE_OUTPUT", "/tmp/beta9-cache-benchmark.json"))
    parser.add_argument("--report", default=os.getenv("BENCH_CACHE_REPORT", "/tmp/beta9-cache-benchmark.md"))
    parser.add_argument("--cache-mount-path", default=os.getenv("BENCH_CACHE_MOUNT_PATH", DEFAULT_CACHE_MOUNT_PATH))
    parser.add_argument("--image-cache-path", default=os.getenv("BENCH_CACHE_IMAGE_CACHE_PATH", DEFAULT_IMAGE_CACHE_PATH))
    parser.add_argument("--cache-locality", default=os.getenv("BENCH_CACHE_LOCALITY", "default"))
    parser.add_argument("--localstack-service", default=os.getenv("BENCH_CACHE_LOCALSTACK_SERVICE", "localstack"))
    parser.add_argument("--localstack-local-port", type=int, default=env_int("BENCH_CACHE_LOCALSTACK_LOCAL_PORT", 4566))
    parser.add_argument("--localstack-service-port", type=int, default=env_int("BENCH_CACHE_LOCALSTACK_SERVICE_PORT", 4566))
    parser.add_argument("--volume-name", default=os.getenv("BENCH_CACHE_VOLUME_NAME", "embedded-cache-benchmark"))
    parser.add_argument("--volume-mount-path", default=os.getenv("BENCH_CACHE_VOLUME_MOUNT_PATH", "cache-bench"))
    parser.add_argument("--volume-subdir", default=os.getenv("BENCH_CACHE_VOLUME_SUBDIR", ""))
    parser.add_argument("--settle-seconds", type=float, default=env_float("BENCH_CACHE_SETTLE_SECONDS", 5))
    parser.add_argument("--cache-proof-timeout-seconds", type=float, default=env_float("BENCH_CACHE_PROOF_TIMEOUT_SECONDS", 30))
    parser.add_argument("--log-tail", type=int, default=env_int("BENCH_CACHE_LOG_TAIL", 30000))
    parser.add_argument("--sandbox-cpu", default=os.getenv("BENCH_CACHE_SANDBOX_CPU", os.getenv("BENCH_SANDBOX_CPU", "0.5")))
    parser.add_argument("--sandbox-memory", default=os.getenv("BENCH_CACHE_SANDBOX_MEMORY", os.getenv("BENCH_SANDBOX_MEMORY", "512")))
    parser.add_argument("--sandbox-keep-warm-seconds", type=int, default=env_int("BENCH_CACHE_KEEP_WARM_SECONDS", 1800))
    parser.add_argument("--sandbox-create-retries", type=int, default=env_int("BENCH_CACHE_CREATE_RETRIES", 20))
    parser.add_argument("--sandbox-ready-timeout-seconds", type=float, default=env_float("BENCH_CACHE_READY_TIMEOUT_SECONDS", 600))
    parser.add_argument("--sandbox-ready-retry-ms", type=int, default=env_int("BENCH_CACHE_READY_RETRY_MS", 500))
    parser.add_argument("--exec-timeout-seconds", type=float, default=env_float("BENCH_CACHE_EXEC_TIMEOUT_SECONDS", 900))

    parser.add_argument("--install", dest="install", action="store_true")
    parser.add_argument("--no-install", dest="install", action="store_false")
    parser.set_defaults(install=env_bool("BENCH_INSTALL", True))
    parser.add_argument("--port-forward", dest="port_forward", action="store_true")
    parser.add_argument("--no-port-forward", dest="port_forward", action="store_false")
    parser.set_defaults(port_forward=env_bool("BENCH_PORT_FORWARD", True))
    parser.add_argument("--localstack-port-forward", dest="localstack_port_forward", action="store_true")
    parser.add_argument("--no-localstack-port-forward", dest="localstack_port_forward", action="store_false")
    parser.set_defaults(localstack_port_forward=env_bool("BENCH_CACHE_LOCALSTACK_PORT_FORWARD", True))
    parser.add_argument("--reset-workers", dest="reset_workers", action="store_true")
    parser.add_argument("--no-reset-workers", dest="reset_workers", action="store_false")
    parser.set_defaults(reset_workers=env_bool("BENCH_RESET_WORKERS", False))
    parser.add_argument("--wait-running", dest="wait_running", action="store_true")
    parser.add_argument("--no-wait-running", dest="wait_running", action="store_false")
    parser.set_defaults(wait_running=env_bool("BENCH_CACHE_WAIT_RUNNING", True))
    parser.add_argument("--require-workspace-storage", dest="require_workspace_storage", action="store_true")
    parser.add_argument("--no-require-workspace-storage", dest="require_workspace_storage", action="store_false")
    parser.set_defaults(require_workspace_storage=env_bool("BENCH_CACHE_REQUIRE_WORKSPACE_STORAGE", True))

    args = parser.parse_args()
    if args.preset == "smoke" and args.sizes_mb == "32,128":
        args.sizes_mb = "32"
    args.sizes = parse_sizes(args.sizes_mb)
    args.pattern_list = parse_csv(args.patterns)
    args.access_type_list = parse_csv(args.access_types)
    valid_patterns = {"sequential", "random"}
    valid_access_types = {"image_archive", "volume_mount", "workspace_fuse"}
    if unknown := sorted(set(args.pattern_list) - valid_patterns):
        raise SystemExit(f"unknown pattern(s): {', '.join(unknown)}")
    if unknown := sorted(set(args.access_type_list) - valid_access_types):
        raise SystemExit(f"unknown access type(s): {', '.join(unknown)}")
    if args.iterations <= 0:
        raise SystemExit("--iterations must be greater than 0")
    if args.random_block_bytes <= 0:
        raise SystemExit("--random-block-bytes must be greater than 0")
    if not args.token_cache:
        args.token_cache = f"/tmp/beta9-startup-benchmark-{args.namespace}.token"
    if not args.sdk_config:
        args.sdk_config = f"/tmp/beta9-cache-benchmark-{args.namespace}.ini"
    if not args.image_uri:
        args.image_uri = f"k3d-registry.localhost:5000/beta9-cache-bench:{int(time.time())}"
    if not args.volume_subdir:
        args.volume_subdir = f"run-{int(time.time())}-{random.getrandbits(32):08x}"
    args.volume_mount_path = args.volume_mount_path.strip("/")
    return args


def start_localstack_port_forward_if_needed(args):
    if not args.localstack_port_forward or not gateway_host_is_local(args.gateway_url):
        return None
    if tcp_reachable("127.0.0.1", args.localstack_local_port):
        log(f"LocalStack is already reachable at 127.0.0.1:{args.localstack_local_port}")
        return None

    proc = subprocess.Popen(
        [
            "kubectl",
            "-n",
            args.namespace,
            "port-forward",
            f"svc/{args.localstack_service}",
            f"{args.localstack_local_port}:{args.localstack_service_port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def stop_port_forward():
        if proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    atexit.register(stop_port_forward)
    deadline = time.monotonic() + args.timeout_seconds
    while time.monotonic() < deadline:
        if tcp_reachable("127.0.0.1", args.localstack_local_port):
            log(f"Started LocalStack port-forward on localhost:{args.localstack_local_port}")
            return proc
        if proc.poll() is not None:
            output = proc.stdout.read() if proc.stdout is not None else ""
            raise RuntimeError(f"LocalStack port-forward exited before becoming reachable. {output}")
        time.sleep(0.1)
    stop_port_forward()
    raise RuntimeError("LocalStack port-forward did not become reachable")


def tcp_reachable(host, port):
    import socket

    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except OSError:
        return False


def host_image_uri(image_uri):
    registry_prefixes = ("registry.localhost:5000/", "k3d-registry.localhost:5000/")
    if image_uri.startswith(registry_prefixes):
        return "localhost:5001/" + image_uri.split("/", 1)[1]
    return image_uri


def deterministic_payload_range(nonce, label, offset, length):
    out = bytearray()
    position = offset
    while len(out) < length:
        block_index = position // DETERMINISTIC_BLOCK_SIZE
        seed = hashlib.sha256(f"{nonce}:{label}:{block_index}".encode()).digest()
        block = (seed * ((DETERMINISTIC_BLOCK_SIZE // len(seed)) + 1))[:DETERMINISTIC_BLOCK_SIZE]
        block_offset = position % DETERMINISTIC_BLOCK_SIZE
        take = min(length - len(out), len(block) - block_offset)
        out.extend(block[block_offset:block_offset + take])
        position += take
    return bytes(out)


def payload_sha256(nonce, label, size):
    hasher = hashlib.sha256()
    offset = 0
    chunk_size = 1024 * 1024
    while offset < size:
        length = min(chunk_size, size - offset)
        hasher.update(deterministic_payload_range(nonce, label, offset, length))
        offset += length
    return hasher.hexdigest()


def expected_manifest(nonce, sizes, access_types, patterns):
    files = []
    for access_type in access_types:
        for pattern in patterns:
            for size_mb in sizes:
                size = size_mb * 1024 * 1024
                label = f"{access_type}:{pattern}:size:{size_mb}mb"
                files.append(
                    {
                        "path": f"files/{access_type}/{pattern}/{size_mb}mb.bin",
                        "accessType": access_type,
                        "pattern": pattern,
                        "size": size,
                        "sizeMiB": size_mb,
                        "sha256": payload_sha256(nonce, label, size),
                        "label": label,
                    }
                )
    return {"version": 3, "nonce": nonce, "files": files}


def build_payload_image(args):
    require_tools("docker")
    build_uri = host_image_uri(args.image_uri)
    nonce = f"{datetime.now(timezone.utc).isoformat()}:{random.getrandbits(64)}"
    manifest = expected_manifest(nonce, args.sizes, args.access_type_list, args.pattern_list)
    with tempfile.TemporaryDirectory(prefix="beta9-cache-bench-image-") as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "generate_payload.py").write_text(PAYLOAD_GENERATOR, encoding="utf-8")
        (tmp_path / "Dockerfile").write_text(
            "\n".join(
                [
                    "ARG BASE_IMAGE",
                    "FROM ${BASE_IMAGE}",
                    "ARG SIZES_MB",
                    "ARG ACCESS_TYPES",
                    "ARG PATTERNS",
                    "ARG BENCH_NONCE",
                    "ENV CACHE_BENCH_SIZES_MB=${SIZES_MB}",
                    "ENV CACHE_BENCH_ACCESS_TYPES=${ACCESS_TYPES}",
                    "ENV CACHE_BENCH_PATTERNS=${PATTERNS}",
                    "ENV CACHE_BENCH_NONCE=${BENCH_NONCE}",
                    "COPY generate_payload.py /tmp/generate_payload.py",
                    "RUN python3 /tmp/generate_payload.py && rm -f /tmp/generate_payload.py",
                    "WORKDIR /",
                    'CMD ["python3", "-c", "import time; time.sleep(3600)"]',
                    "",
                ]
            ),
            encoding="utf-8",
        )
        log(f"Building cache benchmark image {build_uri} with sizes {args.sizes_mb} MiB")
        build_cmd = ["docker"]
        use_buildx = "," in args.image_platform
        if use_buildx:
            build_cmd.extend(["buildx", "build", "--push"])
        else:
            build_cmd.append("build")
        build_cmd.extend(
            [
                "-t",
                build_uri,
                "--build-arg",
                f"BASE_IMAGE={args.source_image}",
                "--build-arg",
                f"SIZES_MB={','.join(str(size) for size in args.sizes)}",
                "--build-arg",
                f"ACCESS_TYPES={','.join(args.access_type_list)}",
                "--build-arg",
                f"PATTERNS={','.join(args.pattern_list)}",
                "--build-arg",
                f"BENCH_NONCE={nonce}",
            ]
        )
        if args.image_platform:
            build_cmd.extend(["--platform", args.image_platform])
        build_cmd.append(str(tmp_path))
        run(build_cmd, timeout=max(600, args.timeout_seconds))
        if not use_buildx:
            log(f"Pushing cache benchmark image {build_uri}")
            run(["docker", "push", build_uri], timeout=max(600, args.timeout_seconds))
    return {"imageUri": args.image_uri, "hostImageUri": build_uri, "nonce": nonce, "manifest": manifest}


def authorize_benchmark(args):
    try:
        return authorize(args.gateway_url, args.token)
    except RuntimeError as exc:
        if not args.token or "Invalid token" not in str(exc):
            raise
        log("Cached benchmark token is invalid; retrying local authorization without it")
        if args.token_cache:
            Path(args.token_cache).unlink(missing_ok=True)
        args.token = ""
        return authorize(args.gateway_url, "")


def verify_workspace_storage(args, token):
    status, body, raw, _ = http_json(
        "GET",
        api_url(args.gateway_url, "/api/v1/workspace/current"),
        token=token,
        timeout=10,
    )
    if status >= 400:
        raise RuntimeError(f"workspace storage check failed with HTTP {status}: {raw[:300]}")
    storage_id = body.get("storage_id") or body.get("storageId")
    storage = body.get("storage") or {}
    storage_available = bool(storage_id) or bool(storage.get("id"))
    if not storage_available and args.require_workspace_storage:
        raise RuntimeError("workspace storage is required for cache benchmark")
    return body


def volume_container_root(args):
    return str(Path(VOLUME_CONTAINER_PREFIX) / args.volume_mount_path / args.volume_subdir)


def workspace_name(workspace):
    for key in ("name", "workspace_name", "workspaceName"):
        value = workspace.get(key)
        if value:
            return value
    raise RuntimeError(f"workspace response does not include a filesystem name: {sorted(workspace.keys())}")


def volume_external_id(volume):
    volume_id = getattr(volume, "volume_id", "") or getattr(volume, "id", "")
    if not volume_id:
        raise RuntimeError("volume external id was not populated by SDK runtime preparation")
    return volume_id


def workspace_fuse_root(args, workspace, volume_id):
    return str(Path("/workspace/data") / workspace_name(workspace) / "volumes" / volume_id / args.volume_subdir)


def prepare_sandbox_runtime(args):
    Image, Sandbox, sandbox_stub_type = import_sdk()
    from beta9 import Volume  # noqa: WPS433
    from beta9.sync import FileSyncer  # noqa: WPS433

    image = Image.from_registry(args.image_uri)
    volume = Volume(name=args.volume_name, mount_path=args.volume_mount_path)
    sandbox = Sandbox(
        name="cache-benchmark",
        image=image,
        cpu=parse_sdk_cpu(args.sandbox_cpu),
        memory=parse_sdk_memory(args.sandbox_memory),
        keep_warm_seconds=args.sandbox_keep_warm_seconds,
        authorized=False,
        volumes=[volume],
        sync_local_dir=False,
    )

    sync_root = Path(tempfile.mkdtemp(prefix="beta9-cache-benchmark-sync-"))
    (sync_root / "cache-benchmark.json").write_text(
        json.dumps({"createdAt": now_rfc3339(), "imageUri": args.image_uri}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    sandbox.syncer = FileSyncer(sandbox.gateway_stub, root_dir=str(sync_root))
    sandbox.sync_local_dir = True
    sandbox.entrypoint = ["tail", "-f", "/dev/null"]
    sandbox._benchmark_sync_root = str(sync_root)

    started = time.monotonic_ns()
    ok = sandbox.prepare_runtime(
        stub_type=sandbox_stub_type,
        force_create_stub=True,
        ignore_patterns=[],
    )
    prepare_ms = (time.monotonic_ns() - started) / 1_000_000
    if not ok:
        raise RuntimeError("SDK Sandbox runtime preparation failed")
    return sandbox, volume, prepare_ms


def parse_json_output(stdout):
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise RuntimeError(f"command did not emit JSON: {stdout[-500:]}")


def wait_process_manager(args, instance, request_start_ns, sample):
    deadline = time.monotonic() + args.sandbox_ready_timeout_seconds
    attempts = 0
    errors = []
    while True:
        attempts += 1
        try:
            started = time.monotonic_ns()
            process = instance.process.exec(["true"], cwd="/")
            accepted = time.monotonic_ns()
            sample["processReadyMs"] = (accepted - request_start_ns) / 1_000_000
            sample["readinessExecAcceptedMs"] = (accepted - started) / 1_000_000
            sample["readinessExecAttempts"] = attempts
            sample["readinessExecErrors"] = errors
            process.wait(timeout=5)
            return
        except Exception as exc:
            errors.append(str(exc))
            if time.monotonic() >= deadline:
                raise RuntimeError(f"process manager did not become ready; last error: {exc}")
            time.sleep(args.sandbox_ready_retry_ms / 1000)


def run_instance_command(args, instance, command, request_start_ns, sample):
    wait_process_manager(args, instance, request_start_ns, sample)
    started = time.monotonic_ns()
    process = instance.process.exec(command, cwd="/")
    accepted = time.monotonic_ns()
    sample["execAcceptedMs"] = (accepted - started) / 1_000_000
    sample["execPid"] = process.pid
    exit_code = process.wait(timeout=args.exec_timeout_seconds)
    done = time.monotonic_ns()
    sample["execCompleteMs"] = (done - request_start_ns) / 1_000_000
    sample["execExitCode"] = exit_code
    sample["stdout"] = process.stdout.read()
    sample["stderr"] = process.stderr.read()
    if exit_code != 0:
        raise RuntimeError(f"command exited {exit_code}: {sample['stderr'] or sample['stdout']}")
    return parse_json_output(sample["stdout"])


def create_sample(args, sandbox, token, sample):
    instance, create_info = create_sandbox_with_retries(args, sandbox)
    sample.update(create_info)
    sample["containerId"] = instance.container_id
    sample["stubId"] = instance.stub_id
    if not instance.ok:
        raise RuntimeError(instance.error_msg or "Sandbox.create returned ok=false")
    running = wait_running(args, token, instance.container_id, sample["_requestStartNs"])
    if running:
        sample["runningObservedMs"] = running["observedMs"]
        sample["runningAttempts"] = running["attempts"]
        sample["containerState"] = running["state"]
    return instance


def read_command(args, root, entry, pattern, seed):
    total_bytes = entry["size"] if pattern == "random" else 0
    return [
        "python3",
        "-c",
        READ_HARNESS,
        "--root",
        root,
        "--file",
        entry["path"],
        "--pattern",
        pattern,
        "--random-block-bytes",
        str(args.random_block_bytes),
        "--random-total-bytes",
        str(total_bytes),
        "--seed",
        str(seed),
    ]


def run_sandbox_read(args, sandbox, token, access_type, root, entry, pattern, cache_state, index):
    instance = None
    started = time.monotonic_ns()
    sample = {
        "_requestStartNs": started,
        "index": index,
        "accessType": access_type,
        "pattern": pattern,
        "cacheState": cache_state,
        "filePath": entry["path"],
        "fileSizeBytes": entry["size"],
        "startedAt": now_rfc3339(),
        "status": "running",
    }
    try:
        instance = create_sample(args, sandbox, token, sample)
        accepted = time.monotonic_ns()
        sample["acceptedMs"] = (accepted - started) / 1_000_000
        payload = run_instance_command(
            args,
            instance,
            read_command(args, root, entry, pattern, args.random_seed + index),
            started,
            sample,
        )
        sample.update(read_result_fields(payload))
        sample["ok"] = True
        sample["status"] = "ok"
        sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
    except Exception as exc:
        sample["ok"] = False
        sample["status"] = "failed"
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        sample.pop("_requestStartNs", None)
        sample["finishedAt"] = now_rfc3339()
    return sample


def run_volume_prepare(args, sandbox, token, index):
    instance = None
    started = time.monotonic_ns()
    sample = {
        "_requestStartNs": started,
        "index": index,
        "accessType": "volume_prepare",
        "startedAt": now_rfc3339(),
        "status": "running",
    }
    try:
        instance = create_sample(args, sandbox, token, sample)
        sample["acceptedMs"] = (time.monotonic_ns() - started) / 1_000_000
        payload = run_instance_command(
            args,
            instance,
            [
                "python3",
                "-c",
                VOLUME_PREPARE_SCRIPT,
                "--source",
                IMAGE_READ_ROOT,
                "--dest",
                volume_container_root(args),
                "--access-types",
                ",".join(access_type for access_type in args.access_type_list if access_type != "image_archive"),
            ],
            started,
            sample,
        )
        sample.update(read_result_fields(payload))
        sample["manifest"] = payload["manifest"]
        sample["ok"] = True
        sample["status"] = "ok"
        sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
    except Exception as exc:
        sample["ok"] = False
        sample["status"] = "failed"
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        sample.pop("_requestStartNs", None)
        sample["finishedAt"] = now_rfc3339()
    return sample


def read_result_fields(payload):
    return {
        "read": payload,
        "readDurationMs": payload.get("durationMs"),
        "bytesRead": payload.get("bytes"),
        "operations": payload.get("operations") or payload.get("files"),
        "mbps": payload.get("mbps"),
        "opsPerSecond": payload.get("opsPerSecond"),
        "digest": payload.get("digest"),
        "expectedSha256": payload.get("expectedSha256"),
    }


def worker_pods(namespace):
    proc = run(
        [
            "kubectl",
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            "run.beam.cloud/role=worker",
            "-o",
            "json",
        ],
        check=False,
        timeout=15,
    )
    if proc.returncode != 0:
        return []
    data = json.loads(proc.stdout)
    pods = []
    for item in data.get("items", []):
        pods.append(
            {
                "name": item.get("metadata", {}).get("name", ""),
                "nodeName": item.get("spec", {}).get("nodeName", ""),
                "podIP": item.get("status", {}).get("podIP", ""),
                "hostIP": item.get("status", {}).get("hostIP", ""),
            }
        )
    return pods


def worker_exec(namespace, pod, command, timeout=60):
    return run(
        ["kubectl", "-n", namespace, "exec", pod, "-c", "worker", "--", *command],
        check=False,
        timeout=timeout,
    )


def find_worker_for_container(args, container_id, root):
    pods = worker_pods(args.namespace)
    for pod in pods:
        proc = run(
            [
                "kubectl",
                "-n",
                args.namespace,
                "logs",
                pod["name"],
                "-c",
                "worker",
                "--since=15m",
                "--tail=3000",
            ],
            check=False,
            timeout=10,
        )
        if proc.returncode == 0 and container_id in proc.stdout:
            return pod

    quoted_root = shlex.quote(root)
    for pod in pods:
        proc = worker_exec(args.namespace, pod["name"], ["sh", "-lc", f"test -f {quoted_root}/manifest.json"])
        if proc.returncode == 0:
            return pod
    raise RuntimeError(f"could not find worker pod for container {container_id}")


def run_workspace_fuse_read(args, sandbox, token, workspace_root, entry, pattern, cache_state, index):
    instance = None
    started = time.monotonic_ns()
    sample = {
        "_requestStartNs": started,
        "index": index,
        "accessType": "workspace_fuse",
        "pattern": pattern,
        "cacheState": cache_state,
        "filePath": entry["path"],
        "fileSizeBytes": entry["size"],
        "workspaceFuseRoot": workspace_root,
        "startedAt": now_rfc3339(),
        "status": "running",
    }
    try:
        instance = create_sample(args, sandbox, token, sample)
        sample["acceptedMs"] = (time.monotonic_ns() - started) / 1_000_000
        pod = find_worker_for_container(args, instance.container_id, workspace_root)
        sample["workerPod"] = pod["name"]
        sample["workerNode"] = pod.get("nodeName", "")
        command = read_command(args, workspace_root, entry, pattern, args.random_seed + index)
        exec_started = time.monotonic_ns()
        proc = worker_exec(args.namespace, pod["name"], command, timeout=int(args.exec_timeout_seconds) + 30)
        sample["execAcceptedMs"] = (time.monotonic_ns() - exec_started) / 1_000_000
        sample["stdout"] = proc.stdout
        sample["stderr"] = proc.stderr
        sample["execExitCode"] = proc.returncode
        sample["execCompleteMs"] = (time.monotonic_ns() - started) / 1_000_000
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
        payload = parse_json_output(proc.stdout)
        sample.update(read_result_fields(payload))
        sample["ok"] = True
        sample["status"] = "ok"
        sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
    except Exception as exc:
        sample["ok"] = False
        sample["status"] = "failed"
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        sample.pop("_requestStartNs", None)
        sample["finishedAt"] = now_rfc3339()
    return sample


def print_sample(sample):
    status = "ok" if sample.get("ok") else "failed"
    print(
        f"{sample.get('index', 0):>4} "
        f"{sample.get('accessType', '-'):<15} "
        f"{sample.get('pattern', '-'):<10} "
        f"{sample.get('cacheState', '-'):<5} "
        f"{sample.get('fileSizeBytes', 0) // (1024 * 1024):>5}MiB "
        f"{format_ms(sample.get('readDurationMs')):>9}ms "
        f"{(sample.get('mbps') or 0):>9.2f} MB/s "
        f"{status:>7}",
        flush=True,
    )
    if not sample.get("ok"):
        print(f"       error: {sample.get('error')}", flush=True)


def summarize_samples(samples):
    ok = [sample for sample in samples if sample.get("ok")]
    if not ok:
        return {"ok": False, "count": len(samples), "okCount": 0, "error": samples[0].get("error") if samples else ""}
    durations = [sample["readDurationMs"] for sample in ok]
    mbps = [sample["mbps"] for sample in ok]
    ops = [sample.get("opsPerSecond") or 0 for sample in ok]
    return {
        "ok": len(ok) == len(samples),
        "count": len(samples),
        "okCount": len(ok),
        "durationMs": percentile(durations, 50),
        "durationMsP95": percentile(durations, 95),
        "mbps": percentile(mbps, 50),
        "mbpsP95": percentile(mbps, 95),
        "bytesRead": ok[0].get("bytesRead"),
        "opsPerSecond": percentile(ops, 50),
        "digest": ok[0].get("digest"),
        "startupMs": ok[0].get("acceptedMs"),
        "readyMs": ok[0].get("runningObservedMs"),
        "processReadyMs": ok[0].get("processReadyMs"),
    }


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * (p / 100)
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    weight = rank - low
    return values[low] * (1 - weight) + values[high] * weight


def strip_ansi(value):
    return ANSI_RE.sub("", value)


def cache_operation_from_log_line(line):
    clean = strip_ansi(line)
    operation = None
    for marker in (
        "loaded image archive from embedded content cache",
        "loaded image archive from embedded cachefs",
        "StoreContent[OK]",
        "StoreContent[ACK]",
        "StoreFromContent[OK]",
        "StoreFromContent[ACK]",
        "StoreContentFromSourceWithLock",
        "Store[OK]",
        "Get[OK]",
        "GetContentStream[ACK]",
        "cache_triggered",
    ):
        if marker in clean:
            operation = marker
            break
    if operation is None:
        return None
    return {"operation": operation, "hashes": re.findall(r"\b[a-f0-9]{64}\b", clean), "line": clean[-700:]}


def cache_log_counters(namespace, since_time, log_tail):
    patterns = {
        "storeFromContentOk": "StoreFromContent[OK]",
        "storeFromContentAck": "StoreFromContent[ACK]",
        "storeFromContentErr": "StoreFromContent[ERR]",
        "storeContentAck": "StoreContent[ACK]",
        "storeContentOk": "StoreContent[OK]",
        "storeOk": "Store[OK]",
        "getContentOk": "Get[OK]",
        "getContentStreamAck": "GetContentStream[ACK]",
        "setStoreFromContentLockAck": "SetStoreFromContentLock[ACK]",
        "removeStoreFromContentLockAck": "RemoveStoreFromContentLock[ACK]",
        "refreshStoreFromContentLockAck": "RefreshStoreFromContentLock[ACK]",
        "geesefsCacheTriggered": "cache_triggered",
        "geesefsEventCallback": "geesefs: event callback fired",
        "hostNotFound": "host not found",
        "sourceNotFound": "source not found",
        "unableToPopulate": "unable to populate",
        "deadlineExceeded": "context deadline exceeded",
        "cacheNoHosts": "cache has no available hosts",
    }
    proc = run(
        [
            "kubectl",
            "-n",
            namespace,
            "logs",
            "-l",
            "run.beam.cloud/role=worker",
            "-c",
            "worker",
            "--since-time",
            since_time,
            "--tail",
            str(log_tail),
        ],
        check=False,
        timeout=30,
    )
    counters = {key: 0 for key in patterns}
    if proc.returncode != 0:
        return {"error": proc.stderr.strip() or proc.stdout.strip(), "counters": counters, "operations": []}
    operations = []
    for line in proc.stdout.splitlines():
        for key, pattern in patterns.items():
            if pattern in line:
                counters[key] += 1
        operation = cache_operation_from_log_line(line)
        if operation:
            operations.append(operation)
    return {
        "counters": counters,
        "operations": operations[-500:],
        "imageArchiveLoads": [
            op for op in operations if op["operation"].startswith("loaded image archive from embedded")
        ][-20:],
    }


def worker_shell(namespace, pod_name, script, timeout=20):
    return run(
        ["kubectl", "-n", namespace, "exec", pod_name, "-c", "worker", "--", "sh", "-c", script],
        check=False,
        timeout=timeout,
    )


def cache_object_candidate_dirs(cache_mount_path, locality, pod, content_hash):
    node_name = pod.get("nodeName") or pod.get("hostIP") or ""
    candidates = []
    if node_name:
        candidates.append(str(Path(cache_mount_path) / locality / node_name / content_hash))
    candidates.append(str(Path(cache_mount_path) / content_hash))
    return candidates


def delete_cache_object(namespace, cache_mount_path, locality, content_hash):
    if not content_hash or len(content_hash) != 64:
        return {"hash": content_hash, "deleted": False}
    result = {"hash": content_hash, "deleted": True, "errors": []}
    for pod in worker_pods(namespace):
        paths = " ".join(shlex.quote(path) for path in cache_object_candidate_dirs(cache_mount_path, locality, pod, content_hash))
        proc = worker_shell(namespace, pod["name"], f"rm -rf {paths}")
        if proc.returncode != 0:
            result["errors"].append({"pod": pod["name"], "error": proc.stderr.strip() or proc.stdout.strip()})
    return result


def cache_object_proof(namespace, cache_mount_path, locality, content_hash):
    if not content_hash:
        return {"hash": content_hash, "exists": False, "error": "empty hash"}
    for pod in worker_pods(namespace):
        dirs = " ".join(shlex.quote(path) for path in cache_object_candidate_dirs(cache_mount_path, locality, pod, content_hash))
        script = (
            f"hash={shlex.quote(content_hash)}; "
            "found=''; for candidate in "
            f"{dirs}; do [ -d \"$candidate\" ] && found=\"$candidate\" && break; done; "
            'if [ -z "$found" ]; then printf \'{"exists":false}\\n\'; exit 0; fi; '
            "i=0; bytes=0; tmp=$(mktemp); "
            'while [ -f "$found/$hash-$i" ]; do cat "$found/$hash-$i" >> "$tmp"; '
            'bytes=$((bytes + $(wc -c < "$found/$hash-$i"))); i=$((i + 1)); done; '
            'sha=$(sha256sum "$tmp" | awk \'{print $1}\'); rm -f "$tmp"; '
            'printf \'{"exists":true,"path":"%s","chunks":%s,"bytes":%s,"sha256":"%s"}\\n\' "$found" "$i" "$bytes" "$sha"'
        )
        proc = worker_shell(namespace, pod["name"], script, timeout=60)
        if proc.returncode != 0:
            continue
        try:
            proof = json.loads(proc.stdout.strip().splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            continue
        proof.update({"hash": content_hash, "pod": pod["name"], "nodeName": pod.get("nodeName", "")})
        if proof.get("exists"):
            proof["matchesHash"] = proof.get("sha256") == content_hash
            return proof
    return {"hash": content_hash, "exists": False}


def wait_cache_object_proof(args, content_hash):
    deadline = time.monotonic() + args.cache_proof_timeout_seconds
    last = {"hash": content_hash, "exists": False}
    while True:
        last = cache_object_proof(args.namespace, args.cache_mount_path, args.cache_locality, content_hash)
        if last.get("exists"):
            return last
        if time.monotonic() >= deadline:
            return last
        time.sleep(1)


def delete_image_archive_cache_files(namespace, image_cache_path, image_id):
    if not image_id:
        return {"deleted": [], "errors": []}
    names = [f"{image_id}.clip", f"{image_id}.rclip", f"{image_id}.clip.lock", f"{image_id}.rclip.lock"]
    quoted = " ".join(shlex.quote(name) for name in names)
    result = {"deleted": names, "errors": []}
    for pod in worker_pods(namespace):
        script = f"path={shlex.quote(image_cache_path)}; for name in {quoted}; do rm -f \"$path/$name\"; done"
        proc = worker_shell(namespace, pod["name"], script)
        if proc.returncode != 0:
            result["errors"].append({"pod": pod["name"], "error": proc.stderr.strip() or proc.stdout.strip()})
    return result


def cache_disk_stats(namespace, cache_mount_path):
    stats = {"path": cache_mount_path, "pods": [], "totalBytes": 0, "totalFiles": 0}
    counted_nodes = set()
    script = (
        f"path={shlex.quote(cache_mount_path)}; "
        'if [ -d "$path" ]; then bytes=$(du -sk "$path" 2>/dev/null | awk \'{print $1 * 1024}\'); '
        'files=$(find "$path" -type f 2>/dev/null | wc -l | tr -d " "); '
        'printf \'{"exists":true,"bytes":%s,"files":%s}\\n\' "${bytes:-0}" "${files:-0}"; '
        'else printf \'{"exists":false,"bytes":0,"files":0}\\n\'; fi'
    )
    for pod in worker_pods(namespace):
        proc = worker_shell(namespace, pod["name"], script)
        entry = {"pod": pod["name"], "nodeName": pod.get("nodeName", "")}
        if proc.returncode == 0:
            try:
                entry.update(json.loads(proc.stdout.strip().splitlines()[-1]))
            except (json.JSONDecodeError, IndexError):
                pass
        node_key = pod.get("nodeName") or pod["name"]
        entry["countedInTotal"] = node_key not in counted_nodes
        stats["pods"].append(entry)
        if entry["countedInTotal"]:
            counted_nodes.add(node_key)
            stats["totalBytes"] += int(entry.get("bytes") or 0)
            stats["totalFiles"] += int(entry.get("files") or 0)
    return stats


def cache_hosts_snapshot(namespace, redis_pod, locality):
    if not redis_pod:
        return {"available": False, "hosts": []}
    raw_keys = redis_cli(namespace, redis_pod, "KEYS", f"cache:host_index:{locality}")
    if raw_keys is None:
        return {"available": False, "hosts": []}
    hosts = []
    for key in [line for line in raw_keys.splitlines() if line.strip()]:
        raw_members = redis_cli(namespace, redis_pod, "SMEMBERS", key) or ""
        for host_id in [line for line in raw_members.splitlines() if line.strip()]:
            hosts.append({"locality": locality, "hostId": host_id})
    return {"available": True, "hosts": hosts}


def manifest_entry(manifest, size_mb, access_type, pattern):
    for entry in manifest["files"]:
        if (
            entry["sizeMiB"] == size_mb
            and entry.get("accessType") == access_type
            and entry.get("pattern") == pattern
        ):
            return entry
    raise RuntimeError(f"manifest missing {access_type}/{pattern}/{size_mb} MiB file")


def row_key(access_type, pattern, size_mb):
    return f"{access_type}:{pattern}:{size_mb}mib"


def print_sample_header():
    print(
        "\n"
        " run access          pattern    state  size      read      throughput status\n"
        "---- --------------- ---------- ----- ----- --------- --------------- -------"
    )


def run_matrix(args, sandbox, token, workspace, volume, manifest, access_types, start_index):
    rows = []
    samples = []
    volume_id = volume_external_id(volume)
    evidence = {"image": {"imageId": getattr(sandbox, "image_id", "")}, "volume": {"volumeId": volume_id}}
    index = start_index

    for access_type in access_types:
        if access_type == "image_archive":
            root = IMAGE_READ_ROOT
        elif access_type == "volume_mount":
            root = volume_container_root(args)
        else:
            root = workspace_fuse_root(args, workspace, volume_id)

        for size_mb in args.sizes:
            for pattern in args.pattern_list:
                entry = manifest_entry(manifest, size_mb, access_type, pattern)

                cold = run_one(args, sandbox, token, workspace, access_type, root, entry, pattern, "cold", index)
                index += 1
                samples.append(cold)
                print_sample(cold)
                if args.settle_seconds > 0:
                    time.sleep(args.settle_seconds)

                hot_samples = []
                for _ in range(args.iterations):
                    if access_type == "image_archive":
                        evidence["image"].setdefault("localArchiveEvictions", []).append(
                            delete_image_archive_cache_files(
                                args.namespace,
                                args.image_cache_path,
                                getattr(sandbox, "image_id", ""),
                            )
                        )

                    hot = run_one(args, sandbox, token, workspace, access_type, root, entry, pattern, "hot", index)
                    index += 1
                    hot_samples.append(hot)
                    samples.append(hot)
                    print_sample(hot)

                row = {
                    "key": row_key(access_type, pattern, size_mb),
                    "accessType": access_type,
                    "pattern": pattern,
                    "fileSizeBytes": entry["size"],
                    "fileSizeMiB": size_mb,
                    "path": entry["path"],
                    "expectedSha256": entry["sha256"],
                    "cold": summarize_samples([cold]),
                    "hot": summarize_samples(hot_samples),
                    "samples": {"cold": [cold], "hot": hot_samples},
                }
                if access_type in {"volume_mount", "workspace_fuse"}:
                    row["cacheEvidence"] = {"casProof": wait_cache_object_proof(args, entry["sha256"])}
                rows.append(row)

    return rows, samples, evidence, index


def run_one(args, sandbox, token, workspace, access_type, root, entry, pattern, cache_state, index):
    if access_type == "workspace_fuse":
        return run_workspace_fuse_read(args, sandbox, token, root, entry, pattern, cache_state, index)
    return run_sandbox_read(args, sandbox, token, access_type, root, entry, pattern, cache_state, index)


def write_markdown_report(path, report):
    lines = [
        "# Cache Benchmark Report",
        "",
        f"- Started: `{report['startedAt']}`",
        f"- Finished: `{report['finishedAt']}`",
        f"- Namespace: `{report['config']['namespace']}`",
        f"- Image: `{report['config']['imageUri']}`",
        f"- Volume: `{report['config']['volumeName']}` mounted at `/volumes/{report['config']['volumeMountPath']}`",
        "",
        "| Access | Pattern | Size | Cold MB/s | Hot MB/s | Cold ms | Hot ms | Speedup |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in report["rows"]:
        cold = row["cold"]
        hot = row["hot"]
        speedup = None
        if cold.get("durationMs") and hot.get("durationMs"):
            speedup = cold["durationMs"] / hot["durationMs"]
        lines.append(
            "| "
            f"{row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
            f"{num(cold.get('mbps'))} | {num(hot.get('mbps'))} | "
            f"{num(cold.get('durationMs'))} | {num(hot.get('durationMs'))} | {num(speedup)} |"
        )

    counters = report["cacheEvidence"]["logs"].get("counters", {})
    lines.extend(["", "## Cache Counters", "", "| Counter | Count |", "| --- | ---: |"])
    for key in sorted(counters):
        lines.append(f"| {key} | {counters[key]} |")

    proofs = report["cacheEvidence"].get("knownHashes", {})
    if proofs:
        lines.extend(["", "## CAS Proof", "", "| Name | Hash | Exists | Bytes | Match |", "| --- | --- | ---: | ---: | ---: |"])
        for name, proof in proofs.items():
            lines.append(
                f"| {name} | `{proof.get('hash', '')}` | {proof.get('exists', False)} | "
                f"{proof.get('bytes', '')} | {proof.get('matchesHash', False)} |"
            )

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def num(value):
    if value is None:
        return ""
    return f"{value:.2f}" if isinstance(value, float) else str(value)


def print_table(rows):
    print("\nCache benchmark:")
    print("| Access | Pattern | Size | Cold MB/s | Hot MB/s | Cold ms | Hot ms | Speedup |")
    print("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in rows:
        cold = row["cold"]
        hot = row["hot"]
        speedup = cold["durationMs"] / hot["durationMs"] if cold.get("durationMs") and hot.get("durationMs") else None
        print(
            f"| {row['accessType']} | {row['pattern']} | {row['fileSizeMiB']} MiB | "
            f"{num(cold.get('mbps'))} | {num(hot.get('mbps'))} | "
            f"{num(cold.get('durationMs'))} | {num(hot.get('durationMs'))} | {num(speedup)} |"
        )


def merge_evidence(target, source):
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            target[key].update(value)
        else:
            target[key] = value


def collect_known_hashes(args, rows, logs):
    known = {}
    for row in rows:
        if row["accessType"] in {"volume_mount", "workspace_fuse"}:
            name = f"{row['accessType']}:{row['pattern']}:{row['fileSizeMiB']}mib"
            if name not in known:
                known[name] = row.get("cacheEvidence", {}).get("casProof") or cache_object_proof(
                    args.namespace,
                    args.cache_mount_path,
                    args.cache_locality,
                    row["expectedSha256"],
                )

    for operation in logs.get("imageArchiveLoads", []):
        if operation.get("hashes"):
            image_hash = operation["hashes"][0]
            known["imageArchive"] = cache_object_proof(
                args.namespace,
                args.cache_mount_path,
                args.cache_locality,
                image_hash,
            )
            break
    return known


def cache_error_counters(counters):
    error_keys = {
        "storeFromContentErr",
        "hostNotFound",
        "sourceNotFound",
        "unableToPopulate",
        "deadlineExceeded",
        "cacheNoHosts",
    }
    return {key: counters.get(key, 0) for key in sorted(error_keys) if counters.get(key, 0)}


def cache_proof_failures(rows, known_hashes):
    failures = []
    for row in rows:
        if row["accessType"] in {"volume_mount", "workspace_fuse"}:
            proof = row.get("cacheEvidence", {}).get("casProof", {})
            if not proof.get("matchesHash"):
                failures.append(f"{row['key']} missing matching CAS proof for {row['expectedSha256']}")
    if any(row["accessType"] == "image_archive" for row in rows):
        image_proof = known_hashes.get("imageArchive")
        if not image_proof or not image_proof.get("matchesHash"):
            failures.append(f"imageArchive missing matching CAS proof for {(image_proof or {}).get('hash')}")
    return failures


def main():
    args = parse_args()
    require_tools("kubectl")
    started_at = now_rfc3339()

    if args.install:
        install_overlay(args.namespace, args.timeout_seconds)
    if args.port_forward:
        start_http_port_forward_if_needed(args.namespace, args.gateway_url, args.timeout_seconds)
        start_grpc_port_forward_if_needed(args)
        start_localstack_port_forward_if_needed(args)
    else:
        wait_http(args.gateway_url, HEALTH_PATH, timeout_seconds=args.timeout_seconds)

    load_cached_token(args)
    token, workspace_id = authorize_benchmark(args)
    save_cached_token(args, token)
    args.token = token
    if workspace_id:
        log(f"Authorized workspace {workspace_id}")
    workspace = verify_workspace_storage(args, token)

    build_info = build_payload_image(args)
    write_sdk_config(args)
    if args.reset_workers:
        log("Deleting worker jobs before cache benchmark")
        delete_workers(args.namespace)

    redis_pod = find_redis_pod(args.namespace)
    disk_before = cache_disk_stats(args.namespace, args.cache_mount_path)
    hosts_before = cache_hosts_snapshot(args.namespace, redis_pod, args.cache_locality)

    sandbox, volume, prepare_ms = prepare_sandbox_runtime(args)
    manifest = build_info["manifest"]
    rows = []
    samples = []
    benchmark_evidence = {"image": {"imageId": getattr(sandbox, "image_id", "")}, "volume": {"volumeId": volume_external_id(volume)}}
    prepare_sample = None
    index = 1
    image_access_types = [access_type for access_type in args.access_type_list if access_type == "image_archive"]
    storage_access_types = [access_type for access_type in args.access_type_list if access_type != "image_archive"]

    print_sample_header()
    if image_access_types:
        batch_rows, batch_samples, batch_evidence, index = run_matrix(
            args, sandbox, token, workspace, volume, manifest, image_access_types, index
        )
        rows.extend(batch_rows)
        samples.extend(batch_samples)
        merge_evidence(benchmark_evidence, batch_evidence)

    if storage_access_types:
        prepare_sample = run_volume_prepare(args, sandbox, token, index)
        index += 1
        print_sample(prepare_sample)
        if not prepare_sample.get("ok"):
            raise SystemExit(f"volume prepare failed: {prepare_sample.get('error')}")
        if prepare_sample["manifest"] != manifest:
            raise SystemExit("volume prepare manifest did not match generated image manifest")

        batch_rows, batch_samples, batch_evidence, index = run_matrix(
            args, sandbox, token, workspace, volume, manifest, storage_access_types, index
        )
        rows.extend(batch_rows)
        samples.extend(batch_samples)
        merge_evidence(benchmark_evidence, batch_evidence)

    logs = cache_log_counters(args.namespace, started_at, args.log_tail)
    known_hashes = collect_known_hashes(args, rows, logs)
    disk_after = cache_disk_stats(args.namespace, args.cache_mount_path)
    hosts_after = cache_hosts_snapshot(args.namespace, redis_pod, args.cache_locality)
    print_table(rows)

    report = {
        "startedAt": started_at,
        "finishedAt": now_rfc3339(),
        "config": {
            "namespace": args.namespace,
            "gatewayUrl": args.gateway_url,
            "grpcAddr": grpc_addr(args),
            "imageUri": args.image_uri,
            "hostImageUri": build_info["hostImageUri"],
            "sourceImage": args.source_image,
            "sizesMiB": args.sizes,
            "patterns": args.pattern_list,
            "accessTypes": args.access_type_list,
            "iterations": args.iterations,
            "randomBlockBytes": args.random_block_bytes,
            "volumeName": args.volume_name,
            "volumeId": volume_external_id(volume),
            "volumeMountPath": args.volume_mount_path,
            "volumeSubdir": args.volume_subdir,
            "cacheMountPath": args.cache_mount_path,
            "workspaceName": workspace_name(workspace),
        },
        "workspace": workspace,
        "sandbox": {
            "stubId": getattr(sandbox, "stub_id", ""),
            "imageId": getattr(sandbox, "image_id", ""),
            "prepareMs": prepare_ms,
        },
        "volumePrepare": prepare_sample,
        "rows": rows,
        "samples": samples,
        "cacheEvidence": {
            "diskBefore": disk_before,
            "diskAfter": disk_after,
            "hostsBefore": hosts_before,
            "hostsAfter": hosts_after,
            "logs": logs,
            "knownHashes": known_hashes,
            "benchmarkEvidence": benchmark_evidence,
        },
        "cluster": cluster_snapshot(args.namespace),
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown_report(args.report, report)
    log(f"Wrote cache benchmark JSON to {output_path}")
    log(f"Wrote cache benchmark report to {args.report}")

    prepare_samples = [prepare_sample] if prepare_sample else []
    failures = [sample for sample in samples + prepare_samples if not sample.get("ok")]
    cache_errors = cache_error_counters(logs.get("counters", {}))
    if cache_errors:
        failures.append({"ok": False, "error": f"cache error counters were non-zero: {cache_errors}"})
    proof_failures = cache_proof_failures(rows, known_hashes)
    if proof_failures:
        failures.append({"ok": False, "error": "; ".join(proof_failures)})
    if failures:
        raise SystemExit(f"{len(failures)} cache benchmark run(s) failed")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
