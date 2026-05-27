#!/usr/bin/env python3
import argparse
import json
import os
import re
import shlex
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple, Union

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
for import_path in (REPO_ROOT, REPO_ROOT / "benchmarks"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from benchmarks.sandbox_parallel import (  # noqa: E402
    cleanup_sandbox,
    create_sandbox_with_retries,
    import_sdk,
    parse_sdk_cpu,
    parse_sdk_memory,
    wait_running as wait_instance_running,
    write_sdk_config,
)
from benchmarks.startup import (  # noqa: E402
    api_url,
    authorize,
    cluster_snapshot,
    delete_workers,
    env_bool,
    env_float,
    env_int,
    find_redis_pod,
    grpc_addr as resolved_grpc_addr,
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


CACHE_TOOLS_DIR = Path(__file__).resolve().parent / "cache_tools"
HEALTH_PATH = "/api/v1/health"
DEFAULT_CACHE_MOUNT_PATH = "/var/lib/beta9/cache"
VOLUME_CONTAINER_PREFIX = "/volumes"

GEESEFS_SUMMARY_RE = re.compile(
    r"geesefs read path summary: .*?"
    r"mmap_page\(attempt=(\d+) hit=(\d+) miss=(\d+) mmap_fail=(\d+) ([0-9.]+)MiB[^)]*\).*?"
    r"read_into\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"stream\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"unary\(attempt=(\d+) hit=(\d+) miss=(\d+) ([0-9.]+)MiB\).*?"
    r"cloud\(req=(\d+) ([0-9.]+)MiB\)"
)
GEESEFS_BUFFER_RE = re.compile(
    r"geesefs read path summary: .*?buffer_hit=(\d+) buffer=([0-9.]+)MiB"
)
FUSE_RESPONSE_RE = re.compile(
    r"geesefs fuse read response complete: .*?"
    r"path=\"([^\"]+)\" hash=\"([^\"]*)\" offset=(\d+) size=(\d+) bytes=(\d+) .*?"
    r"handler_ms=([0-9.]+) response_ms=([0-9.]+)"
)


def now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_json_output(stdout: str) -> dict[str, Any]:
    for line in reversed((stdout or "").splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise RuntimeError(f"command did not emit JSON: {(stdout or '')[-500:]}")


def parse_size_mib(value: str) -> int:
    text = value.strip().lower()
    if text.endswith("mib"):
        text = text[:-3]
    elif text.endswith("mb"):
        text = text[:-2]
    size = int(text)
    if size <= 0:
        raise SystemExit(f"file size must be positive: {value!r}")
    return size


@dataclass(frozen=True)
class WorkloadSpec:
    access_type: str
    pattern: str
    size_mib: int

    @classmethod
    def parse_plan(cls, plan: str) -> list["WorkloadSpec"]:
        specs: list[WorkloadSpec] = []
        seen = set()
        for part in (p.strip() for p in plan.split(",") if p.strip()):
            fields = [field.strip() for field in part.split(":")]
            if len(fields) != 3:
                raise SystemExit(
                    f"invalid file-plan entry {part!r}; expected access:pattern:sizeMiB"
                )
            spec = cls(fields[0], fields[1], parse_size_mib(fields[2]))
            spec.validate()
            key = (spec.access_type, spec.pattern, spec.size_mib)
            if key in seen:
                raise SystemExit(f"duplicate file-plan entry: {part}")
            seen.add(key)
            specs.append(spec)
        return specs or [cls("volume_mount", "sequential", 4096)]

    def validate(self) -> None:
        if self.access_type not in {"volume_mount", "workspace_fuse"}:
            raise SystemExit(
                "b9bench cache supports volume_mount and workspace_fuse storage reads"
            )
        if self.pattern != "sequential":
            raise SystemExit("b9bench cache currently supports sequential reads")

    @property
    def size_bytes(self) -> int:
        return self.size_mib * 1024 * 1024

    @property
    def path(self) -> str:
        return f"files/{self.access_type}/{self.pattern}/{self.size_mib}mb.bin"

    @property
    def label(self) -> str:
        return f"{self.access_type}:{self.pattern}:size:{self.size_mib}mb"


@dataclass
class BenchmarkConfig:
    namespace: str
    gateway_url: str
    grpc_addr: str
    token: str
    token_cache: str
    sdk_config: str
    output: str
    report: str
    file_plan: str
    runtime_python_version: str
    volume_name: str
    volume_mount_path: str
    volume_subdir: str
    cache_mount_path: str
    cache_pool_name: str
    cache_locality: str
    read_method: str
    worker_dd_block_size: str
    remote_cache_proof_chunk_bytes: int
    remote_cache_proof_concurrency: int
    direct_cache_disk_probe_size_mb: int
    sandbox_cpu: str
    sandbox_memory: str
    sandbox_keep_warm_seconds: int
    sandbox_create_retries: int
    sandbox_ready_timeout_seconds: float
    sandbox_ready_retry_ms: int
    exec_timeout_seconds: float
    cache_proof_timeout_seconds: float
    timeout_seconds: float
    poll_interval_ms: int
    cleanup_ttl_seconds: int
    read_path_log_wait_seconds: float
    min_hot_file_read_mbps: float
    min_remote_cache_socket_mbps: float
    min_throughput_size_mb: int
    remote_object_full_read_max_mb: int
    workspace_storage_config: str
    workspace_storage_endpoint_url: str
    workspace_storage_region: str
    workspace_storage_bucket: str
    workspace_storage_bucket_prefix: str
    workspace_storage_access_key: str
    workspace_storage_secret_key: str
    install: bool
    port_forward: bool
    cleanup_cache_before_run: bool
    cleanup_cache_after_run: bool
    verify_reads: bool
    require_remote_read: bool
    require_remote_object_proof: bool
    require_read_path_proof: bool
    direct_cache_disk_probe: bool
    require_direct_cache_disk_probe: bool
    worker_dd_reads: bool
    sandbox_dd_reads: bool
    reset_workers: bool
    reset_workers_after_prepare: bool
    force_new_image: bool
    wait_running: bool
    workloads: list[WorkloadSpec] = field(default_factory=list)

    @classmethod
    def parse(cls) -> "BenchmarkConfig":
        parser = argparse.ArgumentParser(
            description="Run the beta9 embedded cache correctness/performance benchmark."
        )
        add = parser.add_argument
        add("--namespace", default=os.getenv("BENCH_NAMESPACE", "beta9"))
        add(
            "--gateway-url",
            default=os.getenv("BENCH_GATEWAY_URL", "http://127.0.0.1:1994"),
        )
        add("--grpc-addr", default=os.getenv("BENCH_GRPC_ADDR", "127.0.0.1:1993"))
        add(
            "--token",
            default=os.getenv("BENCH_TOKEN") or os.getenv("BETA9_TOKEN") or "",
        )
        add("--token-cache", default=os.getenv("BENCH_TOKEN_CACHE", ""))
        add("--sdk-config", default=os.getenv("BENCH_SDK_CONFIG", ""))
        add(
            "--output",
            default=os.getenv("BENCH_CACHE_OUTPUT", "/tmp/b9bench-cache.json"),
        )
        add(
            "--report",
            default=os.getenv("BENCH_CACHE_REPORT", "/tmp/b9bench-cache.md"),
        )
        add(
            "--file-plan",
            default=os.getenv("BENCH_CACHE_FILE_PLAN", "volume_mount:sequential:4096"),
        )
        add(
            "--runtime-python-version",
            default=os.getenv("BENCH_CACHE_RUNTIME_PYTHON_VERSION", "python3.10"),
        )
        add(
            "--volume-name",
            default=os.getenv("BENCH_CACHE_VOLUME_NAME", "b9bench-cache"),
        )
        add(
            "--volume-mount-path",
            default=os.getenv("BENCH_CACHE_VOLUME_MOUNT_PATH", "cache-bench"),
        )
        add("--volume-subdir", default=os.getenv("BENCH_CACHE_VOLUME_SUBDIR", ""))
        add(
            "--cache-mount-path",
            default=os.getenv("BENCH_CACHE_MOUNT_PATH", DEFAULT_CACHE_MOUNT_PATH),
        )
        add("--cache-pool-name", default=os.getenv("BENCH_CACHE_POOL_NAME", "default"))
        add("--cache-locality", default=os.getenv("BENCH_CACHE_LOCALITY", "default"))
        add(
            "--read-method",
            choices=("read", "readinto"),
            default=os.getenv("BENCH_CACHE_PYTHON_READ_METHOD", "readinto"),
        )
        add(
            "--worker-dd-block-size",
            default=os.getenv("BENCH_CACHE_WORKER_DD_BLOCK_SIZE", "4M"),
        )
        add(
            "--remote-cache-proof-chunk-bytes",
            type=int,
            default=env_int("BENCH_CACHE_REMOTE_PROOF_CHUNK_BYTES", 1024 * 1024),
        )
        add(
            "--remote-cache-proof-concurrency",
            type=int,
            default=env_int("BENCH_CACHE_REMOTE_PROOF_CONCURRENCY", 1),
        )
        add(
            "--direct-cache-disk-probe-size-mb",
            type=int,
            default=env_int("BENCH_CACHE_DIRECT_DISK_PROBE_SIZE_MB", 1024),
        )
        add("--sandbox-cpu", default=os.getenv("BENCH_CACHE_SANDBOX_CPU", "4"))
        add("--sandbox-memory", default=os.getenv("BENCH_CACHE_SANDBOX_MEMORY", "4096"))
        add(
            "--sandbox-keep-warm-seconds",
            type=int,
            default=env_int("BENCH_CACHE_KEEP_WARM_SECONDS", 1800),
        )
        add(
            "--sandbox-create-retries",
            type=int,
            default=env_int("BENCH_CACHE_CREATE_RETRIES", 20),
        )
        add(
            "--sandbox-ready-timeout-seconds",
            type=float,
            default=env_float("BENCH_CACHE_READY_TIMEOUT_SECONDS", 600),
        )
        add(
            "--sandbox-ready-retry-ms",
            type=int,
            default=env_int("BENCH_CACHE_READY_RETRY_MS", 500),
        )
        add(
            "--exec-timeout-seconds",
            type=float,
            default=env_float("BENCH_CACHE_EXEC_TIMEOUT_SECONDS", 2400),
        )
        add(
            "--cache-proof-timeout-seconds",
            type=float,
            default=env_float("BENCH_CACHE_PROOF_TIMEOUT_SECONDS", 1200),
        )
        add(
            "--timeout-seconds",
            type=float,
            default=env_float("BENCH_TIMEOUT_SECONDS", 2400),
        )
        add(
            "--poll-interval-ms",
            type=int,
            default=env_int("BENCH_POLL_INTERVAL_MS", 100),
        )
        add(
            "--cleanup-ttl-seconds",
            type=int,
            default=env_int("BENCH_CLEANUP_TTL_SECONDS", 5),
        )
        add(
            "--read-path-log-wait-seconds",
            type=float,
            default=env_float("BENCH_CACHE_READ_PATH_LOG_WAIT_SECONDS", 6),
        )
        add(
            "--min-hot-file-read-mbps",
            type=float,
            default=env_float("BENCH_CACHE_MIN_HOT_FILE_READ_MBPS", 2000),
        )
        add(
            "--min-remote-cache-socket-mbps",
            type=float,
            default=env_float("BENCH_CACHE_MIN_REMOTE_SOCKET_MBPS", 2000),
        )
        add(
            "--min-throughput-size-mb",
            type=int,
            default=env_int("BENCH_CACHE_MIN_HOT_FILE_READ_SIZE_MB", 1024),
        )
        add(
            "--remote-object-full-read-max-mb",
            type=int,
            default=env_int("BENCH_CACHE_REMOTE_OBJECT_FULL_READ_MAX_MB", 64),
        )
        add(
            "--workspace-storage-config",
            default=os.getenv(
                "BENCH_WORKSPACE_STORAGE_CONFIG", str(REPO_ROOT / "config.local.yaml")
            ),
            help="local beta9 config YAML used to resolve default workspace storage for remote-object proof",
        )
        add(
            "--workspace-storage-endpoint-url",
            default=os.getenv("BENCH_WORKSPACE_STORAGE_ENDPOINT_URL", ""),
        )
        add(
            "--workspace-storage-region",
            default=os.getenv("BENCH_WORKSPACE_STORAGE_REGION", ""),
        )
        add(
            "--workspace-storage-bucket",
            default=os.getenv("BENCH_WORKSPACE_STORAGE_BUCKET", ""),
        )
        add(
            "--workspace-storage-bucket-prefix",
            default=os.getenv("BENCH_WORKSPACE_STORAGE_BUCKET_PREFIX", ""),
        )
        add(
            "--workspace-storage-access-key",
            default=os.getenv("BENCH_WORKSPACE_STORAGE_ACCESS_KEY", ""),
        )
        add(
            "--workspace-storage-secret-key",
            default=os.getenv("BENCH_WORKSPACE_STORAGE_SECRET_KEY", ""),
        )
        add_bool(parser, "install", "BENCH_INSTALL", False)
        add_bool(parser, "port-forward", "BENCH_PORT_FORWARD", True)
        add_bool(
            parser, "cleanup-cache-before-run", "BENCH_CACHE_CLEANUP_BEFORE_RUN", True
        )
        add_bool(
            parser, "cleanup-cache-after-run", "BENCH_CACHE_CLEANUP_AFTER_RUN", True
        )
        add_bool(parser, "verify-reads", "BENCH_CACHE_VERIFY_READS", True)
        add_bool(parser, "require-remote-read", "BENCH_CACHE_REQUIRE_REMOTE_READ", True)
        add_bool(
            parser,
            "require-remote-object-proof",
            "BENCH_CACHE_REQUIRE_REMOTE_OBJECT_PROOF",
            True,
        )
        add_bool(
            parser,
            "require-read-path-proof",
            "BENCH_CACHE_REQUIRE_READ_PATH_PROOF",
            True,
        )
        add_bool(
            parser, "direct-cache-disk-probe", "BENCH_CACHE_DIRECT_DISK_PROBE", True
        )
        add_bool(
            parser,
            "require-direct-cache-disk-probe",
            "BENCH_CACHE_REQUIRE_DIRECT_DISK_PROBE",
            True,
        )
        add_bool(parser, "worker-dd-reads", "BENCH_CACHE_WORKER_DD_READS", True)
        add_bool(parser, "sandbox-dd-reads", "BENCH_CACHE_SANDBOX_DD_READS", True)
        add_bool(parser, "reset-workers", "BENCH_RESET_WORKERS", False)
        add_bool(
            parser,
            "reset-workers-after-prepare",
            "BENCH_CACHE_RESET_WORKERS_AFTER_PREPARE",
            False,
        )
        add_bool(parser, "force-new-image", "BENCH_CACHE_FORCE_NEW_IMAGE", False)
        add_bool(parser, "wait-running", "BENCH_CACHE_WAIT_RUNNING", True)

        data = vars(parser.parse_args())
        data["workloads"] = WorkloadSpec.parse_plan(data["file_plan"])
        if not data["volume_subdir"]:
            data["volume_subdir"] = f"run-{int(time.time())}-{os.urandom(4).hex()}"
        if not data["token_cache"]:
            data["token_cache"] = (
                f"/tmp/b9bench-cache-{data['namespace']}.token"
            )
        if not data["sdk_config"]:
            data["sdk_config"] = f"/tmp/b9bench-cache-{data['namespace']}.ini"
        return cls(**data)


def add_bool(
    parser: argparse.ArgumentParser, name: str, env_name: str, default: bool
) -> None:
    dest = name.replace("-", "_")
    parser.add_argument(f"--{name}", dest=dest, action="store_true")
    parser.add_argument(f"--no-{name}", dest=dest, action="store_false")
    parser.set_defaults(**{dest: env_bool(env_name, default)})


class CacheProbeTools:
    def __init__(self, root: Path = CACHE_TOOLS_DIR) -> None:
        self.root = root
        self._source_cache: dict[str, str] = {}
        self._payload_namespace: Optional[dict[str, Any]] = None

    def source(self, *names: str) -> str:
        return "\n\n".join(self._read(name) for name in names)

    def payload_sha256(self, nonce: str, label: str, size: int) -> str:
        if self._payload_namespace is None:
            namespace: dict[str, Any] = {}
            exec(self._read("payload.py"), namespace)
            self._payload_namespace = namespace
        return self._payload_namespace["deterministic_sha256"](nonce, label, size)

    def build_raw_reader(self, goarch: str) -> Path:
        build_dir = Path(tempfile.gettempdir()) / "b9bench-cache-rawread"
        build_dir.mkdir(parents=True, exist_ok=True)
        source = build_dir / "raw_read.go"
        output = build_dir / f"raw-read-linux-{goarch}"
        source.write_text(self._read("raw_read.go"), encoding="utf-8")
        proc = run(
            [
                "env",
                "-u",
                "GOROOT",
                "GOOS=linux",
                f"GOARCH={goarch}",
                "CGO_ENABLED=0",
                "go",
                "build",
                "-trimpath",
                "-o",
                str(output),
                str(source),
            ],
            check=False,
            timeout=120,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr or proc.stdout)
        return output

    def build_remote_object_probe(self) -> Path:
        build_dir = Path(tempfile.gettempdir()) / "b9bench-cache-remote-object"
        build_dir.mkdir(parents=True, exist_ok=True)
        output = build_dir / "remote-object"
        proc = run(
            [
                "bash",
                "-lc",
                "cd "
                + shlex.quote(str(REPO_ROOT))
                + " && env -u GOROOT go build -trimpath -o "
                + shlex.quote(str(output))
                + " "
                + shlex.quote(str(self.root / "remote_object.go")),
            ],
            check=False,
            timeout=120,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr or proc.stdout)
        return output

    def _read(self, name: str) -> str:
        if name not in self._source_cache:
            self._source_cache[name] = (self.root / name).read_text(encoding="utf-8")
        return self._source_cache[name]


class Kube:
    def __init__(self, config: BenchmarkConfig) -> None:
        self.config = config

    def running_workers(self) -> list[dict[str, str]]:
        return sorted(
            [
                pod
                for pod in self.worker_pods()
                if pod.get("phase") == "Running"
                and not pod.get("deleting")
                and pod.get("ready")
            ],
            key=lambda pod: (
                not pod["name"].startswith("worker-default-"),
                pod["name"],
            ),
        )

    def worker_pods(self) -> list[dict[str, str]]:
        proc = run(
            [
                "kubectl",
                "-n",
                self.config.namespace,
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
        pods = []
        for item in json.loads(proc.stdout).get("items", []):
            worker_statuses = [
                status
                for status in item.get("status", {}).get("containerStatuses", [])
                if status.get("name") == "worker"
            ]
            pods.append(
                {
                    "name": item.get("metadata", {}).get("name", ""),
                    "deleting": bool(
                        item.get("metadata", {}).get("deletionTimestamp", "")
                    ),
                    "nodeName": item.get("spec", {}).get("nodeName", ""),
                    "podIP": item.get("status", {}).get("podIP", ""),
                    "hostIP": item.get("status", {}).get("hostIP", ""),
                    "phase": item.get("status", {}).get("phase", ""),
                    "restartCount": int(
                        worker_statuses[0].get("restartCount", 0)
                        if worker_statuses
                        else 0
                    ),
                    "ready": any(status.get("ready") for status in worker_statuses),
                }
            )
        return pods

    def wait_running_workers(self, min_count: int = 1, timeout: float = 300) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.running_workers()) >= min_count:
                return
            time.sleep(2)
        raise RuntimeError(
            f"timed out waiting for {min_count} ready worker pod(s); current={self.worker_pods()}"
        )

    def restart_worker_pods(
        self, pod_names: list[str], timeout: float = 300
    ) -> dict[str, Any]:
        pods = {pod["name"]: pod for pod in self.worker_pods()}
        before = {
            name: int(pods.get(name, {}).get("restartCount") or 0)
            for name in pod_names
            if name in pods
        }
        result: dict[str, Any] = {"ok": True, "pods": []}
        if not before:
            return {"ok": False, "error": "no worker pods to restart", "pods": []}

        for name in before:
            proc = self.worker_shell(name, "kill -9 $(pidof worker)", timeout=10)
            result["pods"].append(
                {
                    "pod": name,
                    "restartRequested": True,
                    "returnCode": proc.returncode,
                    "stderr": proc.stderr.strip()[-300:],
                }
            )

        deadline = time.monotonic() + timeout
        pending = set(before)
        terminal: dict[str, str] = {}
        while pending and time.monotonic() < deadline:
            pods = {pod["name"]: pod for pod in self.worker_pods()}
            for name in list(pending):
                pod = pods.get(name)
                if not pod:
                    continue
                if pod.get("phase") in {"Succeeded", "Failed"}:
                    terminal[name] = pod.get("phase", "")
                    pending.remove(name)
                    continue
                restart_count = int(pod.get("restartCount") or 0)
                if pod.get("ready") and restart_count > before[name]:
                    pending.remove(name)
            if pending:
                time.sleep(2)

        result["ok"] = not pending and not terminal
        result["pending"] = sorted(pending)
        if terminal:
            result["terminal"] = terminal
        result["after"] = [
            {
                "pod": pod["name"],
                "nodeName": pod.get("nodeName", ""),
                "ready": pod.get("ready", False),
                "restartCount": pod.get("restartCount", 0),
            }
            for pod in self.worker_pods()
            if pod["name"] in before
        ]
        return result

    def worker_exec(
        self, pod: str, command: list[str], timeout: Union[int, float] = 60
    ):
        return run(
            [
                "kubectl",
                "-n",
                self.config.namespace,
                "exec",
                pod,
                "-c",
                "worker",
                "--",
                *command,
            ],
            check=False,
            timeout=timeout,
        )

    def worker_shell(self, pod: str, script: str, timeout: Union[int, float] = 60):
        return self.worker_exec(pod, ["sh", "-lc", script], timeout=timeout)

    def worker_logs_since(self, since_time: str) -> list[str]:
        lines: list[str] = []
        for pod in self.running_workers():
            proc = run(
                [
                    "kubectl",
                    "-n",
                    self.config.namespace,
                    "logs",
                    pod["name"],
                    "-c",
                    "worker",
                    "--since-time",
                    since_time,
                    "--tail",
                    "20000",
                ],
                check=False,
                timeout=30,
            )
            if proc.returncode == 0:
                lines.extend(proc.stdout.splitlines())
        return lines

    def node_arch(self, node_name: str) -> str:
        proc = run(
            [
                "kubectl",
                "get",
                "node",
                node_name,
                "-o",
                "jsonpath={.status.nodeInfo.architecture}",
            ],
            check=False,
            timeout=10,
        )
        arch = (proc.stdout or "").strip().lower()
        return "arm64" if arch in {"arm64", "aarch64"} else "amd64"


class ReadPathParser:
    @staticmethod
    def parse(lines: list[str], geesefs_path: str) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "summaryLines": 0,
            "mmapAttempts": 0,
            "mmapHits": 0,
            "mmapMisses": 0,
            "mmapMiB": 0.0,
            "readIntoHits": 0,
            "readIntoMiB": 0.0,
            "bufferHits": 0,
            "bufferMiB": 0.0,
            "streamHits": 0,
            "unaryHits": 0,
            "cloudReq": 0,
            "cloudMiB": 0.0,
        }
        response = {"lines": 0, "bytes": 0, "handlerMs": 0.0, "responseMs": 0.0}
        external_hit_lines = 0
        for line in lines:
            if "geesefs external page hit" in line and geesefs_path in line:
                external_hit_lines += 1
            m = FUSE_RESPONSE_RE.search(line)
            if m and m.group(1) == geesefs_path:
                response["lines"] += 1
                response["bytes"] += int(m.group(5))
                response["handlerMs"] += float(m.group(6))
                response["responseMs"] += float(m.group(7))
            m = GEESEFS_BUFFER_RE.search(line)
            if m:
                summary["bufferHits"] += int(m.group(1))
                summary["bufferMiB"] += float(m.group(2))
            m = GEESEFS_SUMMARY_RE.search(line)
            if m:
                summary["summaryLines"] += 1
                summary["mmapAttempts"] += int(m.group(1))
                summary["mmapHits"] += int(m.group(2))
                summary["mmapMisses"] += int(m.group(3))
                summary["mmapMiB"] += float(m.group(5))
                summary["readIntoHits"] += int(m.group(7))
                summary["readIntoMiB"] += float(m.group(9))
                summary["streamHits"] += int(m.group(11))
                summary["unaryHits"] += int(m.group(15))
                summary["cloudReq"] += int(m.group(18))
                summary["cloudMiB"] += float(m.group(19))
        return {
            "geesefsSummary": summary,
            "fuseResponses": response,
            "externalPageHitLines": external_hit_lines,
        }


class CacheStore:
    def __init__(self, config: BenchmarkConfig, kube: Kube, tools: CacheProbeTools) -> None:
        self.config = config
        self.kube = kube
        self.tools = tools

    def cleanup_worker_cache(self) -> dict[str, Any]:
        workers = self.kube.running_workers()
        if not workers:
            return {"ok": False, "error": "no running workers"}
        script = r"""
set -eu
for pages in /var/lib/beta9/cache/default/*/pages; do
  [ -d "$pages" ] && find "$pages" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
done
for volume_root in /cache/volumes/*; do
  [ -d "$volume_root" ] && find "$volume_root" -mindepth 1 -maxdepth 1 -name "run-*" -exec rm -rf {} +
done
rm -rf /var/lib/beta9/cache/.benchmark-probes /cache/.benchmark-probes 2>/dev/null || true
"""
        result: dict[str, Any] = {"ok": True, "pods": []}
        seen_nodes = set()
        for pod in workers:
            node_key = pod.get("nodeName") or pod.get("hostIP") or pod["name"]
            if node_key in seen_nodes:
                continue
            seen_nodes.add(node_key)
            proc = self.kube.worker_shell(pod["name"], script, timeout=180)
            entry = {
                "pod": pod["name"],
                "nodeName": pod.get("nodeName", ""),
                "ok": proc.returncode == 0,
            }
            if proc.returncode != 0:
                entry["error"] = proc.stderr.strip() or proc.stdout.strip()
            result["pods"].append(entry)
            result["ok"] = result["ok"] and entry["ok"]
        return result

    def wait_ready(
        self,
        entry: dict[str, Any],
        *,
        timeout_seconds: Optional[float] = None,
        required: bool = False,
        context: str = "cache object",
    ) -> dict[str, Any]:
        timeout = (
            self.config.cache_proof_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        deadline = time.monotonic() + timeout
        started = time.monotonic()
        next_progress = started + 15
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last = self.object_ready(entry["sha256"], entry["size"])
            if last.get("ready"):
                last["waitedMs"] = (time.monotonic() - started) * 1000
                return last
            if time.monotonic() >= next_progress:
                log(
                    f"Still waiting for {context} {entry['path']} "
                    f"hash={entry['sha256'][:12]} last={json.dumps(last, sort_keys=True)[:300]}"
                )
                next_progress = time.monotonic() + 15
            time.sleep(1)
        result = dict(last)
        result.update(
            {
                "hash": entry["sha256"],
                "ready": False,
                "timedOut": True,
                "waitedMs": (time.monotonic() - started) * 1000,
                "error": f"cache object did not become ready for {entry['path']}",
            }
        )
        if required:
            raise RuntimeError(f"{result['error']}: {last}")
        log(
            f"Cache proof not ready for {entry['path']} after {timeout:.0f}s; "
            "continuing so read-path evidence can show whether the cache was hit"
        )
        return result

    def object_ready(self, content_hash: str, expected_size: int) -> dict[str, Any]:
        checked = []
        for pod in self.kube.running_workers():
            candidates = self._candidate_shell_paths(pod, content_hash)
            script = (
                f"hash={shlex.quote(content_hash)}; expected={int(expected_size)}; "
                "found=''; for candidate in "
                f'{candidates}; do [ -d "$candidate" ] && found="$candidate" && break; done; '
                'if [ -z "$found" ]; then printf \'{"exists":false,"ready":false}\\n\'; exit 0; fi; '
                "i=0; bytes=0; "
                'while [ -f "$found/$hash-$i" ]; do size=$(stat -c %s "$found/$hash-$i"); bytes=$((bytes + size)); i=$((i + 1)); done; '
                'ready=false; [ "$bytes" -eq "$expected" ] && ready=true; '
                'printf \'{"exists":true,"ready":%s,"path":"%s","chunks":%s,"bytes":%s}\\n\' "$ready" "$found" "$i" "$bytes"'
            )
            proc = self.kube.worker_shell(pod["name"], script, timeout=30)
            checked.append(
                {
                    "pod": pod["name"],
                    "nodeName": pod.get("nodeName", ""),
                    "returnCode": proc.returncode,
                }
            )
            if proc.returncode != 0:
                continue
            try:
                proof = parse_json_output(proc.stdout)
            except Exception:
                continue
            proof.update(
                {
                    "hash": content_hash,
                    "pod": pod["name"],
                    "nodeName": pod.get("nodeName", ""),
                }
            )
            if proof.get("ready"):
                return proof
        return {
            "hash": content_hash,
            "ready": False,
            "exists": False,
            "checked": checked,
        }

    def object_proof(self, content_hash: str) -> dict[str, Any]:
        for pod in self.kube.running_workers():
            candidates = self._candidate_shell_paths(pod, content_hash)
            script = (
                f"hash={shlex.quote(content_hash)}; "
                "found=''; for candidate in "
                f'{candidates}; do [ -d "$candidate" ] && found="$candidate" && break; done; '
                'if [ -z "$found" ]; then printf \'{"exists":false}\\n\'; exit 0; fi; '
                "i=0; bytes=0; tmp=$(mktemp); "
                'while [ -f "$found/$hash-$i" ]; do cat "$found/$hash-$i" >> "$tmp"; bytes=$((bytes + $(wc -c < "$found/$hash-$i"))); i=$((i + 1)); done; '
                'sha=$(sha256sum "$tmp" | awk \'{print $1}\'); rm -f "$tmp"; '
                'printf \'{"exists":true,"path":"%s","chunks":%s,"bytes":%s,"sha256":"%s"}\\n\' "$found" "$i" "$bytes" "$sha"'
            )
            proc = self.kube.worker_shell(
                pod["name"],
                script,
                timeout=max(120, int(self.config.exec_timeout_seconds)),
            )
            if proc.returncode != 0:
                continue
            try:
                proof = parse_json_output(proc.stdout)
            except Exception:
                continue
            proof.update(
                {
                    "hash": content_hash,
                    "pod": pod["name"],
                    "nodeName": pod.get("nodeName", ""),
                }
            )
            if proof.get("exists"):
                proof["matchesHash"] = proof.get("sha256") == content_hash
                return proof
        return {"hash": content_hash, "exists": False}

    def evict_cache_pages(self, entry: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {"ok": False, "pods": []}
        source = self.tools.source("evict_cache_object.py")
        seen_nodes = set()
        for pod in self.kube.running_workers():
            node_key = pod.get("nodeName") or pod.get("hostIP") or pod["name"]
            if node_key in seen_nodes:
                continue
            seen_nodes.add(node_key)
            proc = self.kube.worker_exec(
                pod["name"],
                [
                    "python3",
                    "-c",
                    source,
                    entry["sha256"],
                    *self.candidate_dirs(pod, entry["sha256"]),
                ],
                timeout=120,
            )
            proof = {
                "pod": pod["name"],
                "nodeName": pod.get("nodeName", ""),
                "returnCode": proc.returncode,
            }
            try:
                proof.update(parse_json_output(proc.stdout))
            except Exception as exc:
                proof["error"] = str(exc)
            result["pods"].append(proof)
        result["ok"] = any(pod.get("evicted") for pod in result["pods"])
        return result

    def evict_mounted_file(
        self, worker_path: str, expected_size: int
    ) -> dict[str, Any]:
        source = self.tools.source("evict_file.py")
        result: dict[str, Any] = {"ok": False, "pods": []}
        for pod in self.kube.running_workers():
            proc = self.kube.worker_exec(
                pod["name"],
                ["python3", "-c", source, worker_path, str(expected_size)],
                timeout=120,
            )
            proof = {
                "pod": pod["name"],
                "nodeName": pod.get("nodeName", ""),
                "returnCode": proc.returncode,
            }
            try:
                proof.update(parse_json_output(proc.stdout))
            except Exception as exc:
                proof["error"] = str(exc)
            result["pods"].append(proof)
            result["ok"] = result["ok"] or proof.get("ok", False)
        return result

    def worker_dd(self, worker_path: str, expected_size: int) -> dict[str, Any]:
        source = self.tools.source("dd_probe.py")
        for pod in self.kube.running_workers():
            proc = self.kube.worker_exec(
                pod["name"],
                [
                    "python3",
                    "-c",
                    source,
                    "--path",
                    worker_path,
                    "--expected-size",
                    str(expected_size),
                    "--block-size",
                    self.config.worker_dd_block_size,
                ],
                timeout=max(120, int(self.config.exec_timeout_seconds)),
            )
            if proc.returncode == 0:
                payload = parse_json_output(proc.stdout)
                payload.update(
                    {"pod": pod["name"], "nodeName": pod.get("nodeName", "")}
                )
                return payload
        return {"ok": False, "error": "no worker dd probe succeeded"}

    def worker_hot_read(
        self, worker_root: str, entry: dict[str, Any], geesefs_path: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        source = self.tools.source("read_file.py")
        command = [
            "python3",
            "-c",
            source,
            "--root",
            worker_root,
            "--file",
            entry["path"],
            "--expected-size",
            str(entry["size"]),
            "--expected-sha256",
            entry["sha256"],
            "--read-method",
            self.config.read_method,
        ]
        if not self.config.verify_reads:
            command.append("--skip-verify")

        log_since = now_rfc3339()
        deadline = time.monotonic() + min(
            self.config.cache_proof_timeout_seconds,
            max(30.0, self.config.exec_timeout_seconds),
        )
        last_error = ""
        while True:
            for pod in self.kube.running_workers():
                started = time.monotonic_ns()
                proc = self.kube.worker_exec(
                    pod["name"],
                    command,
                    timeout=max(120, int(self.config.exec_timeout_seconds)),
                )
                sample = {
                    "label": "worker-hot-read",
                    "startedAt": log_since,
                    "finishedAt": now_rfc3339(),
                    "pod": pod["name"],
                    "nodeName": pod.get("nodeName", ""),
                    "exitCode": proc.returncode,
                    "stdout": proc.stdout,
                    "stderr": proc.stderr,
                    "totalMs": (time.monotonic_ns() - started) / 1_000_000,
                }
                if proc.returncode != 0:
                    last_error = (
                        proc.stderr or proc.stdout or f"command exited {proc.returncode}"
                    )
                    continue
                payload = parse_json_output(proc.stdout)
                sample["ok"] = bool(payload.get("ok", True))
                sample["payload"] = payload
                if self.config.read_path_log_wait_seconds > 0:
                    time.sleep(self.config.read_path_log_wait_seconds)
                sample["readPathProof"] = ReadPathParser.parse(
                    self.kube.worker_logs_since(log_since), geesefs_path
                )
                return payload, sample
            if time.monotonic() >= deadline:
                break
            if not self._retry_worker_probe(last_error):
                break
            time.sleep(2)

        raise RuntimeError(last_error or "no worker hot-read probe succeeded")

    @staticmethod
    def _retry_worker_probe(message: str) -> bool:
        retryable = (
            "No such file or directory",
            "unable to upgrade connection",
            "container not found",
            "pod is not running",
            "cannot exec into a container in a completed pod",
            "current phase is Succeeded",
            "current phase is Failed",
            "not found",
        )
        return any(fragment in message for fragment in retryable)

    def direct_disk_probe(self) -> Optional[dict[str, Any]]:
        if not self.config.direct_cache_disk_probe:
            return None
        workers = self.kube.running_workers()
        if not workers:
            return {"ok": False, "error": "no running workers"}
        pod = workers[0]
        proc = self.kube.worker_exec(
            pod["name"],
            [
                "python3",
                "-c",
                self.tools.source("dd_probe.py"),
                "--probe-dir",
                self.config.cache_mount_path,
                "--probe-size",
                str(self.config.direct_cache_disk_probe_size_mb * 1024 * 1024),
                "--cleanup-probe",
                "--block-size",
                self.config.worker_dd_block_size,
            ],
            timeout=max(180, int(self.config.exec_timeout_seconds)),
        )
        payload = (
            parse_json_output(proc.stdout)
            if proc.returncode == 0
            else {"ok": False, "error": proc.stderr or proc.stdout}
        )
        payload.update(
            {
                "pod": pod["name"],
                "nodeName": pod.get("nodeName", ""),
                "sizeMiB": self.config.direct_cache_disk_probe_size_mb,
            }
        )
        return payload

    def remote_read(
        self, entry: dict[str, Any], cas_proof: dict[str, Any]
    ) -> dict[str, Any]:
        hosts = self.cache_hosts_snapshot()
        target_host = self._choose_cache_host(hosts, cas_proof)
        if target_host is None:
            return {
                "ok": False,
                "error": "no cache host with advertised address",
                "hosts": hosts,
            }
        source_pod, target_pod = self._choose_remote_source(target_host)
        if source_pod is None:
            return {
                "ok": False,
                "error": "no separate worker for remote read",
                "targetHost": target_host,
                "targetPod": target_pod,
            }

        arch = self.kube.node_arch(source_pod.get("nodeName", ""))
        binary = self.tools.build_raw_reader(arch)
        remote_path = f"/tmp/beta9-cache-raw-read-{arch}"
        cp = run(
            [
                "kubectl",
                "-n",
                self.config.namespace,
                "cp",
                str(binary),
                f"{source_pod['name']}:{remote_path}",
                "-c",
                "worker",
            ],
            check=False,
            timeout=60,
        )
        if cp.returncode != 0:
            return {"ok": False, "error": cp.stderr or cp.stdout}
        self.kube.worker_exec(
            source_pod["name"], ["chmod", "+x", remote_path], timeout=10
        )
        proc = self.kube.worker_exec(
            source_pod["name"],
            [
                remote_path,
                "--addr",
                self._cache_host_addr(target_host),
                "--hash",
                entry["sha256"],
                "--size",
                str(entry["size"]),
                "--expected-sha256",
                entry["sha256"],
                "--chunk-bytes",
                str(self.config.remote_cache_proof_chunk_bytes),
                "--concurrency",
                str(self.config.remote_cache_proof_concurrency),
            ],
            timeout=max(120, int(self.config.exec_timeout_seconds)),
        )
        payload = (
            parse_json_output(proc.stdout)
            if proc.returncode == 0
            else {"ok": False, "error": proc.stderr or proc.stdout}
        )
        payload.update(
            {
                "sourcePod": source_pod["name"],
                "targetPod": (target_pod or {}).get("name", ""),
                "targetHost": target_host,
                "differentWorkerPod": bool(target_pod)
                and source_pod["name"] != target_pod["name"],
            }
        )
        return payload

    def cache_hosts_snapshot(self) -> dict[str, Any]:
        redis_pod = find_redis_pod(self.config.namespace)
        if not redis_pod:
            return self.cache_hosts_from_worker_logs()

        index_key = f"cache:coordinator:host_index:{self.config.cache_pool_name}:{self.config.cache_locality}"
        logical_host_ids = [
            line
            for line in (
                redis_cli(self.config.namespace, redis_pod, "SMEMBERS", index_key) or ""
            ).splitlines()
            if line.strip()
        ]
        hosts = []
        for logical_host_id in logical_host_ids:
            registration_ids = self._registration_ids(redis_pod, logical_host_id)
            for registration_id in registration_ids:
                raw = redis_cli(
                    self.config.namespace,
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
                        or self.config.cache_pool_name,
                        "nodeName": registration.get("node_id")
                        or registration.get("nodeId")
                        or "",
                        "addr": registration.get("addr") or "",
                        "privateAddr": registration.get("private_addr")
                        or registration.get("privateAddr")
                        or "",
                    }
                )
                break
        if hosts:
            return {"available": True, "indexKey": index_key, "hosts": hosts}
        return self.cache_hosts_from_worker_logs()

    def cache_hosts_from_worker_logs(self) -> dict[str, Any]:
        hosts: list[dict[str, Any]] = []
        workers = {pod["name"]: pod for pod in self.kube.running_workers()}
        for pod_name, pod in workers.items():
            proc = run(
                [
                    "kubectl",
                    "-n",
                    self.config.namespace,
                    "logs",
                    pod_name,
                    "-c",
                    "worker",
                    "--since",
                    "30m",
                    "--tail",
                    "5000",
                ],
                check=False,
                timeout=30,
            )
            if proc.returncode != 0:
                continue
            for line in proc.stdout.splitlines():
                if "Registered cache host" not in line:
                    continue
                start = line.find("{")
                end = line.rfind("}")
                if start < 0 or end <= start:
                    continue
                try:
                    registration = json.loads(line[start : end + 1])
                except json.JSONDecodeError:
                    continue
                host_id = registration.get("logical_host_id") or registration.get("logicalHostId")
                registration_id = registration.get("registration_id") or registration.get("registrationId")
                addr = registration.get("addr") or ""
                if not host_id or not registration_id or not addr:
                    continue
                hosts.append(
                    {
                        "hostId": host_id,
                        "registrationId": registration_id,
                        "poolName": registration.get("pool_name")
                        or registration.get("poolName")
                        or "",
                        "nodeName": pod.get("nodeName", ""),
                        "addr": addr,
                        "privateAddr": addr,
                        "pod": pod_name,
                        "source": "worker_logs",
                    }
                )
                break
        return {"available": bool(hosts), "source": "worker_logs", "hosts": hosts}

    def candidate_dirs(self, pod: dict[str, str], content_hash: str) -> list[str]:
        node_name = pod.get("nodeName") or pod.get("hostIP") or ""
        bucket = content_hash[:2]
        out = []
        if node_name:
            out.append(
                str(
                    Path(self.config.cache_mount_path)
                    / self.config.cache_locality
                    / node_name
                    / "pages"
                    / bucket
                    / content_hash
                )
            )
            out.append(
                str(
                    Path(self.config.cache_mount_path)
                    / self.config.cache_locality
                    / node_name
                    / content_hash
                )
            )
        out.append(
            str(Path(self.config.cache_mount_path) / "pages" / bucket / content_hash)
        )
        out.append(str(Path(self.config.cache_mount_path) / content_hash))
        return out

    def _candidate_shell_paths(self, pod: dict[str, str], content_hash: str) -> str:
        return " ".join(
            shlex.quote(path) for path in self.candidate_dirs(pod, content_hash)
        )

    def _registration_ids(self, redis_pod: str, logical_host_id: str) -> list[str]:
        active = redis_cli(
            self.config.namespace,
            redis_pod,
            "GET",
            f"cache:coordinator:host:{logical_host_id}:active_registration",
        )
        ids = [active] if active else []
        ids.extend(
            registration_id
            for registration_id in (
                redis_cli(
                    self.config.namespace,
                    redis_pod,
                    "SMEMBERS",
                    f"cache:coordinator:host:{logical_host_id}:registrations",
                )
                or ""
            ).splitlines()
            if registration_id.strip() and registration_id != active
        )
        return ids

    def _choose_cache_host(
        self, hosts_snapshot: dict[str, Any], cas_proof: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        hosts = [
            host
            for host in hosts_snapshot.get("hosts", [])
            if self._cache_host_addr(host)
        ]
        if not hosts:
            return None
        node_name = cas_proof.get("nodeName") or ""
        for host in hosts:
            if node_name and node_name in (host.get("hostId") or ""):
                return host
        return hosts[0]

    def _choose_remote_source(
        self, target_host: dict[str, Any]
    ) -> Tuple[Optional[dict[str, str]], Optional[dict[str, str]]]:
        target_ip = self._host_part(self._cache_host_addr(target_host))
        workers = self.kube.running_workers()
        target_pod = next(
            (pod for pod in workers if pod.get("podIP") == target_ip), None
        )
        source_pod = next(
            (
                pod
                for pod in workers
                if not target_pod or pod["name"] != target_pod["name"]
            ),
            None,
        )
        return source_pod, target_pod

    @staticmethod
    def _cache_host_addr(host: dict[str, Any]) -> str:
        return host.get("privateAddr") or host.get("addr") or ""

    @staticmethod
    def _host_part(addr: str) -> str:
        if addr.startswith("["):
            return addr[1:].partition("]")[0]
        return addr.rsplit(":", 1)[0]


class SandboxRunner:
    def __init__(self, config: BenchmarkConfig, tools: CacheProbeTools, kube: Kube) -> None:
        self.config = config
        self.tools = tools
        self.kube = kube

    def authorize(self) -> tuple[str, str]:
        try:
            return authorize(self.config.gateway_url, self.config.token)
        except RuntimeError as exc:
            if not self.config.token or "Invalid token" not in str(exc):
                raise
            log(
                "Cached benchmark token is invalid; retrying local authorization without it"
            )
            if self.config.token_cache:
                Path(self.config.token_cache).unlink(missing_ok=True)
            self.config.token = ""
            return authorize(self.config.gateway_url, "")

    def verify_workspace_storage(self, token: str) -> dict[str, Any]:
        status, body, raw, _ = http_json(
            "GET",
            api_url(self.config.gateway_url, "/api/v1/workspace/current"),
            token=token,
            timeout=10,
        )
        if status >= 400:
            raise RuntimeError(
                f"workspace storage check failed with HTTP {status}: {raw[:300]}"
            )
        if not (
            body.get("storage_id")
            or body.get("storageId")
            or (body.get("storage") or {}).get("id")
        ):
            raise RuntimeError("workspace storage is required for cache benchmark")
        return body

    def prepare_runtime(self):
        Image, Sandbox, sandbox_stub_type = import_sdk()
        from beta9 import Volume  # noqa: WPS433

        image = Image(python_version=self.config.runtime_python_version)
        runtime_marker = os.getenv("BENCH_CACHE_RUNTIME_MARKER", "")
        if self.config.force_new_image:
            runtime_marker = runtime_marker or str(time.time_ns())
        if runtime_marker:
            image = image.with_envs({"BETA9_CACHE_BENCH_RUN": runtime_marker})
        volume = Volume(
            name=self.config.volume_name, mount_path=self.config.volume_mount_path
        )
        sandbox = Sandbox(
            name="b9bench-cache",
            image=image,
            cpu=parse_sdk_cpu(self.config.sandbox_cpu),
            memory=parse_sdk_memory(self.config.sandbox_memory),
            gpu="",
            keep_warm_seconds=self.config.sandbox_keep_warm_seconds,
            authorized=False,
            volumes=[volume],
            sync_local_dir=False,
        )
        sandbox.entrypoint = ["tail", "-f", "/dev/null"]

        started = time.monotonic_ns()
        ok = sandbox.prepare_runtime(
            stub_type=sandbox_stub_type,
            force_create_stub=True,
            ignore_patterns=["*"],
        )
        if not ok:
            raise RuntimeError("SDK Sandbox runtime preparation failed")
        return sandbox, volume, (time.monotonic_ns() - started) / 1_000_000

    def run_json(
        self, sandbox, token: str, command: list[str], label: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        errors: list[str] = []
        for attempt in range(1, self.config.sandbox_create_retries + 2):
            try:
                payload, sample = self._run_json_once(sandbox, token, command, label)
                if errors:
                    sample["retryErrors"] = errors
                    sample["attempt"] = attempt
                return payload, sample
            except Exception as exc:
                errors.append(str(exc))
                if (
                    attempt > self.config.sandbox_create_retries
                    or not self._is_transient_sandbox_run_error(exc)
                ):
                    raise
                log(
                    f"{label} sandbox attempt {attempt} failed transiently: {exc}; retrying"
                )
                time.sleep(min(5.0, 0.25 * attempt))
        raise RuntimeError(f"{label} failed after retries: {errors[-1]}")

    def _run_json_once(
        self, sandbox, token: str, command: list[str], label: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        instance = None
        sample: dict[str, Any] = {"label": label, "startedAt": now_rfc3339()}
        started = time.monotonic_ns()
        try:
            instance, create_info = create_sandbox_with_retries(self.config, sandbox)
            sample.update(create_info)
            sample["containerId"] = instance.container_id
            sample["stubId"] = instance.stub_id
            if not instance.ok:
                raise RuntimeError(
                    instance.error_msg or "Sandbox.create returned ok=false"
                )
            if self.config.wait_running:
                sample["running"] = wait_instance_running(
                    self.config, token, instance.container_id, started
                )
            proc_started = time.monotonic_ns()
            process = instance.process.exec(command, cwd="/")
            sample["execAcceptedMs"] = (time.monotonic_ns() - proc_started) / 1_000_000
            exit_code = process.wait(timeout=self.config.exec_timeout_seconds)
            sample["exitCode"] = exit_code
            sample["stdout"] = process.stdout.read()
            sample["stderr"] = process.stderr.read()
            if exit_code != 0:
                raise RuntimeError(
                    sample["stderr"]
                    or sample["stdout"]
                    or f"command exited {exit_code}"
                )
            payload = parse_json_output(sample["stdout"])
            sample["ok"] = bool(payload.get("ok", True))
            sample["payload"] = payload
            return payload, sample
        except Exception as exc:
            sample["ok"] = False
            sample["error"] = str(exc)
            raise
        finally:
            self._cleanup_sandbox_bounded(instance, sample)
            sample["totalMs"] = (time.monotonic_ns() - started) / 1_000_000
            sample["finishedAt"] = now_rfc3339()

    @staticmethod
    def _is_transient_sandbox_run_error(exc: Exception) -> bool:
        message = str(exc)
        transient_fragments = (
            "Unavailable",
            "connection refused",
            "error reading from server",
            "Failed to get sandbox status",
            "container state not found",
            "EOF",
        )
        return any(fragment in message for fragment in transient_fragments)

    def _cleanup_sandbox_bounded(self, instance, sample: dict[str, Any]) -> None:
        cleanup_error: list[str] = []

        def cleanup() -> None:
            try:
                cleanup_sandbox(self.config, instance, sample)
            except Exception as exc:  # defensive: cleanup must not hide benchmark evidence
                cleanup_error.append(str(exc))

        thread = threading.Thread(target=cleanup, daemon=True)
        thread.start()
        thread.join(min(60.0, max(5.0, self.config.sandbox_ready_timeout_seconds)))
        if thread.is_alive():
            sample["cleanupError"] = "sandbox cleanup timed out after bounded wait"
            return
        if cleanup_error:
            sample["cleanupError"] = cleanup_error[-1]

    def sandbox_dd(
        self,
        sandbox,
        token: str,
        root: str,
        entry: dict[str, Any],
        geesefs_path: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        log_since = now_rfc3339()
        payload, sample = self.run_json(
            sandbox,
            token,
            [
                "python3",
                "-c",
                self.tools.source("dd_probe.py"),
                "--path",
                str(Path(root) / entry["path"]),
                "--expected-size",
                str(entry["size"]),
                "--block-size",
                self.config.worker_dd_block_size,
            ],
            "sandbox-dd",
        )
        if self.config.read_path_log_wait_seconds > 0:
            time.sleep(self.config.read_path_log_wait_seconds)
        sample["readPathProof"] = ReadPathParser.parse(
            self.kube.worker_logs_since(log_since), geesefs_path
        )
        return payload, sample

    def hot_read(
        self, sandbox, token: str, root: str, entry: dict[str, Any], geesefs_path: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        command = [
            "python3",
            "-c",
            self.tools.source("read_file.py"),
            "--root",
            root,
            "--file",
            entry["path"],
            "--expected-size",
            str(entry["size"]),
            "--expected-sha256",
            entry["sha256"],
            "--read-method",
            self.config.read_method,
        ]
        if not self.config.verify_reads:
            command.append("--skip-verify")
        log_since = now_rfc3339()
        payload, sample = self.run_json(sandbox, token, command, "hot-read")
        if self.config.read_path_log_wait_seconds > 0:
            time.sleep(self.config.read_path_log_wait_seconds)
        sample["readPathProof"] = ReadPathParser.parse(
            self.kube.worker_logs_since(log_since), geesefs_path
        )
        return payload, sample


class BenchmarkPaths:
    def __init__(
        self, config: BenchmarkConfig, workspace: dict[str, Any], volume_id: str
    ) -> None:
        self.config = config
        self.workspace = workspace
        self.volume_id = volume_id

    @property
    def volume_container_root(self) -> str:
        return str(
            Path(VOLUME_CONTAINER_PREFIX)
            / self.config.volume_mount_path
            / self.config.volume_subdir
        )

    @property
    def worker_volume_root(self) -> str:
        return str(
            Path("/workspace/data")
            / self.workspace_name
            / "volumes"
            / self.volume_id
            / self.config.volume_subdir
        )

    @property
    def workspace_name(self) -> str:
        for key in ("name", "workspace_name", "workspaceName"):
            if self.workspace.get(key):
                return self.workspace[key]
        raise RuntimeError(
            f"workspace response does not include a filesystem name: {sorted(self.workspace)}"
        )

    def root_for_access(self, access_type: str) -> str:
        if access_type == "volume_mount":
            return self.volume_container_root
        if access_type == "workspace_fuse":
            return self.worker_volume_root
        raise RuntimeError(f"unsupported access type {access_type}")

    def worker_file_path(self, entry: dict[str, Any]) -> str:
        return str(Path(self.worker_volume_root) / entry["path"])

    def geesefs_log_path(self, entry: dict[str, Any]) -> str:
        return f"volumes/{self.volume_id}/{self.config.volume_subdir}/{entry['path']}"


def storage_value(storage: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = storage.get(key)
        if value:
            return str(value)
    return ""


def redacted_workspace(workspace: dict[str, Any]) -> dict[str, Any]:
    redacted = json.loads(json.dumps(workspace))
    storage = redacted.get("storage")
    if isinstance(storage, dict):
        for key in ("access_key", "accessKey", "secret_key", "secretKey"):
            if storage.get(key):
                storage[key] = "<redacted>"
    return redacted


class Reporter:
    def __init__(self, config: BenchmarkConfig) -> None:
        self.config = config

    def write(self, report: dict[str, Any]) -> None:
        Path(self.config.output).parent.mkdir(parents=True, exist_ok=True)
        Path(self.config.output).write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        self._write_markdown(report)

    @staticmethod
    def _cache_path_label(proof: dict[str, Any]) -> str:
        summary = (proof or {}).get("geesefsSummary") or {}
        return (
            f"mmap={summary.get('mmapHits', 0)} "
            f"read_into={summary.get('readIntoHits', 0)} "
            f"buffer={summary.get('bufferHits', 0)} "
            f"cloud={summary.get('cloudReq', 0)}"
        )

    def validate(self, report: dict[str, Any]) -> list[str]:
        failures = []
        for row in report["rows"]:
            read = row.get("hotRead") or {}
            if self.config.verify_reads and not row.get("shaOK"):
                failures.append(
                    f"{row['accessType']}:{row['sizeMiB']}MiB SHA verification failed"
                )
            if (
                row["sizeMiB"] >= self.config.min_throughput_size_mb
                and read.get("fileReadMBps", 0) < self.config.min_hot_file_read_mbps
            ):
                failures.append(
                    f"{row['accessType']}:{row['sizeMiB']}MiB hot file-read {read.get('fileReadMBps', 0):.2f} MB/s below {self.config.min_hot_file_read_mbps:.2f} MB/s"
                )
            read_path_proofs = [
                proof
                for proof in (
                    row.get("readPathProof"),
                    row.get("ddReadPathProof"),
                )
                if proof
            ]
            external_page_hits = sum(
                int(proof.get("externalPageHitLines") or 0)
                for proof in read_path_proofs
            )
            cache_hits = 0
            for proof in read_path_proofs:
                summary = proof.get("geesefsSummary") or {}
                cache_hits += int(summary.get("mmapHits") or 0)
                cache_hits += int(summary.get("readIntoHits") or 0)
                cache_hits += int(summary.get("bufferHits") or 0)
            if self.config.require_read_path_proof and (
                cache_hits <= 0 and external_page_hits <= 0
            ):
                failures.append(
                    f"{row['accessType']}:{row['sizeMiB']}MiB did not prove geesefs external-cache hits"
                )
            remote = row.get("remoteRead")
            if self.config.require_remote_read:
                if not remote or not remote.get("ok"):
                    failures.append(
                        f"{row['accessType']}:{row['sizeMiB']}MiB remote cache read proof failed: {(remote or {}).get('error')}"
                    )
                elif (
                    row["sizeMiB"] >= self.config.min_throughput_size_mb
                    and remote.get("mbps", 0) < self.config.min_remote_cache_socket_mbps
                ):
                    failures.append(
                        f"{row['accessType']}:{row['sizeMiB']}MiB remote cache read {remote.get('mbps', 0):.2f} MB/s below {self.config.min_remote_cache_socket_mbps:.2f} MB/s"
                    )
            remote_object = row.get("remoteObject")
            if self.config.require_remote_object_proof and (
                not remote_object or not remote_object.get("ok")
            ):
                failures.append(
                    f"{row['accessType']}:{row['sizeMiB']}MiB remote object proof failed: {(remote_object or {}).get('error')}"
                )
        direct = report.get("directDiskProbe")
        if self.config.require_direct_cache_disk_probe and (
            not direct or not direct.get("ok")
        ):
            failures.append(
                f"direct cache disk probe failed: {(direct or {}).get('error')}"
            )
        return failures

    def _write_markdown(self, report: dict[str, Any]) -> None:
        lines = [
            "# Cache Benchmark Report",
            "",
            f"- Started: `{report['startedAt']}`",
            f"- Finished: `{report['finishedAt']}`",
            f"- Namespace: `{report['config']['namespace']}`",
            f"- Volume: `{report['config']['volumeName']}` mounted at `/{report['config']['volumeMountPath']}`",
            "",
            "| Access | Size | Hot MB/s | File-read MB/s | Read Method | SHA OK | Cache Ready | Remote Object | Cache Path | Worker dd MB/s | Sandbox dd MB/s | Remote MB/s | Direct Disk MB/s |",
            "| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
        direct = report.get("directDiskProbe") or {}
        for row in report["rows"]:
            read = row.get("hotRead") or {}
            cache_path = (
                "hot("
                + self._cache_path_label(row.get("readPathProof") or {})
                + ") dd("
                + self._cache_path_label(row.get("ddReadPathProof") or {})
                + ")"
            )
            lines.append(
                "| "
                f"{row['accessType']} | {row['sizeMiB']} MiB | "
                f"{read.get('mbps', 0):.2f} | {read.get('fileReadMBps', 0):.2f} | "
                f"`{read.get('timing', {}).get('readMethod', '')}` | {row.get('shaOK', False)} | "
                f"{(row.get('cacheReady') or {}).get('ready', False)} | "
                f"{(row.get('remoteObject') or {}).get('ok', False)} | "
                f"`{cache_path}` | "
                f"{(row.get('workerDD') or {}).get('mbps', 0):.2f} | "
                f"{(row.get('sandboxDD') or {}).get('mbps', 0):.2f} | "
                f"{(row.get('remoteRead') or {}).get('mbps', 0):.2f} | "
                f"{direct.get('mbps', 0):.2f} |"
            )
        lines.extend(["", "## Validation Failures", ""])
        (
            lines.extend(f"- {failure}" for failure in report["validationFailures"])
            if report["validationFailures"]
            else lines.append("- none")
        )
        report_path = Path(self.config.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class CacheBenchmark:
    def __init__(self, config: BenchmarkConfig) -> None:
        self.config = config
        self.tools = CacheProbeTools()
        self.kube = Kube(config)
        self.cache = CacheStore(config, self.kube, self.tools)
        self.runner = SandboxRunner(config, self.tools, self.kube)
        self.reporter = Reporter(config)

    def run(self) -> None:
        require_tools("kubectl")
        started_at = now_rfc3339()
        self._prepare_gateway()

        load_cached_token(self.config)
        token, workspace_id = self.runner.authorize()
        save_cached_token(self.config, token)
        self.config.token = token
        write_sdk_config(self.config)
        if workspace_id:
            log(f"Authorized workspace {workspace_id}")
        workspace = self.runner.verify_workspace_storage(token)

        if self.config.reset_workers:
            log("Deleting worker jobs before cache benchmark")
            delete_workers(self.config.namespace)
        cleanup_before = (
            self.cache.cleanup_worker_cache()
            if self.config.cleanup_cache_before_run
            else None
        )

        manifest = self._build_manifest()
        sandbox, volume, prepare_ms = self.runner.prepare_runtime()
        volume_id = self._volume_external_id(volume)
        paths = BenchmarkPaths(self.config, workspace, volume_id)

        log(
            f"Writing {len(manifest['files'])} deterministic file(s) into Volume {self.config.volume_name}/{self.config.volume_subdir}"
        )
        prepare_payload, _ = self.runner.run_json(
            sandbox,
            token,
            [
                "python3",
                "-c",
                self.tools.source("payload.py", "prepare_volume.py"),
                "--dest",
                paths.volume_container_root,
                "--manifest-json",
                json.dumps(manifest, sort_keys=True),
            ],
            "prepare-volume",
        )
        prepare_cache_proofs = []
        worker_restart_after_prepare = None
        if self.config.reset_workers_after_prepare:
            log("Waiting for embedded cache page files before worker reset")
            for entry in manifest["files"]:
                prepare_cache_proofs.append(
                    {
                        "path": entry["path"],
                        "proof": self.cache.wait_ready(
                            entry,
                            timeout_seconds=min(
                                120, self.config.cache_proof_timeout_seconds
                            ),
                            context="pre-reset cache proof",
                        ),
                    }
                )
            pods_to_restart = sorted(
                {
                    proof["proof"].get("pod", "")
                    for proof in prepare_cache_proofs
                    if proof.get("proof", {}).get("ready")
                    and proof.get("proof", {}).get("pod")
                }
            )
            log(
                "Restarting worker container(s) after write prepare to clear in-memory state while preserving node-local disk cache"
            )
            worker_restart_after_prepare = self.kube.restart_worker_pods(
                pods_to_restart,
                timeout=max(300, self.config.sandbox_ready_timeout_seconds),
            )
            if not worker_restart_after_prepare.get("ok"):
                raise RuntimeError(
                    f"failed to restart cache worker pod(s): {worker_restart_after_prepare}"
                )
        direct_probe = self.cache.direct_disk_probe()

        self._print_header()
        rows = [
            self._run_workload(sandbox, token, paths, workspace, entry)
            for entry in manifest["files"]
        ]
        cleanup_after = (
            self.cache.cleanup_worker_cache()
            if self.config.cleanup_cache_after_run
            else None
        )

        report = {
            "startedAt": started_at,
            "finishedAt": now_rfc3339(),
            "config": self._report_config(volume_id),
            "workspace": redacted_workspace(workspace),
            "sandbox": {
                "stubId": getattr(sandbox, "stub_id", ""),
                "imageId": getattr(sandbox, "image_id", ""),
                "prepareMs": prepare_ms,
            },
            "volumePrepare": prepare_payload,
            "prepareCacheProofs": prepare_cache_proofs,
            "workerRestartAfterPrepare": worker_restart_after_prepare,
            "directDiskProbe": direct_probe,
            "cleanupBefore": cleanup_before,
            "cleanupAfter": cleanup_after,
            "rows": rows,
            "cluster": cluster_snapshot(self.config.namespace),
        }
        report["validationFailures"] = self.reporter.validate(report)
        self.reporter.write(report)
        log(f"Wrote cache benchmark JSON to {self.config.output}")
        log(f"Wrote cache benchmark report to {self.config.report}")
        if report["validationFailures"]:
            for failure in report["validationFailures"]:
                log(f"Validation failure: {failure}")
            raise SystemExit(
                f"{len(report['validationFailures'])} cache benchmark validation failure(s)"
            )

    def _prepare_gateway(self) -> None:
        if self.config.install:
            install_overlay(self.config.namespace, self.config.timeout_seconds)
        if self.config.port_forward:
            start_http_port_forward_if_needed(
                self.config.namespace,
                self.config.gateway_url,
                self.config.timeout_seconds,
            )
            start_grpc_port_forward_if_needed(self.config)
        else:
            wait_http(
                self.config.gateway_url,
                HEALTH_PATH,
                timeout_seconds=self.config.timeout_seconds,
            )

    def _build_manifest(self) -> dict[str, Any]:
        nonce = f"{datetime.now(timezone.utc).isoformat()}:{os.urandom(8).hex()}"
        files = []
        for workload in self.config.workloads:
            files.append(
                {
                    "path": workload.path,
                    "accessType": workload.access_type,
                    "pattern": workload.pattern,
                    "sizeMiB": workload.size_mib,
                    "size": workload.size_bytes,
                    "label": workload.label,
                    "sha256": self.tools.payload_sha256(
                        nonce, workload.label, workload.size_bytes
                    ),
                }
            )
        return {"version": 5, "nonce": nonce, "files": files}

    def _run_workload(
        self,
        sandbox,
        token: str,
        paths: BenchmarkPaths,
        workspace: dict[str, Any],
        entry: dict[str, Any],
    ) -> dict[str, Any]:
        access_type = entry["accessType"]
        worker_path = paths.worker_file_path(entry)
        geesefs_path = paths.geesefs_log_path(entry)

        log(
            f"Waiting for embedded cache page files for {entry['path']} hash={entry['sha256']}"
        )
        ready = self.cache.wait_ready(
            entry,
            timeout_seconds=min(120, self.config.cache_proof_timeout_seconds),
            context="workload cache proof",
        )
        cache_page_eviction = {
            "ok": True,
            "skipped": True,
            "reason": "preserve embedded cache pages for hot-read measurement",
        }
        workspace_mount_probe = None
        if access_type == "workspace_fuse":
            workspace_mount_probe = self._ensure_workspace_fuse_mounted(
                sandbox, token, paths, entry
            )

        mounted_eviction = self.cache.evict_mounted_file(worker_path, entry["size"])

        if access_type == "workspace_fuse":
            hot_payload, hot_sample = self.cache.worker_hot_read(
                paths.worker_volume_root,
                entry,
                geesefs_path,
            )
        else:
            hot_payload, hot_sample = self.runner.hot_read(
                sandbox,
                token,
                paths.root_for_access(access_type),
                entry,
                geesefs_path,
            )
        cas_proof = self.cache.object_proof(entry["sha256"])
        remote_object = self._remote_object_proof(workspace, paths, entry)
        worker_dd = None
        if self.config.worker_dd_reads:
            self.cache.evict_mounted_file(worker_path, entry["size"])
            worker_dd = self.cache.worker_dd(worker_path, entry["size"])
        sandbox_dd = None
        sandbox_dd_sample = None
        if self.config.sandbox_dd_reads and access_type == "volume_mount":
            sandbox_dd, sandbox_dd_sample = self.runner.sandbox_dd(
                sandbox,
                token,
                paths.root_for_access(access_type),
                entry,
                geesefs_path,
            )
        remote = (
            self.cache.remote_read(entry, cas_proof)
            if self.config.require_remote_read
            else None
        )
        row = {
            "accessType": access_type,
            "pattern": entry["pattern"],
            "sizeMiB": entry["sizeMiB"],
            "path": entry["path"],
            "hash": entry["sha256"],
            "cacheReady": ready,
            "cachePageEviction": cache_page_eviction,
            "casProof": cas_proof,
            "remoteObject": remote_object,
            "workspaceMountProbe": workspace_mount_probe,
            "mountedFileEviction": mounted_eviction,
            "hotRead": hot_payload,
            "hotSample": hot_sample,
            "readPathProof": hot_sample.get("readPathProof"),
            "shaOK": bool(cas_proof.get("matchesHash"))
            and (
                not self.config.verify_reads
                or hot_payload.get("digest") == entry["sha256"]
            ),
            "workerDD": worker_dd,
            "sandboxDD": sandbox_dd,
            "sandboxDDSample": sandbox_dd_sample,
            "ddReadPathProof": (sandbox_dd_sample or {}).get("readPathProof"),
            "remoteRead": remote,
        }
        self._print_row(row)
        return row

    def _ensure_workspace_fuse_mounted(
        self,
        sandbox,
        token: str,
        paths: BenchmarkPaths,
        entry: dict[str, Any],
    ) -> dict[str, Any]:
        script = (
            "import json, os, sys\n"
            "path = os.path.join(sys.argv[1], sys.argv[2])\n"
            "st = os.stat(path)\n"
            "print(json.dumps({'ok': True, 'path': path, 'size': st.st_size}))\n"
        )
        payload, sample = self.runner.run_json(
            sandbox,
            token,
            [
                "python3",
                "-c",
                script,
                paths.volume_container_root,
                entry["path"],
            ],
            "workspace-fuse-mount-probe",
        )
        return {"payload": payload, "sample": sample}

    def _remote_object_proof(
        self, workspace: dict[str, Any], paths: BenchmarkPaths, entry: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        if not self.config.require_remote_object_proof:
            return None
        storage = self._resolve_remote_storage(workspace)
        endpoint = storage.get("endpoint_url", "")
        region = storage.get("region", "")
        bucket = storage.get("bucket_name", "")
        access_key = storage.get("access_key", "")
        secret_key = storage.get("secret_key", "")
        if not all([region, bucket, access_key, secret_key]):
            return {
                "ok": False,
                "error": "workspace storage is missing region, bucket, or credentials",
                "hasEndpoint": bool(endpoint),
                "hasRegion": bool(region),
                "hasBucket": bool(bucket),
                "hasAccessKey": bool(access_key),
                "hasSecretKey": bool(secret_key),
            }

        binary = self.tools.build_remote_object_probe()
        full_read = entry["sizeMiB"] <= self.config.remote_object_full_read_max_mb
        cmd = [
            str(binary),
            "--endpoint",
            endpoint,
            "--region",
            region,
            "--bucket",
            bucket,
            "--key",
            paths.geesefs_log_path(entry),
            "--access-key",
            access_key,
            "--secret-key",
            secret_key,
            "--expected-size",
            str(entry["size"]),
            "--expected-sha256",
            entry["sha256"],
            "--timeout",
            f"{int(max(60, self.config.cache_proof_timeout_seconds))}s",
        ]
        if full_read:
            cmd.append("--full-read")
        proc = run(
            cmd,
            check=False,
            timeout=max(120, int(self.config.cache_proof_timeout_seconds)),
        )
        payload = (
            parse_json_output(proc.stdout)
            if proc.returncode == 0
            else {"ok": False, "error": proc.stderr or proc.stdout}
        )
        payload["fullReadRequested"] = full_read
        return payload

    def _resolve_remote_storage(self, workspace: dict[str, Any]) -> dict[str, str]:
        storage = workspace.get("storage") or {}
        if not isinstance(storage, dict):
            storage = {}

        resolved = {
            "endpoint_url": storage_value(storage, "endpoint_url", "endpointUrl"),
            "region": storage_value(storage, "region"),
            "bucket_name": storage_value(storage, "bucket_name", "bucketName"),
            "access_key": storage_value(storage, "access_key", "accessKey"),
            "secret_key": storage_value(storage, "secret_key", "secretKey"),
            "bucket_prefix": "",
            "source": "workspace_api",
        }

        overrides = {
            "endpoint_url": self.config.workspace_storage_endpoint_url,
            "region": self.config.workspace_storage_region,
            "bucket_name": self.config.workspace_storage_bucket,
            "access_key": self.config.workspace_storage_access_key,
            "secret_key": self.config.workspace_storage_secret_key,
            "bucket_prefix": self.config.workspace_storage_bucket_prefix,
        }
        for key, value in overrides.items():
            if value:
                resolved[key] = value
                resolved["source"] = "cli_or_env"

        if not self._has_complete_remote_storage(resolved):
            local_config = self._load_default_workspace_storage_config()
            for key, value in local_config.items():
                if value and not resolved.get(key):
                    resolved[key] = value
                    resolved["source"] = "local_config"
        else:
            local_config = self._load_default_workspace_storage_config()

        host_endpoint = local_config.get("host_endpoint_url", "")
        if host_endpoint and self._endpoint_needs_host_alias(resolved.get("endpoint_url", "")):
            resolved["endpoint_url"] = host_endpoint
            resolved["source"] = resolved.get("source", "workspace_api") + "+host_endpoint"

        if not resolved.get("bucket_name") and resolved.get("bucket_prefix"):
            workspace_external_id = str(
                workspace.get("external_id") or workspace.get("id") or ""
            )
            if workspace_external_id:
                resolved["bucket_name"] = (
                    f"{resolved['bucket_prefix']}-{workspace_external_id}"
                )

        return resolved

    @staticmethod
    def _has_complete_remote_storage(storage: dict[str, str]) -> bool:
        return all(
            storage.get(key)
            for key in ("region", "bucket_name", "access_key", "secret_key")
        )

    @staticmethod
    def _endpoint_needs_host_alias(endpoint: str) -> bool:
        return "://localstack" in endpoint or endpoint.startswith("localstack:")

    def _load_default_workspace_storage_config(self) -> dict[str, str]:
        path = Path(self.config.workspace_storage_config).expanduser()
        if not path.exists():
            return {}
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            return {}
        storage = (
            ((data.get("storage") or {}).get("workspaceStorage") or {})
            if isinstance(data, dict)
            else {}
        )
        if not isinstance(storage, dict):
            return {}
        host_endpoint = str(storage.get("defaultPresignedEndpointUrl") or "")
        return {
            "endpoint_url": str(storage.get("defaultEndpointUrl") or ""),
            "host_endpoint_url": host_endpoint,
            "region": str(storage.get("defaultRegion") or ""),
            "bucket_prefix": str(storage.get("defaultBucketPrefix") or ""),
            "access_key": str(storage.get("defaultAccessKey") or ""),
            "secret_key": str(storage.get("defaultSecretKey") or ""),
        }

    def _report_config(self, volume_id: str) -> dict[str, Any]:
        return {
            "namespace": self.config.namespace,
            "gatewayUrl": self.config.gateway_url,
            "grpcAddr": resolved_grpc_addr(self.config),
            "filePlan": self.config.file_plan,
            "volumeName": self.config.volume_name,
            "volumeId": volume_id,
            "volumeMountPath": self.config.volume_mount_path,
            "volumeSubdir": self.config.volume_subdir,
            "cacheMountPath": self.config.cache_mount_path,
            "cachePoolName": self.config.cache_pool_name,
            "readMethod": self.config.read_method,
            "sandboxCpu": self.config.sandbox_cpu,
            "sandboxMemory": self.config.sandbox_memory,
            "forceNewImage": self.config.force_new_image,
        }

    @staticmethod
    def _volume_external_id(volume) -> str:
        value = getattr(volume, "volume_id", "") or getattr(volume, "id", "")
        if not value:
            raise RuntimeError("volume id was not populated by SDK runtime preparation")
        return value

    @staticmethod
    def _print_header() -> None:
        print(
            f"{'access':15} {'size':>8} {'hot MB/s':>12} {'file MB/s':>12} {'worker dd':>12} {'sandbox dd':>12} {'remote':>12} {'object':>8} status"
        )

    @staticmethod
    def _print_row(row: dict[str, Any]) -> None:
        read = row.get("hotRead") or {}
        print(
            f"{row['accessType']:15} {row['sizeMiB']:>6}MiB "
            f"{read.get('mbps', 0):>10.2f} "
            f"{read.get('fileReadMBps', 0):>10.2f} "
            f"{(row.get('workerDD') or {}).get('mbps', 0):>10.2f} "
            f"{(row.get('sandboxDD') or {}).get('mbps', 0):>10.2f} "
            f"{(row.get('remoteRead') or {}).get('mbps', 0):>10.2f} "
            f"{str((row.get('remoteObject') or {}).get('ok', False)):>8} "
            f"{'ok' if row.get('shaOK') else 'bad-sha'}"
        )


def main() -> None:
    CacheBenchmark(BenchmarkConfig.parse()).run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
