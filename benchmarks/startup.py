#!/usr/bin/env python3
import argparse
import atexit
import base64
import hashlib
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
API_PREFIX = "/api/v1/gateway"
HEALTH_PATH = "/api/v1/health"
GRPC_IMPORT_PATHS = [
    "googleapis",
    "pkg/types",
    "pkg/gateway",
    "pkg/abstractions/image",
    "pkg/abstractions/pod",
]


def env_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def env_int(name, default):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def env_float(name, default):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark beta9 local k3d/k3s container startup through the gateway HTTP API."
    )
    parser.add_argument("--namespace", default=os.getenv("BENCH_NAMESPACE", "beta9"))
    parser.add_argument("--gateway-url", default=os.getenv("BENCH_GATEWAY_URL", "http://127.0.0.1:11994"))
    parser.add_argument("--grpc-addr", default=os.getenv("BENCH_GRPC_ADDR", ""))
    parser.add_argument("--token", default=os.getenv("BENCH_TOKEN") or os.getenv("BETA9_TOKEN") or "")
    parser.add_argument("--token-cache", default=os.getenv("BENCH_TOKEN_CACHE", ""))
    parser.add_argument("--stub-id", default=os.getenv("BENCH_STUB_ID") or os.getenv("STUB_ID") or "")
    parser.add_argument("--image-id", default=os.getenv("BENCH_IMAGE_ID") or os.getenv("IMAGE_ID") or "")
    parser.add_argument("--checkpoint-id", default=os.getenv("BENCH_CHECKPOINT_ID") or os.getenv("CHECKPOINT_ID") or "")
    parser.add_argument("--bootstrap-image-uri", default=os.getenv("BENCH_BOOTSTRAP_IMAGE_URI", "registry.localhost:5000/beta9-bench-alpine:latest"))
    parser.add_argument("--bootstrap-source-image", default=os.getenv("BENCH_BOOTSTRAP_SOURCE_IMAGE", "alpine:latest"))
    parser.add_argument("--bootstrap-entrypoint", default=os.getenv("BENCH_BOOTSTRAP_ENTRYPOINT", "sh -c 'sleep 3600'"))
    parser.add_argument("--iterations", type=int, default=env_int("BENCH_ITERATIONS", 10))
    parser.add_argument("--warmup", type=int, default=env_int("BENCH_WARMUP", 1))
    parser.add_argument("--timeout-seconds", type=float, default=env_float("BENCH_TIMEOUT_SECONDS", 240))
    parser.add_argument("--poll-interval-ms", type=int, default=env_int("BENCH_POLL_INTERVAL_MS", 50))
    parser.add_argument("--sleep-seconds", type=float, default=env_float("BENCH_SLEEP_SECONDS", 1))
    parser.add_argument("--cleanup-ttl-seconds", type=int, default=env_int("BENCH_CLEANUP_TTL_SECONDS", 5))
    parser.add_argument("--output", default=os.getenv("BENCH_OUTPUT", "/tmp/beta9-startup-benchmark.json"))
    parser.add_argument("--pod-probe-port", type=int, default=env_int("BENCH_POD_PROBE_PORT", 0))
    parser.add_argument("--pod-probe-path", default=os.getenv("BENCH_POD_PROBE_PATH", "/"))

    parser.add_argument("--install", dest="install", action="store_true")
    parser.add_argument("--no-install", dest="install", action="store_false")
    parser.set_defaults(install=env_bool("BENCH_INSTALL", True))

    parser.add_argument("--port-forward", dest="port_forward", action="store_true")
    parser.add_argument("--no-port-forward", dest="port_forward", action="store_false")
    parser.set_defaults(port_forward=env_bool("BENCH_PORT_FORWARD", True))

    parser.add_argument("--reset-workers", dest="reset_workers", action="store_true")
    parser.add_argument("--no-reset-workers", dest="reset_workers", action="store_false")
    parser.set_defaults(reset_workers=env_bool("BENCH_RESET_WORKERS", False))

    parser.add_argument("--reset-workers-each", dest="reset_workers_each", action="store_true")
    parser.add_argument("--no-reset-workers-each", dest="reset_workers_each", action="store_false")
    parser.set_defaults(reset_workers_each=env_bool("BENCH_RESET_WORKERS_EACH", False))

    parser.add_argument("--bootstrap-stub", dest="bootstrap_stub", action="store_true")
    parser.add_argument("--no-bootstrap-stub", dest="bootstrap_stub", action="store_false")
    parser.set_defaults(bootstrap_stub=env_bool("BENCH_BOOTSTRAP_STUB", True))

    parser.add_argument("--bootstrap-local-image", dest="bootstrap_local_image", action="store_true")
    parser.add_argument("--no-bootstrap-local-image", dest="bootstrap_local_image", action="store_false")
    parser.set_defaults(bootstrap_local_image=env_bool("BENCH_BOOTSTRAP_LOCAL_IMAGE", True))

    args = parser.parse_args()
    if not args.token_cache:
        args.token_cache = f"/tmp/beta9-startup-benchmark-{args.namespace}.token"
    return args


class CommandError(RuntimeError):
    def __init__(self, cmd, returncode, stdout="", stderr=""):
        details = [f"{' '.join(cmd)} exited {returncode}"]
        if stdout.strip():
            details.append(f"stdout:\n{stdout.strip()}")
        if stderr.strip():
            details.append(f"stderr:\n{stderr.strip()}")
        super().__init__("\n".join(details))
        self.cmd = cmd
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def run(cmd, *, input_text=None, check=True, timeout=None):
    popen_kwargs = {}
    if sys.platform == "darwin":
        # Prefer posix_spawn over fork on macOS. grpc-python installs fork
        # handlers once the SDK is imported; forking after that can block while
        # grpc waits for C-core worker threads. The benchmark runner executes
        # many local kubectl/go/docker probes after SDK use, so keep subprocess
        # creation out of grpc's atfork path.
        popen_kwargs["close_fds"] = False
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        text=True,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        **popen_kwargs,
    )
    if check and proc.returncode != 0:
        raise CommandError(cmd, proc.returncode, proc.stdout, proc.stderr)
    return proc


def require_tools(*names):
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        raise SystemExit(f"Missing required tool(s): {', '.join(missing)}")


def log(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def load_cached_token(args):
    if args.token:
        return
    path = Path(args.token_cache)
    if path.exists():
        args.token = path.read_text(encoding="utf-8").strip()
        if args.token:
            log(f"Loaded cached benchmark token from {path}")


def save_cached_token(args, token):
    if not token:
        return
    path = Path(args.token_cache)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(token + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


def api_url(base_url, path):
    return base_url.rstrip("/") + path


def grpc_addr(args):
    if args.grpc_addr:
        return args.grpc_addr
    parsed = urllib.parse.urlparse(args.gateway_url)
    host = parsed.hostname or "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{host}:1993"


def auth_header(token):
    if not token:
        return None
    if token.lower().startswith("bearer "):
        return token
    return f"Bearer {token}"


def parse_json(raw):
    if raw == "":
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def http_json(method, url, *, token="", body=None, timeout=10):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None or method.upper() in {"POST", "PUT", "PATCH"}:
        data = json.dumps(body or {}).encode("utf-8")
        headers["Content-Type"] = "application/json"
    authorization = auth_header(token)
    if authorization:
        headers["Authorization"] = authorization

    start = time.monotonic_ns()
    request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
            return response.status, parse_json(raw), raw, elapsed_ms
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
        return exc.code, parse_json(raw), raw, elapsed_ms


def wait_http(base_url, path, *, token="", timeout_seconds=60, ok_statuses=None):
    deadline = time.monotonic() + timeout_seconds
    ok_statuses = ok_statuses or {200}
    last_error = None
    while time.monotonic() < deadline:
        try:
            status, body, raw, elapsed_ms = http_json("GET", api_url(base_url, path), token=token, timeout=2)
            if status in ok_statuses:
                return {"status": status, "body": body, "raw": raw, "elapsedMs": elapsed_ms}
            last_error = f"HTTP {status}: {raw[:200]}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for {base_url}{path}: {last_error}")


def parse_concatenated_json(raw):
    decoder = json.JSONDecoder()
    index = 0
    messages = []
    while index < len(raw):
        while index < len(raw) and raw[index].isspace():
            index += 1
        if index >= len(raw):
            break
        message, index = decoder.raw_decode(raw, index)
        messages.append(message)
    return messages


def grpcurl(args, service_method, payload, *, proto="pkg/gateway/gateway.proto", timeout=None):
    cmd = ["grpcurl", "-plaintext"]
    token = auth_header(args.token)
    if token:
        cmd.extend(["-H", f"authorization: {token}"])
    for import_path in GRPC_IMPORT_PATHS:
        cmd.extend(["-import-path", import_path])
    cmd.extend(["-proto", proto, "-d", "@", grpc_addr(args), service_method])
    proc = run(cmd, input_text=payload, check=True, timeout=timeout or args.timeout_seconds)
    return parse_concatenated_json(proc.stdout)


def prepare_local_image(args):
    if not args.bootstrap_local_image:
        return

    require_tools("docker")
    image_uri = args.bootstrap_image_uri
    host_image_uri = image_uri
    registry_prefixes = ("registry.localhost:5000/", "k3d-registry.localhost:5000/")
    if image_uri.startswith(registry_prefixes):
        host_image_uri = "localhost:5001/" + image_uri.split("/", 1)[1]

    log(f"Publishing benchmark source image {host_image_uri}")
    inspect = run(
        ["docker", "image", "inspect", args.bootstrap_source_image],
        check=False,
        timeout=30,
    )
    if inspect.returncode != 0:
        log(f"Pulling benchmark source image {args.bootstrap_source_image}")
        run(["docker", "pull", args.bootstrap_source_image], timeout=300)
    run(["docker", "tag", args.bootstrap_source_image, host_image_uri], timeout=30)
    run(["docker", "push", host_image_uri], timeout=120)


def build_benchmark_image(args):
    require_tools("grpcurl")
    log(f"Building CLIP image from {args.bootstrap_image_uri}")
    payload = json.dumps({
        "existingImageUri": args.bootstrap_image_uri,
        "pythonVersion": "python3",
        "ignorePython": True,
    })
    messages = grpcurl(
        args,
        "image.ImageService/BuildImage",
        payload,
        proto="pkg/abstractions/image/image.proto",
        timeout=max(600, args.timeout_seconds),
    )
    last = messages[-1] if messages else {}
    if not last.get("success"):
        raise RuntimeError(f"BuildImage failed: {last or messages}")
    image_id = last.get("imageId") or last.get("image_id")
    if not image_id:
        raise RuntimeError(f"BuildImage did not return an image id: {last}")
    return image_id


def make_stub_zip():
    tmp = tempfile.NamedTemporaryFile(prefix="beta9-startup-bench-", suffix=".zip", delete=False)
    tmp.close()
    with zipfile.ZipFile(tmp.name, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("README.txt", "beta9 startup benchmark\n")
    data = Path(tmp.name).read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    return Path(tmp.name), data, digest


def upload_object_stream(args):
    require_tools("grpcurl")
    zip_path, data, digest = make_stub_zip()
    chunk_size = 512 * 1024
    metadata = {"name": digest, "size": len(data)}
    messages = []
    for offset in range(0, len(data), chunk_size):
        chunk = data[offset : offset + chunk_size]
        messages.append({
            "objectContent": base64.b64encode(chunk).decode("ascii"),
            "objectMetadata": metadata,
            "hash": digest,
        })

    try:
        response = grpcurl(
            args,
            "gateway.GatewayService/PutObjectStream",
            "\n".join(json.dumps(message) for message in messages),
            proto="pkg/gateway/gateway.proto",
            timeout=args.timeout_seconds,
        )
    finally:
        zip_path.unlink(missing_ok=True)

    last = response[-1] if response else {}
    if not last.get("ok"):
        raise RuntimeError(f"PutObjectStream failed: {last or response}")
    object_id = last.get("objectId") or last.get("object_id")
    if not object_id:
        raise RuntimeError(f"PutObjectStream did not return an object id: {last}")
    return object_id


def create_benchmark_stub(args, image_id, object_id):
    entrypoint = shlex.split(args.bootstrap_entrypoint)
    status, body, raw, _ = http_json(
        "POST",
        api_url(args.gateway_url, API_PREFIX + "/stubs"),
        token=args.token,
        body={
            "objectId": object_id,
            "imageId": image_id,
            "stubType": "pod/run",
            "name": "startup-benchmark",
            "appName": "startup-benchmark",
            "pythonVersion": "python3",
            "cpu": 1000,
            "memory": 128,
            "keepWarmSeconds": 600,
            "workers": 1,
            "maxPendingTasks": 100,
            "forceCreate": True,
            "authorized": False,
            "autoscaler": {
                "type": "queue_depth",
                "maxContainers": 1,
                "tasksPerContainer": 1,
                "minContainers": 0,
            },
            "taskPolicy": {"maxRetries": 0, "timeout": 3600, "ttl": 0},
            "concurrentRequests": 1,
            "extra": "{}",
            "entrypoint": entrypoint,
            "ports": [],
        },
        timeout=15,
    )
    if status >= 400:
        raise RuntimeError(f"GetOrCreateStub failed with HTTP {status}: {raw[:500]}")
    if not body.get("ok"):
        raise RuntimeError(f"GetOrCreateStub failed: {body.get('errMsg') or body.get('err_msg') or raw[:500]}")
    stub_id = body.get("stubId") or body.get("stub_id")
    if not stub_id:
        raise RuntimeError(f"GetOrCreateStub response did not include stub id: {body}")
    return stub_id


def bootstrap_stub(args):
    prepare_local_image(args)
    image_id = build_benchmark_image(args)
    log("Uploading minimal benchmark object")
    object_id = upload_object_stream(args)
    stub_id = create_benchmark_stub(args, image_id, object_id)
    args.image_id = image_id
    args.stub_id = stub_id
    log(f"Bootstrapped benchmark stub {stub_id} with image {image_id}")


def render_local_overlay(timeout_seconds):
    rendered = run(
        ["kustomize", "build", "--enable-helm", "manifests/kustomize/overlays/cluster-dev"],
        timeout=max(120, timeout_seconds),
    ).stdout
    return strip_localstack_nodeport(rendered)


def strip_localstack_nodeport(rendered):
    documents = re.split(r"\n---\n", rendered)
    patched = []
    for document in documents:
        is_localstack_service = (
            re.search(r"(?m)^kind:\s+Service\s*$", document)
            and re.search(r"(?m)^\s+name:\s+localstack\s*$", document)
        )
        if is_localstack_service:
            document = re.sub(r"(?m)^\s+nodePort:\s+\d+\s*\n", "", document)
        patched.append(document)
    return "\n---\n".join(patched)


def install_overlay(namespace, timeout_seconds):
    require_tools("kubectl", "kustomize")
    log(f"Applying cluster-dev overlay into namespace {namespace}")
    namespace_yaml = run(
        ["kubectl", "create", "namespace", namespace, "--dry-run=client", "-o", "yaml"],
        timeout=30,
    ).stdout
    run(["kubectl", "apply", "-f", "-"], input_text=namespace_yaml, timeout=30)

    rendered = render_local_overlay(timeout_seconds)
    run(["kubectl", "apply", "-f", "-"], input_text=rendered, timeout=max(120, timeout_seconds))
    run(
        [
            "kubectl",
            "-n",
            namespace,
            "rollout",
            "status",
            "deployment/beta9-gateway",
            f"--timeout={int(timeout_seconds)}s",
        ],
        timeout=timeout_seconds + 30,
    )


def gateway_host_is_local(gateway_url):
    parsed = urllib.parse.urlparse(gateway_url)
    return parsed.hostname in {"127.0.0.1", "localhost", "::1"}


def gateway_local_port(gateway_url):
    parsed = urllib.parse.urlparse(gateway_url)
    if parsed.port:
        return parsed.port
    return 443 if parsed.scheme == "https" else 80


def tcp_reachable(host, port, timeout=1):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def start_service_port_forward(namespace, local_port, service_port, label, timeout_seconds):
    proc = subprocess.Popen(
        [
            "kubectl",
            "-n",
            namespace,
            "port-forward",
            "svc/beta9-gateway",
            f"{local_port}:{service_port}",
        ],
        cwd=REPO_ROOT,
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
    log(f"Started {label} port-forward on localhost:{local_port}")

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if tcp_reachable("127.0.0.1", local_port):
            return proc
        if proc.poll() is not None:
            output = proc.stdout.read() if proc.stdout is not None else ""
            raise RuntimeError(f"{label} port-forward exited before becoming reachable. {output}")
        time.sleep(0.1)

    output = ""
    if proc.poll() is not None and proc.stdout is not None:
        output = proc.stdout.read()
    stop_port_forward()
    raise RuntimeError(f"{label} port-forward did not become reachable. {output}")


def start_http_port_forward_if_needed(namespace, gateway_url, timeout_seconds):
    if not gateway_host_is_local(gateway_url):
        wait_http(gateway_url, HEALTH_PATH, timeout_seconds=timeout_seconds)
        return None

    try:
        wait_http(gateway_url, HEALTH_PATH, timeout_seconds=2)
        log(f"Gateway is already reachable at {gateway_url}")
        return None
    except Exception:
        pass

    local_port = gateway_local_port(gateway_url)
    proc = start_service_port_forward(namespace, local_port, 1994, "gateway HTTP", timeout_seconds)

    try:
        wait_http(gateway_url, HEALTH_PATH, timeout_seconds=timeout_seconds)
    except Exception:
        raise RuntimeError("Gateway HTTP port-forward became reachable but health did not pass")

    return proc


def parse_host_port(address):
    if address.startswith("["):
        host, _, rest = address[1:].partition("]")
        port = rest.lstrip(":")
        return host, int(port)
    host, port = address.rsplit(":", 1)
    return host, int(port)


def start_grpc_port_forward_if_needed(args):
    host, local_port = parse_host_port(grpc_addr(args))
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return None
    if tcp_reachable(host, local_port):
        log(f"Gateway gRPC is already reachable at {grpc_addr(args)}")
        return None
    return start_service_port_forward(args.namespace, local_port, 1993, "gateway gRPC", args.timeout_seconds)


def authorize(gateway_url, token):
    status, body, raw, _ = http_json(
        "POST",
        api_url(gateway_url, API_PREFIX + "/auth/authorize"),
        token=token,
        body={},
        timeout=10,
    )
    if status >= 400:
        raise RuntimeError(f"authorize failed with HTTP {status}: {raw[:300]}")
    if not body.get("ok"):
        raise RuntimeError(f"authorize failed: {body.get('errorMsg') or body.get('error_msg') or raw[:300]}")
    new_token = body.get("newToken") or body.get("new_token")
    if new_token:
        token = new_token
        log("Gateway created a new local auth token for this empty cluster")
    if not token:
        raise RuntimeError("No BENCH_TOKEN/BETA9_TOKEN available and authorize did not return a new token")
    return token, body.get("workspaceId") or body.get("workspace_id") or ""


def find_redis_pod(namespace):
    candidates = [
        ["get", "pod", "redis-master-0", "-o", "jsonpath={.metadata.name}"],
        [
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/name=redis,app.kubernetes.io/component=master",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        ["get", "pods", "-l", "app.kubernetes.io/name=redis", "-o", "jsonpath={.items[0].metadata.name}"],
    ]
    for candidate in candidates:
        proc = run(["kubectl", "-n", namespace, *candidate], check=False, timeout=10)
        pod_name = proc.stdout.strip()
        if proc.returncode == 0 and pod_name:
            return pod_name
    return ""


def redis_cli(namespace, pod_name, *args):
    if not pod_name:
        return None

    for redis_cli_path in ("redis-cli", "/opt/bitnami/redis/bin/redis-cli"):
        proc = run(
            ["kubectl", "-n", namespace, "exec", pod_name, "--", redis_cli_path, "--raw", *args],
            check=False,
            timeout=10,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()

    return None


def redis_hgetall(namespace, pod_name, key):
    raw = redis_cli(namespace, pod_name, "HGETALL", key)
    if raw is None:
        return None
    lines = raw.splitlines()
    if len(lines) % 2 != 0:
        return {"raw": raw}
    return {lines[i]: lines[i + 1] for i in range(0, len(lines), 2)}


def redis_int(namespace, pod_name, *args):
    raw = redis_cli(namespace, pod_name, *args)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def redis_snapshot(namespace, pod_name):
    if not pod_name:
        return {}
    queue_keys_raw = redis_cli(namespace, pod_name, "KEYS", "scheduler:worker:requests:*") or ""
    queue_depths = {}
    for key in queue_keys_raw.splitlines():
        depth = redis_int(namespace, pod_name, "LLEN", key)
        queue_depths[key] = depth

    return {
        "backlogDepth": redis_int(namespace, pod_name, "ZCARD", "scheduler:container_requests"),
        "workerCount": redis_int(namespace, pod_name, "SCARD", "scheduler:worker:worker_index"),
        "workerQueueDepths": queue_depths,
    }


NO_POLL_OBSERVATION = object()


def poll_until(args, request_start_ns, label, attempt, *, last_label="response"):
    deadline = time.monotonic() + args.timeout_seconds
    interval = args.poll_interval_ms / 1000
    last = None
    attempts = 0

    while time.monotonic() < deadline:
        attempts += 1
        done, result, observation = attempt()
        if observation is not NO_POLL_OBSERVATION:
            last = observation
        if done:
            result = dict(result)
            result["observedMs"] = (time.monotonic_ns() - request_start_ns) / 1_000_000
            result["attempts"] = attempts
            return result
        time.sleep(interval)

    raise TimeoutError(f"Timed out waiting for {label}; last {last_label}={last}")


def wait_container_running(args, token, container_id, request_start_ns, redis_pod):
    def attempt():
        if redis_pod:
            state = redis_hgetall(args.namespace, redis_pod, f"scheduler:container:state:{container_id}")
            if not state:
                return False, {}, NO_POLL_OBSERVATION
            return state.get("status") == "RUNNING", {"state": state}, state

        status, body, raw, _ = http_json(
            "GET",
            api_url(args.gateway_url, API_PREFIX + f"/pods/{container_id}/status"),
            token=token,
            timeout=5,
        )
        state = body if status < 500 else {"raw": raw}
        running = status == 200 and body.get("ok") and str(body.get("status", "")).lower() == "running"
        return running, {"state": body}, state

    return poll_until(args, request_start_ns, f"{container_id} RUNNING", attempt, last_label="state")


def wait_connect_ready(args, token, container_id, request_start_ns):
    def attempt():
        status, body, raw, _ = http_json(
            "POST",
            api_url(args.gateway_url, API_PREFIX + f"/pods/{container_id}/connect"),
            token=token,
            body={"containerId": container_id},
            timeout=5,
        )
        response = body if status < 500 else {"raw": raw}
        return status == 200 and body.get("ok"), {"body": body}, response

    return poll_until(args, request_start_ns, f"{container_id} connect-ready", attempt)


def wait_pod_probe(args, token, request_start_ns):
    if args.pod_probe_port <= 0:
        return None

    path = args.pod_probe_path
    if not path.startswith("/"):
        path = "/" + path

    probe_path = f"/api/v1/pod/id/{args.stub_id}/{args.pod_probe_port}{path}"

    def attempt():
        try:
            status, body, raw, _ = http_json(
                "GET",
                api_url(args.gateway_url, probe_path),
                token=token,
                timeout=5,
            )
            response = {"status": status, "body": body if body else raw[:200]}
            return status < 500, {"status": status}, response
        except Exception as exc:
            return False, {}, {"error": str(exc)}

    return poll_until(args, request_start_ns, f"pod probe {probe_path}", attempt)


def cleanup_container(args, token, container_id):
    http_json(
        "POST",
        api_url(args.gateway_url, API_PREFIX + f"/pods/{container_id}/ttl"),
        token=token,
        body={"containerId": container_id, "ttl": args.cleanup_ttl_seconds},
        timeout=5,
    )
    http_json(
        "POST",
        api_url(args.gateway_url, API_PREFIX + f"/containers/{container_id}/stop"),
        token=token,
        body={"containerId": container_id},
        timeout=5,
    )


def delete_workers(namespace):
    run(
        [
            "kubectl",
            "-n",
            namespace,
            "delete",
            "job",
            "-l",
            "run.beam.cloud/role=worker",
            "--ignore-not-found=true",
        ],
        check=False,
        timeout=30,
    )

    redis_pod = find_redis_pod(namespace)
    for pattern in ("provider:*", "scheduler:*", "worker:*", "pod:*"):
        raw_keys = redis_cli(namespace, redis_pod, "KEYS", pattern) or ""
        keys = [key for key in raw_keys.splitlines() if key]
        for offset in range(0, len(keys), 100):
            redis_cli(namespace, redis_pod, "DEL", *keys[offset : offset + 100])


def create_pod(args, token):
    body = {"stubId": args.stub_id}
    if args.image_id:
        body["imageId"] = args.image_id
    if args.checkpoint_id:
        body["checkpointId"] = args.checkpoint_id

    request_start_ns = time.monotonic_ns()
    status, response, raw, accepted_ms = http_json(
        "POST",
        api_url(args.gateway_url, API_PREFIX + "/pods"),
        token=token,
        body=body,
        timeout=30,
    )
    if status >= 400:
        raise RuntimeError(f"CreatePod failed with HTTP {status}: {raw[:500]}")
    if not response.get("ok"):
        raise RuntimeError(f"CreatePod failed: {response.get('errorMsg') or response.get('error_msg') or raw[:500]}")
    container_id = response.get("containerId") or response.get("container_id")
    if not container_id:
        raise RuntimeError(f"CreatePod response did not include container id: {response}")
    return container_id, request_start_ns, accepted_ms, response


def server_latency_ms(state):
    if not state:
        return None
    try:
        scheduled_at = int(state.get("scheduled_at") or state.get("scheduledAt") or 0)
        started_at = int(state.get("started_at") or state.get("startedAt") or 0)
    except (TypeError, ValueError):
        return None
    if scheduled_at <= 0 or started_at <= 0:
        return None
    return (started_at - scheduled_at) * 1000


def run_iteration(args, token, redis_pod, index, warmup):
    if args.reset_workers_each:
        log("Deleting worker jobs before iteration")
        delete_workers(args.namespace)

    before = redis_snapshot(args.namespace, redis_pod)
    container_id = ""
    sample = {
        "index": index,
        "warmup": warmup,
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "before": before,
    }

    try:
        container_id, request_start_ns, accepted_ms, response = create_pod(args, token)
        sample["containerId"] = container_id
        sample["createResponse"] = response
        sample["acceptedMs"] = accepted_ms

        running = wait_container_running(args, token, container_id, request_start_ns, redis_pod)
        sample["runningObservedMs"] = running["observedMs"]
        sample["runningAttempts"] = running["attempts"]
        sample["containerState"] = running["state"]
        sample["serverStartLatencyMs"] = server_latency_ms(running["state"])

        connect = wait_connect_ready(args, token, container_id, request_start_ns)
        sample["connectReadyMs"] = connect["observedMs"]
        sample["connectAttempts"] = connect["attempts"]

        probe = wait_pod_probe(args, token, request_start_ns)
        if probe is not None:
            sample["podProbeMs"] = probe["observedMs"]
            sample["podProbeAttempts"] = probe["attempts"]
            sample["podProbeStatus"] = probe["status"]

        sample["ok"] = True
    except Exception as exc:
        sample["ok"] = False
        sample["error"] = str(exc)
    finally:
        if container_id:
            try:
                cleanup_container(args, token, container_id)
            except Exception as exc:
                sample["cleanupError"] = str(exc)
        sample["after"] = redis_snapshot(args.namespace, redis_pod)
        sample["finishedAt"] = datetime.now(timezone.utc).isoformat()

    return sample


def percentile(values, p):
    clean = sorted(v for v in values if v is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (len(clean) - 1) * (p / 100)
    lower = int(rank)
    upper = min(lower + 1, len(clean) - 1)
    weight = rank - lower
    return clean[lower] * (1 - weight) + clean[upper] * weight


def summarize_metric(samples, key):
    values = [sample.get(key) for sample in samples if sample.get("ok") and not sample.get("warmup") and sample.get(key) is not None]
    if not values:
        return None
    return {
        "count": len(values),
        "min": min(values),
        "p50": percentile(values, 50),
        "p90": percentile(values, 90),
        "p95": percentile(values, 95),
        "max": max(values),
    }


def format_ms(value):
    if value is None:
        return "-"
    return f"{value:.1f}"


def print_sample(sample):
    state = sample.get("containerState") or {}
    status = state.get("status") or ("ok" if sample.get("ok") else "failed")
    print(
        f"{sample['index']:>3} "
        f"{'warmup' if sample.get('warmup') else 'bench':>6} "
        f"{sample.get('containerId', '-'):<42} "
        f"{format_ms(sample.get('acceptedMs')):>9} "
        f"{format_ms(sample.get('runningObservedMs')):>9} "
        f"{format_ms(sample.get('serverStartLatencyMs')):>9} "
        f"{format_ms(sample.get('connectReadyMs')):>9} "
        f"{format_ms(sample.get('podProbeMs')):>9} "
        f"{status:>8}",
        flush=True,
    )
    if not sample.get("ok"):
        print(f"      error: {sample.get('error')}", flush=True)


def print_summary(summary):
    print("\nSummary, excluding warmup runs:")
    for key, label in [
        ("acceptedMs", "request accepted"),
        ("runningObservedMs", "observed RUNNING"),
        ("serverStartLatencyMs", "server ScheduledAt->StartedAt"),
        ("connectReadyMs", "container connect ready"),
        ("podProbeMs", "pod proxy probe"),
    ]:
        data = summary.get(key)
        if not data:
            continue
        print(
            f"  {label:<32} "
            f"n={data['count']:<3} "
            f"min={format_ms(data['min'])}ms "
            f"p50={format_ms(data['p50'])}ms "
            f"p90={format_ms(data['p90'])}ms "
            f"p95={format_ms(data['p95'])}ms "
            f"max={format_ms(data['max'])}ms"
        )


def cluster_snapshot(namespace):
    snapshot = {}
    for name, command in {
        "pods": ["kubectl", "-n", namespace, "get", "pods", "-o", "wide"],
        "jobs": ["kubectl", "-n", namespace, "get", "jobs", "-o", "wide"],
        "gatewayService": ["kubectl", "-n", namespace, "get", "svc", "beta9-gateway", "-o", "wide"],
    }.items():
        proc = run(command, check=False, timeout=15)
        snapshot[name] = proc.stdout if proc.returncode == 0 else proc.stderr
    return snapshot


def main():
    args = parse_args()
    require_tools("kubectl")

    if args.install:
        install_overlay(args.namespace, args.timeout_seconds)

    if args.port_forward:
        start_http_port_forward_if_needed(args.namespace, args.gateway_url, args.timeout_seconds)
        if not args.stub_id and args.bootstrap_stub:
            start_grpc_port_forward_if_needed(args)
    else:
        wait_http(args.gateway_url, HEALTH_PATH, timeout_seconds=args.timeout_seconds)

    load_cached_token(args)
    token, workspace_id = authorize(args.gateway_url, args.token)
    save_cached_token(args, token)
    args.token = token
    if workspace_id:
        log(f"Authorized workspace {workspace_id}")

    if not args.stub_id:
        if not args.bootstrap_stub:
            raise SystemExit("BENCH_STUB_ID or STUB_ID is required when BENCH_BOOTSTRAP_STUB=0.")
        bootstrap_stub(args)

    redis_pod = find_redis_pod(args.namespace)
    if redis_pod:
        log(f"Using Redis pod {redis_pod} for server-side RUNNING state and queue snapshots")
    else:
        log("Redis pod was not found; falling back to the gateway pod status endpoint")

    if args.reset_workers:
        log("Deleting worker jobs before benchmark")
        delete_workers(args.namespace)

    total = args.warmup + args.iterations
    samples = []
    print("\nrun   kind container                                       accept   running    server   connect     probe   status")
    print("--- ------ ------------------------------------------ --------- --------- --------- --------- -------- --------")
    for index in range(1, total + 1):
        sample = run_iteration(args, token, redis_pod, index, index <= args.warmup)
        samples.append(sample)
        print_sample(sample)
        if index != total and args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    summary = {
        "acceptedMs": summarize_metric(samples, "acceptedMs"),
        "runningObservedMs": summarize_metric(samples, "runningObservedMs"),
        "serverStartLatencyMs": summarize_metric(samples, "serverStartLatencyMs"),
        "connectReadyMs": summarize_metric(samples, "connectReadyMs"),
        "podProbeMs": summarize_metric(samples, "podProbeMs"),
    }
    print_summary(summary)

    report = {
        "startedAt": samples[0]["startedAt"] if samples else datetime.now(timezone.utc).isoformat(),
        "finishedAt": datetime.now(timezone.utc).isoformat(),
        "config": {
            "namespace": args.namespace,
            "gatewayUrl": args.gateway_url,
            "stubId": args.stub_id,
            "imageId": args.image_id,
            "checkpointId": args.checkpoint_id,
            "iterations": args.iterations,
            "warmup": args.warmup,
            "resetWorkers": args.reset_workers,
            "resetWorkersEach": args.reset_workers_each,
            "podProbePort": args.pod_probe_port,
            "podProbePath": args.pod_probe_path,
        },
        "summary": summary,
        "samples": samples,
        "cluster": cluster_snapshot(args.namespace),
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    log(f"Wrote benchmark report to {output_path}")

    failures = [sample for sample in samples if not sample.get("ok")]
    if failures:
        raise SystemExit(f"{len(failures)} benchmark run(s) failed")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
