#!/usr/bin/env python3
import argparse
import configparser
import json
import os
import shlex
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from startup_benchmark import (
    API_PREFIX,
    api_url,
    authorize,
    cluster_snapshot,
    delete_workers,
    env_bool,
    env_float,
    env_int,
    find_redis_pod,
    format_ms,
    grpc_addr,
    http_json,
    install_overlay,
    load_cached_token,
    log,
    percentile,
    prepare_local_image,
    require_tools,
    redis_hgetall,
    save_cached_token,
    start_grpc_port_forward_if_needed,
    start_http_port_forward_if_needed,
    wait_http,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
HEALTH_PATH = "/api/v1/health"
TABLE_HEADER = "\n".join(
    (
        "",
        " run   kind container                                           create   running  proc_rdy exec_done   status",
        "---- ------ ---------------------------------------------- --------- --------- --------- --------- --------",
    )
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark many local beta9 sandboxes created concurrently via the SDK Sandbox abstraction."
    )
    parser.add_argument("--namespace", default=os.getenv("BENCH_NAMESPACE", "beta9"))
    parser.add_argument("--gateway-url", default=os.getenv("BENCH_GATEWAY_URL", "http://127.0.0.1:11994"))
    parser.add_argument("--grpc-addr", default=os.getenv("BENCH_GRPC_ADDR", "127.0.0.1:11993"))
    parser.add_argument("--token", default=os.getenv("BENCH_TOKEN") or os.getenv("BETA9_TOKEN") or "")
    parser.add_argument("--token-cache", default=os.getenv("BENCH_TOKEN_CACHE", ""))
    parser.add_argument("--timeout-seconds", type=float, default=env_float("BENCH_TIMEOUT_SECONDS", 240))
    parser.add_argument("--poll-interval-ms", type=int, default=env_int("BENCH_POLL_INTERVAL_MS", 50))
    parser.add_argument("--cleanup-ttl-seconds", type=int, default=env_int("BENCH_CLEANUP_TTL_SECONDS", 5))
    parser.add_argument("--output", default=os.getenv("BENCH_OUTPUT", "/tmp/beta9-sandbox-parallel-benchmark.json"))

    parser.add_argument("--count", type=int, default=env_int("BENCH_SANDBOX_COUNT", 100))
    parser.add_argument("--parallelism", type=int, default=env_int("BENCH_SANDBOX_PARALLELISM", env_int("BENCH_SANDBOX_COUNT", 100)))
    parser.add_argument("--prewarm-count", type=int, default=env_int("BENCH_SANDBOX_PREWARM_COUNT", 0))
    parser.add_argument("--prewarm-parallelism", type=int, default=env_int("BENCH_SANDBOX_PREWARM_PARALLELISM", env_int("BENCH_SANDBOX_PREWARM_COUNT", 0)))
    parser.add_argument("--prewarm-cooldown-seconds", type=float, default=env_float("BENCH_SANDBOX_PREWARM_COOLDOWN_SECONDS", 5))
    parser.add_argument("--prewarm-idle-timeout-seconds", type=float, default=env_float("BENCH_SANDBOX_PREWARM_IDLE_TIMEOUT_SECONDS", 180))
    parser.add_argument("--warmup", type=int, default=env_int("BENCH_SANDBOX_WARMUP", 1))
    parser.add_argument("--sandbox-cpu", default=os.getenv("BENCH_SANDBOX_CPU", "0.1"))
    parser.add_argument("--sandbox-memory", default=os.getenv("BENCH_SANDBOX_MEMORY", "192"))
    parser.add_argument("--sandbox-keep-warm-seconds", type=int, default=env_int("BENCH_SANDBOX_KEEP_WARM_SECONDS", 600))
    parser.add_argument("--sandbox-image-uri", default=os.getenv("BENCH_SANDBOX_IMAGE_URI", os.getenv("BENCH_BOOTSTRAP_IMAGE_URI", "k3d-registry.localhost:5000/beta9-bench-alpine:latest")))
    parser.add_argument("--sandbox-exec", default=os.getenv("BENCH_SANDBOX_EXEC", "true"))
    parser.add_argument("--sandbox-exec-cwd", default=os.getenv("BENCH_SANDBOX_EXEC_CWD", "/"))
    parser.add_argument("--sandbox-create-retries", type=int, default=env_int("BENCH_SANDBOX_CREATE_RETRIES", 20))
    parser.add_argument("--sandbox-ready-timeout-seconds", type=float, default=env_float("BENCH_SANDBOX_READY_TIMEOUT_SECONDS", 120))
    parser.add_argument("--sandbox-ready-retry-ms", type=int, default=env_int("BENCH_SANDBOX_READY_RETRY_MS", 500))
    parser.add_argument("--bootstrap-source-image", default=os.getenv("BENCH_BOOTSTRAP_SOURCE_IMAGE", "alpine:latest"))
    parser.add_argument("--sdk-config", default=os.getenv("BENCH_SDK_CONFIG", ""))

    parser.add_argument("--install", dest="install", action="store_true")
    parser.add_argument("--no-install", dest="install", action="store_false")
    parser.set_defaults(install=env_bool("BENCH_INSTALL", True))

    parser.add_argument("--port-forward", dest="port_forward", action="store_true")
    parser.add_argument("--no-port-forward", dest="port_forward", action="store_false")
    parser.set_defaults(port_forward=env_bool("BENCH_PORT_FORWARD", True))

    parser.add_argument("--reset-workers", dest="reset_workers", action="store_true")
    parser.add_argument("--no-reset-workers", dest="reset_workers", action="store_false")
    parser.set_defaults(reset_workers=env_bool("BENCH_RESET_WORKERS", False))

    parser.add_argument("--prewarm-wait-idle", dest="prewarm_wait_idle", action="store_true")
    parser.add_argument("--no-prewarm-wait-idle", dest="prewarm_wait_idle", action="store_false")
    parser.set_defaults(prewarm_wait_idle=env_bool("BENCH_SANDBOX_PREWARM_WAIT_IDLE", True))

    parser.add_argument("--bootstrap-local-image", dest="bootstrap_local_image", action="store_true")
    parser.add_argument("--no-bootstrap-local-image", dest="bootstrap_local_image", action="store_false")
    parser.set_defaults(bootstrap_local_image=env_bool("BENCH_BOOTSTRAP_LOCAL_IMAGE", True))

    parser.add_argument("--prepare-sandbox", dest="prepare_sandbox", action="store_true")
    parser.add_argument("--no-prepare-sandbox", dest="prepare_sandbox", action="store_false")
    parser.set_defaults(prepare_sandbox=env_bool("BENCH_SANDBOX_PREPARE", True))

    parser.add_argument("--fresh-object", dest="fresh_object", action="store_true")
    parser.add_argument("--no-fresh-object", dest="fresh_object", action="store_false")
    parser.set_defaults(fresh_object=env_bool("BENCH_SANDBOX_FRESH_OBJECT", True))

    parser.add_argument("--wait-running", dest="wait_running", action="store_true")
    parser.add_argument("--no-wait-running", dest="wait_running", action="store_false")
    parser.set_defaults(wait_running=env_bool("BENCH_SANDBOX_WAIT_RUNNING", False))

    parser.add_argument("--wait-exec-complete", dest="wait_exec_complete", action="store_true")
    parser.add_argument("--no-wait-exec-complete", dest="wait_exec_complete", action="store_false")
    parser.set_defaults(wait_exec_complete=env_bool("BENCH_SANDBOX_WAIT_EXEC_COMPLETE", False))

    args = parser.parse_args()
    if not args.token_cache:
        args.token_cache = f"/tmp/beta9-startup-benchmark-{args.namespace}.token"
    if not args.sdk_config:
        args.sdk_config = f"/tmp/beta9-sandbox-benchmark-{args.namespace}.ini"
    if args.count <= 0:
        raise SystemExit("BENCH_SANDBOX_COUNT must be greater than 0")
    if args.parallelism <= 0:
        raise SystemExit("BENCH_SANDBOX_PARALLELISM must be greater than 0")
    if args.prewarm_count < 0:
        raise SystemExit("BENCH_SANDBOX_PREWARM_COUNT cannot be negative")
    if args.prewarm_count > 0 and args.prewarm_parallelism <= 0:
        raise SystemExit("BENCH_SANDBOX_PREWARM_PARALLELISM must be greater than 0 when prewarming")
    if args.sandbox_create_retries < 0:
        raise SystemExit("BENCH_SANDBOX_CREATE_RETRIES cannot be negative")
    if args.sandbox_ready_timeout_seconds <= 0:
        raise SystemExit("BENCH_SANDBOX_READY_TIMEOUT_SECONDS must be greater than 0")
    if args.prewarm_idle_timeout_seconds <= 0:
        raise SystemExit("BENCH_SANDBOX_PREWARM_IDLE_TIMEOUT_SECONDS must be greater than 0")
    args.bootstrap_image_uri = args.sandbox_image_uri
    return args


def parse_sdk_cpu(value):
    raw = str(value).strip()
    if raw.endswith("m"):
        return raw
    return float(raw)


def parse_sdk_memory(value):
    raw = str(value).strip()
    if raw.isdigit():
        return int(raw)
    return raw


def parse_host_port(address):
    if address.startswith("["):
        host, _, rest = address[1:].partition("]")
        return host, int(rest.lstrip(":"))
    host, port = address.rsplit(":", 1)
    return host, int(port)


def write_sdk_config(args):
    host, port = parse_host_port(grpc_addr(args))
    config_path = Path(args.sdk_config)
    config_path.parent.mkdir(parents=True, exist_ok=True)

    parser = configparser.ConfigParser()
    parser["default"] = {
        "token": args.token,
        "gateway_host": host,
        "gateway_port": str(port),
    }
    with config_path.open("w", encoding="utf-8") as file:
        parser.write(file)
    os.chmod(config_path, 0o600)

    os.environ["CONFIG_PATH"] = str(config_path)
    os.environ["BETA9_TOKEN"] = args.token
    os.environ["BETA9_GATEWAY_HOST"] = host
    os.environ["BETA9_GATEWAY_PORT"] = str(port)
    log(f"Configured local SDK context at {config_path}")


def import_sdk():
    sdk_src = str(REPO_ROOT / "sdk" / "src")
    if sdk_src not in sys.path:
        sys.path.insert(0, sdk_src)

    from beta9 import Image, Sandbox  # noqa: WPS433
    from beta9.abstractions.base.runner import SANDBOX_STUB_TYPE  # noqa: WPS433

    return Image, Sandbox, SANDBOX_STUB_TYPE


def prepare_sandbox_runtime(args):
    Image, Sandbox, sandbox_stub_type = import_sdk()
    image = Image.from_registry(args.sandbox_image_uri)
    sandbox = Sandbox(
        name="sandbox-parallel-benchmark",
        image=image,
        cpu=parse_sdk_cpu(args.sandbox_cpu),
        memory=parse_sdk_memory(args.sandbox_memory),
        keep_warm_seconds=args.sandbox_keep_warm_seconds,
        authorized=False,
        sync_local_dir=False,
    )

    if args.prepare_sandbox:
        log("Preparing SDK Sandbox runtime; image build and stub sync are excluded from measured samples")
        sandbox.entrypoint = ["tail", "-f", "/dev/null"]
        ok = sandbox.prepare_runtime(
            stub_type=sandbox_stub_type,
            force_create_stub=True,
            ignore_patterns=[] if args.fresh_object else ["*"],
        )
        if not ok:
            raise RuntimeError("SDK Sandbox runtime preparation failed")
        log(f"Prepared sandbox stub {sandbox.stub_id} with image {sandbox.image_id}")

    return sandbox


def write_fresh_object_marker(args, sync_dir):
    if not args.fresh_object:
        return

    marker = Path(sync_dir) / ".beta9-benchmark-object-nonce"
    marker.write_text(
        "\n".join(
            (
                "beta9 sandbox benchmark object marker",
                datetime.now(timezone.utc).isoformat(),
                str(time.monotonic_ns()),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    log(f"Wrote fresh sandbox object marker at {marker}")


def wait_running(args, token, container_id, request_start_ns):
    if not args.wait_running:
        return None

    deadline = time.monotonic() + args.timeout_seconds
    interval = args.poll_interval_ms / 1000
    last = None
    attempts = 0

    while time.monotonic() < deadline:
        attempts += 1
        try:
            status, body, raw, _ = http_json(
                "GET",
                api_url(args.gateway_url, API_PREFIX + f"/pods/{container_id}/status"),
                token=token,
                timeout=5,
            )
            last = body if status < 500 else {"raw": raw[:500]}
            if status == 200 and body.get("ok") and str(body.get("status", "")).lower() == "running":
                return {
                    "observedMs": (time.monotonic_ns() - request_start_ns) / 1_000_000,
                    "attempts": attempts,
                    "state": body,
                }
        except Exception as exc:
            last = {"error": str(exc)}
        time.sleep(interval)

    raise TimeoutError(f"Timed out waiting for {container_id} RUNNING; last response={last}")


def cleanup_sandbox(args, instance, sample):
    if not instance:
        return
    errors = []
    if args.cleanup_ttl_seconds > 0:
        try:
            instance.update_ttl(args.cleanup_ttl_seconds)
        except Exception as exc:
            errors.append(f"update_ttl: {exc}")
    try:
        sample["terminated"] = instance.terminate()
    except Exception as exc:
        errors.append(f"terminate: {exc}")
    if errors:
        sample["cleanupError"] = "; ".join(errors)


def is_transient_create_error(exc):
    message = str(exc)
    is_quota_capacity_error = (
        "concurrency_limit_reached" in message
        and ("cpu quota exceeded" in message or "gpu quota exceeded" in message)
    )
    if is_quota_capacity_error:
        return False

    return (
        "concurrency limit" in message
        or "Unavailable" in message
        or "connection refused" in message
        or "error reading from server" in message
        or "EOF" in message
    )


def create_sandbox_with_retries(args, sandbox):
    attempts = 0
    errors = []
    started_ns = time.monotonic_ns()

    while True:
        attempts += 1
        try:
            instance = sandbox.create()
            return instance, {
                "createAttempts": attempts,
                "createRetryMs": (time.monotonic_ns() - started_ns) / 1_000_000,
                "createErrors": errors,
            }
        except Exception as exc:
            errors.append(str(exc))
            if attempts > args.sandbox_create_retries or not is_transient_create_error(exc):
                raise
            time.sleep(min(2.0, 0.1 * attempts))


def exec_readiness(args, instance, command, request_start_ns, sample=None):
    deadline = time.monotonic() + args.sandbox_ready_timeout_seconds
    attempts = 0
    errors = []

    while True:
        attempts += 1
        exec_start_ns = time.monotonic_ns()
        try:
            process = instance.process.exec(command, cwd=args.sandbox_exec_cwd)
            exec_accepted_ns = time.monotonic_ns()
            result = {
                "execAttempts": attempts,
                "execErrors": errors,
                "execAcceptedMs": (exec_accepted_ns - exec_start_ns) / 1_000_000,
                "processReadyMs": (exec_accepted_ns - request_start_ns) / 1_000_000,
                "execCompleteMs": None,
                "execPid": process.pid,
                "execExitCode": None,
            }
            if args.wait_exec_complete:
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    raise RuntimeError("sandbox readiness command did not finish before timeout")
                exit_code = process.wait(timeout=remaining_seconds)
                exec_done_ns = time.monotonic_ns()
                result["execCompleteMs"] = (exec_done_ns - request_start_ns) / 1_000_000
                result["execExitCode"] = exit_code
            return result
        except Exception as exc:
            errors.append(str(exc))
            if sample is not None:
                sample["execAttempts"] = attempts
                sample["execErrors"] = list(errors)
                sample["lastExecError"] = str(exc)
            if time.monotonic() >= deadline:
                raise RuntimeError(f"sandbox readiness command did not start before timeout; last error: {exc}")
            time.sleep(args.sandbox_ready_retry_ms / 1000)


def run_sandbox_iteration(args, sandbox, token, index, warmup=False, start_event=None):
    if start_event is not None:
        start_event.wait()

    instance = None
    request_start_ns = time.monotonic_ns()
    sample = {
        "index": index,
        "warmup": warmup,
        "startedAt": datetime.now(timezone.utc).isoformat(),
    }

    try:
        instance, create_info = create_sandbox_with_retries(args, sandbox)
        accepted_ns = time.monotonic_ns()
        sample["acceptedMs"] = (accepted_ns - request_start_ns) / 1_000_000
        sample.update(create_info)
        sample["containerId"] = instance.container_id
        sample["stubId"] = instance.stub_id

        if not instance.ok:
            raise RuntimeError(instance.error_msg or "Sandbox.create returned ok=false")

        running = wait_running(args, token, instance.container_id, request_start_ns)
        if running:
            sample["runningObservedMs"] = running["observedMs"]
            sample["runningAttempts"] = running["attempts"]
            sample["containerState"] = running["state"]

        command = shlex.split(args.sandbox_exec)
        exec_info = exec_readiness(args, instance, command, request_start_ns, sample)
        sample.update(exec_info)
        if args.wait_exec_complete and sample["execExitCode"] != 0:
            raise RuntimeError(f"Sandbox readiness command exited {sample['execExitCode']}")

        sample["ok"] = True
    except Exception as exc:
        sample["ok"] = False
        sample["error"] = str(exc)
    finally:
        cleanup_sandbox(args, instance, sample)
        sample["finishedAt"] = datetime.now(timezone.utc).isoformat()

    return sample


def summarize_metric(samples, key):
    values = [
        sample.get(key)
        for sample in measured_ok_samples(samples)
        if sample.get(key) is not None
    ]
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


def measured_ok_samples(samples):
    return [sample for sample in samples if sample.get("ok") and not sample.get("warmup")]


def measured_failed_samples(samples):
    return [sample for sample in samples if sample.get("warmup") is False and not sample.get("ok")]


def print_sample(sample):
    status = "ok" if sample.get("ok") else "failed"
    print(
        f"{sample['index']:>4} "
        f"{'warmup' if sample.get('warmup') else 'bench':>6} "
        f"{sample.get('containerId', '-'):<46} "
        f"{format_ms(sample.get('acceptedMs')):>9} "
        f"{format_ms(sample.get('runningObservedMs')):>9} "
        f"{format_ms(sample.get('processReadyMs')):>9} "
        f"{format_ms(sample.get('execCompleteMs')):>9} "
        f"{status:>8}",
        flush=True,
    )
    if not sample.get("ok"):
        print(f"       error: {sample.get('error')}", flush=True)


def print_summary(summary):
    print("\nSummary, excluding warmup runs:")
    for key, label in [
        ("acceptedMs", "SDK create returned"),
        ("runningObservedMs", "observed RUNNING"),
        ("processReadyMs", "sandbox exec accepted"),
        ("execCompleteMs", "sandbox exec completed"),
    ]:
        data = summary.get(key)
        if not data:
            continue
        print(
            f"  {label:<28} "
            f"n={data['count']:<3} "
            f"min={format_ms(data['min'])}ms "
            f"p50={format_ms(data['p50'])}ms "
            f"p90={format_ms(data['p90'])}ms "
            f"p95={format_ms(data['p95'])}ms "
            f"max={format_ms(data['max'])}ms"
        )

    batch = summary.get("batch")
    if batch:
        print(
            f"  {'parallel batch wall':<28} "
            f"ok={batch['okCount']}/{batch['count']} "
            f"wall={format_ms(batch['wallMs'])}ms "
            f"throughput={batch['throughputPerSecond']:.2f}/s"
        )


def run_batch(args, sandbox, token, count, parallelism, start_index, warmup=False):
    start_event = threading.Event()
    samples = []

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = [
            executor.submit(
                run_sandbox_iteration,
                args,
                sandbox,
                token,
                start_index + offset,
                warmup,
                start_event,
            )
            for offset in range(count)
        ]
        start_event.set()
        for future in as_completed(futures):
            sample = future.result()
            samples.append(sample)
            print_sample(sample)

    return samples


def wait_for_prewarm_idle(args, redis_pod, samples):
    if not args.prewarm_wait_idle:
        return None

    container_ids = [sample.get("containerId") for sample in samples if sample.get("containerId")]
    if not container_ids:
        return None

    if not redis_pod:
        log("Redis pod unavailable; using fixed prewarm cooldown without state validation")
        return None

    active_statuses = {"PENDING", "RUNNING"}
    deadline = time.monotonic() + args.prewarm_idle_timeout_seconds
    interval = max(0.1, args.poll_interval_ms / 1000)
    last_counts = {}

    while time.monotonic() < deadline:
        counts = {}
        active = 0
        for container_id in container_ids:
            state = redis_hgetall(args.namespace, redis_pod, f"scheduler:container:state:{container_id}")
            status = (state or {}).get("status", "")
            counts[status or "missing"] = counts.get(status or "missing", 0) + 1
            if status in active_statuses:
                active += 1

        last_counts = counts
        if active == 0:
            log(f"Prewarm workers are idle; container states: {counts}")
            return counts

        time.sleep(interval)

    raise TimeoutError(
        f"Timed out waiting for prewarm sandboxes to stop before measured batch; last states={last_counts}"
    )


def run_parallel_samples(args, sandbox, token, redis_pod):
    samples = []

    print(TABLE_HEADER)

    if args.prewarm_count > 0:
        log(f"Prewarming worker pool with {args.prewarm_count} sandbox(es)")
        prewarm_samples = run_batch(
            args,
            sandbox,
            token,
            args.prewarm_count,
            args.prewarm_parallelism,
            1,
            warmup=True,
        )
        samples.extend(prewarm_samples)
        wait_for_prewarm_idle(args, redis_pod, prewarm_samples)
        if args.prewarm_cooldown_seconds > 0:
            log(f"Waiting {args.prewarm_cooldown_seconds:g}s for prewarm sandboxes to terminate")
            time.sleep(args.prewarm_cooldown_seconds)

    for index in range(1, args.warmup + 1):
        sample = run_sandbox_iteration(args, sandbox, token, index, warmup=True)
        samples.append(sample)
        print_sample(sample)

    batch_start_ns = time.monotonic_ns()
    samples.extend(
        run_batch(
            args,
            sandbox,
            token,
            args.count,
            args.parallelism,
            args.warmup + args.prewarm_count + 1,
            warmup=False,
        )
    )

    batch_wall_ms = (time.monotonic_ns() - batch_start_ns) / 1_000_000
    ok_count = len(measured_ok_samples(samples))
    return samples, {
        "count": args.count,
        "okCount": ok_count,
        "wallMs": batch_wall_ms,
        "throughputPerSecond": ok_count / (batch_wall_ms / 1000) if batch_wall_ms > 0 else 0,
    }


def main():
    args = parse_args()
    require_tools("kubectl")

    if args.install:
        install_overlay(args.namespace, args.timeout_seconds)

    if args.port_forward:
        start_http_port_forward_if_needed(args.namespace, args.gateway_url, args.timeout_seconds)
        start_grpc_port_forward_if_needed(args)
    else:
        wait_http(args.gateway_url, HEALTH_PATH, timeout_seconds=args.timeout_seconds)

    load_cached_token(args)
    token, workspace_id = authorize(args.gateway_url, args.token)
    save_cached_token(args, token)
    args.token = token
    if workspace_id:
        log(f"Authorized workspace {workspace_id}")

    prepare_local_image(args)
    write_sdk_config(args)

    if args.reset_workers:
        log("Deleting worker jobs before sandbox benchmark")
        delete_workers(args.namespace)

    redis_pod = find_redis_pod(args.namespace)
    if redis_pod:
        log(f"Redis pod {redis_pod} is available for cluster snapshots")

    original_cwd = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="beta9-sandbox-benchmark-") as sdk_sync_dir:
        os.chdir(sdk_sync_dir)
        try:
            write_fresh_object_marker(args, sdk_sync_dir)
            sandbox = prepare_sandbox_runtime(args)
            samples, batch = run_parallel_samples(args, sandbox, args.token, redis_pod)
        finally:
            os.chdir(original_cwd)

    summary = {
        "acceptedMs": summarize_metric(samples, "acceptedMs"),
        "runningObservedMs": summarize_metric(samples, "runningObservedMs"),
        "processReadyMs": summarize_metric(samples, "processReadyMs"),
        "execCompleteMs": summarize_metric(samples, "execCompleteMs"),
        "batch": batch,
    }
    print_summary(summary)

    report = {
        "startedAt": samples[0]["startedAt"] if samples else datetime.now(timezone.utc).isoformat(),
        "finishedAt": datetime.now(timezone.utc).isoformat(),
        "config": {
            "namespace": args.namespace,
            "gatewayUrl": args.gateway_url,
            "grpcAddr": grpc_addr(args),
            "count": args.count,
            "parallelism": args.parallelism,
            "prewarmCount": args.prewarm_count,
            "prewarmParallelism": args.prewarm_parallelism,
            "prewarmCooldownSeconds": args.prewarm_cooldown_seconds,
            "prewarmWaitIdle": args.prewarm_wait_idle,
            "prewarmIdleTimeoutSeconds": args.prewarm_idle_timeout_seconds,
            "warmup": args.warmup,
            "sandboxCpu": args.sandbox_cpu,
            "sandboxMemory": args.sandbox_memory,
            "sandboxKeepWarmSeconds": args.sandbox_keep_warm_seconds,
            "sandboxImageUri": args.sandbox_image_uri,
            "sandboxExec": args.sandbox_exec,
            "sandboxExecCwd": args.sandbox_exec_cwd,
            "prepareSandbox": args.prepare_sandbox,
            "waitRunning": args.wait_running,
            "waitExecComplete": args.wait_exec_complete,
            "sandboxCreateRetries": args.sandbox_create_retries,
            "sandboxReadyTimeoutSeconds": args.sandbox_ready_timeout_seconds,
            "sandboxReadyRetryMs": args.sandbox_ready_retry_ms,
            "freshObject": args.fresh_object,
        },
        "sandbox": {
            "stubId": getattr(sandbox, "stub_id", ""),
            "imageId": getattr(sandbox, "image_id", ""),
        },
        "summary": summary,
        "samples": sorted(samples, key=lambda sample: sample["index"]),
        "cluster": cluster_snapshot(args.namespace),
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    log(f"Wrote sandbox benchmark report to {output_path}")

    failures = measured_failed_samples(samples)
    if failures:
        raise SystemExit(f"{len(failures)} measured sandbox benchmark run(s) failed")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
