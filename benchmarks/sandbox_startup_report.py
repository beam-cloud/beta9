#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

try:
    from .startup import api_url, http_json
except ImportError:  # pragma: no cover - script execution path
    from startup import api_url, http_json


EVENTS_API_PREFIX = "/api/v1/events"
DEFAULT_EVENT_LIMIT = 50_000
DEFAULT_TOP_LIFECYCLE = 20
DEFAULT_EVENT_WAIT_SECONDS = 5.0
DEFAULT_EVENT_POLL_MS = 500

REQUIRED_LIFECYCLE_IDS = (
    "container.startup",
    "image.load",
    "runtime.start_to_pid",
)

REQUIRED_SUMMARY_KEYS = (
    "scheduler_queue_to_worker_receive_ms",
    "worker_queue_ms",
    "worker_receive_to_running_ms",
    "image_ms",
    "network_ms",
    "runtime_ms",
    "running_to_first_log_ms",
)

CLIENT_TIMINGS = (
    ("acceptedMs", "SDK create returned"),
    ("runningObservedMs", "Observed RUNNING"),
    ("processReadyMs", "Sandbox exec accepted"),
    ("execCompleteMs", "Time to interactive"),
)

SERVER_PHASES = (
    ("scheduler_ms", "scheduler", "Scheduler total", True),
    ("scheduler_queue_push_ms", "scheduler.queue_push", "Queue request", False),
    ("scheduler_backlog_ms", "scheduler.backlog_wait", "Scheduler backlog wait", False),
    ("scheduler_worker_selection_ms", "scheduler.worker_selection", "Worker selection", False),
    ("scheduler_reservation_ms", "scheduler.reservation", "Capacity reservation", False),
    ("scheduler_provision_worker_ms", "scheduler.provision_worker", "Worker provisioning", False),
    ("scheduler_queue_to_worker_receive_ms", "scheduler.queue_to_worker_receive", "Scheduler queue to worker receive", False),
    ("worker_ms", "worker", "Worker total", True),
    ("worker_queue_ms", "worker.queue_receive", "Worker queue receive", False),
    ("worker_receive_to_running_ms", "worker.receive_to_running", "Worker receive to running", True),
    ("worker_set_worker_address_ms", "worker.set_worker_address", "Set worker address", False),
    ("worker_port_allocation_ms", "worker.port_allocation", "Port allocation", False),
    ("worker_read_bundle_config_ms", "worker.read_bundle_config", "Read bundle config", False),
    ("worker_spec_from_request_ms", "worker.spec_from_request", "Spec from request", False),
    ("worker_set_container_address_ms", "worker.set_container_address", "Set container address", False),
    ("worker_set_address_map_ms", "worker.set_address_map", "Set address map", False),
    ("worker_gpu_assignment_ms", "worker.gpu_assignment", "GPU assignment", False),
    ("worker_start_queue_wait_ms", "worker.start_queue_wait", "Worker start queue wait", False),
    ("image_ms", "image.load", "Image load", False),
    ("image_local_mount_lock_ms", "image.local_mount_lock", "Image local mount lock", False),
    ("image_remote_mount_lock_ms", "image.remote_mount_lock", "Image remote mount lock", False),
    ("image_mount_archive_ms", "image.mount_archive", "Image mount archive", False),
    ("image_registry_pull_ms", "image.registry_pull", "Image registry pull", False),
    ("image_clip_metadata_extract_ms", "image.clip_metadata_extract", "Image CLIP metadata extract", False),
    ("image_clip_mounted_fuse_hit_ms", "image.clip_mounted_fuse_hit", "Image mounted FUSE hit", False),
    (
        "image_clip_mounted_fuse_hit_after_local_lock_ms",
        "image.clip_mounted_fuse_hit_after_local_lock",
        "Image mounted FUSE hit after local lock",
        False,
    ),
    (
        "image_clip_mounted_fuse_hit_after_lock_ms",
        "image.clip_mounted_fuse_hit_after_lock",
        "Image mounted FUSE hit after remote lock",
        False,
    ),
    ("image_embedded_cache_metadata_copy_ms", "image.embedded_cache_metadata_copy", "Image embedded cache metadata copy", False),
    ("image_embedded_cache_store_ms", "image.embedded_cache_store", "Image embedded cache store", False),
    ("image_embedded_cache_restore_ms", "image.embedded_cache_restore", "Image embedded cache restore", False),
    ("clip_ms", "clip.read", "CLIP lazy reads", False),
    ("clip_read_ms", "clip.read", "CLIP read", False),
    ("clip_oci_read_ms", "clip.oci_read", "CLIP OCI read", False),
    ("clip_archive_read_ms", "clip.archive_read", "CLIP archive read", False),
    ("clip_disk_cache_read_ms", "clip.disk_cache_read", "CLIP disk cache read", False),
    ("clip_content_cache_read_ms", "clip.content_cache_read", "CLIP content cache read", False),
    ("clip_checkpoint_read_ms", "clip.checkpoint_read", "CLIP checkpoint read", False),
    ("clip_layer_decompress_ms", "clip.layer_decompress", "CLIP layer decompress", False),
    ("clip_layer_decompress_wait_ms", "clip.layer_decompress_wait", "CLIP layer decompress wait", False),
    ("mount_ms", "mount", "Mount total", True),
    ("mount_setup_ms", "mount.setup", "Mount setup", False),
    ("mount_overlay_setup_ms", "mount.overlay_setup", "Overlay setup", False),
    ("network_ms", "network", "Network total", True),
    ("network_setup_ms", "network.setup", "Network setup", False),
    ("network_create_veth_ms", "network.create_veth", "Create veth pair", False),
    ("network_setup_bridge_ms", "network.setup_bridge", "Setup bridge", False),
    ("network_create_namespace_ms", "network.create_namespace", "Create namespace", False),
    ("network_configure_namespace_ms", "network.configure_namespace", "Configure namespace", False),
    ("network_ip_lock_ms", "network.ip_lock", "Acquire IP lock", False),
    ("network_ip_load_ms", "network.ip_load", "Load allocated IPs", False),
    ("network_ip_assign_ms", "network.ip_assign", "Assign container IP", False),
    ("network_set_container_ip_ms", "network.set_container_ip", "Persist container IP", False),
    ("network_restrictions_ms", "network.restrictions", "Network restrictions", False),
    ("network_expose_ports_ms", "network.expose_ports", "Expose ports", False),
    ("runtime_ms", "container.startup", "Runtime/container startup", False),
    ("runtime_config_write_ms", "runtime.config_write", "Runtime config write", False),
    ("runtime_start_to_pid_ms", "runtime.start_to_pid", "Runtime start to PID", False),
    ("sandbox_process_manager_tcp_ready_ms", "sandbox.process_manager_tcp_ready", "Sandbox process manager TCP ready", False),
    ("sandbox_process_manager_ready_ms", "sandbox.process_manager_ready", "Sandbox process manager ready", False),
    ("running_to_first_log_ms", "logs.first_byte", "RUNNING to first log", False),
    ("running_to_runner_process_started_ms", "runner.process_started", "RUNNING to runner process", False),
    ("running_to_runner_main_ms", "runner.main_entered", "RUNNING to runner main", False),
    ("runner_process_to_module_loaded_ms", "runner.module_loaded", "Runner process to module loaded", False),
    ("runner_module_loaded_to_main_ms", "runner.main_entered", "Runner module loaded to main", False),
    ("runner_main_to_start_task_ms", "runner.start_task", "Runner main to StartTask", False),
    ("runner_ms", "runner", "Runner total", True),
    ("runner_start_to_get_args_ms", "runner.start_task_to_get_args", "Runner StartTask to GetArgs", False),
    ("runner_get_args_to_set_result_ms", "runner.get_args_to_set_result", "Runner GetArgs to SetResult", False),
    ("runner_start_to_set_result_ms", "runner.start_task_to_set_result", "Runner StartTask to SetResult", False),
    ("runner_start_to_end_task_ms", "runner.start_task_to_end_task", "Runner StartTask to EndTask", False),
    ("runner_gateway_channel_open_ms", "runner.gateway_channel_open", "Runner gateway channel open", False),
    ("runner_start_task_rpc_ms", "runner.start_task_rpc", "Runner StartTask RPC", False),
    ("runner_get_args_rpc_ms", "runner.get_args_rpc", "Runner GetArgs RPC", False),
    ("runner_user_code_import_ms", "runner.user_code_import", "Runner user code import", False),
    ("runner_handler_execution_ms", "runner.handler_execution", "Runner handler execution", False),
    ("runner_set_result_rpc_ms", "runner.set_result_rpc", "Runner SetResult RPC", False),
    ("runner_end_task_rpc_ms", "runner.end_task_rpc", "Runner EndTask RPC", False),
    ("result_ms", "result", "Result total", True),
    ("result_set_to_end_task_ms", "result.set_result_to_end_task", "SetResult to EndTask", False),
    ("result_delivery_ms", "result.delivery", "Result delivery", False),
)

def default_markdown_path(output_path: str | Path) -> Path:
    return Path(output_path).with_suffix(".md")


def percentile(values: list[float], p: float) -> float | None:
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (len(clean) - 1) * (p / 100)
    lower = int(rank)
    upper = min(lower + 1, len(clean) - 1)
    weight = rank - lower
    return clean[lower] * (1 - weight) + clean[upper] * weight


def summarize_values(values: list[float]) -> dict[str, float] | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return {
        "count": len(clean),
        "min": min(clean),
        "p50": percentile(clean, 50),
        "p90": percentile(clean, 90),
        "p95": percentile(clean, 95),
        "p99": percentile(clean, 99),
        "max": max(clean),
    }


def measured_samples(benchmark: dict[str, Any]) -> list[dict[str, Any]]:
    return [sample for sample in benchmark.get("samples", []) if not sample.get("warmup")]


def interactive_samples(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        sample
        for sample in samples
        if sample.get("ok")
        and sample.get("execCompleteMs") is not None
        and sample.get("execExitCode") in (None, 0)
        and sample_command_verified(sample)
    ]


def sample_command_verified(sample: dict[str, Any]) -> bool:
    if "execVerified" in sample:
        return bool(sample.get("execVerified"))
    return True


def sample_metric_summary(samples: list[dict[str, Any]], key: str) -> dict[str, float] | None:
    return summarize_values(
        [sample.get(key) for sample in samples if sample.get("ok") and sample.get(key) is not None]
    )


def fetch_event_batch(
    gateway_url: str,
    workspace_id: str,
    token: str,
    container_ids: list[str],
    *,
    targets: list[dict[str, str]] | None = None,
    limit: int = DEFAULT_EVENT_LIMIT,
    top_lifecycle: int = DEFAULT_TOP_LIFECYCLE,
    timeout: int = 20,
) -> dict[str, Any]:
    body = {
        "include_events": False,
        "top_lifecycle": top_lifecycle,
        "limit": limit,
    }
    if targets:
        body["targets"] = targets
    else:
        body["container_ids"] = container_ids
    status, payload, raw, _ = http_json(
        "POST",
        api_url(gateway_url, f"{EVENTS_API_PREFIX}/{workspace_id}/containers/batch"),
        token=token,
        body=body,
        timeout=timeout,
    )
    if status != 200:
        detail = payload.get("message") if isinstance(payload, dict) else raw
        raise RuntimeError(f"events API returned HTTP {status}: {detail or raw[:500]}")
    return payload


def fetch_event_batch_with_poll(
    gateway_url: str,
    workspace_id: str,
    token: str,
    container_ids: list[str],
    *,
    targets: list[dict[str, str]] | None = None,
    limit: int = DEFAULT_EVENT_LIMIT,
    top_lifecycle: int = DEFAULT_TOP_LIFECYCLE,
    wait_seconds: float = DEFAULT_EVENT_WAIT_SECONDS,
    poll_ms: int = DEFAULT_EVENT_POLL_MS,
) -> tuple[dict[str, Any] | None, str]:
    if not container_ids and not targets:
        return None, "no measured container IDs"
    if not workspace_id:
        return None, "workspace ID unavailable"
    if not token:
        return None, "token unavailable"

    deadline = time.monotonic() + max(0, wait_seconds)
    best: dict[str, Any] | None = None
    best_score = (-1, -1, -1)
    last_error = ""

    while True:
        try:
            payload = fetch_event_batch(
                gateway_url,
                workspace_id,
                token,
                container_ids,
                targets=targets,
                limit=limit,
                top_lifecycle=top_lifecycle,
            )
            score = event_batch_score(payload)
            if score > best_score:
                best = payload
                best_score = score
        except Exception as exc:  # noqa: BLE001 - diagnostics should survive partial failures
            last_error = str(exc)

        if time.monotonic() >= deadline:
            return best, last_error
        time.sleep(max(0.05, poll_ms / 1000))


def event_batch_score(payload: dict[str, Any]) -> tuple[int, int, int, int, int]:
    eventful = 0
    present_required = 0
    missing_required = 0
    total_events = 0
    total_summaries = 0
    for item in payload.get("items", []):
        if item.get("error") or item.get("event_count", 0) <= 0:
            missing_required += len(REQUIRED_LIFECYCLE_IDS) + len(REQUIRED_SUMMARY_KEYS)
            continue
        if item.get("event_count", 0) > 0:
            eventful += 1
            total_events += int(item.get("event_count") or 0)
            total_summaries += len(item.get("summary") or {})
        missing = set(item.get("missing") or [])
        summary = item.get("summary") or {}
        missing_required += sum(1 for event_id in REQUIRED_LIFECYCLE_IDS if event_id in missing)
        present_required += sum(1 for event_id in REQUIRED_LIFECYCLE_IDS if event_id not in missing)
        missing_required += sum(1 for key in REQUIRED_SUMMARY_KEYS if key not in summary)
        present_required += sum(1 for key in REQUIRED_SUMMARY_KEYS if key in summary)
    return eventful, present_required, -missing_required, total_events, total_summaries


def build_startup_report(
    benchmark: dict[str, Any],
    *,
    events: dict[str, Any] | None = None,
    event_error: str = "",
    markdown_path: str = "",
) -> dict[str, Any]:
    samples = measured_samples(benchmark)
    ok_samples = [sample for sample in samples if sample.get("ok")]
    interactive = interactive_samples(samples)
    client_timings = build_client_timings(benchmark, samples)
    event_items = events.get("items", []) if events else []
    event_by_container = {item.get("container_id"): item for item in event_items if item.get("container_id")}
    server_phases = build_server_phases(events, len(samples))
    bottleneck = normalize_api_phase(events.get("primary_bottleneck")) if events else None
    if not bottleneck:
        bottleneck = select_primary_bottleneck(server_phases, len(samples))
    coverage = build_event_coverage(samples, events, event_items)
    exec_verification = build_exec_verification(samples)
    image_drilldown = build_image_drilldown(event_items, server_phases, len(samples))
    slowest = build_slowest_containers(samples, event_by_container)
    data_quality = build_data_quality(samples, coverage, event_items, event_error, bottleneck, exec_verification)
    tti = client_timings.get("execCompleteMs")

    verdict_text = f"{len(interactive)}/{len(samples)} sandboxes interactive"
    if len(interactive) != len(samples):
        verdict_text += f" ({len(ok_samples)}/{len(samples)} benchmark samples ok)"

    return {
        "markdownPath": markdown_path,
        "verdict": {
            "interactive": len(interactive),
            "total": len(samples),
            "ok": len(ok_samples),
            "text": verdict_text,
        },
        "timeToInteractive": tti,
        "primaryBottleneck": bottleneck,
        "eventCoverage": coverage,
        "execVerification": exec_verification,
        "clientTimings": client_timings,
        "serverPhases": server_phases,
        "imageDrilldown": image_drilldown,
        "slowestContainers": slowest,
        "dataQuality": data_quality,
        "eventError": event_error,
    }


def build_client_timings(benchmark: dict[str, Any], samples: list[dict[str, Any]]) -> dict[str, Any]:
    existing = benchmark.get("summary") or {}
    timings: dict[str, Any] = {}
    for key, _ in CLIENT_TIMINGS:
        source_samples = interactive_samples(samples) if key == "execCompleteMs" else samples
        timings[key] = existing.get(key) or sample_metric_summary(source_samples, key)
    timings["batch"] = existing.get("batch")
    return timings


def build_exec_verification(samples: list[dict[str, Any]]) -> dict[str, Any]:
    verified = [sample for sample in samples if sample.get("execVerified") is True]
    failed = [sample for sample in samples if sample.get("execVerified") is False]
    unknown = [sample for sample in samples if "execVerified" not in sample]
    completed = [sample for sample in samples if sample.get("execExitCode") is not None]
    mismatched = [sample for sample in samples if sample.get("execOutputMatched") is False]
    nonzero = [
        sample
        for sample in samples
        if sample.get("execExitCode") is not None and sample.get("execExitCode") != 0
    ]
    return {
        "count": len(samples),
        "completed": len(completed),
        "verified": len(verified),
        "failed": len(failed),
        "unknown": len(unknown),
        "mismatched": len(mismatched),
        "nonzeroExit": len(nonzero),
    }


def build_server_phases(events: dict[str, Any] | None, measured_count: int) -> list[dict[str, Any]]:
    if not events:
        return []

    api_phases = events.get("phases") or []
    if api_phases:
        phases = [phase for phase in (normalize_api_phase(row) for row in api_phases) if phase]
        if phases:
            return phases

    summary = events.get("summary") or {}
    phases: list[dict[str, Any]] = []
    for metric_key, event_id, label, rollup in SERVER_PHASES:
        metric = summary.get(metric_key)
        if not metric:
            continue
        phases.append(phase_record(metric_key, event_id, label, metric, measured_count, rollup))

    return phases


def normalize_api_phase(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    metric_key = row.get("metric_key") or row.get("metricKey")
    if metric_key and (metric_key == "to_running_ms" or not str(metric_key).endswith("_ms")):
        return None
    return {
        "metricKey": metric_key,
        "eventId": row.get("event_id") or row.get("eventId"),
        "label": row.get("label") or row.get("event_id") or row.get("eventId") or row.get("metric_key"),
        "domain": row.get("domain"),
        "parentId": row.get("parent_id") or row.get("parentId"),
        "count": int(row.get("count") or 0),
        "coverage": float(row.get("coverage") or 0),
        "coverageStatus": row.get("coverage_status") or row.get("coverageStatus") or coverage_status(float(row.get("coverage") or 0)),
        "rollup": bool(row.get("rollup")),
        "minMs": row.get("min_ms") if "min_ms" in row else row.get("minMs"),
        "avgMs": row.get("avg_ms") if "avg_ms" in row else row.get("avgMs"),
        "p50Ms": row.get("p50_ms") if "p50_ms" in row else row.get("p50Ms"),
        "p90Ms": row.get("p90_ms") if "p90_ms" in row else row.get("p90Ms"),
        "p95Ms": row.get("p95_ms") if "p95_ms" in row else row.get("p95Ms"),
        "p99Ms": row.get("p99_ms") if "p99_ms" in row else row.get("p99Ms"),
        "maxMs": row.get("max_ms") if "max_ms" in row else row.get("maxMs"),
        "totalMs": row.get("total_ms") if "total_ms" in row else row.get("totalMs"),
    }


def build_image_drilldown(
    event_items: list[dict[str, Any]],
    server_phases: list[dict[str, Any]],
    measured_count: int,
) -> dict[str, Any]:
    image_phases = [
        phase
        for phase in server_phases
        if phase.get("eventId", "").startswith(("image.", "clip."))
    ]
    image_phases.sort(
        key=lambda phase: (
            float(phase.get("p95Ms") or 0),
            float(phase.get("maxMs") or 0),
            int(phase.get("count") or 0),
        ),
        reverse=True,
    )

    clip_accesses, containers_with_clip = summarize_clip_accesses(event_items)
    slowest_phase = image_phases[0] if image_phases else None
    notes = []
    if not image_phases:
        notes.append("No image or CLIP lifecycle phases were returned by the events API.")
    if not clip_accesses:
        notes.append(
            "No CLIP lazy-read access rollups were returned. This can mean no lazy reads occurred, "
            "the container did not reach user code, or CLIP read tracing did not flush for this run."
        )
    if containers_with_clip and measured_count and containers_with_clip < measured_count:
        notes.append(
            f"CLIP access rollups were present for {containers_with_clip}/{measured_count} measured containers."
        )

    return {
        "phases": image_phases,
        "slowestPhase": slowest_phase,
        "clipAccesses": clip_accesses,
        "containersWithClipAccesses": containers_with_clip,
        "measuredContainers": measured_count,
        "notes": notes,
    }


def summarize_clip_accesses(
    event_items: list[dict[str, Any]],
    *,
    limit: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    rollups: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    containers_with_clip = set()

    for item in event_items:
        container_id = item.get("container_id")
        accesses = item.get("clip_accesses") or []
        if accesses and container_id:
            containers_with_clip.add(container_id)

        for access in accesses:
            key = (
                str(access.get("operation") or ""),
                str(access.get("path") or ""),
                str(access.get("source") or ""),
                str(access.get("layer_digest") or ""),
                str(access.get("decompressed_hash") or ""),
                str(access.get("content_hash") or ""),
            )
            rollup = rollups.get(key)
            if rollup is None:
                rollup = {
                    "operation": key[0],
                    "path": key[1],
                    "source": key[2],
                    "layerDigest": key[3],
                    "decompressedHash": key[4],
                    "contentHash": key[5],
                    "count": 0,
                    "containerCount": 0,
                    "totalUs": 0,
                    "maxUs": 0,
                    "totalMs": 0,
                    "maxMs": 0,
                    "bytesRead": 0,
                    "_containers": set(),
                }
                rollups[key] = rollup

            count = int(access.get("count") or 0)
            total_us = int(access.get("total_us") or 0)
            max_us = int(access.get("max_us") or 0)
            if total_us <= 0:
                total_us = int(access.get("total_ms") or 0) * 1000
            if max_us <= 0:
                max_us = int(access.get("max_ms") or 0) * 1000

            rollup["count"] += count
            rollup["totalUs"] += total_us
            rollup["maxUs"] = max(int(rollup["maxUs"]), max_us)
            rollup["bytesRead"] += int(access.get("bytes_read") or 0)
            if container_id:
                rollup["_containers"].add(container_id)

    rows = []
    for rollup in rollups.values():
        row = dict(rollup)
        containers = row.pop("_containers")
        row["containerCount"] = len(containers)
        row["totalMs"] = duration_us_to_ms(int(row["totalUs"]))
        row["maxMs"] = duration_us_to_ms(int(row["maxUs"]))
        rows.append(row)

    rows.sort(
        key=lambda row: (
            int(row.get("totalUs") or 0),
            int(row.get("maxUs") or 0),
            int(row.get("count") or 0),
        ),
        reverse=True,
    )
    return rows[:limit], len(containers_with_clip)


def phase_record(
    metric_key: str,
    event_id: str,
    label: str,
    metric: dict[str, Any],
    measured_count: int,
    rollup: bool,
) -> dict[str, Any]:
    count = int(metric.get("count") or 0)
    coverage = count / measured_count if measured_count else 0
    return {
        "metricKey": metric_key,
        "eventId": event_id,
        "label": label,
        "count": count,
        "coverage": coverage,
        "coverageStatus": coverage_status(coverage),
        "rollup": rollup,
        "minMs": metric.get("min_ms"),
        "avgMs": metric.get("avg_ms"),
        "p50Ms": metric.get("p50_ms"),
        "p90Ms": metric.get("p90_ms"),
        "p95Ms": metric.get("p95_ms"),
        "p99Ms": metric.get("p99_ms"),
        "maxMs": metric.get("max_ms"),
        "totalMs": metric.get("total_ms"),
    }


def coverage_status(coverage: float) -> str:
    if coverage >= 0.95:
        return "full"
    if coverage >= 0.50:
        return "partial"
    if coverage > 0:
        return "low"
    return "missing"


def select_primary_bottleneck(phases: list[dict[str, Any]], measured_count: int) -> dict[str, Any] | None:
    candidates = [
        phase
        for phase in phases
        if not phase.get("rollup") and phase.get("p95Ms") is not None and phase.get("coverage", 0) >= 0.5
    ]
    if not candidates:
        candidates = [
            phase
            for phase in phases
            if not phase.get("rollup") and phase.get("p95Ms") is not None and phase.get("count", 0) > 0
        ]
    if not candidates:
        candidates = [phase for phase in phases if phase.get("p95Ms") is not None and phase.get("count", 0) > 0]
    if not candidates:
        return None

    winner = max(candidates, key=lambda phase: (float(phase.get("p95Ms") or 0), int(phase.get("count") or 0)))
    result = dict(winner)
    if measured_count and result.get("count", 0) < measured_count:
        result["note"] = f"covers {result['count']}/{measured_count} measured containers"
    return result


def build_event_coverage(
    samples: list[dict[str, Any]],
    events: dict[str, Any] | None,
    event_items: list[dict[str, Any]],
) -> dict[str, Any]:
    if events and events.get("coverage"):
        return normalize_api_coverage(events["coverage"])

    requested = len([sample for sample in samples if sample.get("containerId")])
    item_by_container = {item.get("container_id"): item for item in event_items if item.get("container_id")}
    missing_containers = [
        sample.get("containerId")
        for sample in samples
        if sample.get("containerId") and sample.get("containerId") not in item_by_container
    ]
    event_errors = [item for item in event_items if item.get("error")]
    eventful = len([item for item in event_items if item.get("event_count", 0) > 0 and not item.get("error")])
    missing_required_by_id = {event_id: 0 for event_id in REQUIRED_LIFECYCLE_IDS}
    missing_required_by_metric = {key: 0 for key in REQUIRED_SUMMARY_KEYS}

    for container_id in missing_containers:
        if container_id:
            for event_id in REQUIRED_LIFECYCLE_IDS:
                missing_required_by_id[event_id] += 1
            for key in REQUIRED_SUMMARY_KEYS:
                missing_required_by_metric[key] += 1
    for item in event_items:
        missing = set(item.get("missing") or [])
        summary = item.get("summary") or {}
        if item.get("error") or item.get("event_count", 0) <= 0:
            missing = set(REQUIRED_LIFECYCLE_IDS)
            summary = {}
        for event_id in REQUIRED_LIFECYCLE_IDS:
            if event_id in missing:
                missing_required_by_id[event_id] += 1
        for key in REQUIRED_SUMMARY_KEYS:
            if key not in summary:
                missing_required_by_metric[key] += 1

    total_required = requested * len(REQUIRED_LIFECYCLE_IDS)
    missing_required = sum(missing_required_by_id.values())
    present_required = max(0, total_required - missing_required)
    total_required_metrics = requested * len(REQUIRED_SUMMARY_KEYS)
    missing_required_metrics = sum(missing_required_by_metric.values())
    present_required_metrics = max(0, total_required_metrics - missing_required_metrics)

    return {
        "requestedContainers": requested,
        "items": len(event_items),
        "containersWithEvents": eventful,
        "eventErrors": len(event_errors),
        "missingContainers": missing_containers,
        "requiredLifecyclePresent": present_required,
        "requiredLifecycleTotal": total_required,
        "requiredLifecycleMissing": missing_required_by_id,
        "requiredMetricPresent": present_required_metrics,
        "requiredMetricTotal": total_required_metrics,
        "requiredMetricMissing": missing_required_by_metric,
        "eventsAvailable": bool(events),
    }


def normalize_api_coverage(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "requestedContainers": int(row.get("requested_containers") or row.get("requestedContainers") or 0),
        "items": int(row.get("items") or 0),
        "containersWithEvents": int(row.get("containers_with_events") or row.get("containersWithEvents") or 0),
        "eventErrors": int(row.get("event_errors") or row.get("eventErrors") or 0),
        "missingContainers": row.get("missing_containers") or row.get("missingContainers") or [],
        "requiredLifecyclePresent": int(row.get("required_lifecycle_present") or row.get("requiredLifecyclePresent") or 0),
        "requiredLifecycleTotal": int(row.get("required_lifecycle_total") or row.get("requiredLifecycleTotal") or 0),
        "requiredLifecycleMissing": row.get("required_lifecycle_missing") or row.get("requiredLifecycleMissing") or {},
        "requiredMetricPresent": int(row.get("required_metric_present") or row.get("requiredMetricPresent") or 0),
        "requiredMetricTotal": int(row.get("required_metric_total") or row.get("requiredMetricTotal") or 0),
        "requiredMetricMissing": row.get("required_metric_missing") or row.get("requiredMetricMissing") or {},
        "eventsAvailable": True,
    }


def build_slowest_containers(
    samples: list[dict[str, Any]],
    event_by_container: dict[str, dict[str, Any]],
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    ordered = sorted(
        samples,
        key=lambda sample: sample.get("execCompleteMs") if sample.get("execCompleteMs") is not None else -1,
        reverse=True,
    )
    rows = []
    for sample in ordered[:limit]:
        container_id = sample.get("containerId", "")
        item = event_by_container.get(container_id) or {}
        slowest_phase = first_slowest_phase(item)
        running_ms = sample.get("runningObservedMs")
        exec_complete_ms = sample.get("execCompleteMs")
        rows.append(
            {
                "containerId": container_id,
                "ok": sample.get("ok"),
                "timeToInteractiveMs": exec_complete_ms,
                "runningObservedMs": running_ms,
                "postRunningInteractiveGapMs": (
                    exec_complete_ms - running_ms
                    if exec_complete_ms is not None and running_ms is not None
                    else None
                ),
                "slowestPhase": slowest_phase,
                "missing": item.get("missing") or [],
                "error": sample.get("error") or item.get("error") or "",
            }
        )
    return rows


def first_slowest_phase(item: dict[str, Any]) -> dict[str, Any] | None:
    summary = item.get("summary") or {}
    candidates = []
    for metric_key, event_id, _label, rollup in SERVER_PHASES:
        if rollup or metric_key not in summary:
            continue
        candidates.append(
            {
                "eventId": event_id,
                "durationMs": summary.get(metric_key),
                "coverage": "present",
            }
        )
    if candidates:
        return max(candidates, key=lambda value: value.get("durationMs") or 0)

    phases = item.get("slowest_lifecycle") or item.get("lifecycle") or []
    if not phases:
        return None
    phase = max(phases, key=lambda value: value.get("duration_ms") or 0)
    return {
        "eventId": phase.get("event_id"),
        "durationMs": phase.get("duration_ms"),
        "coverage": "present",
    }


def build_data_quality(
    samples: list[dict[str, Any]],
    coverage: dict[str, Any],
    event_items: list[dict[str, Any]],
    event_error: str,
    bottleneck: dict[str, Any] | None,
    exec_verification: dict[str, Any],
) -> list[str]:
    notes = []
    if exec_verification.get("failed"):
        notes.append(
            f"{exec_verification['failed']} sandbox readiness command check(s) failed."
        )
    if exec_verification.get("unknown"):
        notes.append(
            f"{exec_verification['unknown']} sample(s) did not include command verification metadata."
        )
    if event_error:
        notes.append(f"Events API issue: {event_error}")
    if not coverage.get("eventsAvailable"):
        notes.append("Server-side event data is unavailable; bottleneck evidence is limited to client timings.")
    if coverage.get("missingContainers"):
        notes.append(
            f"{len(coverage['missingContainers'])} measured container(s) were absent from the events response."
        )
    if coverage.get("eventErrors"):
        notes.append(f"{coverage['eventErrors']} container event lookup(s) returned errors.")
    if coverage.get("requiredLifecyclePresent") != coverage.get("requiredLifecycleTotal"):
        notes.append(
            "Required lifecycle span coverage is "
            f"{coverage.get('requiredLifecyclePresent', 0)}/{coverage.get('requiredLifecycleTotal', 0)}."
        )
    if coverage.get("requiredMetricPresent") != coverage.get("requiredMetricTotal"):
        notes.append(
            "Required timing metric coverage is "
            f"{coverage.get('requiredMetricPresent', 0)}/{coverage.get('requiredMetricTotal', 0)}."
        )
    if bottleneck and bottleneck.get("coverageStatus") not in {"full"}:
        notes.append(
            f"Primary bottleneck {bottleneck['eventId']} has {bottleneck['coverageStatus']} coverage "
            f"({bottleneck['count']}/{len(samples)} containers)."
        )
    if not notes:
        notes.append("Event coverage is complete for the required lifecycle spans and timing metrics.")
    return notes


def render_console_summary(report: dict[str, Any]) -> str:
    tti = report.get("timeToInteractive")
    bottleneck = report.get("primaryBottleneck")
    coverage = report.get("eventCoverage") or {}
    verification = report.get("execVerification") or {}
    lines = [
        "",
        "Sandbox startup report:",
        f"  Verdict: {report['verdict']['text']}",
        f"  Time to interactive: {summary_inline(tti)}",
        "  Command check: "
        f"{verification.get('verified', 0)}/{verification.get('count', 0)} verified "
        f"(completed={verification.get('completed', 0)}, failed={verification.get('failed', 0)})",
    ]
    if bottleneck:
        lines.append(
            "  Primary bottleneck: "
            f"{bottleneck['eventId']} p95={format_ms(bottleneck.get('p95Ms'))}ms "
            f"count={bottleneck.get('count', 0)} coverage={bottleneck.get('coverageStatus', 'unknown')}"
        )
    else:
        lines.append("  Primary bottleneck: unavailable")
    lines.append(
        "  Event coverage: "
        f"{coverage.get('containersWithEvents', 0)}/{coverage.get('requestedContainers', 0)} containers, "
        "required lifecycle spans "
        f"{coverage.get('requiredLifecyclePresent', 0)}/{coverage.get('requiredLifecycleTotal', 0)}, "
        "required timing metrics "
        f"{coverage.get('requiredMetricPresent', 0)}/{coverage.get('requiredMetricTotal', 0)}"
    )
    image = report.get("imageDrilldown") or {}
    image_phase = image.get("slowestPhase")
    if image_phase:
        lines.append(
            "  Image/CLIP slowest phase: "
            f"{image_phase['eventId']} p95={format_ms(image_phase.get('p95Ms'))}ms "
            f"count={image_phase.get('count', 0)}"
        )
    clip_accesses = image.get("clipAccesses") or []
    if clip_accesses:
        access = clip_accesses[0]
        lines.append(
            "  Slowest CLIP access: "
            f"{access.get('operation') or '-'} source={access.get('source') or '-'} "
            f"total={format_ms(access.get('totalMs'))}ms max={format_ms(access.get('maxMs'))}ms "
            f"count={access.get('count', 0)}"
        )
    if report.get("markdownPath"):
        lines.append(f"  Report: {report['markdownPath']}")
    return "\n".join(lines)


def render_markdown(report: dict[str, Any]) -> str:
    bottleneck = report.get("primaryBottleneck")
    coverage = report.get("eventCoverage") or {}
    verification = report.get("execVerification") or {}
    lines = [
        "# Sandbox Startup Benchmark Report",
        "",
        f"Verdict: {report['verdict']['text']}",
        f"Time to interactive: {summary_inline(report.get('timeToInteractive'))}",
        "Command check: "
        f"{verification.get('verified', 0)}/{verification.get('count', 0)} verified "
        f"(completed={verification.get('completed', 0)}, failed={verification.get('failed', 0)})",
        "Primary bottleneck: "
        + (
            f"{bottleneck['eventId']} p95={format_ms(bottleneck.get('p95Ms'))}ms count={bottleneck.get('count', 0)}"
            if bottleneck
            else "unavailable"
        ),
        "Event coverage: "
        f"{coverage.get('containersWithEvents', 0)}/{coverage.get('requestedContainers', 0)} containers, "
        f"required lifecycle spans {coverage.get('requiredLifecyclePresent', 0)}/{coverage.get('requiredLifecycleTotal', 0)}, "
        f"required timing metrics {coverage.get('requiredMetricPresent', 0)}/{coverage.get('requiredMetricTotal', 0)}",
        "",
        "## Client Timings",
        "",
        "| Metric | Count | Min | P50 | P90 | P95 | Max |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key, label in CLIENT_TIMINGS:
        lines.append(summary_table_row(label, report.get("clientTimings", {}).get(key)))
    batch = (report.get("clientTimings") or {}).get("batch")
    if batch:
        lines.extend(
            [
                "",
                f"- Parallel batch: `{batch.get('okCount', 0)}/{batch.get('count', 0)}` ok, "
                f"wall `{format_ms(batch.get('wallMs'))}ms`, throughput `{batch.get('throughputPerSecond', 0):.2f}/s`.",
            ]
        )

    lines.extend(
        [
            "",
            "## Server Phase Rollup",
            "",
            "| Phase | Event | Count | Coverage | P50 | P95 | Max |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: |",
        ]
    )
    for phase in report.get("serverPhases", []):
        lines.append(phase_table_row(phase))
    if not report.get("serverPhases"):
        lines.append("| unavailable | - | 0 | missing | - | - | - |")

    image = report.get("imageDrilldown") or {}
    lines.extend(
        [
            "",
            "## Image And CLIP Drilldown",
            "",
            f"- CLIP access coverage: `{image.get('containersWithClipAccesses', 0)}/{image.get('measuredContainers', 0)}` containers.",
        ]
    )
    slowest_image_phase = image.get("slowestPhase")
    if slowest_image_phase:
        lines.append(
            "- Slowest image/CLIP phase: "
            f"`{slowest_image_phase['eventId']}` p95 `{format_ms(slowest_image_phase.get('p95Ms'))}ms`, "
            f"max `{format_ms(slowest_image_phase.get('maxMs'))}ms`."
        )
    for note in image.get("notes") or []:
        lines.append(f"- {note}")

    lines.extend(
        [
            "",
            "### Image/CLIP Phases",
            "",
            "| Phase | Count | Coverage | P50 | P95 | Max |",
            "| --- | ---: | --- | ---: | ---: | ---: |",
        ]
    )
    for phase in image.get("phases") or []:
        lines.append(
            f"| `{phase['eventId']}` | {phase.get('count', 0)} | {phase.get('coverageStatus', 'unknown')} | "
            f"{format_ms(phase.get('p50Ms'))} | {format_ms(phase.get('p95Ms'))} | {format_ms(phase.get('maxMs'))} |"
        )
    if not image.get("phases"):
        lines.append("| unavailable | 0 | missing | - | - | - |")

    lines.extend(
        [
            "",
            "### Top CLIP Accesses",
            "",
            "| Rank | Operation | Source | Path / Object | Count | Containers | Bytes | Total | Max |",
            "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for index, access in enumerate(image.get("clipAccesses") or [], 1):
        lines.append(
            f"| {index} | `{table_cell(access.get('operation') or '-')}` | "
            f"`{table_cell(access.get('source') or '-')}` | "
            f"{table_cell(clip_access_target(access))} | "
            f"{access.get('count', 0)} | {access.get('containerCount', 0)} | "
            f"{access.get('bytesRead', 0)} | {format_ms(access.get('totalMs'))} | "
            f"{format_ms(access.get('maxMs'))} |"
        )
    if not image.get("clipAccesses"):
        lines.append("| 1 | unavailable | - | - | 0 | 0 | 0 | - | - |")

    lines.extend(
        [
            "",
            "## Top Bottlenecks",
            "",
            "| Rank | Phase | Count | Coverage | P50 | P95 | Max |",
            "| ---: | --- | ---: | --- | ---: | ---: | ---: |",
        ]
    )
    for index, phase in enumerate(top_bottleneck_phases(report.get("serverPhases", [])), 1):
        lines.append(
            f"| {index} | `{phase['eventId']}` | {phase['count']} | {phase['coverageStatus']} | "
            f"{format_ms(phase.get('p50Ms'))} | {format_ms(phase.get('p95Ms'))} | {format_ms(phase.get('maxMs'))} |"
        )

    lines.extend(
        [
            "",
            "## Slowest Containers",
            "",
            "| Container | TTI | Running | Post-running gap | Slowest server phase | Missing spans |",
            "| --- | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in report.get("slowestContainers", []):
        slowest = row.get("slowestPhase") or {}
        lines.append(
            "| "
            f"`{row.get('containerId') or '-'}` | "
            f"{format_ms(row.get('timeToInteractiveMs'))} | "
            f"{format_ms(row.get('runningObservedMs'))} | "
            f"{format_ms(row.get('postRunningInteractiveGapMs'))} | "
            f"`{slowest.get('eventId') or '-'}` {format_ms(slowest.get('durationMs'))}ms | "
            f"{', '.join(row.get('missing') or []) or '-'} |"
        )

    lines.extend(["", "## Data Quality", ""])
    lines.extend(f"- {note}" for note in report.get("dataQuality", []))
    lines.append("")
    return "\n".join(lines)


def top_bottleneck_phases(phases: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    candidates = [
        phase
        for phase in phases
        if not phase.get("rollup") and phase.get("p95Ms") is not None and phase.get("count", 0) > 0
    ]
    candidates.sort(key=lambda phase: (float(phase.get("p95Ms") or 0), int(phase.get("count") or 0)), reverse=True)
    return candidates[:limit]


def summary_inline(summary: dict[str, Any] | None) -> str:
    if not summary:
        return "unavailable"
    return (
        f"p50={format_ms(summary.get('p50'))}ms "
        f"p95={format_ms(summary.get('p95'))}ms "
        f"max={format_ms(summary.get('max'))}ms"
    )


def summary_table_row(label: str, summary: dict[str, Any] | None) -> str:
    if not summary:
        return f"| {label} | 0 | - | - | - | - | - |"
    return (
        f"| {label} | {summary.get('count', 0)} | {format_ms(summary.get('min'))} | "
        f"{format_ms(summary.get('p50'))} | {format_ms(summary.get('p90'))} | "
        f"{format_ms(summary.get('p95'))} | {format_ms(summary.get('max'))} |"
    )


def phase_table_row(phase: dict[str, Any]) -> str:
    return (
        f"| {phase['label']} | `{phase['eventId']}` | {phase.get('count', 0)} | "
        f"{phase.get('coverageStatus', 'unknown')} | {format_ms(phase.get('p50Ms'))} | "
        f"{format_ms(phase.get('p95Ms'))} | {format_ms(phase.get('maxMs'))} |"
    )


def format_ms(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.1f}"
    except (TypeError, ValueError):
        return "-"


def duration_us_to_ms(duration_us: int) -> int:
    if duration_us <= 0:
        return 0
    return (duration_us + 999) // 1000


def clip_access_target(access: dict[str, Any]) -> str:
    for key in ("path", "layerDigest", "decompressedHash", "contentHash"):
        value = access.get(key)
        if value:
            return str(value)
    return "-"


def table_cell(value: Any) -> str:
    text = str(value if value is not None else "")
    return text.replace("|", "\\|").replace("\n", " ")


def write_startup_report(
    benchmark_path: str | Path,
    *,
    markdown_path: str | Path | None = None,
    gateway_url: str = "",
    token: str = "",
    workspace_id: str = "",
    event_wait_seconds: float = DEFAULT_EVENT_WAIT_SECONDS,
    event_poll_ms: int = DEFAULT_EVENT_POLL_MS,
    event_limit: int = DEFAULT_EVENT_LIMIT,
    top_lifecycle: int = DEFAULT_TOP_LIFECYCLE,
) -> dict[str, Any]:
    benchmark_path = Path(benchmark_path)
    markdown_path = Path(markdown_path) if markdown_path else default_markdown_path(benchmark_path)
    benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
    container_ids = [
        sample.get("containerId")
        for sample in measured_samples(benchmark)
        if sample.get("containerId")
    ]
    event_targets = container_event_targets(measured_samples(benchmark))

    events, event_error = fetch_event_batch_with_poll(
        gateway_url or benchmark.get("config", {}).get("gatewayUrl", ""),
        workspace_id,
        token,
        container_ids,
        targets=event_targets,
        limit=event_limit,
        top_lifecycle=top_lifecycle,
        wait_seconds=event_wait_seconds,
        poll_ms=event_poll_ms,
    )
    report = build_startup_report(
        benchmark,
        events=events,
        event_error=event_error,
        markdown_path=str(markdown_path),
    )
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(report), encoding="utf-8")

    benchmark["startupReport"] = report
    benchmark_path.write_text(json.dumps(benchmark, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def container_event_targets(samples: list[dict[str, Any]]) -> list[dict[str, str]]:
    targets: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for sample in samples:
        container_id = str(sample.get("containerId") or "").strip()
        if not container_id:
            continue
        stub_id = str(sample.get("stubId") or "").strip()
        key = (container_id, stub_id)
        if key in seen:
            continue
        seen.add(key)
        target = {"container_id": container_id}
        if stub_id:
            target["stub_id"] = stub_id
        targets.append(target)
    return targets
