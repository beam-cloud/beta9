#!/usr/bin/env python3
"""Benchmark an OpenAI-compatible Beam LLM endpoint and collect S2 container metrics."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=os.getenv("BEAM_LLM_ENDPOINT", ""))
    parser.add_argument("--model", default=os.getenv("BEAM_LLM_MODEL", ""))
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--shared-prefix", default="Shared prefix for Beam LLM routing benchmark.")
    parser.add_argument("--prompt", default="Return one concise sentence confirming inference is healthy.")
    parser.add_argument("--session", default=f"llm-bench-{uuid.uuid4().hex[:12]}")
    parser.add_argument("--api-key", default=os.getenv("OPENAI_API_KEY", ""))
    parser.add_argument("--json-output", default="")
    parser.add_argument("--app-gateway", default=os.getenv("BEAM_APP_GATEWAY", "https://app.stage.beam.cloud"))
    parser.add_argument("--beam-token", default=os.getenv("BEAM_TOKEN") or os.getenv("BETA9_TOKEN", ""))
    parser.add_argument("--workspace-id", default=os.getenv("BEAM_WORKSPACE_ID", ""))
    parser.add_argument("--stub-id", default=os.getenv("BEAM_STUB_ID", ""))
    parser.add_argument("--container-id", action="append", default=[])
    parser.add_argument("--metrics-delay", type=float, default=8)
    parser.add_argument("--route-event-limit", type=int, default=1000)
    return parser.parse_args()


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    rank = (len(ordered) - 1) * pct
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return round(ordered[low], 6)
    weight = rank - low
    return round(ordered[low] * (1 - weight) + ordered[high] * weight, 6)


def request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None,
    headers: dict[str, str],
    timeout: float,
) -> tuple[int, dict[str, Any], float]:
    data = None
    request_headers = {"Accept": "application/json", **headers}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            elapsed = time.perf_counter() - started
            body = json.loads(raw.decode("utf-8") or "{}")
            return response.status, body, elapsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed = time.perf_counter() - started
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"error": raw}
        return exc.code, body, elapsed


def inference_headers(args: argparse.Namespace, run_index: int) -> dict[str, str]:
    headers = {
        "X-Beam-LLM-Session": args.session,
        "X-Request-ID": f"{args.session}-{run_index}",
    }
    if args.api_key:
        headers["Authorization"] = f"Bearer {args.api_key}"
    return headers


def chat_payload(args: argparse.Namespace, run_index: int) -> dict[str, Any]:
    return {
        "model": args.model,
        "messages": [
            {"role": "system", "content": "You are a low-latency inference benchmark model."},
            {
                "role": "user",
                "content": f"{args.shared_prefix} {args.prompt} Request {run_index}.",
            },
        ],
        "max_tokens": args.max_tokens,
        "temperature": 0,
    }


def run_one(args: argparse.Namespace, endpoint: str, run_index: int) -> dict[str, Any]:
    status, body, latency = request_json(
        "POST",
        f"{endpoint}/v1/chat/completions",
        chat_payload(args, run_index),
        inference_headers(args, run_index),
        args.timeout,
    )
    usage = body.get("usage") or {}
    return {
        "index": run_index,
        "status": status,
        "latency_seconds": round(latency, 6),
        "prompt_tokens": int(usage.get("prompt_tokens") or 0),
        "completion_tokens": int(usage.get("completion_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
        "id": body.get("id"),
        "error": body.get("error") if status >= 400 else None,
    }


def summarize_results(results: list[dict[str, Any]], wall_seconds: float) -> dict[str, Any]:
    latencies = [float(item["latency_seconds"]) for item in results if 200 <= int(item["status"]) < 300]
    total_tokens = sum(int(item.get("total_tokens") or 0) for item in results)
    completion_tokens = sum(int(item.get("completion_tokens") or 0) for item in results)
    status_counts: dict[str, int] = {}
    for item in results:
        key = str(item["status"])
        status_counts[key] = status_counts.get(key, 0) + 1

    return {
        "requests": len(results),
        "successful_requests": len(latencies),
        "status_counts": status_counts,
        "wall_seconds": round(wall_seconds, 6),
        "request_throughput_per_s": round(len(results) / wall_seconds, 6) if wall_seconds > 0 else 0,
        "total_tokens": total_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens_per_s": round(total_tokens / wall_seconds, 6) if wall_seconds > 0 else 0,
        "completion_tokens_per_s": round(completion_tokens / wall_seconds, 6) if wall_seconds > 0 else 0,
        "latency_seconds": {
            "min": round(min(latencies), 6) if latencies else 0,
            "mean": round(statistics.fmean(latencies), 6) if latencies else 0,
            "p50": percentile(latencies, 0.50),
            "p90": percentile(latencies, 0.90),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "max": round(max(latencies), 6) if latencies else 0,
        },
    }


def event_history_url(args: argparse.Namespace, container_id: str, start: datetime, end: datetime) -> str:
    query = urllib.parse.urlencode(
        {
            "stub_id": args.stub_id,
            "container_id": container_id,
            "event_types": "container.metrics",
            "start_time": iso(start),
            "end_time": iso(end),
            "limit": "500",
        }
    )
    base = args.app_gateway.rstrip("/")
    return f"{base}/api/v1/events/{args.workspace_id}/history?{query}"


def llm_route_events_url(args: argparse.Namespace, start: datetime, end: datetime) -> str:
    query = urllib.parse.urlencode(
        {
            "start_time": iso(start),
            "end_time": iso(end),
            "limit": str(args.route_event_limit),
        }
    )
    base = args.app_gateway.rstrip("/")
    return f"{base}/api/v1/events/{args.workspace_id}/stubs/{args.stub_id}/llm-routes?{query}"


def collect_llm_route_events(args: argparse.Namespace, start: datetime, end: datetime) -> dict[str, Any]:
    if not args.beam_token or not args.workspace_id or not args.stub_id:
        return {"enabled": False, "reason": "missing beam token, workspace id, or stub id"}

    headers = {"Authorization": f"Bearer {args.beam_token}"}
    status, body, latency = request_json(
        "GET",
        llm_route_events_url(args, start, end),
        None,
        headers,
        args.timeout,
    )
    events = body.get("events") or []
    containers = sorted({event.get("container_id") for event in events if event.get("container_id")})
    reasons: dict[str, int] = {}
    engine_samples = []
    for event in events:
        reason = str(event.get("route_reason") or "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1
        if any(
            event.get(key)
            for key in (
                "engine_running_requests",
                "engine_waiting_requests",
                "engine_decode_tokens_per_s",
                "engine_gpu_cache_usage_milli",
                "engine_prefix_cache_hit_milli",
            )
        ):
            engine_samples.append(
                {
                    "container_id": event.get("container_id"),
                    "running": event.get("engine_running_requests"),
                    "waiting": event.get("engine_waiting_requests"),
                    "decode_tokens_per_s": event.get("engine_decode_tokens_per_s"),
                    "gpu_cache_usage_milli": event.get("engine_gpu_cache_usage_milli"),
                    "prefix_cache_hit_milli": event.get("engine_prefix_cache_hit_milli"),
                }
            )

    return {
        "enabled": True,
        "status": status,
        "query_latency_seconds": round(latency, 6),
        "count": body.get("count", len(events)),
        "containers": containers,
        "reasons": reasons,
        "summary": body.get("summary"),
        "engine_samples": engine_samples[:20],
        "events": events,
    }


def collect_s2_metrics(
    args: argparse.Namespace, start: datetime, end: datetime, container_ids: list[str]
) -> dict[str, Any]:
    if not args.beam_token or not args.workspace_id or not args.stub_id or not container_ids:
        return {"enabled": False, "reason": "missing beam token, workspace id, stub id, or container ids"}

    headers = {"Authorization": f"Bearer {args.beam_token}"}
    out: dict[str, Any] = {"enabled": True, "containers": {}}
    for container_id in container_ids:
        status, body, latency = request_json(
            "GET",
            event_history_url(args, container_id, start, end),
            None,
            headers,
            args.timeout,
        )
        events = body.get("events") or []
        rows = []
        for event in events:
            data = event.get("data") or (event.get("cloud_event") or {}).get("data") or {}
            metrics = data.get("metrics") or {}
            rows.append(
                {
                    "time": event.get("timestamp") or (event.get("cloud_event") or {}).get("time"),
                    "worker_id": data.get("worker_id"),
                    "network_recv_bytes": int(metrics.get("network_recv_bytes") or 0),
                    "network_sent_bytes": int(metrics.get("network_sent_bytes") or 0),
                    "network_recv_packets": int(metrics.get("network_recv_packets") or 0),
                    "network_sent_packets": int(metrics.get("network_sent_packets") or 0),
                    "cpu_pct": float(metrics.get("cpu_pct") or 0),
                    "gpu_memory_used_bytes": int(metrics.get("gpu_memory_used_bytes") or 0),
                    "gpu_memory_total_bytes": int(metrics.get("gpu_memory_total_bytes") or 0),
                    "gpu_type": metrics.get("gpu_type") or "",
                }
            )
        rows.sort(key=lambda item: item["time"] or "")
        container_summary: dict[str, Any] = {
            "status": status,
            "query_latency_seconds": round(latency, 6),
            "samples": len(rows),
        }
        if rows:
            first = rows[0]
            last = rows[-1]
            cpu_values = [row["cpu_pct"] for row in rows]
            positive_recv_delta = 0
            positive_sent_delta = 0
            positive_recv_packets_delta = 0
            positive_sent_packets_delta = 0
            for previous, current in zip(rows, rows[1:]):
                positive_recv_delta += max(0, current["network_recv_bytes"] - previous["network_recv_bytes"])
                positive_sent_delta += max(0, current["network_sent_bytes"] - previous["network_sent_bytes"])
                positive_recv_packets_delta += max(0, current["network_recv_packets"] - previous["network_recv_packets"])
                positive_sent_packets_delta += max(0, current["network_sent_packets"] - previous["network_sent_packets"])
            container_summary.update(
                {
                    "worker_id": first["worker_id"],
                    "window_start": first["time"],
                    "window_end": last["time"],
                    "network_recv_bytes_delta": last["network_recv_bytes"] - first["network_recv_bytes"],
                    "network_sent_bytes_delta": last["network_sent_bytes"] - first["network_sent_bytes"],
                    "network_recv_packets_delta": last["network_recv_packets"] - first["network_recv_packets"],
                    "network_sent_packets_delta": last["network_sent_packets"] - first["network_sent_packets"],
                    "network_recv_bytes_positive_delta": positive_recv_delta,
                    "network_sent_bytes_positive_delta": positive_sent_delta,
                    "network_recv_packets_positive_delta": positive_recv_packets_delta,
                    "network_sent_packets_positive_delta": positive_sent_packets_delta,
                    "network_recv_bytes_sample_sum": sum(row["network_recv_bytes"] for row in rows),
                    "network_sent_bytes_sample_sum": sum(row["network_sent_bytes"] for row in rows),
                    "network_recv_packets_sample_sum": sum(row["network_recv_packets"] for row in rows),
                    "network_sent_packets_sample_sum": sum(row["network_sent_packets"] for row in rows),
                    "network_recv_bytes_max": max(row["network_recv_bytes"] for row in rows),
                    "network_sent_bytes_max": max(row["network_sent_bytes"] for row in rows),
                    "cpu_pct_mean": round(statistics.fmean(cpu_values), 6) if cpu_values else 0,
                    "cpu_pct_max": round(max(cpu_values), 6) if cpu_values else 0,
                    "gpu_type": last["gpu_type"],
                    "gpu_memory_used_gb": round(last["gpu_memory_used_bytes"] / 1024**3, 6),
                    "gpu_memory_total_gb": round(last["gpu_memory_total_bytes"] / 1024**3, 6),
                }
            )
        out["containers"][container_id] = container_summary
    return out


def main() -> int:
    args = parse_args()
    endpoint = args.endpoint.rstrip("/")
    if not endpoint:
        print("missing --endpoint or BEAM_LLM_ENDPOINT", file=sys.stderr)
        return 2
    if not args.model:
        print("missing --model or BEAM_LLM_MODEL", file=sys.stderr)
        return 2
    if args.requests < 1 or args.concurrency < 1:
        print("requests and concurrency must be positive", file=sys.stderr)
        return 2

    start = datetime.now(UTC)
    wall_start = time.perf_counter()
    results: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [executor.submit(run_one, args, endpoint, i + 1) for i in range(args.requests)]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    wall_seconds = time.perf_counter() - wall_start
    end = datetime.now(UTC)
    results.sort(key=lambda item: int(item["index"]))

    metrics_start = start - timedelta(seconds=6)
    if args.metrics_delay > 0 and args.beam_token and args.workspace_id and args.stub_id:
        time.sleep(args.metrics_delay)
    metrics_end = datetime.now(UTC) + timedelta(seconds=3)
    route_events = collect_llm_route_events(args, metrics_start, metrics_end)
    container_ids = list(args.container_id)
    if not container_ids:
        container_ids = list(route_events.get("containers") or [])
    output = {
        "ok": all(200 <= int(item["status"]) < 300 for item in results),
        "endpoint": endpoint,
        "model": args.model,
        "session": args.session,
        "started_at": iso(start),
        "ended_at": iso(end),
        "summary": summarize_results(results, wall_seconds),
        "llm_route_events": route_events,
        "s2_metrics": collect_s2_metrics(args, metrics_start, metrics_end, container_ids),
        "results": results,
    }

    rendered = json.dumps(output, indent=2, sort_keys=True)
    print(rendered)
    if args.json_output:
        with open(args.json_output, "w", encoding="utf-8") as f:
            f.write(rendered + "\n")
    return 0 if output["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
