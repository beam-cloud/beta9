#!/usr/bin/env python3
"""Run an OpenAI-compatible inference smoke test against a Beam app endpoint."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=os.getenv("BEAM_LLM_ENDPOINT", ""))
    parser.add_argument("--model", default=os.getenv("BEAM_LLM_MODEL", ""))
    parser.add_argument("--prompt", default="Say Beam staging inference is working in one short sentence.")
    parser.add_argument("--chat-runs", type=int, default=3)
    parser.add_argument("--completion-runs", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--session", default=f"llm-smoke-{uuid.uuid4().hex[:12]}")
    parser.add_argument("--json-output", default="")
    parser.add_argument("--api-key", default=os.getenv("OPENAI_API_KEY", ""))
    return parser.parse_args()


def request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None,
    headers: dict[str, str],
    timeout: float,
) -> tuple[int, dict[str, Any], float]:
    data = None
    req_headers = {"Accept": "application/json", **headers}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=req_headers, method=method)
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


def extract_chat_text(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    return str(message.get("content") or choices[0].get("text") or "")


def extract_completion_text(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        return ""
    return str(choices[0].get("text") or "")


def request_headers(args: argparse.Namespace) -> dict[str, str]:
    headers = {"X-Beam-LLM-Session": args.session}
    if args.api_key:
        headers["Authorization"] = f"Bearer {args.api_key}"
    return headers


def chat_payload(args: argparse.Namespace, run_index: int) -> dict[str, Any]:
    return {
        "model": args.model,
        "messages": [
            {"role": "system", "content": "You are a concise staging smoke test model."},
            {
                "role": "user",
                "content": (
                    "Shared prefix for Beam LLM locality smoke test. "
                    f"{args.prompt} Run {run_index}."
                ),
            },
        ],
        "max_tokens": 32,
        "temperature": 0,
    }


def run_chat(args: argparse.Namespace, endpoint: str, run_index: int) -> dict[str, Any]:
    status, body, latency = request_json(
        "POST",
        f"{endpoint}/v1/chat/completions",
        chat_payload(args, run_index),
        request_headers(args),
        args.timeout,
    )
    return {
        "index": run_index,
        "status": status,
        "latency_seconds": round(latency, 6),
        "text": extract_chat_text(body)[:500],
        "usage": body.get("usage"),
        "id": body.get("id"),
        "error": body.get("error") if status >= 400 else None,
    }


def run_completion(args: argparse.Namespace, endpoint: str, run_index: int) -> dict[str, Any]:
    status, body, latency = request_json(
        "POST",
        f"{endpoint}/v1/completions",
        {
            "model": args.model,
            "prompt": (
                "Shared prefix for Beam completion smoke test. "
                f"{args.prompt} Completion run {run_index}:"
            ),
            "max_tokens": 32,
            "temperature": 0,
        },
        request_headers(args),
        args.timeout,
    )
    return {
        "index": run_index,
        "status": status,
        "latency_seconds": round(latency, 6),
        "text": extract_completion_text(body)[:500],
        "usage": body.get("usage"),
        "id": body.get("id"),
        "error": body.get("error") if status >= 400 else None,
    }


def latency_summary(items: list[dict[str, Any]]) -> dict[str, float]:
    latencies = [float(item["latency_seconds"]) for item in items]
    if not latencies:
        return {}
    return {
        "min": round(min(latencies), 6),
        "max": round(max(latencies), 6),
        "mean": round(statistics.fmean(latencies), 6),
    }


def main() -> int:
    args = parse_args()
    endpoint = args.endpoint.rstrip("/")
    if not endpoint:
        print("missing --endpoint or BEAM_LLM_ENDPOINT", file=sys.stderr)
        return 2
    if not args.model:
        print("missing --model or BEAM_LLM_MODEL", file=sys.stderr)
        return 2
    if args.chat_runs < 0 or args.completion_runs < 0 or args.concurrency < 1:
        print("invalid run counts or concurrency", file=sys.stderr)
        return 2

    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    headers = request_headers(args)
    models_status, models_body, models_latency = request_json(
        "GET",
        f"{endpoint}/v1/models",
        None,
        headers,
        args.timeout,
    )

    chat_results: list[dict[str, Any]] = []
    if args.chat_runs:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [
                executor.submit(run_chat, args, endpoint, i + 1)
                for i in range(args.chat_runs)
            ]
            for future in concurrent.futures.as_completed(futures):
                chat_results.append(future.result())
        chat_results.sort(key=lambda item: int(item["index"]))

    completion_results = [
        run_completion(args, endpoint, i + 1)
        for i in range(args.completion_runs)
    ]

    output = {
        "ok": (
            200 <= models_status < 300
            and all(200 <= item["status"] < 300 for item in chat_results)
            and all(200 <= item["status"] < 300 for item in completion_results)
        ),
        "endpoint": endpoint,
        "model": args.model,
        "session": args.session,
        "started_at": started_at,
        "models": {
            "status": models_status,
            "latency_seconds": round(models_latency, 6),
            "body": models_body,
        },
        "chat": chat_results,
        "chat_latency": latency_summary(chat_results),
        "completions": completion_results,
        "completion_latency": latency_summary(completion_results),
    }

    rendered = json.dumps(output, indent=2, sort_keys=True)
    print(rendered)
    if args.json_output:
        with open(args.json_output, "w", encoding="utf-8") as f:
            f.write(rendered + "\n")

    return 0 if output["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
