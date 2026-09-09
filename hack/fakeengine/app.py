"""
Fake inference engine: a CPU-only ManagedEndpoint for local end-to-end testing
of the managed endpoints platform without a GPU. Deploy it with
``beta9 --context <admin-context> deploy app.py:endpoint``; the container runs
this file with ``serve``: an OpenAI-compatible HTTP server (chat/completions,
completions, embeddings; SSE streaming with real ``usage``), ``/health``,
vLLM-named ``/metrics`` and a minimal EndpointHarnessService client. Live-tunable
knobs: ``tokens_per_sec``, ``max_concurrency``, ``fail_rate``, ``ttft_ms``.
"""

from __future__ import annotations

import json
import math
import os
import random
import signal
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from beta9 import Catalog, GpuTarget, Image, ManagedEndpoint, Pricing
from beta9.channel import Channel
from beta9.clients import managedendpoint as pb

ENDPOINT_ID = "beam/fake-engine"

endpoint = ManagedEndpoint(
    id=ENDPOINT_ID,
    kind="llm",
    engine="fake",
    image=Image(python_version="python3.12"),
    entrypoint=["python3", "app.py", "serve"],
    port=8000,
    health="/health",
    metrics="/metrics",
    gpu=[
        GpuTarget("cpu", min_replicas=1, max_replicas=3, share=1.0, harness={"tokens_per_sec": 200})
    ],
    pricing=Pricing(prompt_tokens="0.0000001", completion_tokens="0.0000002"),
    catalog=Catalog(
        name="Beam Fake Engine",
        description="Deterministic CPU engine for platform testing.",
        context_length=8192,
        max_completion_tokens=1024,
        tokenizer="Other",
        modalities=["text->text"],
        supported_parameters=["max_tokens", "stream", "temperature"],
        public=True,
    ),
    harness=True,
    cpu=1,
    memory="512Mi",
)

WORDS = "the quick brown fox jumps over the lazy dog while the platform streams tokens".split()


class Engine:
    """Shared mutable state: tunable knobs plus vLLM-shaped counters."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.knobs = dict(tokens_per_sec=200.0, max_concurrency=32, fail_rate=0.0, ttft_ms=20)
        self.running = self.waiting = 0
        self.prompt_tokens_total = self.generation_tokens_total = 0
        self.ttft_sum = self.tpot_sum = 0.0
        self.ttft_count = self.tpot_count = 0
        self.draining = False
        self.applied_revision = 0

    def apply(self, config: dict) -> dict:
        with self.lock:
            for key, value in (config or {}).items():
                if key in self.knobs:
                    self.knobs[key] = type(self.knobs[key])(value)
            return dict(self.knobs)

    def admit(self) -> bool:
        with self.lock:
            if self.draining or self.running >= int(self.knobs["max_concurrency"]):
                return False
            self.running += 1
            return True

    def release(self) -> None:
        with self.lock:
            self.running = max(0, self.running - 1)

    def record(self, prompt: int, completion: int, ttft: float, tpot: float) -> None:
        with self.lock:
            self.prompt_tokens_total += prompt
            self.generation_tokens_total += completion
            self.ttft_sum += ttft
            self.ttft_count += 1
            if completion > 1:
                self.tpot_sum += tpot * (completion - 1)
                self.tpot_count += completion - 1

    def prometheus(self) -> str:
        with self.lock:
            cache = min(1.0, self.running / max(1, int(self.knobs["max_concurrency"])))
            metrics = {
                "num_requests_running": self.running,
                "num_requests_waiting": self.waiting,
                "gpu_cache_usage_perc": f"{cache:.4f}",
                "prompt_tokens_total": self.prompt_tokens_total,
                "generation_tokens_total": self.generation_tokens_total,
                "time_to_first_token_seconds_sum": f"{self.ttft_sum:.6f}",
                "time_to_first_token_seconds_count": self.ttft_count,
                "time_per_output_token_seconds_sum": f"{self.tpot_sum:.6f}",
                "time_per_output_token_seconds_count": self.tpot_count,
            }
        return "".join(f"vllm:{k} {v}\n" for k, v in metrics.items())

    def capacity(self) -> dict:
        with self.lock:
            tps = float(self.knobs["tokens_per_sec"])
            maxc = max(1, int(self.knobs["max_concurrency"]))
            return {
                "in_flight": self.running,
                "max_concurrency": int(self.knobs["max_concurrency"]),
                "running": self.running,
                "waiting": self.waiting,
                "kv_cache_free_milli": int(1000 * (1 - self.running / maxc)),
                "decode_tokens_per_sec": int(tps * max(1, self.running)),
                "ttft_ms": int(self.knobs["ttft_ms"]),
                "tpot_ms": int(1000 / tps) if tps > 0 else 0,
            }


ENGINE = Engine()


def count_tokens(text: str) -> int:
    return max(1, math.ceil(len(text) / 4))


def fake_vector(value) -> list[float]:
    return [((hash(str(value)) >> s) % 1000) / 1000 for s in range(8)]


def prompt_text(body: dict) -> str:
    if "messages" in body:
        return " ".join(str(m.get("content", "")) for m in body["messages"] if isinstance(m, dict))
    prompt = body.get("prompt", "")
    return " ".join(prompt) if isinstance(prompt, list) else str(prompt)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "fake-engine/1.0"

    def log_message(self, fmt, *args):
        if os.environ.get("FAKE_ENGINE_VERBOSE"):
            super().log_message(fmt, *args)

    def send(self, status: int, body: bytes, ctype: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status: int, payload: dict) -> None:
        self.send(status, json.dumps(payload).encode())

    def error(self, status: int, message: str, kind: str = "invalid_request_error") -> None:
        self.send_json(status, {"error": {"message": message, "type": kind, "code": status}})

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/health", "/healthz", "/ready"):
            if ENGINE.draining:
                return self.error(503, "draining", "unavailable")
            return self.send_json(200, {"status": "ok"})
        if self.path == "/metrics":
            return self.send(200, ENGINE.prometheus().encode(), "text/plain; version=0.0.4")
        if self.path == "/v1/models":
            models = [{"id": ENDPOINT_ID, "object": "model"}]
            return self.send_json(200, {"object": "list", "data": models})
        self.error(404, f"no route for {self.path}")

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}") if length else {}
        except json.JSONDecodeError:
            body = {}
        path = self.path.split("?", 1)[0]
        if path not in ("/v1/chat/completions", "/v1/completions", "/v1/embeddings"):
            return self.error(404, f"no route for {path}")
        if not ENGINE.admit():
            return self.error(503 if ENGINE.draining else 429, "engine is busy", "overloaded")
        try:
            if random.random() < float(ENGINE.knobs["fail_rate"]):
                return self.error(500, "injected failure", "server_error")
            if path == "/v1/embeddings":
                return self.embeddings(body)
            self.completion(body, chat=path.endswith("/chat/completions"))
        finally:
            ENGINE.release()

    def embeddings(self, body: dict) -> None:
        inputs = body.get("input", "")
        inputs = inputs if isinstance(inputs, list) else [inputs]
        tokens = sum(count_tokens(str(i)) for i in inputs)
        data = [
            {"object": "embedding", "index": i, "embedding": fake_vector(x)}
            for i, x in enumerate(inputs)
        ]
        ENGINE.record(tokens, 0, 0.0, 0.0)
        usage = {"prompt_tokens": tokens, "total_tokens": tokens}
        self.send_json(200, {"object": "list", "data": data, "model": ENDPOINT_ID, "usage": usage})

    def completion(self, body: dict, chat: bool) -> None:
        prompt_tokens = count_tokens(prompt_text(body))
        requested = body.get("max_tokens") or body.get("max_completion_tokens") or 32
        n_tokens = max(1, min(int(requested), 1024))
        tps = float(ENGINE.knobs["tokens_per_sec"])
        step = 1.0 / tps if tps > 0 else 0.0
        ttft = float(ENGINE.knobs["ttft_ms"]) / 1000.0
        words = [WORDS[i % len(WORDS)] for i in range(n_tokens)]
        usage = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": n_tokens,
            "total_tokens": prompt_tokens + n_tokens,
        }
        base = {
            "id": f"{'chatcmpl' if chat else 'cmpl'}-{uuid.uuid4().hex[:24]}",
            "created": int(time.time()),
            "model": ENDPOINT_ID,
        }

        if not body.get("stream"):
            time.sleep(ttft + step * (n_tokens - 1))
            ENGINE.record(prompt_tokens, n_tokens, ttft, step)
            text = " ".join(words)
            content = (
                {"message": {"role": "assistant", "content": text}} if chat else {"text": text}
            )
            choice = {"index": 0, **content, "finish_reason": "stop"}
            obj = "chat.completion" if chat else "text_completion"
            return self.send_json(200, {**base, "object": obj, "choices": [choice], "usage": usage})

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        def raw(data: bytes) -> None:
            self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
            self.wfile.flush()

        def chunk(choices: list, **extra) -> None:
            obj = "chat.completion.chunk" if chat else "text_completion"
            payload = {**base, "object": obj, "choices": choices, **extra}
            raw(b"data: " + json.dumps(payload).encode() + b"\n\n")

        def choice(piece: str, first: bool = False, finish: str | None = None) -> dict:
            if not chat:
                return {"index": 0, "text": piece, "finish_reason": finish}
            delta = {"content": piece} if piece else {}
            if first:
                delta["role"] = "assistant"
            return {"index": 0, "delta": delta, "finish_reason": finish}

        time.sleep(ttft)
        for i, word in enumerate(words):
            chunk([choice(word if i == 0 else " " + word, first=i == 0)])
            if i < n_tokens - 1:
                time.sleep(step)
        chunk([choice("", finish="stop")])
        if (body.get("stream_options") or {}).get("include_usage"):
            chunk([], usage=usage)
        raw(b"data: [DONE]\n\n")
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()
        ENGINE.record(prompt_tokens, n_tokens, ttft, step)


class Harness:
    """Minimal EndpointHarnessService client over the beta9 SDK channel."""

    def __init__(self, stop: threading.Event, server: ThreadingHTTPServer) -> None:
        addr = f"{os.environ['BETA9_GATEWAY_HOST']}:{os.environ['BETA9_GATEWAY_PORT']}"
        channel = Channel(addr=addr, token=os.environ["BETA9_TOKEN"])
        self.stub = pb.EndpointHarnessServiceStub(channel)
        self.stop = stop
        self.server = server
        self.replica_id = ""
        self.interval = 5

    def log(self, msg: str) -> None:
        print(f"[harness] {msg}", flush=True)

    def register(self) -> None:
        req = pb.HarnessRegisterRequest(
            container_id=os.environ.get("CONTAINER_ID", ""),
            engine="fake",
            engine_version="1.0",
            capabilities_json=json.dumps({"knobs": sorted(ENGINE.knobs), "engine": "fake"}),
            limits_json=json.dumps({"max_concurrency": ENGINE.knobs["max_concurrency"]}),
        )
        for attempt in range(60):
            try:
                resp = self.stub.register(req)
                if not resp.ok:
                    raise RuntimeError(f"rejected: {resp.err_msg}")
            except Exception as exc:  # noqa: BLE001
                self.log(f"register failed ({exc}); retrying")
                time.sleep(min(10, 1 + attempt))
                continue
            self.replica_id = resp.replica_id
            self.interval = max(1, resp.heartbeat_interval_seconds or 5)
            if resp.current and resp.current.config_json:
                self.apply(resp.current.revision, resp.current.config_json)
            self.log(f"registered as {self.replica_id} ({resp.endpoint_id} {resp.role} {resp.gpu})")
            return
        raise RuntimeError("could not register with the control plane")

    def apply(self, revision: int, config_json: str) -> None:
        ack = pb.HarnessAckConfigRequest(replica_id=self.replica_id, revision=revision)
        try:
            effective = ENGINE.apply(json.loads(config_json or "{}"))
            ENGINE.applied_revision = revision
            ack.applied, ack.effective_json = True, json.dumps(effective)
            self.log(f"applied revision {revision}: {effective}")
        except Exception as exc:  # noqa: BLE001
            ack.error = str(exc)
        try:
            self.stub.ack_config(ack)
        except Exception as exc:  # noqa: BLE001
            self.log(f"ack failed: {exc}")

    def watch(self) -> None:
        while not self.stop.is_set():
            try:
                after = ENGINE.applied_revision
                req = pb.HarnessWatchConfigRequest(replica_id=self.replica_id, after_revision=after)
                for rev in self.stub.watch_config(req):
                    if self.stop.is_set():
                        return
                    self.apply(rev.revision, rev.config_json)
            except Exception as exc:  # noqa: BLE001
                if self.stop.is_set():
                    return
                self.log(f"watch disconnected ({exc}); reconnecting")
                time.sleep(2)

    def heartbeat_loop(self) -> None:
        while not self.stop.is_set():
            try:
                req = pb.HarnessHeartbeatRequest(
                    replica_id=self.replica_id,
                    status="draining" if ENGINE.draining else "ready",
                    capacity=pb.ReplicaCapacity(**ENGINE.capacity()),
                    applied_revision=ENGINE.applied_revision,
                )
                resp = self.stub.heartbeat(req)
                if resp.heartbeat_interval_seconds:
                    self.interval = max(1, resp.heartbeat_interval_seconds)
                if resp.drain and not ENGINE.draining:
                    self.log(f"drain requested ({resp.drain_seconds}s)")
                    start_drain(self.server, self.stop, resp.drain_seconds or 5)
            except Exception as exc:  # noqa: BLE001
                self.log(f"heartbeat failed: {exc}")
            self.stop.wait(self.interval)


def drain_and_exit(server: ThreadingHTTPServer, stop: threading.Event, seconds: int) -> None:
    """Stop admitting work, wait (at most ``seconds``) for in-flight requests, then shut
    down. Idempotent: the harness drain RPC and SIGTERM both land here."""
    with ENGINE.lock:
        if ENGINE.draining:
            return
        ENGINE.draining = True
    deadline = time.time() + seconds
    while time.time() < deadline:
        with ENGINE.lock:
            if ENGINE.running == 0:
                break
        time.sleep(0.1)
    stop.set()
    server.shutdown()


def start_drain(server: ThreadingHTTPServer, stop: threading.Event, seconds: int) -> None:
    threading.Thread(target=drain_and_exit, args=(server, stop, seconds), daemon=True).start()


def serve() -> None:
    port = int(os.environ.get("BEAM_ENDPOINT_PORT") or 8000)
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    stop = threading.Event()
    print(f"[fake-engine] listening on :{port}", flush=True)

    # The worker sends SIGTERM on eviction/scale-down and kills after BEAM_DRAIN_SECONDS.
    drain_seconds = int(os.environ.get("BEAM_DRAIN_SECONDS") or 5)

    def on_sigterm(*_: object) -> None:
        print(f"[fake-engine] SIGTERM: draining ({drain_seconds}s)", flush=True)
        start_drain(server, stop, drain_seconds)

    signal.signal(signal.SIGTERM, on_sigterm)

    if os.environ.get("BEAM_HARNESS_ENABLED", "").lower() == "true":
        if os.environ.get("BEAM_HARNESS_CONFIG"):
            ENGINE.apply(json.loads(os.environ["BEAM_HARNESS_CONFIG"]))
        harness = Harness(stop, server)
        harness.register()
        threading.Thread(target=harness.watch, daemon=True).start()
        threading.Thread(target=harness.heartbeat_loop, daemon=True).start()

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        print("[fake-engine] stopped", flush=True)


if __name__ == "__main__" and sys.argv[1:] == ["serve"]:
    serve()
