"""
Fake inference engine: a CPU-only ManagedEndpoint used for local end-to-end
testing of the managed endpoints platform without a GPU.

Deploy it like any endpoint (or let GitOps discover it):

    cd hack/fakeengine && beta9 --context <admin-context> deploy app.py:endpoint

The container runs this same file with ``serve``: an OpenAI-compatible HTTP
server (chat/completions, completions, embeddings; SSE streaming with real
``usage``), ``/health``, vLLM-named ``/metrics``, and a minimal
EndpointHarnessService client (register, heartbeat, WatchConfig, AckConfig,
drain). Live-tunable knobs: ``tokens_per_sec``, ``max_concurrency``,
``fail_rate``, ``ttft_ms``. No third-party dependencies beyond the beta9 SDK.
"""

from __future__ import annotations

import json
import math
import os
import random
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from beta9 import Catalog, GpuTarget, Image, ManagedEndpoint, Pricing

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
        GpuTarget(
            type="cpu", min_replicas=1, max_replicas=3, share=1.0, harness={"tokens_per_sec": 200}
        )
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


# --------------------------------------------------------------------------
# Engine state
# --------------------------------------------------------------------------


class Engine:
    """Shared mutable state: tunable knobs plus vLLM-shaped counters."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.knobs = {
            "tokens_per_sec": 200.0,
            "max_concurrency": 32,
            "fail_rate": 0.0,
            "ttft_ms": 20,
        }
        self.running = 0
        self.waiting = 0
        self.prompt_tokens_total = 0
        self.generation_tokens_total = 0
        self.requests_total = 0
        self.ttft_sum = 0.0
        self.ttft_count = 0
        self.tpot_sum = 0.0
        self.tpot_count = 0
        self.draining = False
        self.applied_revision = 0
        self.started = time.time()

    def apply(self, config: dict) -> dict:
        with self.lock:
            for key, value in (config or {}).items():
                if key in self.knobs:
                    self.knobs[key] = type(self.knobs[key])(value)
            return dict(self.knobs)

    def knob(self, name: str):
        with self.lock:
            return self.knobs[name]

    def admit(self) -> bool:
        with self.lock:
            if self.draining or self.running >= int(self.knobs["max_concurrency"]):
                return False
            self.running += 1
            self.requests_total += 1
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
            lines = [
                f"vllm:num_requests_running {self.running}",
                f"vllm:num_requests_waiting {self.waiting}",
                f"vllm:gpu_cache_usage_perc {min(1.0, self.running / max(1, int(self.knobs['max_concurrency']))):.4f}",
                f"vllm:prompt_tokens_total {self.prompt_tokens_total}",
                f"vllm:generation_tokens_total {self.generation_tokens_total}",
                f"vllm:time_to_first_token_seconds_sum {self.ttft_sum:.6f}",
                f"vllm:time_to_first_token_seconds_count {self.ttft_count}",
                f"vllm:time_per_output_token_seconds_sum {self.tpot_sum:.6f}",
                f"vllm:time_per_output_token_seconds_count {self.tpot_count}",
            ]
        return "\n".join(lines) + "\n"

    def capacity(self) -> dict:
        with self.lock:
            tps = float(self.knobs["tokens_per_sec"])
            return {
                "in_flight": self.running,
                "max_concurrency": int(self.knobs["max_concurrency"]),
                "running": self.running,
                "waiting": self.waiting,
                "kv_cache_free_milli": int(
                    1000 * (1 - self.running / max(1, int(self.knobs["max_concurrency"])))
                ),
                "decode_tokens_per_sec": int(tps * max(1, self.running)),
                "ttft_ms": int(self.knobs["ttft_ms"]),
                "tpot_ms": int(1000 / tps) if tps > 0 else 0,
            }


ENGINE = Engine()
WORDS = "the quick brown fox jumps over the lazy dog while the platform streams tokens".split()


def count_tokens(text: str) -> int:
    return max(1, math.ceil(len(text) / 4))


def prompt_text(body: dict) -> str:
    if "messages" in body:
        return " ".join(
            str(m.get("content", "")) for m in body.get("messages", []) if isinstance(m, dict)
        )
    prompt = body.get("prompt", "")
    return " ".join(prompt) if isinstance(prompt, list) else str(prompt)


def completion_tokens_for(body: dict) -> int:
    requested = body.get("max_tokens") or body.get("max_completion_tokens") or 32
    return max(1, min(int(requested), 1024))


# --------------------------------------------------------------------------
# HTTP server
# --------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "fake-engine/1.0"

    def log_message(self, fmt, *args):  # quiet
        if os.environ.get("FAKE_ENGINE_VERBOSE"):
            super().log_message(fmt, *args)

    # -- helpers -------------------------------------------------------------

    def send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return {}

    def error(self, status: int, message: str, kind: str = "invalid_request_error") -> None:
        self.send_json(status, {"error": {"message": message, "type": kind, "code": status}})

    # -- routes --------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/health", "/healthz", "/ready"):
            if ENGINE.draining:
                return self.error(503, "draining", "unavailable")
            return self.send_json(200, {"status": "ok"})
        if self.path == "/metrics":
            body = ENGINE.prometheus().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/v1/models":
            return self.send_json(
                200, {"object": "list", "data": [{"id": ENDPOINT_ID, "object": "model"}]}
            )
        self.error(404, f"no route for {self.path}")

    def do_POST(self) -> None:  # noqa: N802
        body = self.read_json()
        path = self.path.split("?", 1)[0]
        if path not in ("/v1/chat/completions", "/v1/completions", "/v1/embeddings"):
            return self.error(404, f"no route for {path}")
        if not ENGINE.admit():
            return self.error(503 if ENGINE.draining else 429, "engine is busy", "overloaded")
        try:
            if random.random() < float(ENGINE.knob("fail_rate")):
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
            {
                "object": "embedding",
                "index": i,
                "embedding": [((hash(str(x)) >> s) % 1000) / 1000 for s in range(8)],
            }
            for i, x in enumerate(inputs)
        ]
        ENGINE.record(tokens, 0, 0.0, 0.0)
        self.send_json(
            200,
            {
                "object": "list",
                "data": data,
                "model": ENDPOINT_ID,
                "usage": {"prompt_tokens": tokens, "total_tokens": tokens},
            },
        )

    def completion(self, body: dict, chat: bool) -> None:
        prompt_tokens = count_tokens(prompt_text(body))
        n_tokens = completion_tokens_for(body)
        tps = float(ENGINE.knob("tokens_per_sec"))
        step = 1.0 / tps if tps > 0 else 0.0
        ttft = float(ENGINE.knob("ttft_ms")) / 1000.0
        request_id = (
            f"chatcmpl-{uuid.uuid4().hex[:24]}" if chat else f"cmpl-{uuid.uuid4().hex[:24]}"
        )
        created = int(time.time())
        words = [WORDS[i % len(WORDS)] for i in range(n_tokens)]
        text = " ".join(words)
        usage = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": n_tokens,
            "total_tokens": prompt_tokens + n_tokens,
        }

        if not body.get("stream"):
            time.sleep(ttft + step * (n_tokens - 1))
            ENGINE.record(prompt_tokens, n_tokens, ttft, step)
            if chat:
                choice = {
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "stop",
                }
                return self.send_json(
                    200,
                    {
                        "id": request_id,
                        "object": "chat.completion",
                        "created": created,
                        "model": ENDPOINT_ID,
                        "choices": [choice],
                        "usage": usage,
                    },
                )
            choice = {"index": 0, "text": text, "finish_reason": "stop"}
            return self.send_json(
                200,
                {
                    "id": request_id,
                    "object": "text_completion",
                    "created": created,
                    "model": ENDPOINT_ID,
                    "choices": [choice],
                    "usage": usage,
                },
            )

        include_usage = bool((body.get("stream_options") or {}).get("include_usage"))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        def raw(data: bytes) -> None:
            self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
            self.wfile.flush()

        def chunk(payload: dict) -> None:
            raw(b"data: " + json.dumps(payload).encode() + b"\n\n")

        obj = "chat.completion.chunk" if chat else "text_completion"
        time.sleep(ttft)
        for i, word in enumerate(words):
            piece = word if i == 0 else " " + word
            if chat:
                delta = {"content": piece}
                if i == 0:
                    delta["role"] = "assistant"
                choice = {"index": 0, "delta": delta, "finish_reason": None}
            else:
                choice = {"index": 0, "text": piece, "finish_reason": None}
            chunk(
                {
                    "id": request_id,
                    "object": obj,
                    "created": created,
                    "model": ENDPOINT_ID,
                    "choices": [choice],
                }
            )
            if i < n_tokens - 1:
                time.sleep(step)
        final = (
            {"index": 0, "delta": {}, "finish_reason": "stop"}
            if chat
            else {"index": 0, "text": "", "finish_reason": "stop"}
        )
        chunk(
            {
                "id": request_id,
                "object": obj,
                "created": created,
                "model": ENDPOINT_ID,
                "choices": [final],
            }
        )
        if include_usage:
            chunk(
                {
                    "id": request_id,
                    "object": obj,
                    "created": created,
                    "model": ENDPOINT_ID,
                    "choices": [],
                    "usage": usage,
                }
            )
        raw(b"data: [DONE]\n\n")
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()
        ENGINE.record(prompt_tokens, n_tokens, ttft, step)


# --------------------------------------------------------------------------
# Harness client
# --------------------------------------------------------------------------


class Harness:
    """Minimal EndpointHarnessService client over the beta9 SDK channel."""

    def __init__(self, stop: threading.Event) -> None:
        from beta9.channel import Channel
        from beta9.clients.managedendpoint import EndpointHarnessServiceStub

        addr = f"{os.environ['BETA9_GATEWAY_HOST']}:{os.environ['BETA9_GATEWAY_PORT']}"
        self.channel = Channel(addr=addr, token=os.environ["BETA9_TOKEN"])
        self.stub = EndpointHarnessServiceStub(self.channel)
        self.stop = stop
        self.replica_id = ""
        self.interval = 5

    def log(self, msg: str) -> None:
        print(f"[harness] {msg}", flush=True)

    def register(self) -> None:
        from beta9.clients.managedendpoint import HarnessRegisterRequest

        caps = {"knobs": sorted(ENGINE.knobs), "engine": "fake"}
        for attempt in range(60):
            try:
                resp = self.stub.register(
                    HarnessRegisterRequest(
                        container_id=os.environ.get("CONTAINER_ID", ""),
                        engine="fake",
                        engine_version="1.0",
                        capabilities_json=json.dumps(caps),
                        limits_json=json.dumps(
                            {"max_concurrency": ENGINE.knobs["max_concurrency"]}
                        ),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                self.log(f"register failed ({exc}); retrying")
                time.sleep(min(10, 1 + attempt))
                continue
            if not resp.ok:
                self.log(f"register rejected: {resp.err_msg}; retrying")
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
        from beta9.clients.managedendpoint import HarnessAckConfigRequest

        try:
            effective = ENGINE.apply(json.loads(config_json or "{}"))
            ENGINE.applied_revision = revision
            ack = HarnessAckConfigRequest(
                replica_id=self.replica_id,
                revision=revision,
                applied=True,
                effective_json=json.dumps(effective),
            )
            self.log(f"applied revision {revision}: {effective}")
        except Exception as exc:  # noqa: BLE001
            ack = HarnessAckConfigRequest(
                replica_id=self.replica_id, revision=revision, applied=False, error=str(exc)
            )
        try:
            self.stub.ack_config(ack)
        except Exception as exc:  # noqa: BLE001
            self.log(f"ack failed: {exc}")

    def watch(self) -> None:
        from beta9.clients.managedendpoint import HarnessWatchConfigRequest

        while not self.stop.is_set():
            try:
                for rev in self.stub.watch_config(
                    HarnessWatchConfigRequest(
                        replica_id=self.replica_id, after_revision=ENGINE.applied_revision
                    )
                ):
                    if self.stop.is_set():
                        return
                    self.apply(rev.revision, rev.config_json)
            except Exception as exc:  # noqa: BLE001
                if self.stop.is_set():
                    return
                self.log(f"watch disconnected ({exc}); reconnecting")
                time.sleep(2)

    def heartbeat_loop(self, server: ThreadingHTTPServer) -> None:
        from beta9.clients.managedendpoint import HarnessHeartbeatRequest, ReplicaCapacity

        while not self.stop.is_set():
            status = "draining" if ENGINE.draining else "ready"
            try:
                resp = self.stub.heartbeat(
                    HarnessHeartbeatRequest(
                        replica_id=self.replica_id,
                        status=status,
                        capacity=ReplicaCapacity(**ENGINE.capacity()),
                        applied_revision=ENGINE.applied_revision,
                    )
                )
                if resp.heartbeat_interval_seconds:
                    self.interval = max(1, resp.heartbeat_interval_seconds)
                if resp.drain and not ENGINE.draining:
                    self.log(f"drain requested ({resp.drain_seconds}s)")
                    threading.Thread(
                        target=drain_and_exit,
                        args=(server, self.stop, resp.drain_seconds or 5),
                        daemon=True,
                    ).start()
            except Exception as exc:  # noqa: BLE001
                self.log(f"heartbeat failed: {exc}")
            self.stop.wait(self.interval)


def drain_and_exit(server: ThreadingHTTPServer, stop: threading.Event, seconds: int) -> None:
    ENGINE.draining = True
    deadline = time.time() + seconds
    while time.time() < deadline:
        with ENGINE.lock:
            if ENGINE.running == 0:
                break
        time.sleep(0.1)
    stop.set()
    server.shutdown()


def serve() -> None:
    port = int(os.environ.get("BEAM_ENDPOINT_PORT") or 8000)
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    stop = threading.Event()
    print(f"[fake-engine] listening on :{port}", flush=True)

    if os.environ.get("BEAM_HARNESS_ENABLED", "").lower() == "true":
        initial = os.environ.get("BEAM_HARNESS_CONFIG")
        if initial:
            ENGINE.apply(json.loads(initial))
        harness = Harness(stop)
        harness.register()
        threading.Thread(target=harness.watch, daemon=True).start()
        threading.Thread(target=harness.heartbeat_loop, args=(server,), daemon=True).start()

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
