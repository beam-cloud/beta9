#!/usr/bin/env python3
"""Measure a native probe through a protected VM URL using a real browser."""

import argparse
import dataclasses
import json
import math
import os
from pathlib import Path
import statistics
import time
from urllib.parse import urlsplit, urlunsplit


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summarize(frames, inputs, pending=0):
    intervals = [right - left for left, right in zip(frames, frames[1:])]
    return {
        "presented_fps": (len(frames) - 1) * 1000 / (frames[-1] - frames[0])
        if len(frames) > 1
        else 0,
        "frame_interval_p95_ms": percentile(intervals, 0.95),
        "frame_interval_p99_ms": percentile(intervals, 0.99),
        "input_response_p50_ms": percentile(inputs, 0.5),
        "input_response_p95_ms": percentile(inputs, 0.95),
        "input_samples": len(inputs),
        "unanswered_inputs": pending,
    }


def gate(measurements, rtt_ms):
    return (
        rtt_ms <= 20
        and measurements["presented_fps"] >= 55
        and measurements["frame_interval_p95_ms"] is not None
        and measurements["frame_interval_p95_ms"] <= 25
        and measurements["input_samples"] >= 30
        and measurements["input_response_p95_ms"] <= 80
        and measurements["unanswered_inputs"] == 0
    )


def main():
    os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")
    from playwright.sync_api import sync_playwright
    from beta9 import VM
    from beta9.config import get_config_context

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vm", required=True)
    parser.add_argument("--context", default="default")
    parser.add_argument(
        "--gateway-host", help="Override gRPC host, e.g. Okteto's local forward"
    )
    parser.add_argument("--gateway-port", type=int)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--backend", choices=("selkies-ws", "selkies-webrtc", "xpra"), required=True
    )
    parser.add_argument(
        "--family",
        choices=("gtk", "qt", "electron", "canvas", "opengl", "media"),
        required=True,
    )
    parser.add_argument("--width", type=int, default=1920)
    parser.add_argument("--height", type=int, default=1080)
    parser.add_argument("--dpr", type=int, default=1)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--channel", default="chrome")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    config = get_config_context(args.context)
    if args.gateway_host:
        config = dataclasses.replace(
            config,
            gateway_host=args.gateway_host,
            gateway_port=args.gateway_port or config.gateway_port,
        )
    vm = VM.get(args.vm, context=config)
    before = vm.metrics()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            channel=args.channel, headless=args.headless
        )
        context = browser.new_context(
            viewport={"width": args.width, "height": args.height},
            device_scale_factor=args.dpr,
        )
        context.add_init_script(path=Path(__file__).with_name("probe.js"))
        page = context.new_page()
        try:
            page.goto(vm.access_url(args.port), wait_until="commit", timeout=60000)
        except Exception:
            # Browser exceptions echo navigation URLs, including entry credentials.
            raise RuntimeError(
                "Browser could not reach the protected application stream"
            ) from None
        page.bring_to_front()
        deadline = time.monotonic() + 90
        frame = None
        while time.monotonic() < deadline:
            frame = next(
                (
                    item
                    for item in page.frames
                    if item.evaluate("Boolean(window.beamStreamProbe?.valid)")
                ),
                None,
            )
            if frame:
                break
            page.wait_for_timeout(250)
        if frame is None:
            page.screenshot(path=str(args.output.with_suffix(".failure.png")))
            raise RuntimeError(
                "No native probe calibration pattern reached the browser; result cannot pass"
            )
        page.mouse.click(args.width / 2, args.height / 2)
        page.wait_for_timeout(2000)
        rtts = []
        for _ in range(10):
            rtts.append(
                frame.evaluate("""async () => {
              const start = performance.now();
              await fetch(new URL('api/health', location.href), {cache:'no-store'});
              return performance.now() - start;
            }""")
            )
        # HTTP probes include server/gateway processing. They are an upper bound
        # on path RTT and never subtracted from the measured input response.
        frame.evaluate(
            "window.beamStreamProbe.frames=[]; window.beamStreamProbe.inputs=[]; window.beamStreamProbe.pending=[]"
        )
        deadline = time.monotonic() + args.seconds
        while time.monotonic() < deadline:
            page.keyboard.press("a")
            page.wait_for_timeout(250)
        page.wait_for_timeout(1000)
        samples = frame.evaluate("window.beamStreamProbe")
        measurements = summarize(
            samples["frames"], samples["inputs"], len(samples["pending"])
        )
        rtc = frame.evaluate("""async () => {
          const reports = [];
          for (const peer of window.beamProbePeers || []) {
            const stats = await peer.getStats();
            const selected = [];
            stats.forEach(report => {
              if (!['inbound-rtp','outbound-rtp','candidate-pair'].includes(report.type)) return;
              const safe = {};
              for (const key of ['type','kind','state','nominated','framesReceived','framesDecoded','framesDropped','framesPerSecond','totalDecodeTime','totalProcessingDelay','jitter','jitterBufferDelay','jitterBufferEmittedCount','packetsLost','packetsReceived','bytesReceived','currentRoundTripTime','availableIncomingBitrate','availableOutgoingBitrate']) {
                if (report[key] !== undefined) safe[key] = report[key];
              }
              if (report.type === 'candidate-pair') {
                safe.localProtocol = stats.get(report.localCandidateId)?.protocol;
                safe.remoteProtocol = stats.get(report.remoteCandidateId)?.protocol;
              }
              selected.push(safe);
            });
            reports.push({connectionState:peer.connectionState,iceConnectionState:peer.iceConnectionState,stats:selected});
          }
          return reports;
        }""")
        page.screenshot(path=str(args.output.with_suffix(".png")))
        parts = urlsplit(page.url)
        result = {
            "backend": args.backend,
            "family": args.family,
            "viewport": {"width": args.width, "height": args.height, "dpr": args.dpr},
            "url": urlunsplit((parts.scheme, parts.netloc, parts.path, "", "")),
            "http_round_trip_median_ms": statistics.median(rtts),
            "measurements": measurements,
            "raw_samples": samples,
            "browser": browser.version,
            "headless": args.headless,
            "network_path": "public-gateway",
            "webrtc": rtc,
            "vm_spec": {
                key: vm.info["spec"].get(key)
                for key in ("cpu", "memory", "image_id", "pool")
            },
            "metrics_before": before,
            "metrics_after": vm.metrics(),
            "performance_gate_passed": gate(measurements, statistics.median(rtts)),
            "release_qualified": False,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2))
        print(
            json.dumps(
                {
                    key: result[key]
                    for key in (
                        "backend",
                        "family",
                        "measurements",
                        "performance_gate_passed",
                    )
                },
                indent=2,
            )
        )
        browser.close()


if __name__ == "__main__":
    main()
