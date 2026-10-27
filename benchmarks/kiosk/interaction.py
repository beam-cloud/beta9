#!/usr/bin/env python3
"""Repeatable native-app interaction benchmark in an isolated browser profile.

This never attaches to the user's open browser or changes application files.
The selected keys and optical region must produce a known visible transition.
"""

import argparse
import base64
import dataclasses
import json
import math
from pathlib import Path
import time
from urllib.parse import urlsplit, urlunsplit

from beta9 import VM
from beta9.config import get_config_context
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


def percentile(values, p):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * p) - 1)] if values else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vm", required=True)
    parser.add_argument("--context", default="staging")
    parser.add_argument("--gateway-host")
    parser.add_argument("--gateway-port", type=int)
    parser.add_argument("--width", type=int, default=1728)
    parser.add_argument("--height", type=int, default=789)
    parser.add_argument("--dpr", type=float, default=2)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--keys", nargs="+", default=["Control+a", "Escape"])
    parser.add_argument(
        "--click", nargs=2, type=float, help="Alternate a normalized canvas click and Escape"
    )
    parser.add_argument(
        "--wheel", type=int, help="Alternate this wheel delta and its inverse over the canvas"
    )
    parser.add_argument(
        "--repeat-click",
        action="store_true",
        help="Click every sample instead of alternating with Escape",
    )
    parser.add_argument(
        "--compare-region",
        action="append",
        nargs=5,
        metavar=("NAME", "X", "Y", "W", "H"),
        default=[],
    )
    parser.add_argument("--api-url")
    parser.add_argument(
        "--bootstrap-script",
        type=Path,
        help="Test a local bootstrap without restarting the shared gateway",
    )
    parser.add_argument("--region", nargs=4, type=float, default=[0, 0.16, 0.17, 0.12])
    parser.add_argument("--headed", action="store_true")
    parser.add_argument(
        "--recovery-check",
        action="store_true",
        help="Verify transient status failures and a page reconnect",
    )
    parser.add_argument(
        "--trace-transport",
        action="store_true",
        help="Requires the isolated guest diagnostic echo patch",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.samples < 1 or args.samples > 500:
        parser.error("--samples must be between 1 and 500")
    if args.click and args.wheel:
        parser.error("--click and --wheel are mutually exclusive")
    if args.repeat_click and not args.click:
        parser.error("--repeat-click requires --click")
    config = get_config_context(args.context)
    config = dataclasses.replace(
        config,
        **{
            name: value
            for name, value in {
                "gateway_host": args.gateway_host,
                "gateway_port": args.gateway_port,
                "api_url": args.api_url,
            }.items()
            if value is not None
        },
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with VM.get(args.vm, context=config) as vm, sync_playwright() as pw:
        before = vm.metrics()
        browser = pw.chromium.launch(channel="chrome", headless=not args.headed)
        context = browser.new_context(
            viewport={"width": args.width, "height": args.height},
            device_scale_factor=args.dpr,
        )
        context.add_init_script(path=str(Path(__file__).with_suffix(".js")))
        if args.bootstrap_script:
            context.route(
                "**/_beam/kiosk/webrtc.js",
                lambda route: route.fulfill(
                    content_type="text/javascript", body=args.bootstrap_script.read_bytes()
                ),
            )
        page = context.new_page()
        started = time.monotonic()
        try:
            page.goto(vm.access_url(8080), wait_until="commit")
            page.bring_to_front()
            page.wait_for_function(
                """() => {
              const v=document.querySelector('video#stream'),c=document.querySelector('canvas#videoCanvas');
              const fitted=window.beamKiosk?.status?.transport==='websockets'?
                (document.querySelector('video#videoStream')?.readyState>=2 || c?.width>0 && c.getBoundingClientRect().width>0):
                v && v.videoWidth===v.width && v.videoHeight===v.height && !v.paused && v.readyState>=2;
              return fitted && window.beamKiosk?.status?.application==='ready' &&
                document.querySelector('[data-beam-kiosk]')?.hidden;
            }""",
                timeout=90000,
            )
        except Exception:
            # Browser errors can include the credential-bearing entry URL.
            browser.close()
            raise RuntimeError("Protected application stream did not become ready") from None
        first_frame_seconds = time.monotonic() - started
        recovery = None
        if args.recovery_check:
            status_route = "**/_beam/kiosk/status"
            page.route(status_route, lambda route: route.abort())
            page.wait_for_timeout(2400)
            if not page.evaluate("document.querySelector('[data-beam-kiosk]').hidden"):
                raise RuntimeError("Transient status failure covered a healthy application")
            page.unroute(status_route)
            page.reload(wait_until="commit")
            page.wait_for_function(
                "document.querySelector('[data-beam-kiosk]')?.hidden && document.querySelector('video')?.readyState>=2",
                timeout=30000,
            )
            vm.desktop.press("alt+F4")
            reopen = page.get_by_role("button", name="Reopen application", exact=True)
            reopen.wait_for(state="visible", timeout=15000)
            reopen.click()
            page.wait_for_function(
                "document.querySelector('[data-beam-kiosk]')?.hidden && document.querySelector('video')?.readyState>=2",
                timeout=30000,
            )
            recovery = {
                "transient_status_failure": True,
                "page_reconnect": True,
                "application_reopen": True,
            }
        page.screenshot(path=str(args.output.with_suffix(".initial.png")))
        page.mouse.click(args.width * 0.64, args.height * 0.57)
        page.keyboard.press("Escape")
        if args.click:
            page.mouse.move(args.width * args.click[0], args.height * args.click[1])
        page.evaluate("region => window.beamInteractionProbe.setRegion(region)", args.region)
        for name, *rect in args.compare_region:
            page.evaluate(
                "([name,rect]) => window.beamInteractionProbe.addRegion(name,rect)",
                [name, list(map(float, rect))],
            )
        page.wait_for_function("window.beamInteractionProbe.settledForMs>350")
        page.evaluate("window.beamInteractionProbe.reset()")
        if args.trace_transport:
            for _ in range(10):
                page.evaluate("window.beamInteractionTransport.ping()")
                page.wait_for_timeout(120)
            page.evaluate("window.beamInteractionTransport.trace=true")
        failures = []
        for index in range(args.samples):
            key = args.keys[index % len(args.keys)]
            count = page.evaluate("window.beamInteractionProbe.samples.length")
            if args.wheel:
                page.mouse.move(args.width * 0.48, args.height * 0.50)
                page.evaluate("window.beamInteractionProbe.arm('wheel')")
                page.mouse.wheel(0, args.wheel if index % 2 == 0 else -args.wheel)
                key = "wheel"
            elif args.click and (args.repeat_click or index % 2 == 0):
                page.evaluate("window.beamInteractionProbe.arm('mousedown')")
                page.mouse.click(args.width * args.click[0], args.height * args.click[1])
                key = "click"
            else:
                expected_key = "Escape" if args.click else key.split("+")[-1]
                if expected_key == "Space":
                    expected_key = " "
                page.evaluate("key=>window.beamInteractionProbe.arm('keydown',key)", expected_key)
                page.keyboard.press("Escape" if args.click else key)
            try:
                page.wait_for_function(
                    "count => window.beamInteractionProbe.samples.length>count",
                    arg=count,
                    timeout=2000,
                )
            except PlaywrightTimeout:
                failures.append({"index": index, "key": key})
                page.evaluate("window.beamInteractionProbe.expire()")
                if len(failures) <= 3:
                    page.screenshot(path=str(args.output.with_suffix(f".failure-{index}.png")))
            page.wait_for_function("window.beamInteractionProbe.settledForMs>150")
        result = page.evaluate("""() => ({
          samples:window.beamInteractionProbe.samples,unanswered:window.beamInteractionProbe.unanswered,
          events:window.beamInteractionProbe.events,
          unexpected_inputs:window.beamInteractionProbe.unexpectedInputs,
          transport:window.beamInteractionTransport.samples,input_timings:window.beamInteractionTransport.inputs,
          comparisons:Object.fromEntries(Object.entries(window.beamInteractionProbe.comparisons).map(([name,r])=>[name,r.samples])),
          frames:window.beamInteractionProbe.frames,
          probe_costs_ms:window.beamInteractionProbe.probeCosts,
          viewport:[innerWidth,innerHeight,window.beamKiosk?.devicePixelRatio||devicePixelRatio],
          render_pixel_ratio:window.beamKiosk?.pixelRatio||devicePixelRatio,
          video:(()=>{const s=[...document.querySelectorAll('video#stream,video#videoStream,canvas#videoCanvas')].find(s=>s.getBoundingClientRect().width>0);return [s.videoWidth||s.width,s.videoHeight||s.height]})()
        })""")
        if args.trace_transport:
            count = page.evaluate("window.beamInteractionTransport.samples.length")
            page.evaluate("window.beamInteractionTransport.ping()")
            page.wait_for_function(
                "count=>window.beamInteractionTransport.samples.length>count",
                arg=count,
                timeout=3000,
            )
            result["transport"] = page.evaluate("window.beamInteractionTransport.samples")
        result["http_rtt_ms"] = page.evaluate("""async()=>{
          const result=[];
          for(let i=0;i<5;i++) {
            const start=performance.now();await fetch(new URL('../_beam/kiosk/status',location.href),{cache:'no-store'});
            result.push(performance.now()-start);
          }
          return result;
        }""")
        result["webrtc"] = page.evaluate("""async () => {
          const result=[];
          for(const p of window.beamKiosk.peers){
            if(p.connectionState!=='connected')continue;
            const stats=await p.getStats(), reports=[];
            stats.forEach(s=>{
              if(s.type==='inbound-rtp'&&['video','audio'].includes(s.kind))reports.push({kind:s.kind,
                frames:s.framesDecoded,dropped:s.framesDropped,jitter:s.jitter,
                codec:stats.get(s.codecId)?.mimeType,fmtp:stats.get(s.codecId)?.sdpFmtpLine,decoder:s.decoderImplementation,
                decode_seconds:s.totalDecodeTime,buffer_seconds:s.jitterBufferDelay,
                target_buffer_seconds:s.jitterBufferTargetDelay,minimum_buffer_seconds:s.jitterBufferMinimumDelay,
                processing_seconds:s.totalProcessingDelay,power_efficient:s.powerEfficientDecoder,
                buffer_count:s.jitterBufferEmittedCount,bytes:s.bytesReceived,loss:s.packetsLost,
              });
              if(s.type==='candidate-pair'&&s.nominated)reports.push({rtt_seconds:s.currentRoundTripTime});
            });
            result.push(reports);
          }
          return result;
        }""")
        page.screenshot(path=str(args.output.with_suffix(".png")))
        encoded = page.evaluate("""() => {
          const v=[...document.querySelectorAll('video#stream,video#videoStream,canvas#videoCanvas')].find(s=>s.getBoundingClientRect().width>0),c=document.createElement('canvas');
          c.width=v.videoWidth||v.width;c.height=v.videoHeight||v.height;c.getContext('2d').drawImage(v,0,0);
          return c.toDataURL('image/png').split(',')[1];
        }""")
        args.output.with_suffix(".decoded.png").write_bytes(base64.b64decode(encoded))
        vm.desktop.screenshot(str(args.output.with_suffix(".guest.png")))
        parts = urlsplit(page.url)
        result.update(
            url=urlunsplit((parts.scheme, parts.netloc, parts.path, "", "")),
            browser=browser.version,
            headless=not args.headed,
            measurement_method="optical-serial-v3",
            trace_transport=args.trace_transport,
            first_frame_seconds=first_frame_seconds,
            failures=failures,
            recovery=recovery,
            planned_samples=args.samples,
            controlled_inputs=not result["unexpected_inputs"],
            vm_spec={k: vm.info["spec"].get(k) for k in ("cpu", "memory", "pool", "image_id")},
            metrics_before=before,
            metrics_after=vm.metrics(),
            release_qualified=False,
        )
        latencies = [
            s["latency_ms"]
            for s in result["samples"]
            if s["type"] in ("keydown", "mousedown", "wheel")
        ]
        costs = result["probe_costs_ms"]
        result["probe_overhead_ms"] = {
            "p50": percentile(costs, 0.5),
            "p95": percentile(costs, 0.95),
            "max": max(costs, default=None),
        }
        result["measurements"] = {
            "input_samples": len(latencies),
            "input_p50_ms": percentile(latencies, 0.5),
            "input_p95_ms": percentile(latencies, 0.95),
            "input_max_ms": max(latencies, default=None),
            "unanswered": result["unanswered"],
            "timeouts": len(failures),
            "unexpected_inputs": len(result["unexpected_inputs"]),
        }
        page.evaluate("window.beamInteractionProbe.stop()")
        browser.close()
        args.output.write_text(json.dumps(result, indent=2))
        print(
            json.dumps(
                {k: result[k] for k in ("measurements", "first_frame_seconds", "video")}, indent=2
            )
        )


if __name__ == "__main__":
    main()
