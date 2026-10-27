#!/usr/bin/env python3
"""Check browser density and window roles against real native GTK/Qt windows."""

import argparse
import dataclasses
import json
from pathlib import Path
from beta9 import VM
from beta9.config import get_config_context
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vm", required=True)
    parser.add_argument("--context", default="staging")
    parser.add_argument("--gateway-host")
    parser.add_argument("--gateway-port", type=int)
    parser.add_argument("--api-url")
    parser.add_argument("--bootstrap-script", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--window-roles",
        nargs="+",
        default=["utility"],
        choices=["utility", "dialog", "transient", "normal"],
    )
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    overrides = dict(
        gateway_host=args.gateway_host, gateway_port=args.gateway_port, api_url=args.api_url
    )
    config = dataclasses.replace(
        get_config_context(args.context), **{k: v for k, v in overrides.items() if v is not None}
    )
    fixture = Path(__file__).with_name("toolkit_density.py").read_bytes()
    launcher = b"""import runpy,subprocess,sys
sys.path.insert(0,'/opt/beam-vm')
m=runpy.run_path('/opt/beam-vm/kiosk')
e=m['Session']({'command':['density-fixture']}).env
r=subprocess.run(['/usr/bin/python3','/tmp/beam-toolkit-density.py',*sys.argv[1:]],env=e,capture_output=True,text=True,timeout=10)
sys.stdout.write(r.stdout)
if r.returncode:sys.stderr.write(r.stderr)
sys.exit(r.returncode)
"""
    rows = []
    with VM.get(args.vm, context=config) as vm, sync_playwright() as pw:
        print("Connected to isolated density test VM", flush=True)
        vm.fs.write_bytes("/tmp/beam-toolkit-density.py", fixture)
        vm.fs.write_bytes("/tmp/beam-density-launch.py", launcher)
        browser = pw.chromium.launch(channel="chrome")
        for density in (1, 1.5, 2):
            ctx = browser.new_context(
                viewport={"width": 1728, "height": 789}, device_scale_factor=density
            )
            if args.bootstrap_script:
                ctx.route(
                    "**/_beam/kiosk/webrtc.js",
                    lambda route: route.fulfill(
                        content_type="text/javascript", body=args.bootstrap_script.read_bytes()
                    ),
                )
            page = ctx.new_page()
            try:
                page.goto(vm.access_url(8080), wait_until="commit")
                page.wait_for_function(
                    "document.querySelector('[data-beam-kiosk]')?.hidden && document.querySelector('video#stream')?.videoWidth>0",
                    timeout=45000,
                )
            except Exception:
                raise RuntimeError("Density test stream did not become ready") from None
            result = page.evaluate("""()=>{
              const v=document.querySelector('video#stream'),r=v.getBoundingClientRect();
              return {native_density:window.beamKiosk.devicePixelRatio,render_density:window.beamKiosk.pixelRatio,
                viewport:[innerWidth,innerHeight],display_css:[r.width,r.height],pixels:[v.videoWidth,v.videoHeight]};
            }""")
            assert result["native_density"] == density, result
            assert result["render_density"] == 2, result
            assert result["pixels"] == [3456, 1578], result
            assert result["display_css"] == [1728, 789], result
            print(json.dumps({"browser_density": density, "pixels": result["pixels"]}), flush=True)
            result["toolkits"] = []
            for toolkit in ("gtk", "qt"):
                for role in args.window_roles:
                    if toolkit == "qt" and role == "transient":
                        continue  # Explicit X11 normal-transient hints use the GTK fixture.
                    p = vm.process.exec(
                        "/usr/bin/python3", "/tmp/beam-density-launch.py", toolkit, role, cwd="/"
                    )
                    if p.wait(15) != 0:
                        raise RuntimeError(
                            "Native density fixture failed: " + p.stderr.read()[:500]
                        )
                    item = json.loads(p.stdout.read())
                    assert item["scale"] == 2, item
                    expected = result["pixels"] if role == "normal" else [1280, 960]
                    assert list(map(int, item["physical_window"])) == expected, item
                    assert list(item["logical_window"]) == [v // 2 for v in expected], item
                    result["toolkits"].append(item)
            rows.append(result)
            page.screenshot(path=str(args.output.with_suffix(f".{density}x.png")))
            ctx.close()
        browser.close()
    args.output.write_text(
        json.dumps({"passed": True, "cases": rows, "release_qualified": False}, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                "passed": True,
                "browser_densities": [r["native_density"] for r in rows],
                "render_density": 2,
                "toolkit_scales": [i["scale"] for i in rows[0]["toolkits"]],
            }
        )
    )


if __name__ == "__main__":
    main()
