#!/usr/bin/env python3
"""Exercise real native popup controls and wheel input through the kiosk gateway."""

import argparse
import dataclasses
import json
from pathlib import Path
import time

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
    args = parser.parse_args()
    overrides = {
        k: getattr(args, k)
        for k in ("gateway_host", "gateway_port", "api_url")
        if getattr(args, k)
    }
    context = dataclasses.replace(get_config_context(args.context), **overrides)
    output = []
    with VM.get(args.vm, context=context) as guest, sync_playwright() as pw:
        guest.fs.write_bytes(
            "/tmp/beam-input-fixture.py",
            Path(__file__).with_name("input_fixture.py").read_bytes(),
        )
        browser = pw.chromium.launch(channel="chrome")
        try:
            for toolkit in ("gtk", "qt"):
                reset = guest.process.exec(
                    "env",
                    "DISPLAY=:1",
                    "XAUTHORITY=/run/beam-desktop/.Xauthority",
                    "xdotool",
                    "key",
                    "Escape",
                    "Escape",
                    "Escape",
                    cwd="/",
                )
                assert reset.wait(10) == 0
                state_path = f"/tmp/beam-input-{toolkit}.json"
                guest.fs.write_bytes(state_path, b"{}")
                launch = """import os,runpy,sys
sys.path.insert(0,'/opt/beam-vm')
env=runpy.run_path('/opt/beam-vm/kiosk')['Session']({'command':['input-fixture']}).env
os.execve('/usr/bin/python3',['/usr/bin/python3','/tmp/beam-input-fixture.py',*sys.argv[1:]],env)
"""
                process = guest.process.exec(
                    "/usr/bin/python3", "-c", launch, toolkit, state_path, cwd="/"
                )
                ctx = browser.new_context(
                    viewport={"width": 1440, "height": 900}, device_scale_factor=2
                )
                page = ctx.new_page()
                if args.bootstrap_script:
                    ctx.route(
                        "**/_beam/kiosk/webrtc.js",
                        lambda route: route.fulfill(
                            content_type="text/javascript",
                            body=args.bootstrap_script.read_bytes(),
                        ),
                    )
                try:
                    page.goto(guest.access_url(8080), wait_until="commit")
                    page.wait_for_function(
                        "document.querySelector('[data-beam-kiosk]')?.hidden",
                        timeout=60000,
                    )
                    page.evaluate("""()=>{window.inputPackets=[];
                      const input=window.webrtcInput,send=input.send;
                      input.send=value=>{window.inputPackets.push(value);return send(value);};
                    }""")

                    def state():
                        return json.loads(guest.fs.read_bytes(state_path))

                    def wait(check):
                        deadline = time.monotonic() + 8
                        while time.monotonic() < deadline:
                            value = state()
                            if check(value):
                                return value
                            page.wait_for_timeout(100)
                        raise AssertionError(f"{toolkit} input state: {state()}")

                    wait(
                        lambda s: s.get("controls", {}).get("canvas", [0, 0, 0])[2]
                        > 1300
                    )

                    def point(rect):
                        x, y, w, h = rect
                        return x + w / 2, y + h / 2

                    def click(name):
                        page.mouse.click(*point(state()["controls"][name]))
                        # These are separate activations, not native double
                        # clicks. Popup/keyboard readiness is checked separately.
                        page.wait_for_timeout(state()["double_click_ms"] + 20)

                    def menu_item(name):
                        s = wait(
                            lambda s: any(
                                name in popup["items"] for popup in s["popups"]
                            )
                        )
                        return point(
                            next(
                                popup["items"][name]
                                for popup in s["popups"]
                                if name in popup["items"]
                            )
                        )

                    # Native combo keyboard behavior, including Escape dismissal.
                    click("combo")
                    wait(lambda s: bool(s["popups"]))
                    page.keyboard.press("End")
                    page.keyboard.press("ArrowUp")
                    page.keyboard.press("Enter")
                    wait(lambda s: s["selection"] == "Beta")
                    click("combo")
                    page.mouse.click(*menu_item("Gamma"))
                    wait(lambda s: s["selection"] == "Gamma" and not s["popups"])
                    for _ in range(10):
                        click("combo")
                        wait(lambda s: bool(s["popups"]))
                        page.keyboard.press("Escape")
                        wait(lambda s: not s["popups"])
                    # Menus must track hover without erasing the preceding click.
                    click("menu")
                    page.mouse.move(*menu_item("Nested"), steps=8)
                    page.mouse.click(*menu_item("Second"))
                    wait(lambda s: s["menu"] == "Second" and not s["popups"])
                    click("menu")
                    wait(lambda s: bool(s["popups"]))
                    page.keyboard.press("Escape")
                    wait(lambda s: not s["popups"])
                    click("menu")
                    wait(lambda s: bool(s["popups"]))
                    click("canvas")
                    wait(lambda s: not s["popups"])
                    for _ in range(20):
                        click("button")
                    wait(lambda s: s["zoom"] == 20)
                    x, y = point(state()["controls"]["canvas"])
                    page.mouse.move(x, y)

                    def wheel(deltas, **options):
                        page.evaluate(
                            """({deltas,options,x,y})=>{
                          for (const deltaY of deltas) window.webrtcInput.element.dispatchEvent(new WheelEvent('wheel',
                            {bubbles:true,cancelable:true,clientX:x,clientY:y,deltaY,...options}));
                        }""",
                            {"deltas": deltas, "options": options, "x": x, "y": y},
                        )

                    wheel([1] * 10)
                    page.wait_for_timeout(250)
                    assert state()["wheel"] == [], (
                        "tiny trackpad events became full notches"
                    )
                    wheel([0.25] * 360)
                    wait(lambda s: len(s["wheel"]) == 1)
                    assert state()["wheel"][-1][:2] == [0, -120]
                    wheel([-100])
                    wait(lambda s: len(s["wheel"]) == 2)
                    assert state()["wheel"][-1][:2] == [0, 120]
                    wheel([3], deltaMode=1)
                    wait(lambda s: len(s["wheel"]) == 3)
                    wheel([0], deltaX=100)
                    wait(lambda s: len(s["wheel"]) == 4)
                    assert abs(state()["wheel"][-1][0]) == 120
                    wheel([-20], ctrlKey=True)
                    wait(lambda s: len(s["wheel"]) >= 5)
                    assert state()["wheel"][-1][2] & (
                        4 if toolkit == "gtk" else 0x4000000
                    )
                    # The live application retains pointer capture after dismissal.
                    click("button")
                    wait(lambda s: s["zoom"] == 21)
                    assert page.evaluate("innerWidth") == 1440
                    output.append(
                        {
                            "toolkit": toolkit,
                            "passed": True,
                            "popup_cycles": 10,
                            "button_clicks": 21,
                            "fractional_wheel_steps": 1,
                            "nested_menu": True,
                            "outside_and_escape_dismissal": True,
                            "pinch_modifier": True,
                        }
                    )
                    page.screenshot(
                        path=str(args.output.with_suffix(f".{toolkit}.png"))
                    )
                    print(json.dumps(output[-1]), flush=True)
                except Exception:
                    print(
                        json.dumps(
                            page.evaluate("""()=>({
                      packets:window.inputPackets?.slice(-30),
                      pointer:[window.webrtcInput?.x,window.webrtcInput?.y,window.webrtcInput?.buttonMask],
                      ordered:window.webrtcInput?.sendMotion?.toString(),
                      focus:document.activeElement?.id,
                    })""")
                        ),
                        flush=True,
                    )
                    page.screenshot(
                        path=str(args.output.with_suffix(f".{toolkit}.failure.png"))
                    )
                    raise
                finally:
                    process.kill()
                    ctx.close()
        finally:
            browser.close()
    args.output.write_text(
        json.dumps({"passed": True, "cases": output}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
