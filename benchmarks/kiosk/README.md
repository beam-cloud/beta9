# Application streaming qualification

These tests measure an ordinary native application through a protected VM URL.
They deliberately keep the results separate from API and guest lifecycle tests.

`probe.py` supplies GTK, Qt, and OpenGL fixtures with a moving line canvas, text,
and optically readable frame and key-response counters. `probe.js` samples the
actual received pixels of visible video/canvas surfaces. Hidden fallback canvases
and repeated animation ticks do not count as delivered frames. The measurement
is browser-observed and quantized by the client display's animation cadence;
physical input-to-photon qualification additionally needs a high-speed camera.

## Run a sample

Use a dedicated test VM with 4 CPUs and 8 GiB initially. Install `python3-gi`,
`python3-gi-cairo`, `python3-pyqt5`, and a current supported comparison backend.
Run `probe.py --toolkit gtk` as its kiosk application. Use `qt` and `opengl` for
the other native fixtures. Install Playwright locally and use an existing Chrome
installation:

Build the supplied fixture image with `beam vm image build benchmarks/kiosk --kiosk`.
The image includes the toolkit's native Python bindings. Kiosk image preparation
keeps the distribution interpreter instead of injecting a serverless Python ABI.

```sh
cd sdk
CONFIG_PATH=~/.beam/config.ini uv run --with playwright python \
  ../benchmarks/kiosk/run.py --vm kiosk-bench --backend selkies-ws \
  --family gtk --output /tmp/kiosk/selkies-gtk-1080.json
```

The runner obtains a short-lived browser entry link through the selected SDK
context. Reports store the clean URL, never the entry credential. `--context`
selects a different saved context. `--headless` is useful for diagnostics; release
qualification requires the normal headed browser.
For Okteto, `--gateway-host 127.0.0.1 --gateway-port 11993` overrides only the
management transport; browser signaling and media still use public Beam URLs.

Repeat with `--width 1440 --height 900 --dpr 2`. Keep the same VM CPU/memory,
client machine/browser, network, application, and physical viewport when
comparing transports. Run candidates sequentially so they do not contend for
encoder resources. Save browser screenshots and inspect fine lines, text, dialogs,
and scaling alongside the numeric results.

## Candidate comparison

Compare Selkies 2.1.0 WebSockets/WebCodecs, Selkies WebRTC, and Xpra's current
HTML5 seamless application mode. Record exact server/client versions and encoder
profiles with each run. `--backend` labels an already configured candidate; it
does not silently change the server. Give Xpra a separate protected port and
start the same fixture in an isolated Xpra application session. Configure
WebRTC's ICE/STUN/TURN and network reachability explicitly; do not use a public
demo TURN relay as a production dependency.

The mandatory family matrix is GTK, Qt, Electron, drawing canvas, accelerated
3D, and media playback. The supplied native fixtures cover GTK, Qt, drawing, and
OpenGL; Electron and media require additional representative application runs.
A fixture result does not qualify an entire family automatically.

Initial gates for continuous interactive motion are at least 55 observed FPS,
p95 frame interval <=25 ms, at least 30 input-response samples, p95 input response
<=80 ms, and zero unanswered keys, on a path with <=20 ms RTT. The runner records
HTTP round-trip time, including gateway/server processing, as a conservative
upper bound rather than subtracting it from the input result.

Repeat under 60 ms RTT, constrained bandwidth, and packet loss. Restore any
`tc netem` changes before comparing candidates. Verify the stream returns to
current state after congestion, without replaying a growing queue of old frames.
Capture encode/decode timing and queue/bandwidth telemetry from the candidate's
diagnostics as well as the runner's VM CPU/memory samples.

`performance_gate_passed` is a single-sample result. Every report deliberately
sets `release_qualified: false`: release requires the full matrix, network recovery,
visual/input checks, and a passing headed-browser run. Select the qualifying
backend with lowest p95 input latency, breaking ties by frame pacing and then
resource use. If none qualifies, preserve the gate and address the measured
bottleneck (codec/queues, placement, or accelerated VM rendering/encoding).

## Functional integration checklist

Run application startup, clean exit and reopen, bounded crash recovery, menus,
modal dialogs, secondary windows, browser resizing, HiDPI, text/binary clipboard,
scrolling, modifier release on blur, common shortcuts, reconnect, and access
revocation. With an application MCP server, change the same project and verify
the change appears in the viewer. Save the project, snapshot/fork, and confirm
reopening preserves files and settings while the fork can change independently.

Stop the dedicated benchmark VM when done. Do not modify an unrelated user's
active desktop to obtain comparison results.

## Unmodified application measurements

`interaction.py` measures visible transitions in an installed application without
an application adapter. Use an isolated VM: the runner sends real input. Choose
an optical region that must change for the selected action, and inspect the
saved screenshots to confirm the action happened. A click on empty canvas is not
a valid selection measurement. For example, alternate a known object click and
Escape, or a wheel delta and its inverse:

```sh
CONFIG_PATH=~/.beam/config.ini uv run --with playwright python \
  ../benchmarks/kiosk/interaction.py --context staging --vm application-test \
  --click .46 .82 --region 0 .16 .17 .7 --samples 30 \
  --output /tmp/kiosk/selection.json
```

The default viewport is 1728 × 789 CSS pixels at DPR 2. Reports include native
received and guest PNGs, input failures, decode/presentation timing where the
browser exposes it, WebRTC receiver statistics, and VM resource samples. The
probe expires an unanswered input before sending another action so a later
response cannot accidentally answer both. These reports remain diagnostic;
headless runs and a single application do not meet release qualification.

## Native density and window roles

`input.py` exercises real GTK 3 and Qt 5 controls through the protected browser
URL: combo keyboard/mouse selection, nested menu hover, Escape/outside-click
dismissal, repeated single-click activation, fractional and horizontal wheels,
and pinch modifiers. Run only in an isolated VM with both native bindings:

```sh
CONFIG_PATH=~/.beam/config.ini uv run --with playwright python \
  ../benchmarks/kiosk/input.py --vm application-test --context staging \
  --output /tmp/kiosk/input.json
```

The management-forward flags below also work here. `--bootstrap-script` tests
an exact local browser script while retaining the real gateway/media path.
Single-click cycles honor each toolkit's configured double-click interval;
these are functional checks, separate from the optical latency benchmark.

`density.py` checks real GTK 3 and Qt 5 windows at browser DPR 1, 1.5, and 2.
Run it on an isolated kiosk with the native bindings installed:

```sh
CONFIG_PATH=~/.beam/config.ini uv run --with playwright python \
  ../benchmarks/kiosk/density.py --vm application-test --context staging \
  --window-roles utility dialog transient normal --output /tmp/kiosk/density.json
```

With Okteto, use `--gateway-host 127.0.0.1 --gateway-port 11993
--api-url http://127.0.0.1:11994` for the existing management forwards. Browser
media and signaling continue through the protected public staging URL.

The [recorded density/window result](results/2026-10-26-summary.json)
includes the matched KiCad schematic comparison and resize/reconnect checks.
The full run checks 21 cases: both toolkits' normal, utility, and dialog windows,
plus GTK's explicit NORMAL + WM_TRANSIENT_FOR window, at all three densities.
Qt's ordinary Qt.Window does not publish that transient hint from a QObject
parent; the fixture uses Qt's native dialog and utility paths. Each floating
window requests 640 × 480 logical pixels and must occupy 1280 × 960 guest pixels;
normal editors must fill the requested browser viewport at 2× density. Reports
record font metrics and framebuffer dimensions as well as screenshots. These
functional checks do not establish streaming latency or qualify release.

The interaction runner now arms one expected input at a time and records
unplanned browser input separately (`optical-serial-v3`). A run with unexpected
input is unsuitable for causal comparison, even if its optical transitions
look fast. Reports also retain the per-frame pixel-probe overhead; that cost is
included in the observed response and is never subtracted. Full-frame and
properties-title regions can be compared with `--compare-region`, at additional
sampling cost. An unchanged optical region is not proof that a key was dropped;
verify the application's state and the selected action before interpreting a
missed transition. Headless diagnostics avoid interference with an active user,
but release qualification still needs a controlled headed-browser run.

The [diagnostic summary](results/2026-10-26-summary.json) records CPU,
density, input, reconnect, and cold-restore checks, with historical reports
linked separately. The latest CPU diagnostic observed 30/30 single-object
selection transitions at 126 ms median and 176 ms p95 at 3456 × 1578 pixels.
It exceeds the latency target; the full family/network qualification remains
outstanding. Different workloads and measurement versions are not evidence
of an optimization win.
