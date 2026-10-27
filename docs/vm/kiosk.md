# Application kiosk VMs (experimental)

A kiosk is a private graphical application session inside a persistent Linux
VM. It accepts an ordinary application command, fills a browser tab with its
windows, and keeps the operating system shell out of the interface. Applications
need no streaming integration. Dialogs, secondary windows, and file choosers
remain part of the session.

Authenticated tabs share the same application and each has full keyboard and
pointer control. The first connected tab owns the framebuffer size; other tabs
fit that stream, and ownership passes when it leaves. Connections remain subject
to the gateway's configured WebRTC limits.

Kiosk images use a pinned, checksum-verified Selkies 2.1.0 distribution and an
application-only X11 window manager. They do not install or launch KasmVNC.
With the shared TCP gateway enabled, kiosk sessions default to WebRTC over
ICE-TCP. HTTPS carries signaling and an authenticated session registration;
encrypted DTLS/SRTP media uses the existing raw TCP load balancer on port 443.
The gateway validates the first STUN check's MESSAGE-INTEGRITY before routing
to private guest port 8443. VM generation, traffic-token rotation, and owner
revocation fence old connections. No public guest media port or demo TURN
service is required. An HTTPS-only proxy cannot replace this raw L4 path.

Set `"transport": "websockets"` in kiosk configuration for a comparison run or
an installation without a shared TCP gateway. Explicit `"transport": "webrtc"`
fails before launch if that gateway is unavailable. Application commands,
project files, and MCP configuration work with either transport. TCP can still
stall behind packet loss; transport support does not waive the quality gate.
Existing full-desktop VMs retain their current behavior. Ubuntu 24.04 and 22.04
are supported image targets, on amd64 and arm64; the live integration work uses
Ubuntu 24.04 amd64.

## Launch an application

Install your application in a Dockerfile based on Ubuntu. The VM image builder
adds the system services and application streaming dependencies:

```sh
beam vm image build . --kiosk --json
beam vm new design-project --image-id IMAGE_ID --kiosk-config kiosk.json
beam vm kiosk design-project
```

Kiosk image builds preserve the distribution Python interpreter used by native
GUI packages and Selkies. Install application Python dependencies in the image;
the serverless Python runtime is not injected into kiosk images.

`kiosk.json` describes the application, independent of the streaming backend:

```json
{
  "command": ["/usr/local/bin/my-application", "/workspace/project"],
  "cwd": "/workspace",
  "env": {"APPLICATION_SETTING": "value"}
}
```

Commands are argv arrays, with no shell interpretation. Use a launcher script
in the image when startup needs several steps. The launch command should own
the application lifetime; launchers that return after opening their windows
are also supported. Keep project data and application configuration on the
writable root, normally `/workspace` and `/root/.config`. Additional shared
volumes retain their existing independent lifecycle.

The Python equivalent uses the same configuration:

```python
from beta9 import Image, Kiosk, VM

vm = VM(
    "design-project",
    image=Image(dockerfile="Dockerfile"),
    kiosk=Kiosk(
        command=["/usr/local/bin/my-application", "/workspace/project"],
        cwd="/workspace",
        env={"APPLICATION_SETTING": "value"},
    ),
).create()
vm.wait(services=True)
print(vm.kiosk_url)
```

Kiosk configuration enables the display; `desktop=False` / `--no-desktop` cannot
be combined with an explicit kiosk configuration in the SDK or CLI. Prebuilt
images must include the kiosk backend. Guest initialization rejects incompatible
images explicitly, rather than opening a legacy desktop.

The management API accepts `spec.kiosk` with the fields above. `create_vm`
advertises the same schema through MCP. `vm_build_image` accepts `kiosk: true`.
Responses provide `kiosk_url` and retain `desktop_url` as the compatible display
entry point. Environment values are redacted from management responses and
artifact listings.

## Add an application MCP server

The image author supplies the application integration. Hosting and streaming do
not require one. Add a Streamable HTTP server to the launch configuration:

```json
{
  "command": ["/usr/local/bin/my-application", "/workspace/project"],
  "mcp": {
    "command": ["/usr/local/bin/application-mcp", "--host", "0.0.0.0", "--port", "8000"],
    "port": 8000,
    "path": "/mcp"
  }
}
```

Both processes inherit the application environment, working directory, display,
X authority, and session bus. The server must listen on its declared guest port;
the protected browser gateway reaches the guest interface. Readiness performs an
MCP initialize/initialized exchange, supporting JSON and SSE responses, and
closes its probe session where supported.

The MCP port is protected automatically. `mcp_url` supplies its stable endpoint.
For a local agent, use the existing authenticated tunnel:

```sh
beam vm port-forward design-project 8000
```

Configure the agent with `http://127.0.0.1:8000/mcp`. The CLI prints this connection
command; no workspace token or application credential is embedded in the URL.

## Readiness, recovery, and projects

`create()` retains its existing exec-readiness contract. `wait(services=True)`
also waits for an application window and the optional MCP handshake. Read state
with `vm.kiosk_status()` or `beam vm kiosk-status NAME`. The browser shows startup,
reconnection, clean exit, and application errors, with an explicit reopen action.
Application and MCP crashes have bounded recovery. Reconnecting a browser does
not restart the application.

The viewer uses the existing VM traffic authentication, sign-in bridge, share
links, and token revocation. `beam vm share NAME --desktop` shares the application
entry point. The readiness listener on guest port 8081 is reserved and tunnel-only.

Closing a tab detaches it; the VM idle policy determines compute lifetime.
Snapshots, templates, and forks preserve the launch configuration, installed
application, saved files, and application settings. Forks get independent
writable roots and fresh VM access credentials. A disk snapshot does not promise
unsaved edits, undo history, or exact in-memory window state.

Kiosk is a presentation mode, not a guest security restriction. Applications and
authorized agents retain ordinary file and process capabilities.

## Display density and window behavior

The Linux session establishes 2× render density before applications initialize:
one application logical pixel corresponds to one browser CSS pixel. GTK widget
scale, font DPI, X11 screen DPI, and Qt high-DPI defaults agree. Saved geometry
includes density, preserving the logical workspace across reconnects and screens.
Applications can override Qt defaults through the kiosk environment.

The application window manager maximizes ordinary editor/document windows
before their first map, allowing initial canvas fitting to use the correct
workspace. Dialogs, utility windows, and explicit X11 transient windows keep
native dimensions. The image builder compiles the distribution's signed Openbox
source with a transient-classification patch. Its source, patch, version, and
license are included under `/opt/beam-vm/openbox/share/source`.

The viewer waits for the correct initial framebuffer dimensions and preserves
the live image during subsequent resizing. The native client negotiates the
viewport after reconnects and external resizing. Density and window rules use
toolkit and X11 conventions independently of the application command.

Wheel input uses browser CSS units independently of render density. Fractional
trackpad deltas accumulate into X11 wheel steps. Browser pinch gestures target
the guest's conventional Control+wheel zoom at the current pointer location.
Applications retain their keyboard shortcuts and zoom behavior.

Pointer hover, press, drag, and release share the ordered control channel.
Popup placement, pointer grabs, and dismissal remain native toolkit behavior.

## Performance qualification

The implementation remains experimental until the application-family and
network-recovery matrix passes through production forwarding. The gate requires near
60 FPS, p95 frame intervals ≤25 ms, and p95 input response ≤80 ms with ≤20 ms RTT,
at 1080p and a 1440×900 viewport at 2× density. Current VM placement is CPU-only;
hardware acceleration requires additional runtime support.

## Operational policy

Gateway media limits live in `types.VMWebRTCConfig` under
`abstractions.vm.webrtc`. Defaults allow 256 concurrent media connections per
gateway, 8 per ICE session across gateways, and 64 registrations per VM per
minute. Handshakes have a 5-second timeout; registration expires after 10 minutes,
active connection leases after 30 seconds, and ownership is rechecked every
10 seconds. Deployments may tune these fields through normal configuration.
Invalid or unsafe timing relationships fail at startup. Expired leases close
active media rather than recreating their authority during renewal.

The SDK and guest share the `Kiosk`/`KioskMCP` configuration models.
`KioskRuntime` owns image policy for render density, encoder quality,
geometry limits, and supervisor timing; the installed client receives its render
density from this policy at image preparation. Changing image policy requires
rebuilding and qualifying the image. SDK and MCP resizing use one guest operation
that restores physical DPI together with the framebuffer dimensions.

The supervisor owns process groups by role and stops each before replacement.
Launchers may hand off to child windows within the application startup deadline.
MCP has an independent deadline; adapter failures leave the application available.
Readiness probes bound response size and wall time.
