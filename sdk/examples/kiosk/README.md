# Kiosk applications

A kiosk combines an image, an application command and optional named MCP servers. Install the application, harness dependencies and native plugins in the image. The VM supplies the browser session and one authenticated MCP endpoint; application code uses the ordinary MCP protocol.

```python
from beta9 import Image, Kiosk, MCPServer, VM

vm = VM(
    "my-app",
    image=Image.from_dockerfile("Dockerfile"),
    kiosk=Kiosk(
        command=["my-application", "/workspace/project"],
        mcp={"app": MCPServer(command="node", args=["/opt/my-app/server.js"])},
        transport="webrtc",
    ),
).create()
vm.wait(services=True)
print(vm.access_url(8080))
```

Omit `mcp` for an application without an adapter. Stdio commands inherit the application's environment, display and working directory. To connect an existing Streamable HTTP service, use `MCPServer(url="http://127.0.0.1:9000/mcp", headers={...})`; that service owns its startup. Different applications use the same configuration interface.

The printed link authenticates a browser temporarily. `vm.kiosk_url` is the stable protected URL; `beta9 vm kiosk my-app` opens it using your local profile. Closing a viewer detaches it; the VM's idle policy controls compute lifetime.

## Connect an agent

Add this standard configuration to a local agent:

```python
config = {"mcpServers": {"my-app": vm.mcp.client_config()}}
```

The configuration includes a private VM traffic credential. Tools are namespaced by adapter (`app_edit`); resources and prompts are forwarded too. Stdio backends stay alive between clients, and requests to each adapter are serialized against the shared project.

Agents inside the VM connect directly to `vm.mcp.local_url`. For local forwarding, run `beta9 vm port-forward my-app 8082` and connect to `http://127.0.0.1:8082/mcp`.

## Add or remove a server

Install packages with image build steps or `vm.process.exec`, then register their command:

```python
vm.mcp.install("custom", MCPServer(command="/opt/custom/bin/server"))
print(vm.mcp.list())    # server configurations, credentials omitted
print(vm.mcp.status())  # readiness and tool count per adapter
vm.mcp.remove("custom")
```

Registration changes restart the MCP gateway without restarting the application. Runtime installs, overrides and removals take precedence over image defaults and persist through stops, snapshots and forks. Saved project files and the environment persist; forks receive independent writable state and fresh VM credentials.

A broken adapter leaves the application and other adapters usable. `vm.mcp.status()` distinguishes adapter readiness from gateway availability; inspect failures with `beta9 vm logs my-app --unit beam-desktop`. Configuration listings omit arguments, URLs, environment and headers. Keep the original configuration in the image recipe and use the existing VM secret mechanism for credentials.

## Runnable applications

Run either recipe from the repository root with your configured SDK context:

| Application | Command | Harness setup in the image |
| --- | --- | --- |
| [KiCad](kicad/app.py) | `python sdk/examples/kiosk/kicad/app.py` | [KiCAD-MCP-Server](https://github.com/mixelpixx/KiCAD-MCP-Server), IPC enabled and first-run libraries configured |
| [Blender](blender/app.py) | `python sdk/examples/kiosk/blender/app.py` | [MCP for Blender](https://github.com/ahujasid/mcp-for-blender), native addon installed and enabled |

For live KiCad edits, call `kicad_open_board` with the GUI's project path and confirm `kicad_get_backend_state` reports `ipc`. Some upstream tools operate on files; check each result's backend. Blender's tools connect to its enabled addon. Application-specific setup belongs in these recipes, so a new or replacement harness needs no VM runtime changes.
