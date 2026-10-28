# Application MCPs in a kiosk VM

Install the application and its harness with normal image build steps. Describe its MCP server using the usual `command`, `args`, `env`, and optional `cwd`. An existing Streamable HTTP server can use `url` and `headers` instead. No Beam-specific server implementation is needed.

```python
from beta9 import Image, Kiosk, MCPServer, VM

vm = VM(
    "my-app",
    image=Image.from_dockerfile("Dockerfile"),
    kiosk=Kiosk(
        ["my-application", "/workspace/project"],
        mcp={"app": MCPServer("node", ["/opt/my-app/server.js"])},
    ),
).create()
```

The [KiCad example](kicad/app.py) installs [KiCAD-MCP-Server](https://github.com/mixelpixx/KiCAD-MCP-Server) and enables KiCad's IPC API. The [Blender example](blender/app.py) installs [MCP for Blender](https://github.com/ahujasid/mcp-for-blender) and its native addon. Run `python app.py` from either directory with your configured SDK context. Set a project path in the launch command to reopen it at boot. The recipes include native first-run setup so the adapters can connect on a clean boot. Application-specific setup belongs in the image; the VM integration is identical.

The example prints a temporary authenticated viewer link. `vm.kiosk_url` is the stable protected browser URL; `beta9 vm kiosk NAME` opens it using your local profile.

Every kiosk VM exposes one authenticated `vm.mcp.url`, on the same host as its browser application. Namespaces prevent collisions: `app_edit`, `kicad_move_component`, `blender_execute_blender_code`. Tools, resources and prompts use the same MCP protocol. Stdio backends stay alive between calls; requests to a single application adapter are serialized because they share one GUI/project. Different adapters can operate independently.

For a local agent, add `{"mcpServers": {"my-app": vm.mcp.client_config()}}` to its MCP configuration. This includes a VM traffic credential: keep it private. Alternatively, run `beta9 vm port-forward my-app 8082` and connect to `http://127.0.0.1:8082/mcp`. Agents inside the VM use that same loopback endpoint directly. A client disconnect does not close the application or its backend. VM idle policy still controls compute lifetime.

To add an already-installed server to a running VM:

```python
vm.mcp.install("custom", MCPServer("/opt/custom/bin/server"))
print(vm.mcp.list())    # credentials redacted
print(vm.mcp.status())  # readiness and tool count per adapter
vm.mcp.remove("custom")
```

Use `vm.process.exec` or image build steps for package installation. Registration does not run package-manager commands. Config changes reconnect MCP clients without restarting the GUI. Runtime installs, overrides and removals persist on the writable disk through stops, snapshots and forks. Image defaults follow the launch configuration; runtime overrides take precedence. Forks get independent writable state and fresh VM access credentials. Store server credentials through the existing VM secret mechanism rather than image layers.

A failed adapter does not close or block the application. Inspect `vm.mcp.status()` and `beta9 vm logs NAME --unit beam-desktop` for failures. The proxy uses a pinned [FastMCP](https://gofastmcp.com/servers/providers/proxy) runtime for MCP protocol handling; its dependency is confined to kiosk guests.

For KiCad, connect tools to the board open in the GUI using `kicad_open_board`, then check `kicad_get_backend_state`: the session must say `ipc` for live edits. Some upstream tools operate on files and can lag the live GUI; check the backend reported by each operation. Blender's tools connect to its enabled addon. The hosting interface cannot turn a file-only tool into a live application API; choose or build the application harness accordingly.
