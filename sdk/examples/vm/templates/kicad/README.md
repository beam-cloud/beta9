# KiCad

[KiCad](https://www.kicad.org) 10's PCB editor for agents: Claude Code, Codex,
Cursor or any MCP client lays out boards through
[KiCAD-MCP-Server](https://github.com/mixelpixx/KiCAD-MCP-Server) while you watch
the editor full screen in your browser, streamed over WebRTC from a kiosk
desktop. The starter board is `/workspace/project.kicad_pcb`.

VMs from the template get an RTX 4090, which draws the editor's canvas. A GPU VM
stops rather than pauses, and the GPU is billed while it runs.

```sh
beam vm new kicad --template TEMPLATE_ID    # the ID is on beam.cloud/templates
beam vm desktop kicad                       # watch KiCad
beam vm port-forward kicad 8082             # the MCP server, at http://127.0.0.1:8082/mcp
```

With the port forwarded, add the server to your agent, then ask it for a board:

```sh
claude mcp add --transport http kicad http://127.0.0.1:8082/mcp
codex mcp add kicad --url http://127.0.0.1:8082/mcp
```

Have the agent call `kicad_open_board` with `/workspace/project.kicad_pcb` first.
That connects it to the open editor over KiCad's IPC API, so its edits appear as
it makes them, and `kicad_get_backend_state` then reports `ipc`. A few tools,
such as rectangular board outlines, edit the file on disk instead; each result
names the backend it used.

The template is built from the [kiosk example](../../../kiosk/kicad)'s
Dockerfile on Beam's NVIDIA image. It installs KiCad 10 from the KiCad PPA, with
its IPC API enabled and its libraries configured so it opens without first-run
dialogs, plus KiCAD-MCP-Server; `publish.py` registers the server on the VM, so
it starts with KiCad. Without a tunnel, `VM.get("kicad").mcp.client_config()`
returns the VM's MCP URL and the token it needs; keep the token secret. The
[kiosk README](../../../kiosk/README.md) covers adding and removing servers.

Rebuild the template with `python sdk/examples/vm/templates/publish.py kicad`.
