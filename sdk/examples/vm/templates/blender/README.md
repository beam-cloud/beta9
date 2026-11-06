# Blender

[Blender](https://www.blender.org) for agents: Claude Code, Codex, Cursor or any
MCP client builds scenes through [MCP for Blender](https://github.com/ahujasid/mcp-for-blender)
while you watch Blender full screen in your browser, streamed over WebRTC from a
kiosk desktop. The starter project is `/workspace/project.blend`.

VMs from the template get an RTX 4090. The desktop draws Blender's viewport on
it, and Cycles renders with OptiX: a startup script enables the GPU whenever a
file opens, so renders started over MCP use it too. A GPU VM stops rather than
pauses, and the GPU is billed while it runs.

```sh
beam vm new blender --template TEMPLATE_ID    # the ID is on beam.cloud/templates
beam vm desktop blender                       # watch Blender
beam vm port-forward blender 8082             # the MCP server, at http://127.0.0.1:8082/mcp
```

With the port forwarded, add the server to your agent, then ask it for a scene:

```sh
claude mcp add --transport http blender http://127.0.0.1:8082/mcp
codex mcp add blender --url http://127.0.0.1:8082/mcp
```

The template is built from the [kiosk example](../../../kiosk/blender)'s
Dockerfile on Beam's NVIDIA image. It installs Blender 4.5 LTS from
blender.org, since Ubuntu's package can't render on NVIDIA GPUs, plus MCP for
Blender with its addon enabled; `publish.py` registers the server on the VM, so
it starts with Blender. Without a tunnel,
`VM.get("blender").mcp.client_config()` returns the VM's MCP URL and the token
it needs; keep the token secret. The [kiosk README](../../../kiosk/README.md)
covers adding and removing servers.

Rebuild the template with `python sdk/examples/vm/templates/publish.py blender`.
