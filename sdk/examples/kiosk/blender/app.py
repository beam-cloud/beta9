"""A Blender addon and ordinary stdio server, using the same VM MCP interface."""

from pathlib import Path

from beta9 import Image, Kiosk, MCPServer, VM

server = MCPServer(
    "/opt/blender-mcp/bin/mcp-for-blender",
    env={"DISABLE_TELEMETRY": "1"},
)

if __name__ == "__main__":
    vm = VM(
        "blender-app",
        image=Image.from_dockerfile(str(Path(__file__).with_name("Dockerfile"))),
        kiosk=Kiosk(
            ["blender", "/workspace/project.blend"],
            mcp={"blender": server},
            transport="webrtc",
        ),
    ).create()
    print(vm.access_url(8080))
    print(vm.mcp.url)
