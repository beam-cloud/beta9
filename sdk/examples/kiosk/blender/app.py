"""A Blender addon and ordinary stdio server, using the same VM MCP interface."""

from beta9 import Image, Kiosk, MCPServer, VM

server = MCPServer(
    "/opt/blender-mcp/bin/mcp-for-blender",
    env={"DISABLE_TELEMETRY": "1"},
)

if __name__ == "__main__":
    vm = VM(
        "blender-app",
        image=Image.from_dockerfile("Dockerfile"),
        kiosk=Kiosk(
            ["blender", "/workspace/project.blend"],
            env={"BLENDER_USER_SCRIPTS": "/opt/blender-scripts", "BLENDERMCP_NO_UPDATE_CHECK": "1"},
            mcp={"blender": server},
        ),
    ).create()
    print(vm.kiosk_url)
    print(vm.mcp.url)
