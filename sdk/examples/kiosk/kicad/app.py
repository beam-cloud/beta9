"""KiCad with its ordinary stdio MCP server."""

from pathlib import Path

from beta9 import Image, Kiosk, MCPServer, VM

server = MCPServer(
    "node",
    ["/opt/kicad-mcp/dist/index.js"],
    env={
        "KICAD_PYTHON": "/opt/kicad-mcp/.venv/bin/python",
        "KICAD_BACKEND": "auto",
        "KICAD_AUTO_LAUNCH": "false",
        "KICAD_MCP_TOOLBOXES": "core",
    },
)

if __name__ == "__main__":
    vm = VM(
        "kicad-app",
        image=Image.from_dockerfile(str(Path(__file__).with_name("Dockerfile"))),
        kiosk=Kiosk(
            ["pcbnew", "/workspace/project.kicad_pcb"],
            mcp={"kicad": server},
            transport="webrtc",
        ),
    ).create()
    print(vm.access_url(8080))
    print(vm.mcp.url)
