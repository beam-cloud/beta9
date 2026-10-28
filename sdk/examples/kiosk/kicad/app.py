"""Run from this directory: python app.py."""

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
        image=Image.from_dockerfile("Dockerfile"),
        kiosk=Kiosk(["pcbnew", "/workspace/project.kicad_pcb"], mcp={"kicad": server}),
    ).create()
    print(vm.kiosk_url)
    print(vm.mcp.url)
