"""Application MCP configuration over the existing authenticated VM transport."""

import json

from .kiosk_config import KioskRuntime, MCPServer


class VMMCP:
    def __init__(self, vm):
        self.vm = vm

    @property
    def url(self):
        return self.vm.mcp_url

    @property
    def local_url(self):
        """Endpoint for agents running inside this VM (or an authenticated port-forward)."""
        policy = KioskRuntime()
        return f"http://127.0.0.1:{policy.mcp_port}{policy.mcp_path}"

    def client_config(self):
        """Standard HTTP MCP config. Treat the returned traffic credential as a secret."""
        url = self.url
        if not url:
            raise ValueError("Application MCP requires kiosk mode")
        return {
            "url": url,
            "headers": {"X-Beam-VM-Token": self.vm.traffic_access_token},
        }

    def _run(self, *args, value=None):
        if self.vm.refresh().info["spec"].get("kiosk") is None:
            raise ValueError("Application MCP requires kiosk mode")
        self.vm.wait()
        return self.vm._guest_json(
            "/opt/beam-vm/kiosk-mcp",
            *args,
            stdin=json.dumps(value).encode() if value is not None else None,
            timeout=30,
            error="Unable to configure application MCP",
        )

    def install(self, name: str, server: MCPServer):
        """Register an installed server. Use Image build steps or VM exec to install its dependencies.

        Configuration lives on the VM's disk and survives stops, snapshots and forks.
        Changing it reconnects MCP clients without restarting the application.
        """
        self._run("--install", name, value=server.to_dict())

    def remove(self, name: str):
        """Unregister a server without removing its packages or project files."""
        self._run("--remove", name)

    def list(self):
        """List installed servers, omitting arguments, URLs, environment and headers."""
        return self._run("--list")

    def status(self):
        """Read per-server readiness without waiting for a slow or unavailable adapter."""
        return self._run("--status")
