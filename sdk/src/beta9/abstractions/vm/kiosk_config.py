"""Application-independent configuration for a browser application session."""

from dataclasses import asdict, dataclass, field, replace
from typing import Dict, List, Optional, Tuple


@dataclass
class MCPServer:
    """An installed MCP server, using the usual mcpServers configuration."""

    command: Optional[str] = None
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    cwd: Optional[str] = None
    url: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if bool(self.command) == bool(self.url):
            raise ValueError("MCP servers require either command or url")

    def to_dict(self):
        return {key: value for key, value in asdict(self).items() if value}

    def without_credentials(self):
        return replace(self, args=[], env={}, headers={}, url="[redacted]" if self.url else None)


@dataclass
class Kiosk:
    """Launch an ordinary Linux GUI application without a desktop shell.

    Commands are argv arrays. Application files and configuration belong on
    the VM's durable disk; credentials should use the existing VM secret API.
    """

    command: List[str]
    cwd: str = "/workspace"
    env: Dict[str, str] = field(default_factory=dict)
    mcp: Dict[str, MCPServer] = field(default_factory=dict)
    transport: Optional[str] = None

    @classmethod
    def from_dict(cls, value):
        value = dict(value)
        value["mcp"] = {
            name: MCPServer(**server) for name, server in (value.get("mcp") or {}).items()
        }
        return cls(**value)

    def to_dict(self):
        config = {key: value for key, value in asdict(self).items() if value is not None}
        config["mcp"] = {name: server.to_dict() for name, server in self.mcp.items()}
        return config


@dataclass(frozen=True)
class KioskRuntime:
    """Guest policy shared by image preparation, supervision and desktop controls.

    Launch configuration stays independent of the streaming backend. Policy
    changes require rebuilding and qualifying the kiosk image.
    """

    display_port: int = 8080
    status_port: int = 8081
    media_port: int = 8443
    pixel_ratio: int = 2
    max_pixel_ratio: int = 4
    base_dpi: int = 96
    xsettings_unit: int = 1024
    logical_size: Tuple[int, int] = (1920, 1080)
    max_dimension: int = 8192
    min_size: Tuple[int, int] = (320, 200)
    framerate: int = 60
    crf: int = 18
    min_bitrate: int = 3000
    max_bitrate: int = 24000
    frame_queue: int = 2
    mcp_port: int = 8082
    mcp_path: str = "/mcp"
    mcp_version: str = "4.1.4"
    mcp_config: str = "/root/.config/beam-vm/mcp.json"
    mcp_defaults: str = "/run/beam-desktop/mcp-defaults.json"
    mcp_poll_interval: float = 5
    mcp_max_servers: int = 16
    mcp_max_config_bytes: int = 64 << 10
    command_timeout: float = 2
    process_timeout: float = 3
    display_timeout: float = 30
    manager_timeout: float = 10
    resize_timeout: float = 10
    application_timeout: float = 120
    retry_window: float = 60
    max_retries: int = 3
    poll_interval: float = 0.25
    geometry_interval: float = 1

    @property
    def dpi(self):
        return self.base_dpi * self.pixel_ratio

    @property
    def default_size(self):
        return tuple(v * self.pixel_ratio for v in self.logical_size)

    def valid_size(self, size):
        return len(size) == len(self.min_size) and all(
            type(v) is int and low <= v <= self.max_dimension for v, low in zip(size, self.min_size)
        )
