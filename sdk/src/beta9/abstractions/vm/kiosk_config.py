"""Application-independent configuration for a browser application session."""

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


@dataclass
class KioskMCP:
    """An image-provided Streamable HTTP MCP server sharing the app's session."""

    command: List[str]
    port: int
    path: str = "/mcp"


@dataclass
class Kiosk:
    """Launch an ordinary Linux GUI application without a desktop shell.

    Commands are argv arrays. Application files and configuration belong on
    the VM's durable disk; credentials should use the existing VM secret API.
    """

    command: List[str]
    cwd: str = "/workspace"
    env: Dict[str, str] = field(default_factory=dict)
    mcp: Optional[KioskMCP] = None
    transport: Optional[str] = None

    def to_dict(self):
        value = asdict(self)
        if value["mcp"] is None:
            del value["mcp"]
        if value["transport"] is None:
            del value["transport"]
        return value
