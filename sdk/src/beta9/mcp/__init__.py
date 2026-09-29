"""
The `mcp` command's stdio server: forwards the workspace MCP server on the
gateway to a local agent client and adds the tools that only make sense on
this machine (deploying the project directory, signing in).
"""

from .server import StdioProxy, run_stdio

__all__ = ["StdioProxy", "run_stdio"]
