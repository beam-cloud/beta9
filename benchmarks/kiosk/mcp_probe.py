#!/usr/bin/env python3
"""Image-author MCP fixture proving tools share the visible application session."""

import json
import os
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        if self.path != "/mcp":
            self.send_error(404)
            return
        value = json.loads(self.rfile.read(min(int(self.headers.get("Content-Length", 0)), 65536)))
        method = value.get("method")
        if method == "notifications/initialized":
            self.send_response(202)
            self.end_headers()
            return
        if method == "initialize":
            result = {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "beam-kiosk-test", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "press_a",
                        "description": "Increment the visible probe's key counter",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": False,
                        },
                    }
                ]
            }
        elif method == "tools/call" and value.get("params", {}).get("name") == "press_a":
            subprocess.run(["xdotool", "key", "a"], check=True, timeout=2)
            result = {
                "content": [
                    {"type": "text", "text": "Key sent to " + os.environ.get("DISPLAY", "")}
                ]
            }
        else:
            self.send_error(400)
            return
        body = json.dumps({"jsonrpc": "2.0", "id": value.get("id"), "result": result}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
