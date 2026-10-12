"""Native OAuth acceptance with the official MCP client (mcp 2.3+).

Run with an isolated Python environment. Open the authorization URL saved at
/tmp/beam-mcp-authorization-url.txt and approve the staging consent page.
Tokens remain in memory; this script prints no credentials.
"""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx2
from mcp import Client
from mcp.client.auth import OAuthClientProvider
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.auth import AuthorizationCodeResult, OAuthClientMetadata

URL = "https://app.stage.beam.cloud/api/v1/mcp"
received = threading.Event()
callback = {}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if urlsplit(self.path).path != "/callback":
            self.send_error(404)
            return
        callback.update(
            {k: v[0] for k, v in parse_qs(urlsplit(self.path).query).items()}
        )
        body = b"Beam connected. You can return to your terminal."
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        received.set()

    def log_message(self, *_):
        pass


class Storage:
    tokens = None
    info = None
    writes = 0

    async def get_tokens(self):
        return self.tokens

    async def set_tokens(self, tokens):
        self.tokens = tokens
        self.writes += 1

    async def get_client_info(self):
        return self.info

    async def set_client_info(self, info):
        self.info = info


async def redirect(url):
    path = Path("/tmp/beam-mcp-authorization-url.txt")
    path.write_text(url)
    path.chmod(0o600)
    print(
        "Waiting for staging browser consent; authorization URL saved privately.",
        flush=True,
    )


async def handle_callback():
    await asyncio.to_thread(received.wait, 900)
    if not received.is_set():
        raise TimeoutError("Browser consent was not completed")
    if callback.get("error"):
        raise RuntimeError(callback["error"])
    return AuthorizationCodeResult(**callback)


async def main():
    server = ThreadingHTTPServer(("127.0.0.1", 18888), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    storage = Storage()
    provider = OAuthClientProvider(
        URL,
        OAuthClientMetadata(
            client_name="Beam staging acceptance",
            redirect_uris=["http://127.0.0.1:18888/callback"],
            token_endpoint_auth_method="none",
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
            scope="beam:read beam:write beam:exec beam:credentials",
        ),
        storage,
        redirect_handler=redirect,
        callback_handler=handle_callback,
    )
    results = {}
    try:
        async with httpx2.AsyncClient(auth=provider, timeout=60) as http:
            async with Client(streamable_http_client(URL, http_client=http)) as client:
                catalog = await client.list_tools()
                results["official_client_discovery"] = len(catalog.tools)
                first = await client.call_tool("whoami", {})
                assert not first.is_error, first
                before = storage.tokens.refresh_token
                await asyncio.sleep(12)
                second = await client.call_tool("whoami", {})
                assert not second.is_error, second
                assert before != storage.tokens.refresh_token
                results["automatic_refresh"] = True
                results["token_rotations"] = storage.writes
    finally:
        try:
            if storage.tokens is not None and storage.info is not None:
                async with httpx2.AsyncClient(timeout=30) as plain:
                    response = await plain.post(
                        "https://api.stage.beam.cloud/v2/oauth/revoke",
                        data={
                            "token": storage.tokens.refresh_token,
                            "client_id": storage.info.client_id,
                        },
                    )
                    assert response.status_code == 200
        finally:
            server.shutdown()
            server.server_close()
    async with httpx2.AsyncClient(timeout=30) as plain:
        response = await plain.post(
            URL,
            headers={"Authorization": "Bearer " + storage.tokens.access_token},
            json={"jsonrpc": "2.0", "id": 7, "method": "tools/list"},
        )
        assert response.status_code == 401
        assert "resource_metadata" in response.headers.get("WWW-Authenticate", "")
        results["revocation_immediate"] = True
    Path("/tmp/beam-mcp-oauth-result.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
