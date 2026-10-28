"""Exercise installed servers through the same proxy shipped in the Linux guest."""

import asyncio
import json
import os
from pathlib import Path
import runpy
import sys
import textwrap
from unittest.mock import MagicMock, patch

import pytest

from beta9 import Kiosk, MCPServer
from beta9.abstractions.vm import __file__ as vm_file
from beta9.abstractions.vm.mcp_api import VMMCP


def guest():
    import beta9.abstractions.vm.kiosk_config as config

    with patch.dict(sys.modules, {"kiosk_config": config}):
        return runpy.run_path(str(Path(vm_file).parent / "assets/kiosk-mcp"))


@pytest.fixture
def backend(tmp_path):
    pytest.importorskip("fastmcp")
    script = tmp_path / "server.py"
    script.write_text(
        textwrap.dedent("""\
        import asyncio, os
        from fastmcp import FastMCP
        server = FastMCP("Ordinary application server")
        count = 0
        busy = False
        @server.tool()
        async def edit():
            global count, busy
            overlap = busy
            busy = True
            await asyncio.sleep(0.02)
            count += 1
            busy = False
            return {"count": count, "pid": os.getpid(), "cwd": os.getcwd(), "env": os.getenv("PROJECT"), "overlap": overlap}
        @server.tool()
        def extend():
            @server.tool()
            def added(): return "new capability"
        @server.tool()
        def crash(): os._exit(1)
        @server.resource("project://current")
        def project(): return "shared project"
        @server.prompt()
        def edit_project(): return "Edit the current project"
        server.run(show_banner=False)
    """)
    )
    return MCPServer(sys.executable, [str(script)], env={"PROJECT": "a project"}, cwd=str(tmp_path))


def test_registry_defaults_overrides_removal_and_atomic_validation(tmp_path, backend):
    defaults = tmp_path / "defaults.json"
    defaults.write_text(json.dumps({"original": backend.to_dict()}))
    registry = guest()["Registry"](tmp_path / "mcp.json", defaults)
    registry.update({"second": backend.to_dict()})
    registry.update({"original": None})
    assert set(registry.read()) == {"second"}
    assert registry.path.stat().st_mode & 0o777 == 0o600
    before = registry.path.read_bytes()
    for name, config in [
        ("invalid/name", backend.to_dict()),
        ("bad", {}),
        ("bad", {"command": "node", "url": "http://localhost/mcp"}),
    ]:
        with pytest.raises(ValueError):
            registry.update({name: config})
        assert registry.path.read_bytes() == before
    # New image defaults apply without losing runtime installs or re-enabling removals.
    defaults.write_text(json.dumps({"original": backend.to_dict(), "new": backend.to_dict()}))
    assert set(registry.read()) == {"second", "new"}
    registry.update({"original": backend.to_dict()})
    assert set(registry.read()) == {"original", "second", "new"}


def test_proxy_preserves_state_and_serializes_clients_across_namespaced_features(backend):
    from fastmcp import Client

    gateway = guest()["Gateway"](
        {
            "app": backend.to_dict(),
            "other": backend.to_dict(),
            "broken": {"command": "/missing/mcp"},
        },
        "/tmp",
    )

    async def exercise():
        async with Client(gateway.server, mode="legacy") as first, Client(
            gateway.server, mode="legacy"
        ) as second:
            assert {tool.name for tool in await first.list_tools()} >= {"app_edit", "other_edit"}
            edits = await asyncio.gather(first.call_tool("app_edit"), second.call_tool("app_edit"))
            data = [result.data for result in edits]
            assert sorted(item["count"] for item in data) == [1, 2]
            assert len({item["pid"] for item in data}) == 1
            assert all(
                item["env"] == "a project" and item["cwd"] == backend.cwd and not item["overlap"]
                for item in data
            )
            other = (await first.call_tool("other_edit")).data
            assert other["count"] == 1 and other["pid"] != data[0]["pid"]
            assert "shared project" in (await first.read_resource("project://app/current"))[0].text
            assert (await first.get_prompt("app_edit_project")).messages
            await first.call_tool("app_extend")
            assert (await second.call_tool("app_added")).content[0].text == "new capability"
            return [item["pid"] for item in data] + [other["pid"]]

    pids = asyncio.run(exercise())
    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_proxy_recovers_a_crashed_adapter_without_restarting_the_application(backend):
    from fastmcp import Client

    gateway = guest()["Gateway"]({"app": backend.to_dict()}, "/tmp")

    async def exercise():
        async with Client(gateway.server, mode="legacy") as client:
            before = (await client.call_tool("app_edit")).data
            with pytest.raises(Exception):
                await client.call_tool("app_crash")
            after = (await client.call_tool("app_edit")).data
            assert after["count"] == 1
            assert after["pid"] != before["pid"]

    asyncio.run(exercise())


def test_sdk_uses_existing_vm_transport_and_standard_client_configuration():
    process = MagicMock()
    process.wait.return_value = 0
    process.stdout.read.return_value = '{"state":"ready","servers":{}}'
    vm = MagicMock(mcp_url="https://application.example/mcp", traffic_access_token="private")
    vm.refresh.return_value.info = {"spec": {"kiosk": {}}}
    vm.process.exec.return_value = process
    mcp = VMMCP(vm)
    config = MCPServer("node", ["/opt/my-app/server.js"], env={"TOKEN": "secret"})
    mcp.install("app", config)
    args, kwargs = vm.process.exec.call_args
    assert args == ("/opt/beam-vm/kiosk-mcp", "--install", "app")
    assert json.loads(kwargs["stdin"]) == config.to_dict()
    assert mcp.client_config() == {
        "url": vm.mcp_url,
        "headers": {"X-Beam-VM-Token": "private"},
    }
    assert mcp.status()["state"] == "ready"
    mcp.remove("app")
    assert vm.process.exec.call_args.args[-2:] == ("--remove", "app")
    vm.refresh.return_value.info = {"spec": {}}
    with pytest.raises(ValueError, match="kiosk"):
        mcp.list()


def test_image_configuration_round_trips_both_standard_transports():
    kiosk = Kiosk(
        ["application"],
        mcp={
            "stdio": MCPServer("node", ["server.js"]),
            "http": MCPServer(
                url="http://127.0.0.1:9000/mcp", headers={"Authorization": "Bearer app-secret"}
            ),
        },
    )
    assert Kiosk.from_dict(kiosk.to_dict()) == kiosk


def test_http_backend_keeps_its_credentials_separate_from_client_headers():
    pytest.importorskip("fastmcp")
    from contextlib import asynccontextmanager
    import socket
    import uvicorn
    from fastmcp import Client, FastMCP
    from fastmcp.client.transports import StreamableHttpTransport
    from fastmcp.server.dependencies import get_http_headers

    @asynccontextmanager
    async def serve(server):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            http = uvicorn.Server(uvicorn.Config(server.http_app(), log_level="error"))
            task = asyncio.create_task(http.serve(sockets=[listener]))
            try:
                for _ in range(100):
                    if http.started:
                        break
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
                assert http.started
                yield f"http://127.0.0.1:{listener.getsockname()[1]}/mcp"
            finally:
                http.should_exit = True
                await asyncio.wait_for(task, 5)

    backend = FastMCP("Application HTTP server")

    @backend.tool()
    def headers() -> dict:
        request = get_http_headers(include={"authorization"})
        return {
            "credential": request.get("authorization"),
            "client_header": request.get("x-client-secret"),
        }

    async def exercise():
        async with serve(backend) as url:
            gateway = guest()["Gateway"](
                {"app": {"url": url, "headers": {"Authorization": "Bearer app-credential"}}}, "/tmp"
            )
            async with serve(gateway.server) as proxy:
                transport = StreamableHttpTransport(
                    proxy,
                    headers={
                        "Authorization": "Bearer viewer-credential",
                        "X-Client-Secret": "private",
                    },
                )
                async with Client(transport, mode="legacy") as client:
                    result = await client.call_tool("app_headers")
                    assert result.data == {
                        "credential": "Bearer app-credential",
                        "client_header": None,
                    }

    asyncio.run(exercise())
