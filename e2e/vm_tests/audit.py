"""CPU VM integration checks; invoke with an explicit profile and prepared image.

Resources created by this script are prefixed audit-. A failed run retains its
VM for diagnosis; --cleanup removes only that run's resources. Traffic credentials
are kept in memory and never included in the report.
"""

import argparse
import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

import requests

from beta9 import DurableDisk, Image, VM, Volume
from beta9.channel import GatewayHTTPError, ServiceClient
from beta9.clients.disk import DeleteDiskRequest
from beta9.clients.volume import DeleteVolumeRequest
from beta9.config import get_config_context


SERVER = """import http.server,json,os,socket,time,uuid
boot=str(uuid.uuid4()); started=time.time(); posts=0
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def reply(self):
  body=json.dumps(dict(pid=os.getpid(),boot=boot,uptime=time.time()-started,posts=posts,
   credential=self.headers.get('X-Beam-VM-Token'),cookie=self.headers.get('Cookie'))).encode()
  self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def do_GET(self): self.reply()
 def do_POST(self):
  global posts
  self.rfile.read(int(self.headers.get('Content-Length','0')));posts+=1;self.reply()
route=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);route.connect(('1.1.1.1',53))
address=route.getsockname()[0];route.close()
http.server.ThreadingHTTPServer((address,8000),Handler).serve_forever()
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    if not args.name.startswith("audit-"):
        parser.error("Use an audit- resource name")
    disk_name, volume_name = args.name + "-data", args.name + "-shared"
    report = {
        "profile": args.profile,
        "image_id": args.image_id,
        "name": args.name,
        "checks": {},
    }

    def passed(name, detail=True):
        report["checks"][name] = detail
        Path(args.report).write_text(json.dumps(report, indent=2))
        print("PASS", name, flush=True)

    def cli(*command, expected=0, timeout=240):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from beta9.cli.main import start; start()",
                "--context",
                args.profile,
                "vm",
                *command,
            ],
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        if (expected is None and result.returncode == 0) or (
            expected is not None and result.returncode != expected
        ):
            raise RuntimeError(
                (command, result.returncode, result.stdout, result.stderr)
            )
        return result.stdout + result.stderr if expected is None else result.stdout

    with ServiceClient(get_config_context(args.profile)) as service:
        if args.cleanup:
            try:
                VM.get(args.name, _service=service).remove()
            except GatewayHTTPError as exc:
                if exc.status != 404:
                    raise
            disk = service.disk.delete_disk(DeleteDiskRequest(name=disk_name))
            volume = service.volume.delete_volume(DeleteVolumeRequest(name=volume_name))
            print(
                "Own VM removed; disk/volume cleanup:", disk.ok, volume.ok, flush=True
            )
            return

        vm = VM(
            args.name,
            image=Image.from_id(args.image_id),
            desktop=True,
            ssh=False,
            cpu=2,
            memory=2048,
            disk_size="8GiB",
            ttl=0,
            auto_resume=True,
            ports=[8000],
            protected_ports=[8000, 8080, 7681],
            metadata={"audit": args.name},
            disks=[DurableDisk(disk_name, "1GiB", "/data")],
            volumes=[Volume(volume_name, "/shared")],
            _service=service,
        ).create()
        initial_id = vm.info["container_id"]
        stable_url, desktop_url = vm.get_url(8000), vm.desktop_url
        assert vm.info["spec"]["pool"] == "vms"
        passed("create_default_cpu_pool")
        replay = vm.create()
        assert replay.info["container_id"] == initial_id
        matches = VM.list(
            metadata={"audit": args.name}, status="running", _service=service
        )
        assert [item["id"] for item in matches] == [vm.id]
        passed("idempotent_create_and_filtered_discovery")

        def execute(*command, timeout=60):
            process = vm.process.exec(*command, cwd="/")
            code = process.wait(timeout)
            stdout, stderr = process.stdout.read(), process.stderr.read()
            if code:
                raise RuntimeError((command, code, stdout, stderr))
            return stdout.strip()

        assert execute("cat", "/proc/1/comm") == "systemd"
        vm.fs.write_text("/data/persistent.txt", "durable-extra-disk")
        vm.fs.write_text("/shared/persistent.txt", "shared-volume")
        vm.fs.write_text("/run/ram-marker", "warm-only")
        vm.fs.write_text("/workspace/audit-server.py", SERVER)
        vm.fs.write_text(
            "/etc/systemd/system/audit-web.service",
            "[Unit]\nAfter=network.target\n[Service]\nExecStart=/usr/bin/python3 /workspace/audit-server.py\nRestart=always\n[Install]\nWantedBy=multi-user.target\n",
        )
        execute("systemctl", "daemon-reload")
        execute("systemctl", "enable", "--now", "audit-web.service")
        token = vm.traffic_access_token

        def web(method="GET", session=None, **kwargs):
            client = session or requests
            return client.request(method, stable_url, timeout=220, **kwargs)

        assert web().status_code == 403
        headers = {"X-Beam-VM-Token": token}
        before = web(headers=headers).json()
        assert before["credential"] is None
        browser = requests.Session()
        response = browser.get(vm.access_url(8000), timeout=220)
        assert response.status_code == 200 and response.url == stable_url
        assert response.json()["cookie"] is None
        passed("systemd_and_protected_browser_access")
        token = vm.rotate_access_token()
        assert web(headers=headers).status_code == 403
        assert web(session=browser).status_code == 403
        headers = {"X-Beam-VM-Token": token}
        passed("traffic_rotation_revokes_tokens_and_sessions")

        async def async_files():
            await vm.aio.fs.write_text("/workspace/async.txt", "async-file")
            return await vm.aio.fs.read_text("/workspace/async.txt")

        assert asyncio.run(async_files()) == "async-file"
        metrics = vm.metrics()
        assert metrics["memory_total_bytes"] > 0 and metrics["disk_total_bytes"] > 0
        passed("shared_async_files_and_metrics", metrics)
        pid = json.loads(
            cli(
                "exec",
                "--detach",
                "--json",
                args.name,
                "--",
                "sh",
                "-c",
                "printf first; sleep 1; printf second",
            )
        )["pid"]
        time.sleep(2)
        assert "firstsecond" in cli("logs", args.name, "--pid", str(pid))
        assert isinstance(json.loads(cli("ps", args.name, "--json")), list)
        passed("detached_exec_reattachment_and_logs")
        output = cli(
            "exec",
            "--timeout",
            "1",
            "--json",
            args.name,
            "--",
            "sleep",
            "600",
            expected=None,
            timeout=20,
        )
        assert "timeout" in output.lower() or "timed out" in output.lower()
        active = json.loads(cli("ps", args.name, "--json"))
        assert not any(
            p["args"] == ["sleep", "600"] and p["exit_code"] < 0 for p in active
        )
        passed("json_exec_timeout_cancels_child")

        assert execute(
            "curl",
            "-ks",
            "--connect-timeout",
            "5",
            "--max-time",
            "8",
            "https://1.1.1.1/",
        )
        vm.update_network_permissions(block_network=True)
        denied = vm.process.exec(
            "curl",
            "-ks",
            "--connect-timeout",
            "3",
            "--max-time",
            "4",
            "https://1.1.1.1/",
        )
        assert denied.wait(10) != 0
        vm.update_network_permissions(allow_list=["1.1.1.1/32"])
        assert execute(
            "curl",
            "-ks",
            "--connect-timeout",
            "5",
            "--max-time",
            "8",
            "https://1.1.1.1/",
        )
        passed("live_egress_block_and_cidr_allow")
        vm.update_network_permissions()

        before = web(headers=headers).json()
        pause_started = time.monotonic()
        vm.pause()
        assert vm.info["status"] == "paused", vm.info["status"]
        assert web().status_code == 403
        assert vm.refresh().info["status"] == "paused"
        vm.start()
        after = web(headers=headers).json()
        assert after["boot"] == before["boot"] and after["pid"] == before["pid"]
        assert vm.fs.read_text("/run/ram-marker") == "warm-only"
        assert vm.info["container_id"] != initial_id
        assert vm.get_url(8000) == stable_url and vm.desktop_url == desktop_url
        passed(
            "ram_pause_warm_resume",
            {
                "seconds": round(time.monotonic() - pause_started, 2),
                "same_guest_process": True,
            },
        )

        vm.stop()
        assert vm.info["status"] == "stopped"
        boot_started = time.monotonic()
        post = web("POST", headers=headers, data=b"one-post")
        assert post.status_code == 200, post.status_code
        body = post.json()
        assert body["boot"] != before["boot"] and body["posts"] == 1
        vm.wait()
        assert vm.fs.read_text("/data/persistent.txt") == "durable-extra-disk"
        assert vm.fs.read_text("/shared/persistent.txt") == "shared-volume"
        assert execute("sh", "-c", "test ! -e /run/ram-marker && echo cold") == "cold"
        passed(
            "http_auto_resume_single_post_and_storage",
            {"seconds": round(time.monotonic() - boot_started, 2)},
        )

        size = vm.desktop.screen_size()
        vm.desktop.resize(1280, 720)
        assert vm.desktop.screen_size() == (1280, 720)
        png = vm.desktop.screenshot()
        assert png.startswith(b"\x89PNG\r\n\x1a\n")
        Path(args.report).with_suffix(".png").write_bytes(png)
        vm.desktop.move_mouse(200, 200)
        vm.desktop.click(200, 200)
        vm.desktop.scroll()
        text = "Unicode: café 日本語 👋"
        vm.desktop.write(text)
        assert execute("xclip", "-selection", "clipboard", "-out") == text
        recording = vm.desktop.record("/workspace/audit.mp4", fps=15)
        time.sleep(3)
        vm.desktop.stop_recording(recording)
        probe = json.loads(
            execute(
                "ffprobe",
                "-v",
                "error",
                "-show_streams",
                "-of",
                "json",
                "/workspace/audit.mp4",
            )
        )
        assert probe["streams"][0]["width"] == 1280
        assert len(vm.fs.read_bytes("/workspace/audit.mp4")) > 1024
        passed(
            "desktop_png_resize_unicode_input_and_playable_recording",
            {"original_size": size, "recording_fps": 15},
        )
        desktop_browser = requests.Session()
        assert desktop_browser.get(vm.access_url(8080), timeout=220).status_code == 200
        assert desktop_browser.get(desktop_url, timeout=20).status_code == 200
        passed("protected_desktop_browser_url")

        vm.update(ttl=2)
        with vm.keep_alive():
            time.sleep(14)
            assert vm.refresh().info["status"] == "running"
        vm.update(ttl=0)
        passed("activity_lease_survives_short_idle_timeout")
        print(
            "All integration checks passed; VM retained for browser validation and explicit cleanup.",
            flush=True,
        )


if __name__ == "__main__":
    main()
