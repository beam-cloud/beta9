"""Named, persistent CPU microVMs with durable roots and stable service URLs."""

import base64
import copy
import io
import gzip
import subprocess
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

from ..channel import Channel, ServiceClient
from ..clients.image import ImageServiceStub
from ..clients.pod import PodSandboxConnectRequest, PodServiceStub
from ..config import ConfigContext, get_config_context, get_settings
from ..exceptions import ImageBuildError, SandboxConnectionError
from .image import Image
from .sandbox import SandboxInstance


def identity() -> Path:
    """One local SSH identity; private key never leaves this machine."""
    path = get_settings().config_path.parent / "ssh" / "vm_ed25519"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.exists():
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(path)], check=True)
    return path


class _VMImage(Image):
    @property
    def channel(self):
        return self._vm_channel


def prepare_image(image: Image, service: ServiceClient, desktop: bool) -> Image:
    """Append the VM contract to registry images and the final Dockerfile stage."""
    if image.gpu:
        raise ValueError("VM images cannot be built with a GPU")
    prepared = _VMImage.__new__(_VMImage)
    prepared.__dict__ = copy.copy(image.__dict__)
    prepared._vm_channel = service.channel
    prepared._stub = ImageServiceStub(service.channel)
    prepared.gateway_stub = service.gateway
    prepared.build_steps = list(image.build_steps)
    if image._explicit_image_id:
        # Existing IDs must already include the VM services; no image changes
        # can be silently appended to a prebuilt, content-addressed image.
        return prepared
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for path in sorted(Path(__file__).with_name("vm_image").iterdir()):
            if path.is_file():
                data = path.read_bytes()
                entry = tarfile.TarInfo(path.name)
                entry.size = len(data)
                entry.mode = 0o644 if path.suffix == ".service" else 0o755
                archive.addfile(entry, io.BytesIO(data))
    data = base64.b64encode(gzip.compress(buffer.getvalue(), mtime=0)).decode("ascii")
    prepared.add_commands(
        [
            "mkdir -p /tmp/beam-vm-assets && "
            f"printf '%s' '{data}' | base64 -d | tar -xz -C /tmp/beam-vm-assets && "
            f"bash /tmp/beam-vm-assets/install {'true' if desktop else 'false'}"
        ]
    )
    return prepared


@dataclass
class _VMSandbox(SandboxInstance):
    vm_channel: Optional[Channel] = None

    @property
    def channel(self):
        return self.vm_channel


class VM:
    """A durable resource. Stop releases compute and retains the complete root.

    Resumes are cold boots: enabled systemd units start again. URLs and machine
    identity stay fixed. Forks and templates get independent disks/identities.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        *,
        image: Optional[Image] = None,
        cpu: Optional[float] = None,
        memory: Optional[int] = None,
        disk_size: Optional[str] = None,
        desktop: Optional[bool] = None,
        docker_enabled: Optional[bool] = None,
        env: Optional[Dict[str, str]] = None,
        secrets: Optional[List[str]] = None,
        ports: Optional[List[int]] = None,
        ssh: Optional[bool] = None,
        ttl: Optional[int] = None,
        pool: Optional[str] = None,
        template: Optional[str] = None,
        snapshot: Optional[str] = None,
        context: Optional[Union[ConfigContext, str]] = None,
        _service: Optional[ServiceClient] = None,
    ):
        config = get_config_context(context) if isinstance(context, str) else context
        self._owns_service = _service is None
        self._service = _service or ServiceClient(config)
        self.name = name
        self.image = image
        self.template = template
        self._snapshot_source = snapshot
        if template and snapshot:
            raise ValueError("Choose a template or snapshot")
        self.info: Dict[str, Any] = {}
        self._connected = None
        self._spec = {
            "cpu": int(cpu * 1000) if cpu is not None else None,
            "memory": memory,
            "disk_size": disk_size,
            "desktop": desktop,
            "docker_enabled": docker_enabled,
            "env": [f"{key}={value}" for key, value in env.items()] if env is not None else None,
            "secrets": secrets,
            "ports": ports,
            "ssh": ssh,
            "idle_timeout": ttl,
            "pool": pool,
        }

        self._spec = {key: value for key, value in self._spec.items() if value is not None}

    def _api(self, method, path="", **kwargs):
        return self._service.http.json(method, "/api/v1/vm/{ws}" + path, timeout=240, **kwargs)

    def _path(self):
        if not self.name:
            raise ValueError("Create or resolve the VM first")
        return "/" + quote(self.info.get("id") or self.name, safe="")

    def _set(self, info):
        if info.get("container_id") != self.info.get("container_id"):
            self._connected = None
        self.info = info
        self.name = info["name"]
        return self

    def create(self, wait: bool = True) -> "VM":
        spec = dict(self._spec)
        if spec.get("ssh", True):
            spec["ssh_public_key"] = identity().with_suffix(".pub").read_text().strip()
        if not (self.template or self._snapshot_source):
            spec.setdefault("ssh", True)
            image = self.image or Image(base_image="ubuntu:22.04")
            if self.image is None:
                image.ignore_python = True
            result = prepare_image(image, self._service, spec.get("desktop", False)).build()
            if not result.success:
                raise ImageBuildError(result.error or "VM image build failed")
            spec["image_id"] = result.image_id
        self._set(
            self._api(
                "POST",
                json={
                    "name": self.name or "",
                    "spec": spec,
                    "template": self.template or "",
                    **({"snapshot": self._snapshot_source} if self._snapshot_source else {}),
                },
            )
        )
        return self.wait() if wait else self

    @classmethod
    def get(cls, name: str, *, context=None, _service=None) -> "VM":
        return cls(name, context=context, _service=_service).refresh()

    @classmethod
    def list(cls, *, context=None, all=False, _service=None):
        client = cls(context=context, _service=_service)
        try:
            return client._api("GET", params={"all": str(all).lower()})
        finally:
            client.close()

    def close(self):
        """Release client connections; the persistent VM keeps running."""
        if self._owns_service:
            self._service.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def refresh(self) -> "VM":
        return self._set(self._api("GET", self._path()))

    def wait(self, timeout: float = 180) -> "VM":
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.refresh()
            if self.info["status"] == "error":
                raise RuntimeError(self.info.get("error") or "VM failed to start")
            if self.info["status"] == "running":
                # Runtime readiness includes the process manager. Check that
                # the guest services have started, too, before returning.
                try:
                    sandbox = self._sandbox()
                except SandboxConnectionError:
                    time.sleep(0.5)
                    continue
                units = ["beam-terminal.service"]
                if self.info["spec"].get("ssh"):
                    units.append("ssh.service")
                if self.info["spec"].get("desktop"):
                    units.append("beam-desktop.service")
                # is-active with several units succeeds if any is active.
                ready = True
                for unit in units:
                    check = sandbox.process.exec("systemctl", "is-active", "--quiet", unit)
                    if check.wait(10) != 0:
                        ready = False
                        break
                if ready:
                    # Active services may still be binding their sockets.
                    ports = [7681]
                    if self.info["spec"].get("ssh"):
                        ports.append(2222)
                    if self.info["spec"].get("desktop"):
                        ports.append(8080)
                    probe = (
                        "import socket; "
                        f"[socket.create_connection(('127.0.0.1', p), 1).close() for p in {ports!r}]"
                    )
                    if sandbox.process.exec("python3", "-c", probe).wait(10) == 0:
                        return self
            time.sleep(0.5)
        raise TimeoutError("VM did not become ready; inspect vm get/logs")

    def _action(self, action, **body):
        return self._api("POST", self._path() + "/" + action, json=body)

    def start(self, wait=True) -> "VM":
        self._set(self._action("start"))
        return self.wait() if wait else self

    resume = start

    def stop(self, no_snapshot=False) -> "VM":
        return self._set(self._action("stop", no_snapshot=no_snapshot))

    def remove(self):
        self._api("DELETE", self._path())
        self._connected = None

    def snapshot(self, name=None):
        return self._action("snapshot", name=name or "")

    def create_template(self, name=None, description=""):
        return self._action("template", name=name or self.name, description=description)

    def fork(self, name=None, wait=True) -> "VM":
        key = identity().with_suffix(".pub").read_text().strip()
        info = self._action("fork", name=name or "", ssh_public_key=key)
        child = VM(name, _service=self._service)._set(info)
        return child.wait() if wait else child

    def expose(self, port: int) -> str:
        self._set(self._action("expose", port=port))
        return self.info["urls"][str(port)]

    def unexpose(self, port: int):
        self._set(self._action("unexpose", port=port))

    def bind(self, port: int):
        """Bind a port for authenticated tunnels without publishing a URL."""
        self._set(self._action("bind", port=port))

    def _sandbox(self):
        self.refresh()
        container = self.info.get("container_id")
        if not container or self.info["status"] != "running":
            raise SandboxConnectionError("VM is not running; call start() first")
        if self._connected is None:
            response = PodServiceStub(self._service.channel).sandbox_connect(
                PodSandboxConnectRequest(container_id=container)
            )
            if not response.ok:
                raise SandboxConnectionError(response.error_msg)
            self._connected = _VMSandbox(
                container_id=container,
                stub_id=response.stub_id,
                ok=True,
                vm_channel=self._service.channel,
            )
        self._action("touch")
        return self._connected

    @property
    def process(self):
        return self._sandbox().process

    @property
    def fs(self):
        return self._sandbox().fs

    @property
    def docker(self):
        return self._sandbox().docker

    @property
    def desktop_url(self):
        return self.refresh().info.get("desktop_url")

    @property
    def terminal_url(self):
        return self.refresh().info["terminal_url"]

    @property
    def id(self):
        return self.info["id"]
