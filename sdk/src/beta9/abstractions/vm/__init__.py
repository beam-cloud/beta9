"""Named, persistent CPU microVMs with durable roots and stable service URLs."""

import base64
import copy
import io
import math
import gzip
import os
import subprocess
import tarfile
import tempfile
import time
import uuid
import json
import betterproto
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

from ...channel import Channel, GatewayHTTPError, ServiceClient
from ...clients.image import ImageServiceStub
from ...clients.pod import PodSandboxConnectRequest, PodServiceStub
from ...config import ConfigContext, get_config_context, get_settings
from ...exceptions import ImageBuildError, SandboxConnectionError, SandboxProcessError
from ...type import DurableDisk
from ...clients.volume import VolumeServiceStub
from ..volume import Volume
from ..image import Image
from ..sandbox import SandboxInstance


def identity() -> Path:
    """One local SSH identity; private key never leaves this machine."""
    path = get_settings().config_path.parent / "ssh" / "vm_ed25519"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists() and path.with_suffix(".pub").exists():
        return path
    # Generate off-path and publish with an atomic, non-overwriting link.
    # Concurrent processes always derive the public key from the winning key.
    try:
        with tempfile.TemporaryDirectory(dir=path.parent) as directory:
            temporary = Path(directory) / "identity"
            if not path.exists():
                subprocess.run(
                    ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(temporary)],
                    stdin=subprocess.DEVNULL,
                    check=True,
                )
                _publish_identity(temporary, path)
            public = subprocess.check_output(
                ["ssh-keygen", "-y", "-f", str(path)], stdin=subprocess.DEVNULL
            )
            temporary_public = Path(directory) / "identity.pub"
            temporary_public.write_bytes(public)
            _publish_identity(temporary_public, path.with_suffix(".pub"))
    except FileNotFoundError as exc:
        raise RuntimeError("VM SSH requires OpenSSH; install ssh-keygen and try again") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Unable to prepare VM SSH identity at {path}: ssh-keygen failed"
        ) from exc
    return path


def _publish_identity(source: Path, target: Path):
    try:
        os.link(source, target)
    except FileExistsError:
        pass


def public_key() -> str:
    return identity().with_suffix(".pub").read_text().strip()


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
        for name in (
            "beam-desktop.service",
            "beam-terminal.service",
            "boot",
            "desktop",
            "install",
            "xstartup",
        ):
            path = Path(__file__).with_name(name)
            data = path.read_bytes()
            entry = tarfile.TarInfo(name)
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

    Stop/start cold boots enabled systemd units. Pause/resume preserves RAM
    and running processes. URLs and machine identity stay fixed. Forks and templates get independent disks/identities.
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
        idle_action: Optional[str] = None,
        pool: Optional[str] = None,
        template: Optional[str] = None,
        snapshot: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        auto_resume: Optional[bool] = None,
        block_network: Optional[bool] = None,
        allow_list: Optional[List[str]] = None,
        protected_ports: Optional[List[int]] = None,
        request_id: Optional[str] = None,
        disks: Optional[List[DurableDisk]] = None,
        volumes: Optional[List[Volume]] = None,
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
        self.request_id = str(uuid.UUID(request_id)) if request_id else str(uuid.uuid4())
        self._metadata = metadata
        self._creation_body = None
        self._volumes = volumes
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
            "idle_action": idle_action,
            "pool": pool,
            "auto_resume": auto_resume,
            "block_network": block_network,
            "allow_list": allow_list,
            "protected_ports": protected_ports,
            "disks": [disk.export().to_dict(casing=betterproto.Casing.SNAKE) for disk in disks]
            if disks is not None
            else None,
        }

        self._spec = {key: value for key, value in self._spec.items() if value is not None}

    def _api(self, method, path="", **kwargs):
        deadline = time.monotonic() + 240
        while True:
            try:
                return self._service.http.json(
                    method,
                    "/api/v1/vm/{ws}" + path,
                    timeout=max(1, math.ceil(deadline - time.monotonic())),
                    **kwargs,
                )
            except GatewayHTTPError as exc:
                # Advisory-lock conflicts have no side effects. Wait for the
                # in-flight operation, while preserving other 409 errors.
                if (
                    exc.status != 409
                    or "operation already in progress" not in exc.message
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.25)

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
        # Reusing this object after a lost response must send exactly the same
        # creation request, including the image and SSH key already prepared.
        if self._creation_body is not None:
            self._set(self._api("POST", json=self._creation_body))
            return self.wait() if wait else self
        # Verify the selected gateway before building an image or creating keys.
        # Older gateways return a generic route 404, which otherwise appears
        # after a successful (and potentially expensive) image build.
        try:
            self._api("GET")
        except GatewayHTTPError as exc:
            if exc.status != 404:
                raise
            raise GatewayHTTPError(
                404,
                f"Persistent VMs are unavailable at {self._service.http.base_url} "
                "(HTTP 404). Deploy a gateway with persistent VM support, "
                "or select the correct --context.",
            ) from exc
        spec = dict(self._spec)
        if self._volumes is not None:
            spec["volumes"] = []
            for volume in self._volumes:
                selected = copy.copy(volume)
                selected.stub = VolumeServiceStub(self._service.channel)
                if not selected.get_or_create():
                    raise RuntimeError(f"Unable to prepare volume {selected.name}")
                spec["volumes"].append(selected.export().to_dict(casing=betterproto.Casing.SNAKE))
        if spec.get("ssh", True):
            spec["ssh_public_key"] = public_key()
        if not (self.template or self._snapshot_source):
            spec.setdefault("ssh", True)
            image = self.image or Image(base_image="ubuntu:22.04")
            if self.image is None:
                image.ignore_python = True
            result = prepare_image(image, self._service, spec.get("desktop", False)).build()
            if not result.success:
                raise ImageBuildError(result.error or "VM image build failed")
            spec["image_id"] = result.image_id
        self._creation_body = {
            "name": self.name or "",
            "spec": spec,
            "template": self.template or "",
            "request_id": self.request_id,
            **({"metadata": self._metadata} if self._metadata is not None else {}),
            **({"snapshot": self._snapshot_source} if self._snapshot_source else {}),
        }
        self._set(self._api("POST", json=self._creation_body))
        return self.wait() if wait else self

    @classmethod
    def get(cls, name: str, *, context=None, _service=None) -> "VM":
        return cls(name, context=context, _service=_service).refresh()

    @classmethod
    def list(cls, *, context=None, all=False, metadata=None, status=None, _service=None):
        client = cls(context=context, _service=_service)
        try:
            params = {"all": str(all).lower()}
            if metadata is not None:
                params["metadata"] = json.dumps(metadata)
            if status is not None:
                params["status"] = status
            return client._api("GET", params=params)
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
                if self._services_ready(sandbox, deadline):
                    return self
            time.sleep(0.5)
        raise TimeoutError("VM did not become ready; inspect vm get/logs")

    def _services_ready(self, sandbox, deadline):
        services = [("beam-terminal.service", 7681)]
        for feature, unit, port in (
            ("ssh", "ssh.service", 2222),
            ("desktop", "beam-desktop.service", 8080),
        ):
            if self.info["spec"].get(feature):
                services.append((unit, port))
        try:
            # is-active with several units succeeds if any is active.
            for unit, _ in services:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                check = sandbox.process.exec("systemctl", "is-active", "--quiet", unit)
                if check.wait(min(10, remaining)) != 0:
                    return False
            ports = [port for _, port in services]
            probe = (
                "import socket; "
                f"[socket.create_connection(('127.0.0.1', p), 1).close() for p in {ports!r}]"
            )
            remaining = deadline - time.monotonic()
            return (
                remaining > 0
                and sandbox.process.exec("python3", "-c", probe).wait(min(10, remaining)) == 0
            )
        except (SandboxConnectionError, SandboxProcessError):
            return False

    def _action(self, action, **body):
        return self._api("POST", self._path() + "/" + action, json=body)

    def start(self, wait=True, *, cold=False) -> "VM":
        """Start or restore. cold=True explicitly discards saved RAM."""
        self._set(self._action("start", **({"cold": True} if cold else {})))
        return self.wait() if wait else self

    resume = start

    @classmethod
    def connect(cls, name: str, *, context=None, _service=None) -> "VM":
        """Resolve a persistent VM and start it if stopped."""
        vm = cls.get(name, context=context, _service=_service)
        try:
            return vm.start() if vm.info["status"] != "running" else vm.wait()
        except Exception:
            vm.close()
            raise

    def update(self, *, ttl=None, idle_action=None, auto_resume=None, metadata=None) -> "VM":
        """Change idle stopping and metadata without replacing the VM or URLs.

        Metadata replaces the complete map; pass {} to clear it. It is visible
        in management responses, so use workspace secrets for credentials.
        """
        body = {
            "idle_timeout": ttl,
            "idle_action": idle_action,
            "auto_resume": auto_resume,
            "metadata": metadata,
        }
        return self._set(
            self._api("PATCH", self._path(), json={k: v for k, v in body.items() if v is not None})
        )

    def update_network_permissions(self, block_network=False, allow_list=None) -> "VM":
        """Use the host firewall and preserve the policy across stop/start."""
        return self._set(
            self._api(
                "PATCH",
                self._path(),
                json={"block_network": block_network, "allow_list": allow_list or []},
            )
        )

    def rotate_access_token(self) -> str:
        """Immediately revoke the previous token for protected application ports."""
        self._set(self._action("rotate-access-token"))
        return self.info["traffic_access_token"]

    @property
    def traffic_access_token(self):
        return self.refresh().info.get("traffic_access_token")

    def access_url(self, port: int, *, ttl=600) -> str:
        """Create an expiring browser entry URL for a protected application port.

        It exchanges the credential for an HTTP-only cookie and redirects to
        the stable URL. Rotating the traffic token also revokes these sessions.
        """
        return self._action("access-session", port=port, ttl=ttl)["url"]

    def get_url(self, port: int) -> str:
        """Return an already published URL; this does not publish a private port."""
        url = self.refresh().info["urls"].get(str(port))
        if url is None:
            raise ValueError(f"Port {port} is not published; call expose() first")
        return url

    def pause(self) -> "VM":
        """Release compute while retaining RAM and paired durable disks."""
        return self._set(self._action("pause"))

    def stop(self, no_snapshot=False) -> "VM":
        return self._set(self._action("stop", no_snapshot=no_snapshot))

    def remove(self):
        self._api("DELETE", self._path())
        self._connected = None

    def snapshot(self, name=None):
        return self._action("snapshot", name=name or "")

    def snapshots(self):
        return [a for a in self._api("GET", "/artifacts/snapshot") if a["vm_id"] == self.id]

    def remove_snapshot(self, name: str):
        self._api("DELETE", "/artifacts/snapshot/" + quote(name, safe=""))

    def create_template(self, name=None, description=""):
        return self._action("template", name=name or self.name, description=description)

    def fork(self, name=None, wait=True) -> "VM":
        info = self._action("fork", name=name or "", ssh_public_key=public_key())
        # SDK-owned children have independent connections; caller-owned
        # clients retain their explicit shared lifetime.
        child = (
            VM(name, context=self._service.channel.config)
            if self._owns_service
            else VM(name, _service=self._service)
        )._set(info)
        return child.wait() if wait else child

    def expose(self, port: int, *, protected: Optional[bool] = None) -> str:
        body = {"port": port}
        if protected is not None:
            body["protected"] = protected
        self._set(self._action("expose", **body))
        return self.info["urls"][str(port)]

    def unexpose(self, port: int):
        self._set(self._action("unexpose", port=port))

    def bind(self, port: int):
        """Bind a port for authenticated tunnels without publishing a URL."""
        self._set(self._action("bind", port=port))

    def _sandbox(self):
        self.refresh()
        container = self.info.get("container_id")
        if self.info["status"] != "running" and self.info.get("spec", {}).get("auto_resume"):
            self._set(self._action("wake"))
            self.wait()
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

    def metrics(self):
        """Sample guest CPU, memory, and root disk usage over one second.

        CPU percent is averaged across assigned guest vCPUs. This operation
        counts as activity and may resume an enabled VM.
        """
        code = """
import json, os, time
def cpu():
    values = list(map(int, open('/proc/stat').readline().split()[1:9]))
    return sum(values), values[3] + values[4]
before = cpu()
time.sleep(1)
after = cpu()
total, idle = after[0]-before[0], after[1]-before[1]
memory = {k: int(v.strip().split()[0])*1024 for k,v in (line.split(':',1) for line in open('/proc/meminfo'))}
disk = os.statvfs('/')
print(json.dumps({'timestamp': time.time(), 'cpu_count': os.cpu_count(),
 'cpu_used_percent': 100*(total-idle)/total if total else 0,
 'memory_total_bytes': memory['MemTotal'], 'memory_used_bytes': memory['MemTotal']-memory['MemAvailable'],
 'disk_total_bytes': disk.f_blocks*disk.f_frsize, 'disk_used_bytes': (disk.f_blocks-disk.f_bfree)*disk.f_frsize}))
"""
        process = self.process.exec("python3", "-c", code, cwd="/")
        if process.wait(10) != 0:
            raise RuntimeError(process.stderr.read() or "Unable to sample VM metrics")
        return json.loads(process.stdout.read())

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
    def desktop(self):
        """Screenshot and control the desktop through authenticated SDK calls."""
        from .desktop_api import VMDesktop

        return VMDesktop(self)

    @property
    def desktop_url(self):
        return self.refresh().info.get("desktop_url")

    @property
    def terminal_url(self):
        return self.refresh().info["terminal_url"]

    @property
    def id(self):
        return self.info["id"]
