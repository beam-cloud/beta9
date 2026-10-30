"""Named, persistent microVMs with durable roots and stable service URLs."""

import base64
import copy
import json
import math
import os
import subprocess
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import betterproto

from ...channel import (
    RECOVERY_TIMEOUT,
    Channel,
    GatewayHTTP,
    GatewayHTTPError,
    ServiceClient,
    rpc_timeout,
    transient_error,
)
from ...clients.image import ImageServiceStub
from ...clients.pod import PodSandboxConnectRequest, PodServiceStub
from ...clients.volume import VolumeServiceStub
from ...config import ConfigContext, get_config_context, get_settings
from ...exceptions import ImageBuildError, SandboxConnectionError, SandboxProcessError
from ...terminal import cli_name
from ...type import DurableDisk, GpuType, GpuTypeAlias, normalize_gpu_type
from ...utils import local_process_kwargs
from ..image import Image
from ..sandbox import SandboxInstance
from ..volume import Volume
from .kiosk_config import Kiosk, MCPServer as MCPServer


# Keep the stock guest reproducible and avoid a registry tag lookup on launch.
# Refresh this digest when updating the default Ubuntu guest.
DEFAULT_VM_BASE_IMAGE = (
    "ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55"
)
DEFAULT_VM_GPU_BASE_IMAGE = "public.ecr.aws/n4e0e1y0/beta9-microvm:f54e5dd00d65-nvidia"


def default_base_image(gpu: Optional[GpuTypeAlias] = None) -> str:
    selected = GpuType(normalize_gpu_type(gpu)) if gpu is not None else GpuType.NoGPU
    if selected in (GpuType.Any, GpuType.V100, GpuType.V100_32, GpuType.GAUDI2):
        raise ValueError(
            f"The default VM image does not support {selected.value}; "
            "provide an image with a compatible guest driver"
        )
    return DEFAULT_VM_GPU_BASE_IMAGE if selected != GpuType.NoGPU else DEFAULT_VM_BASE_IMAGE


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
                    [
                        "ssh-keygen",
                        "-q",
                        "-t",
                        "ed25519",
                        "-N",
                        "",
                        "-f",
                        str(temporary),
                    ],
                    stdin=subprocess.DEVNULL,
                    check=True,
                    **local_process_kwargs("ssh-keygen"),
                )
                _publish_identity(temporary, path)
            public = subprocess.check_output(
                ["ssh-keygen", "-y", "-f", str(path)],
                stdin=subprocess.DEVNULL,
                **local_process_kwargs("ssh-keygen"),
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

    def build(self):
        return super().build(quiet=True)


def prepare_image(
    image: Image, service: ServiceClient, desktop: bool, *, kiosk: bool = False
) -> Image:
    """Append the VM contract to registry images and the final Dockerfile stage."""
    if image.gpu:
        raise ValueError("VM images cannot be built with a GPU")
    prepared = _VMImage.__new__(_VMImage)
    prepared.__dict__ = copy.copy(image.__dict__)
    if kiosk:
        # GUI packages and the native streaming backend depend on the distro's
        # Python ABI. Serverless Python injection replaces /usr/bin/python3.
        prepared.ignore_python = True
    prepared._vm_channel = service.channel
    prepared._stub = ImageServiceStub(service.channel)
    prepared.gateway_stub = service.gateway
    prepared.build_steps = list(image.build_steps)
    if image._explicit_image_id:
        # Existing IDs must already include the VM services; no image changes
        # can be silently appended to a prebuilt, content-addressed image.
        return prepared
    # Ship one bundle: tar headers and compression can differ between clients,
    # and the encoded bytes are part of the image cache key.
    data = base64.b64encode(Path(__file__).with_name("assets.tar.gz").read_bytes()).decode("ascii")
    prepared.add_commands(
        [
            "mkdir -p /tmp/beam-vm-assets && "
            f"printf '%s' '{data}' | base64 -d | tar -xz -C /tmp/beam-vm-assets && "
            f"bash /tmp/beam-vm-assets/install {'true' if desktop or kiosk else 'false'}"
            + (" true" if kiosk else "")
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
    and running processes. URLs and machine identity stay fixed. Forks and
    templates get independent disks and identities.

    disk_size sets the writable root capacity over the base image
    (1–100 GiB; default 50 GiB).

    Fresh VMs default to digest-pinned Ubuntu 24.04, 4 CPUs, 8192 MiB RAM
    and a 50 GiB root. Desktop is opt-in and needs at least 1024 MiB RAM.
    GPU defaults include modern NVIDIA open drivers built for the worker's
    matching kernel. Other drivers require an explicit image.
    create() waits for exec readiness; wait(services=True) also waits for
    enabled SSH/terminal/desktop services. Docker helpers wait separately.

    Runtime secrets require approved hosts and arrive as placeholders for
    literal HTTPS request headers. No workspace token is injected automatically;
    BETA9_TOKEN/BEAM_TOKEN bindings are explicit plaintext opt-ins.
    Terminal 7681 and enabled desktop 8080 are protected by default; use
    access_url() for browsers or traffic_access_token for HTTP clients.
    public_ipv6 reads the persistent address from resolved metadata, or None
    when no address is allocated. Inbound protection also applies over IPv6.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        *,
        image: Optional[Image] = None,
        cpu: Optional[float] = None,
        memory: Optional[int] = None,
        gpu: Optional[GpuTypeAlias] = None,
        disk_size: Optional[str] = None,
        desktop: Optional[bool] = None,
        kiosk: Optional[Union[Kiosk, Dict[str, Any]]] = None,
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
        public_ports: Optional[List[int]] = None,
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
        self._vm_http = None
        self._volumes = volumes
        if kiosk is not None and desktop is False:
            raise ValueError("kiosk enables the display; remove desktop=False")
        if kiosk is not None and not isinstance(kiosk, (Kiosk, dict)):
            raise ValueError("kiosk must be a Kiosk configuration or an object")
        if template and snapshot:
            raise ValueError("Choose a template or snapshot")
        if gpu is not None and (not isinstance(gpu, str) or "," in gpu):
            raise ValueError("gpu must be a single GPU type")

        self.info: Dict[str, Any] = {}
        self._connected = None
        self._connected_at = 0
        self._services_container = None
        self._spec = {
            "cpu": int(cpu * 1000) if cpu is not None else None,
            "memory": memory,
            "gpu": GpuType(normalize_gpu_type(gpu)).value if gpu is not None else None,
            "disk_size": disk_size,
            "desktop": True if kiosk is not None else desktop,
            "kiosk": copy.deepcopy(kiosk.to_dict() if isinstance(kiosk, Kiosk) else kiosk),
            "docker_enabled": docker_enabled,
            "env": ([f"{key}={value}" for key, value in env.items()] if env is not None else None),
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
            "public_ports": public_ports,
            "disks": (
                [disk.export().to_dict(casing=betterproto.Casing.SNAKE) for disk in disks]
                if disks is not None
                else None
            ),
        }

        self._spec = {key: value for key, value in self._spec.items() if value is not None}

    def _api(self, method, path="", *, timeout=240, **kwargs):
        deadline = time.monotonic() + timeout
        transport_retries = 0
        body = kwargs.get("json") or {}
        replayable = method == "GET" or (
            method == "POST" and (path.endswith("/start") or (not path and body.get("request_id")))
        )
        while True:
            try:
                return (self._vm_http or self._service.http).json(
                    method,
                    "/api/v1/vm/{ws}" + path,
                    timeout=max(1, math.ceil(deadline - time.monotonic())),
                    **kwargs,
                )
            except GatewayHTTPError as exc:
                # Keyed creation, reads and start can be replayed even when
                # the gateway committed before its connection was interrupted.
                if (
                    exc.status == 0
                    and replayable
                    and transport_retries < 2
                    and time.monotonic() < deadline
                ):
                    transport_retries += 1
                    time.sleep(0.25)
                    continue
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

    def _touch(self, *, timeout=240, lease=None, release=False):
        params = {"lease": lease, "release": "1" if release else "0"} if lease else {}
        return self._api("POST", self._path() + "/touch", timeout=timeout, json={}, params=params)

    def _set(self, info):
        if info.get("container_id") != self.info.get("container_id"):
            self._connected = None
            self._services_container = None

        self.info = info
        self.name = info["name"]

        if info.get("exec_ready"):
            self._connected = _VMSandbox(
                container_id=info["container_id"],
                stub_id=info["stub_id"],
                ok=True,
                vm_channel=self._service.channel,
            )
            self._connected_at = time.monotonic()

        return self

    def create(self, wait: bool = True) -> "VM":
        if (
            self._creation_body is None
            and self.image is None
            and not (self.template or self._snapshot_source or self._volumes)
            and isinstance(getattr(self._service, "vm_http", None), GatewayHTTP)
        ):
            # A cached default image can be resolved by the gateway in the
            # creation request, without transport discovery or an image RPC.
            self._vm_http = self._service.vm_http
            spec = self._prepare_spec()
            image = self._prepare_image()._verify_request()
            self._creation_body = self._creation_request(
                spec, image=image.to_dict(casing=betterproto.Casing.SNAKE)
            )
            try:
                return self._submit_creation(wait)
            except GatewayHTTPError as exc:
                # These responses precede resource creation. Older gateways
                # reject the alias/body; a missing image needs the build flow.
                unsupported_body = exc.status == 400 and 'unknown field "image"' in exc.message
                if (exc.status not in (401, 404, 428) and not unsupported_body) or self.info:
                    raise
                self._vm_http = None
                self._creation_body = None
        self.prepare()
        return self._submit_creation(wait)

    def prepare(self) -> "VM":
        """Prepare the immutable image, keys and launch request without starting compute."""
        # Reusing this object after a lost response must send exactly the same
        # creation request, including the image and SSH key already prepared.
        if self._creation_body is not None:
            return self

        # Verify the selected gateway before building an image or creating keys.
        # Older gateways return a generic route 404, which otherwise appears
        # after a successful (and potentially expensive) image build.
        try:
            # New gateways advertise VM support in the transport handshake.
            # Keep the preflight for older gateways before building an image.
            if getattr(self._service.http, "persistent_vms", False) is not True:
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

        spec = self._prepare_spec()
        if not (self.template or self._snapshot_source):
            result = self._prepare_image().build()
            if not result.success:
                raise ImageBuildError(result.error or "VM image build failed")
            spec["image_id"] = result.image_id

        self._creation_body = self._creation_request(
            spec,
            template=self.template or "",
            **({"snapshot": self._snapshot_source} if self._snapshot_source else {}),
        )
        return self

    def _creation_request(self, spec, **source):
        return copy.deepcopy(
            {
                "name": self.name or "",
                "spec": spec,
                "request_id": self.request_id,
                **({"metadata": self._metadata} if self._metadata is not None else {}),
                **source,
            }
        )

    def _prepare_image(self):
        image = self.image or Image(base_image=default_base_image(self._spec.get("gpu")))
        if self.image is None:
            image.ignore_python = True
        return prepare_image(
            image,
            self._service,
            self._spec.get("desktop", False),
            kiosk=self._spec.get("kiosk") is not None,
        )

    def _prepare_spec(self):
        """Resolve workspace volumes and SSH identity before freezing the request."""
        spec = dict(self._spec)
        if self._volumes is not None:
            spec["volumes"] = []
            for volume in self._volumes:
                selected = copy.copy(volume)
                selected.stub = VolumeServiceStub(self._service.channel)
                if not selected.get_or_create():
                    raise RuntimeError(f"Unable to prepare volume {selected.name}")
                spec["volumes"].append(selected.export().to_dict(casing=betterproto.Casing.SNAKE))

        if not (self.template or self._snapshot_source):
            spec.setdefault("ssh", True)
        if spec.get("ssh", True):
            spec["ssh_public_key"] = public_key()

        return spec

    def _submit_creation(self, wait):
        kwargs = {"params": {"wait": "exec"}} if wait else {}
        self._set(self._api("POST", json=self._creation_body, **kwargs))
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

    def wait(self, timeout: float = 180, *, services: bool = False) -> "VM":
        """Wait for exec; services=True also waits for SSH, terminal and desktop readiness."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.info:
                self.refresh()
            if self.info["status"] == "error":
                raise RuntimeError(self.info.get("error") or "VM failed to start")
            if self.info["status"] in ("starting", "running"):
                # SandboxConnect waits on the worker's readiness event, so
                # neither lifecycle reconciliation nor fixed polling delays
                # are part of the normal startup path.
                try:
                    with rpc_timeout(max(0.001, deadline - time.monotonic())):
                        sandbox = self._sandbox(refresh=False, allow_starting=True)
                except SandboxConnectionError:
                    pass
                else:
                    self.info["status"] = "running"
                    if not services:
                        return self
                    if (
                        self._services_container is not None
                        and self._services_container == self.info.get("container_id")
                    ) or self._services_ready(sandbox, deadline):
                        self._services_container = self.info.get("container_id")
                        return self
            time.sleep(min(0.01, max(0, deadline - time.monotonic())))
            self.refresh()
        raise TimeoutError("VM did not become ready; inspect vm get/logs")

    def _services_ready(self, sandbox, deadline):
        spec = self.info["spec"]
        kiosk = spec.get("kiosk") is not None
        services = [("beam-terminal.service", 7681)]
        for feature, unit, port in (
            ("ssh", "ssh.service", 2222),
            ("desktop", "beam-desktop.service", 8080),
        ):
            if spec.get(feature):
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
            if kiosk:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                check = sandbox.process.exec("/opt/beam-vm/kiosk", "--check")
                if check.wait(min(10, remaining)) != 0:
                    try:
                        status = json.loads(check.stdout.read())
                    except (ValueError, TypeError):
                        status = {}
                    if status.get("state") in ("error", "closed"):
                        raise RuntimeError(
                            status.get("error") or "Kiosk application is unavailable"
                        )
                    return False
            ports = [port for _, port in services]
            probe = (
                "import socket; "
                f"[socket.create_connection(('127.0.0.1', p), 1).close() for p in {ports!r}]"
            )
            if spec.get("desktop") and not kiosk:
                # Xvnc accepts connections before xstartup publishes font
                # resources and starts the window manager.
                probe += (
                    "; import re, subprocess; "
                    "raise SystemExit(not re.search(rb'window id # 0x[1-9a-fA-F][0-9a-fA-F]*', "
                    "subprocess.check_output(['xprop', '-root', '_NET_SUPPORTING_WM_CHECK'])))"
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
        self._set(
            self._api(
                "POST",
                self._path() + "/start",
                json={"cold": True} if cold else {},
                **({"params": {"wait": "exec"}} if wait else {}),
            )
        )
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
            self._api(
                "PATCH",
                self._path(),
                json={k: v for k, v in body.items() if v is not None},
            )
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
        return self._action("rotate-access-token")["token"]

    @property
    def traffic_access_token(self):
        return self._action("access-token")["token"]

    def access_url(self, port: int, *, ttl=600) -> str:
        """Create an expiring browser entry URL for a protected application port.

        Requires a published protected HTTP port and a VM subdomain. TTL is
        1–3600 seconds (default 600). Anyone holding the link can access that
        port until expiry. It sets a Secure, HTTP-only, host-scoped cookie and
        redirects to the clean stable URL. Rotating the traffic token also
        revokes these sessions. Raw IPv6 URLs cannot use browser sessions.
        """
        return self._action("access-session", port=port, ttl=ttl)["url"]

    def get_url(self, port: int) -> str:
        """Return a stable published URL; protected ports still need authentication.

        Use access_url() for a browser link. This does not publish a private port.
        """
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

    def _sandbox(self, *, auto_resume=True, refresh=True, allow_starting=False):
        if refresh and (self._connected is None or time.monotonic() - self._connected_at >= 1):
            self.refresh()
        container = self.info.get("container_id")
        if (
            auto_resume
            and self.info["status"] not in ("starting", "running")
            and self.info.get("spec", {}).get("auto_resume")
        ):
            self._set(self._action("wake"))
            self.wait()
            container = self.info.get("container_id")
        if not container or self.info["status"] not in (
            ("starting", "running") if allow_starting else ("running",)
        ):
            raise SandboxConnectionError(
                f"VM is not running. Start it with: {cli_name()} vm start {self.name}"
            )
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
            self._connected_at = time.monotonic()
        if self.info.get("spec", {}).get("idle_timeout"):
            self._touch()
        return self._connected

    @contextmanager
    def keep_alive(self):
        """Keep an idle-limited VM active during a long SDK operation.

        This lease ends when the context exits. Detached work should disable
        idle stopping, or be owned by an application that maintains a lease.
        """
        self.refresh()
        ttl = self.info.get("spec", {}).get("idle_timeout", 0)
        if not ttl:
            self._touch()
            yield self
            return
        lease = str(uuid.uuid4())
        self._touch(lease=lease)
        interval = max(0.25, min(15, ttl / 3))
        done = threading.Event()
        errors = []

        def heartbeat():
            recovery_started = None
            while not done.wait(interval):
                try:
                    self._touch(timeout=3, lease=lease)
                    recovery_started = None
                except Exception as exc:
                    if transient_error(exc):
                        recovery_started = recovery_started or time.monotonic()
                        if time.monotonic() - recovery_started < RECOVERY_TIMEOUT:
                            continue
                    errors.append(exc)
                    return

        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        try:
            yield self
        finally:
            done.set()
            thread.join(4)
            try:
                self._touch(timeout=3, lease=lease, release=True)
            except Exception as exc:
                if not transient_error(exc):
                    errors.append(exc)
        if errors:
            raise RuntimeError("VM activity lease failed") from errors[0]

    def metrics(self):
        """Sample guest CPU, memory, and root disk usage over one second.

        CPU percent is averaged across assigned guest vCPUs. This operation
        counts as activity and may resume an enabled VM.
        """
        script = (Path(__file__).parent / "assets" / "metrics").read_text()
        return self._guest_json("python3", "-c", script, error="Unable to sample VM metrics")

    @staticmethod
    def _guest_output(process, *, error, timeout=30):
        try:
            code = process.wait(timeout)
        except SandboxProcessError:
            try:
                process.kill()
            except Exception:
                pass
            raise
        if code != 0:
            raise RuntimeError(process.stderr.read() or error)
        return process.stdout.read().strip()

    def _guest_json(self, *command, error, timeout=10, **options):
        process = self.process.exec(*command, cwd="/", **options)
        return json.loads(self._guest_output(process, error=error, timeout=timeout))

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
    def aio(self):
        """Existing async process, filesystem and Docker transports."""
        return self._sandbox().aio

    @property
    def mcp(self):
        """Install and connect application-specific MCP servers in a kiosk VM."""
        from .mcp_api import VMMCP

        return VMMCP(self)

    @property
    def desktop(self):
        """Screenshot and control the desktop through authenticated SDK calls."""
        from .desktop_api import VMDesktop

        return VMDesktop(self)

    @property
    def public_ipv6(self) -> Optional[str]:
        """Persistent public IPv6 address, or None when one has not been allocated.

        Available after create(), get() or refresh(); reading it does not make
        a network request. The address survives stop/start and pause/resume.
        """
        return self.info.get("public_ipv6") or None

    @property
    def desktop_url(self):
        return self.refresh().info.get("desktop_url")

    @property
    def kiosk_url(self):
        return self.refresh().info.get("kiosk_url")

    @property
    def mcp_url(self):
        """Authenticated endpoint aggregating the kiosk VM's application MCP servers."""
        return self.refresh().info.get("mcp_url")

    def kiosk_status(self):
        """Read application readiness and errors without restarting anything."""
        if self.refresh().info["spec"].get("kiosk") is None:
            raise ValueError("VM does not have kiosk mode enabled")
        return self._guest_json(
            "/opt/beam-vm/kiosk", "--status", error="Kiosk status is unavailable"
        )

    @property
    def terminal_url(self):
        return self.refresh().info["terminal_url"]

    @property
    def id(self):
        return self.info["id"]
