from dataclasses import dataclass
from typing import Optional

from ..abstractions.base import BaseAbstraction
from ..clients.gateway import MountPointConfig as VolumeConfigGateway
from ..clients.gateway import Volume as VolumeGateway
from ..clients.volume import GetOrCreateVolumeRequest, GetOrCreateVolumeResponse, VolumeServiceStub


@dataclass
class VolumeConfig:
    """
    Configuration for a volume.

    Parameters:
        external (bool):
            Whether the volume is from an external provider.
        access_key (str):
            The S3 access key for the external provider.
        secret_key (str):
            The S3 secret key for the external provider.
        endpoint (Optional[str]):
            The S3 endpoint for the external provider.
    """

    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    endpoint: Optional[str] = None
    external: bool = False


class Volume(BaseAbstraction):
    def __init__(
        self, name: str, mount_path: str, config: VolumeConfig = VolumeConfig(external=False)
    ) -> None:
        """
        Creates a Volume instance.

        When your container runs, your volume will be available at `./{name}` and `/volumes/{name}`.

        Parameters:
            name (str):
                The name of the volume, a descriptive identifier for the data volume.
            mount_path (str):
                The path where the volume is mounted within the container environment.
            config (Optional[VolumeConfig]):
                Optional configuration for the volume if it is from an external provider.

        Example:
            ```python
            from beta9 import Volume

            # Shared Volume
            shared_volume = Volume(name="model_weights", mount_path="./my-weights")
            ```
        """
        super().__init__()

        self.name = name
        self.ready = False
        self.volume_id = None
        self.mount_path = mount_path
        self.config = config
        self._stub: Optional[VolumeServiceStub] = None

    @property
    def stub(self) -> VolumeServiceStub:
        if not self._stub:
            self._stub = VolumeServiceStub(self.channel)
        return self._stub

    @stub.setter
    def stub(self, value: VolumeServiceStub):
        self._stub = value

    def get_or_create(self) -> bool:
        if self.config.external:
            return True

        resp: GetOrCreateVolumeResponse
        resp = self.stub.get_or_create_volume(GetOrCreateVolumeRequest(name=self.name))

        if resp.ok:
            self.ready = True
            self.volume_id = resp.volume.id
            return True

        return False

    def export(self):
        vol = VolumeGateway(
            id=self.volume_id,
            mount_path=self.mount_path,
        )

        # FIXME: is the external flag necessary?
        if self.config.external:
            vol.config = VolumeConfigGateway(
                bucket_name=self.name,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                bucket_url=self.config.endpoint,
            )

        return vol
