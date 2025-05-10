from dataclasses import dataclass
from typing import Optional

from ..abstractions.base import BaseAbstraction
from ..clients.gateway import Volume as VolumeGateway
from ..clients.types import MountPointConfig as VolumeConfigGateway
from ..clients.volume import GetOrCreateVolumeRequest, GetOrCreateVolumeResponse, VolumeServiceStub


class Volume(BaseAbstraction):
    def __init__(
        self,
        name: str,
        mount_path: str,
    ) -> None:
        """
        Creates a Volume instance.

        When your container runs, your volume will be available at `./{name}` and `/volumes/{name}`.

        Parameters:
            name (str):
                The name of the volume, a descriptive identifier for the data volume. Note that when using an external provider, the name must be the same as the bucket name.
            mount_path (str):
                The path where the volume is mounted within the container environment.
            config (Optional[VolumeConfig]):
                Optional configuration for the volumes from external providers (AWS, Cloudflare, Tigris).

        Example:
            ```python
            from beta9 import Volume, VolumeConfig

            # Shared Volume
            shared_volume = Volume(name="model_weights", mount_path="./my-weights")

            @function(volumes=[shared_volume])
            def my_function():
                pass
            ```
        """
        super().__init__()

        self.name = name
        self.ready = False
        self.volume_id = None
        self.mount_path = mount_path
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

        return vol


@dataclass
class CloudBucketConfig:
    """
    Configuration for a cloud bucket.

    Parameters:
        read_only (bool):
            Whether the volume is read-only.
        force_path_style (bool):
            Whether to use the force path style option while mounting the volume (some non-AWS S3 providers require this).
        access_key (str):
            The name of the secret containing the S3 access key for the external provider.
        secret_key (str):
            The name of the secret containing the S3 secret key for the external provider.
        endpoint (Optional[str]):
            The S3 endpoint for the external provider.
        region (Optional[str]):
            The region for the external provider.
    """

    read_only: bool = False
    force_path_style: bool = False
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    endpoint: Optional[str] = None
    region: Optional[str] = None


class CloudBucket(Volume):
    def __init__(self, name: str, mount_path: str, config: CloudBucketConfig) -> None:
        """
        Creates a CloudBucket instance.

        When your container runs, your cloud bucket will be available at `./{name}` and `/volumes/{name}`.

        Parameters:
            name (str):
                The name of the cloud bucket, must be the same as the bucket name in the cloud provider.
            mount_path (str):
                The path where the cloud bucket is mounted within the container environment.
            config (CloudBucketConfig):
                Configuration for the cloud bucket.

        Example:
            ```python
            from beta9 import CloudBucket, CloudBucketConfig

            # Cloud Bucket
            cloud_bucket = CloudBucket(
                name="other_model_weights",
                mount_path="./other-weights",
                config=CloudBucketConfig(
                    access_key="MY_ACCESS_KEY_SECRET",
                    secret_key="MY_SECRET_KEY_SECRET",
                    endpoint="https://s3-endpoint.com",
                ),
            )

            @function(volumes=[cloud_bucket])
            def my_function():
                pass
            ```
        """
        super().__init__(name, mount_path)
        self.config = config

    def get_or_create(self) -> bool:
        return True

    def export(self):
        vol = super().export()
        vol.config = VolumeConfigGateway(
            bucket_name=self.name,
            access_key=self.config.access_key,
            secret_key=self.config.secret_key,
            endpoint_url=self.config.endpoint,
            region=self.config.region,
            read_only=self.config.read_only,
            force_path_style=self.config.force_path_style,
        )
        return vol
