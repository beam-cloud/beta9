from typing import Optional

from ..abstractions.base import BaseAbstraction
from ..clients.gateway import Volume as VolumeConfig
from ..clients.volume import GetOrCreateVolumeRequest, GetOrCreateVolumeResponse, VolumeServiceStub


class Volume(BaseAbstraction):
    def __init__(self, name: str, mount_path: str) -> None:
        """
        Creates a Volume instance.

        When your container runs, your volume will be available at `./{name}` and `/volumes/{name}`.

        Parameters:
            name (str):
                The name of the volume, a descriptive identifier for the data volume.
            mount_path (str):
                The path where the volume is mounted within the container environment.

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
        return VolumeConfig(
            id=self.volume_id,
            mount_path=self.mount_path,
        )
