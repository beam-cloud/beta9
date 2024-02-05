from beta9.abstractions.base import BaseAbstraction
from beta9.clients.gateway import Volume as VolumeConfig
from beta9.clients.volume import VolumeServiceStub


class Volume(BaseAbstraction):
    def __init__(self, name: str, mount_path: str) -> "Volume":
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

        self.stub: VolumeServiceStub = VolumeServiceStub(self.channel)

    def get_or_create(self) -> bool:
        resp = self.run_sync(self.stub.get_or_create_volume(name=self.name))

        if resp.ok:
            self.ready = True
            self.volume_id = resp.volume_id
            return True

        return False

    def export(self):
        return VolumeConfig(
            id=self.volume_id,
            mount_path=self.mount_path,
        )
