from beam.abstractions.base import BaseAbstraction
from beam.clients.volume import VolumeServiceStub


class Volume(BaseAbstraction):
    def __init__(self, name: str, mount_path: str) -> None:
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

    def to_dict(self):
        return {
            "id": self.volume_id,
            "mount_path": self.mount_path,
        }
