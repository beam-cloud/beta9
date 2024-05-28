from unittest import TestCase
from unittest.mock import MagicMock

from beta9.abstractions.volume import Volume
from beta9.clients.volume import GetOrCreateVolumeResponse, VolumeInstance


class TestVolumes(TestCase):
    def setUp(self):
        pass

    def test_get_or_create(self):
        mock_stub = MagicMock()

        # Test that a valid grpc response sets the volume id and ready flag
        mock_stub.get_or_create_volume = MagicMock(
            return_value=(GetOrCreateVolumeResponse(ok=True, volume=VolumeInstance(id="1234")))
        )

        volume = Volume(name="test", mount_path="/test")
        volume.stub = mock_stub

        self.assertFalse(volume.ready)
        self.assertTrue(volume.get_or_create())
        self.assertTrue(volume.ready)
        self.assertEqual(volume.volume_id, "1234")

        # Test that an invalid grpc response does not set the volume id or ready flag
        mock_stub.get_or_create_volume = MagicMock(
            return_value=(GetOrCreateVolumeResponse(ok=False, volume=VolumeInstance(id="")))
        )

        volume = Volume(name="test", mount_path="/test")
        volume.stub = mock_stub

        self.assertFalse(volume.get_or_create())
        self.assertFalse(volume.ready)
        self.assertEqual(volume.volume_id, None)
