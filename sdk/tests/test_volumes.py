from unittest import TestCase
from unittest.mock import MagicMock

from beam.abstractions.volume import Volume
from beam.clients.volume import GetOrCreateVolumeResponse

from .utils import override_run_sync


class TestVolumes(TestCase):
    def setUp(self):
        pass

    def test_get_or_create(self):
        mock_stub = MagicMock()

        # Test that a valid grpc response sets the volume id and ready flag
        mock_stub.get_or_create_volume.return_value = GetOrCreateVolumeResponse(
            ok=True, volume_id="1234"
        )

        volume = Volume(name="test", mount_path="/test")
        volume.stub = mock_stub
        volume.run_sync = override_run_sync

        self.assertFalse(volume.ready)
        self.assertTrue(volume.get_or_create())
        self.assertTrue(volume.ready)
        self.assertEqual(volume.volume_id, "1234")

        # Test that an invalid grpc response does not set the volume id or ready flag
        mock_stub.get_or_create_volume.return_value = GetOrCreateVolumeResponse(
            ok=False, volume_id=""
        )

        volume = Volume(name="test", mount_path="/test")
        volume.stub = mock_stub
        volume.run_sync = override_run_sync

        self.assertFalse(volume.get_or_create())
        self.assertFalse(volume.ready)
        self.assertEqual(volume.volume_id, None)
