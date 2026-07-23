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

    def test_volume_sub_path_and_limits(self):
        # 1. Test parsing and initialization
        volume = Volume(
            name="test-vol",
            mount_path="/app/data",
            sub_path="/users/user_123",
            max_storage="5GiB",
        )
        self.assertEqual(volume.sub_path, "/users/user_123")
        self.assertEqual(volume.max_storage_bytes, 5368709120)

        # 2. Test export method
        volume.volume_id = "vol-id-123"
        exported = volume.export()
        self.assertEqual(exported.id, "vol-id-123")
        self.assertEqual(exported.mount_path, "/app/data")
        self.assertEqual(exported.sub_path, "/users/user_123")
        self.assertEqual(exported.max_storage_bytes, 5368709120)

    def test_parse_storage_to_bytes_valid_formats(self):
        from beta9.abstractions.volume import parse_storage_to_bytes
        self.assertEqual(parse_storage_to_bytes(1024), 1024)
        self.assertEqual(parse_storage_to_bytes("5GiB"), 5 * 1024 * 1024 * 1024)
        self.assertEqual(parse_storage_to_bytes("100MB"), 100 * 1000 * 1000)
        self.assertEqual(parse_storage_to_bytes("10GB"), 10 * 1000 * 1000 * 1000)
        self.assertEqual(parse_storage_to_bytes("100"), 100)
        self.assertIsNone(parse_storage_to_bytes(None))

    def test_parse_storage_to_bytes_invalid_formats(self):
        from beta9.abstractions.volume import parse_storage_to_bytes
        with self.assertRaises(ValueError):
            parse_storage_to_bytes("invalid")
        with self.assertRaises(ValueError):
            parse_storage_to_bytes("5XYZ")
        # Floating point or other non-string types are passed through as-is, matching RAM/parse_memory
        self.assertEqual(parse_storage_to_bytes(1.23), 1.23)

