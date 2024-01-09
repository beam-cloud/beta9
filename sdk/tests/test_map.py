from unittest import TestCase
from unittest.mock import MagicMock

import cloudpickle

from beam.abstractions.map import Map
from beam.clients.map import (
    MapCountResponse,
    MapDeleteResponse,
    MapGetResponse,
    MapKeysResponse,
    MapSetResponse,
)

from .utils import override_run_sync


class TestMap(TestCase):
    def setUp(self):
        pass

    def test_set(self):
        mock_stub = MagicMock()

        mock_stub.map_set.return_value = MapSetResponse(ok=True)

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertTrue(map.set("test", "test"))

        mock_stub.map_set.return_value = MapSetResponse(ok=False)

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertFalse(map.set("test", "test"))

    def test_get(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.map_get.return_value = MapGetResponse(ok=True, value=pickled_value)

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertEqual(map.get("test"), "test")

        mock_stub.map_get.return_value = MapGetResponse(ok=False, value=b"")

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertEqual(map.get("test"), None)

    def test_delitem(self):
        mock_stub = MagicMock()

        mock_stub.map_delete.return_value = MapDeleteResponse(ok=True)

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        del map["test"]

        mock_stub.map_delete.return_value = MapDeleteResponse(ok=False)

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        def _del():
            del map["test"]

        self.assertRaises(KeyError, _del)

    def test_len(self):
        mock_stub = MagicMock()

        mock_stub.map_count.return_value = MapCountResponse(ok=True, count=1)

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertEqual(len(map), 1)

        mock_stub.map_count.return_value = MapCountResponse(ok=False, count=1)

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertEqual(len(map), 0)

    def test_iter(self):
        mock_stub = MagicMock()

        mock_stub.map_keys.return_value = MapKeysResponse(ok=True, keys=["test"])

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertEqual(list(map), ["test"])

        mock_stub.map_keys.return_value = MapKeysResponse(ok=False, keys=[])

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertEqual(list(map), [])

    def test_items(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.map_keys.return_value = MapKeysResponse(ok=True, keys=["test"])
        mock_stub.map_get.return_value = MapGetResponse(ok=True, value=pickled_value)

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertEqual(list(map.items()), [("test", "test")])

        mock_stub.map_keys.return_value = MapKeysResponse(ok=False, keys=[])

        map = Map(name="test")
        map.stub = mock_stub
        map.run_sync = override_run_sync

        self.assertEqual(list(map.items()), [])
