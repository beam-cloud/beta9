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

from .utils import mock_coroutine_with_result


class TestMap(TestCase):
    def setUp(self):
        pass

    def test_set(self):
        mock_stub = MagicMock()

        mock_stub.map_set = mock_coroutine_with_result(MapSetResponse(ok=True))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertTrue(map.set("test", "test"))

        mock_stub.map_set = mock_coroutine_with_result(MapSetResponse(ok=False))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertFalse(map.set("test", "test"))

    def test_get(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.map_get = mock_coroutine_with_result(MapGetResponse(ok=True, value=pickled_value))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertEqual(map.get("test"), "test")

        mock_stub.map_get = mock_coroutine_with_result(MapGetResponse(ok=False, value=b""))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertEqual(map.get("test"), None)

    def test_delitem(self):
        mock_stub = MagicMock()

        mock_stub.map_delete = mock_coroutine_with_result(MapDeleteResponse(ok=True))

        map = Map(name="test")
        map.stub = mock_stub

        del map["test"]

        mock_stub.map_delete = mock_coroutine_with_result(MapDeleteResponse(ok=False))

        map = Map(name="test")
        map.stub = mock_stub

        def _del():
            del map["test"]

        self.assertRaises(KeyError, _del)

    def test_len(self):
        mock_stub = MagicMock()

        mock_stub.map_count = mock_coroutine_with_result(MapCountResponse(ok=True, count=1))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertEqual(len(map), 1)

        mock_stub.map_count = mock_coroutine_with_result(MapCountResponse(ok=False, count=1))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertEqual(len(map), 0)

    def test_iter(self):
        mock_stub = MagicMock()

        mock_stub.map_keys = mock_coroutine_with_result(MapKeysResponse(ok=True, keys=["test"]))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertEqual(list(map), ["test"])

        mock_stub.map_keys = mock_coroutine_with_result(MapKeysResponse(ok=False, keys=[]))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertEqual(list(map), [])

    def test_items(self):
        mock_stub = MagicMock()

        pickled_value = cloudpickle.dumps("test")

        mock_stub.map_keys = mock_coroutine_with_result(MapKeysResponse(ok=True, keys=["test"]))
        mock_stub.map_get = mock_coroutine_with_result(MapGetResponse(ok=True, value=pickled_value))

        map = Map(name="test")
        map.stub = mock_stub
        self.assertListEqual(list(map.items()), [("test", "test")])

        mock_stub.map_keys = mock_coroutine_with_result(MapKeysResponse(ok=False, keys=[]))

        map = Map(name="test")
        map.stub = mock_stub

        self.assertEqual(list(map.items()), [])
