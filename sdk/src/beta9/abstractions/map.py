from typing import Any

import cloudpickle
from beta9.abstractions.base import BaseAbstraction
from beta9.clients.map import (
    MapCountResponse,
    MapDeleteResponse,
    MapGetResponse,
    MapKeysResponse,
    MapServiceStub,
    MapSetResponse,
)


class Map(BaseAbstraction):
    """A distributed python dictionary."""

    def __init__(self, *, name: str) -> "Map":
        """
        Creates a Map Instance.

        Use this a concurrency safe key/value store, accessible both locally and within
        remote containers. Serialization is done using cloudpickle, so any object that supported
        by that should work here. The interface is that of a standard python dictionary.

        Because this is backed by a distributed dictionary, it will persist between runs.

        Parameters:
            name (str):
                The name of the map (any arbitrary string).

        Example:
        ```python
        from beta9 import Map

        # Name the map
        m = Map(name="test")

        # Set a key
        m["some_key"] = True

        # Delete a key
        del m["some_key"]

        # Iterate through the map
        for k, v in m.items():
            print("key: ", k)
            print("value: ", v)
        ```
        """
        super().__init__()

        self.name: str = name
        self.stub: MapServiceStub = MapServiceStub(self.channel)

    def set(self, key: str, value: Any) -> bool:
        r: MapSetResponse = self.run_sync(
            self.stub.map_set(name=self.name, key=key, value=cloudpickle.dumps(value))
        )
        return r.ok

    def get(self, key: str) -> Any:
        r: MapGetResponse = self.run_sync(self.stub.map_get(name=self.name, key=key))
        return cloudpickle.loads(r.value) if r.ok else None

    def __setitem__(self, key, value):
        self.set(key, value)

    def __getitem__(self, key):
        return self.get(key)

    def __delitem__(self, key):
        r: MapDeleteResponse = self.run_sync(self.stub.map_delete(name=self.name, key=key))
        if not r.ok:
            raise KeyError(key)

    def __len__(self):
        r: MapCountResponse = self.run_sync(self.stub.map_count(name=self.name))
        return r.count if r.ok else 0

    def __iter__(self):
        r: MapKeysResponse = self.run_sync(self.stub.map_keys(name=self.name))
        return iter(r.keys) if r.ok else iter([])

    def items(self):
        keys_response: MapKeysResponse = self.run_sync(self.stub.map_keys(name=self.name))
        if not keys_response.ok:
            return iter([])

        def _generate_items():
            for key in keys_response.keys:
                value_response: MapGetResponse = self.run_sync(
                    self.stub.map_get(name=self.name, key=key)
                )

                if value_response.ok:
                    value = cloudpickle.loads(value_response.value)
                    yield (key, value)

        return _generate_items()
