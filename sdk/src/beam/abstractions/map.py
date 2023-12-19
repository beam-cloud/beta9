from typing import Any

import cloudpickle
from grpclib.client import Channel

from beam.abstractions.base import BaseAbstraction, GatewayConfig, get_gateway_config
from beam.clients.map import (
    MapCountResponse,
    MapDeleteResponse,
    MapGetResponse,
    MapKeysResponse,
    MapServiceStub,
    MapSetResponse,
)


class Map(BaseAbstraction):
    def __init__(self, *, name: str) -> None:
        super().__init__()

        config: GatewayConfig = get_gateway_config()

        self.name: str = name
        self.channel: Channel = Channel(
            host=config.host,
            port=config.port,
            ssl=True if config.port == 443 else False,
        )
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

    def remote(self):
        raise NotImplementedError

    def __del__(self):
        self.channel.close()
