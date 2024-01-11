from typing import Any

import cloudpickle

from beam.abstractions.base import BaseAbstraction
from beam.clients.simplequeue import (
    SimpleQueueEmptyResponse,
    SimpleQueuePeekResponse,
    SimpleQueuePopResponse,
    SimpleQueuePutResponse,
    SimpleQueueServiceStub,
)


class SimpleQueueInternalServerError(Exception):
    pass


class SimpleQueue(BaseAbstraction):
    def __init__(self, *, name: str, max_size=100) -> None:
        super().__init__()

        self.name: str = name
        self.stub: SimpleQueueServiceStub = SimpleQueueServiceStub(self.channel)
        self.max_size: int = max_size

    def __len__(self):
        r = self.run_sync(self.stub.simple_queue_size(name=self.name))
        return r.size if r.ok else 0

    def __del__(self):
        self.channel.close()

    def put(self, value: Any) -> bool:
        r: SimpleQueuePutResponse = self.run_sync(
            self.stub.simple_queue_put(name=self.name, value=cloudpickle.dumps(value))
        )

        if not r.ok:
            raise SimpleQueueInternalServerError

        return True

    def pop(self) -> Any:
        r: SimpleQueuePopResponse = self.run_sync(self.stub.simple_queue_pop(name=self.name))
        if not r.ok:
            raise SimpleQueueInternalServerError

        if len(r.value) > 0:
            return cloudpickle.loads(r.value)

        return None

    def empty(self) -> bool:
        r: SimpleQueueEmptyResponse = self.run_sync(self.stub.simple_queue_empty(name=self.name))
        if not r.ok:
            raise SimpleQueueInternalServerError

        return r.empty if r.ok else True

    def peek(self) -> Any:
        r: SimpleQueuePeekResponse = self.run_sync(self.stub.simple_queue_peek(name=self.name))
        if not r.ok:
            raise SimpleQueueInternalServerError

        if len(r.value) > 0:
            return cloudpickle.loads(r.value)

        return None

    def remote(self):
        raise NotImplementedError
