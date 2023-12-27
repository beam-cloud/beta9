from typing import Any

import cloudpickle

from beam.abstractions.base import BaseAbstraction
from beam.clients.queue import SimpleQueueServiceStub
from beam.exceptions import grpc_debug


class SimpleQueueInternalServerError(Exception):
    pass


@grpc_debug()
class SimpleQueue(BaseAbstraction):
    def __init__(self, *, name: str) -> None:
        super().__init__()

        self.name: str = name
        self.stub: SimpleQueueServiceStub = SimpleQueueServiceStub(self.channel)

    def __len__(self):
        r = self.run_sync(self.stub.size(name=self.name))
        return r.size if r.ok else 0

    def __del__(self):
        self.channel.close()

    def enqueue(self, value: Any) -> bool:
        r = self.run_sync(self.stub.enqueue(name=self.name, value=cloudpickle.dumps(value)))

        if not r.ok:
            raise SimpleQueueInternalServerError

        return True

    def dequeue(self) -> Any:
        r = self.run_sync(self.stub.dequeue(name=self.name))
        if not r.ok:
            return SimpleQueueInternalServerError

        if len(r.value) > 0:
            return cloudpickle.loads(r.value)

        return None

    def empty(self) -> bool:
        r = self.run_sync(self.stub.empty(name=self.name))

        if not r.ok:
            raise SimpleQueueInternalServerError

        return r.empty if r.ok else True

    def peek(self) -> Any:
        r = self.run_sync(self.stub.peek(name=self.name))

        if not r.ok:
            raise SimpleQueueInternalServerError

        if len(r.value) > 0:
            return cloudpickle.loads(r.value)

        return None

    def remote(self):
        raise NotImplementedError
