from typing import Any

import cloudpickle
from beta9.abstractions.base import BaseAbstraction
from beta9.clients.simplequeue import (
    SimpleQueueEmptyResponse,
    SimpleQueuePeekResponse,
    SimpleQueuePopResponse,
    SimpleQueuePutResponse,
    SimpleQueueServiceStub,
)


class SimpleQueueInternalServerError(Exception):
    pass


class SimpleQueue(BaseAbstraction):
    """A distributed python queue."""

    def __init__(self, *, name: str, max_size=100) -> "SimpleQueue":
        """
        Creates a Queue instance.

        Use this a concurrency safe distributed queue, accessible both locally and within
        remote containers. Serialization is done using cloudpickle, so any object that supported
        by that should work here. The interface is that of a standard python queue.

        Because this is backed by a distributed queue, it will persist between runs.

        Parameters:
            name (str):
                The name of the queue (any arbitrary string).

        Example:
        ```python
        from beta9 import Queue

        val = [1, 2, 3]

        # Initialize the Queue
        q = Queue(name="myqueue")

        for i in range(100):
            # Insert something to the queue
            q.put(val)
        while not q.empty():
            # Remove something from the queue
            val = q.pop()
            print(val)
        ```
        """
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
