from typing import Any, Optional

import cloudpickle

from ..abstractions.base import BaseAbstraction
from ..clients.simplequeue import (
    SimpleQueueEmptyResponse,
    SimpleQueuePeekResponse,
    SimpleQueuePopRequest,
    SimpleQueuePopResponse,
    SimpleQueuePutRequest,
    SimpleQueuePutResponse,
    SimpleQueueRequest,
    SimpleQueueServiceStub,
)


class SimpleQueueInternalServerError(Exception):
    pass


class SimpleQueue(BaseAbstraction):
    """A distributed python queue."""

    def __init__(self, *, name: str, max_size=100) -> None:
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
        self._stub: Optional[SimpleQueueServiceStub] = None
        self.max_size: int = max_size

    @property
    def stub(self) -> SimpleQueueServiceStub:
        if not self._stub:
            self._stub = SimpleQueueServiceStub(self.channel)
        return self._stub

    def __len__(self):
        r = self.stub.simple_queue_size(SimpleQueueRequest(name=self.name))
        return r.size if r.ok else 0

    def put(self, value: Any) -> bool:
        r: SimpleQueuePutResponse = self.stub.simple_queue_put(
            SimpleQueuePutRequest(name=self.name, value=cloudpickle.dumps(value))
        )

        if not r.ok:
            raise SimpleQueueInternalServerError

        return True

    def pop(self) -> Any:
        r: SimpleQueuePopResponse = self.stub.simple_queue_pop(
            SimpleQueuePopRequest(name=self.name)
        )

        if not r.ok:
            raise SimpleQueueInternalServerError

        if len(r.value) > 0:
            return cloudpickle.loads(r.value)

        return None

    def empty(self) -> bool:
        r: SimpleQueueEmptyResponse = self.stub.simple_queue_empty(
            SimpleQueueRequest(name=self.name)
        )

        if not r.ok:
            raise SimpleQueueInternalServerError

        return r.empty if r.ok else True

    def peek(self) -> Any:
        r: SimpleQueuePeekResponse = self.stub.simple_queue_peek(SimpleQueueRequest(name=self.name))

        if not r.ok:
            raise SimpleQueueInternalServerError

        if len(r.value) > 0:
            return cloudpickle.loads(r.value)

        return None

    def remote(self):
        raise NotImplementedError
