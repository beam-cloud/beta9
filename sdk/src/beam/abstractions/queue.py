from typing import Any
from typing_extensions import SupportsIndex
import cloudpickle

from grpclib.client import Channel
from beam.abstractions.base import BaseAbstraction, GatewayConfig, get_gateway_config

from beam.clients.queue import (
    SimpleQueueServiceStub
)


class SimpleQueue(BaseAbstraction):
    def __init__(self, *, name: str) -> None:
        super().__init__()
        
        config: GatewayConfig = get_gateway_config()
        self.name: str = name
        self.channel: Channel = Channel(
            host=config.host,
            port=config.port,
            ssl=True if config.port == 443 else False,
        )
        self.stub: SimpleQueueServiceStub = SimpleQueueServiceStub(self.channel)
    
    def __len__(self):
        r = self.run_sync(
            self.stub.size(name=self.name)
        )
        return r.size if r.ok else 0
    
    def __del__(self):
        self.channel.close()
    
    def enqueue(self, value: str) -> bool:
        r = self.run_sync(
            self.stub.enqueue(name=self.name, value=cloudpickle.dumps(value))
        )
        return r.ok
    
    def dequeue(self) -> str:
        r = self.run_sync(
            self.stub.dequeue(name=self.name)
        )
        if not r.ok:
            return None
        
        if len(r.value) > 0 :
            return cloudpickle.loads(r.value)

        return None

    def empty(self) -> bool:
        r = self.run_sync(
            self.stub.empty(name=self.name)
        )
        
        if not r.ok:
            raise Exception("Error checking if queue is empty")
        
        return r.empty if r.ok else True
    
    def peek(self) -> str:
        r = self.run_sync(
            self.stub.peek(name=self.name)
        )
        
        if not r.ok:
            return None
        
        if len(r.value) > 0 :
            return cloudpickle.loads(r.value)
            
        return None

    def remote(self):
        raise NotImplementedError