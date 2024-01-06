# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: taskqueue.proto
# plugin: python-betterproto
from dataclasses import dataclass

import betterproto
import grpclib


@dataclass
class TaskQueuePutRequest(betterproto.Message):
    stub_id: str = betterproto.string_field(1)
    payload: bytes = betterproto.bytes_field(2)


@dataclass
class TaskQueuePutResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    task_id: str = betterproto.string_field(2)


@dataclass
class TaskQueuePopRequest(betterproto.Message):
    stub_id: str = betterproto.string_field(1)
    container_id: str = betterproto.string_field(2)


@dataclass
class TaskQueuePopResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    task_msg: bytes = betterproto.bytes_field(2)


class TaskQueueServiceStub(betterproto.ServiceStub):
    async def task_queue_put(
        self, *, stub_id: str = "", payload: bytes = b""
    ) -> TaskQueuePutResponse:
        request = TaskQueuePutRequest()
        request.stub_id = stub_id
        request.payload = payload

        return await self._unary_unary(
            "/taskqueue.TaskQueueService/TaskQueuePut",
            request,
            TaskQueuePutResponse,
        )

    async def task_queue_pop(
        self, *, stub_id: str = "", container_id: str = ""
    ) -> TaskQueuePopResponse:
        request = TaskQueuePopRequest()
        request.stub_id = stub_id
        request.container_id = container_id

        return await self._unary_unary(
            "/taskqueue.TaskQueueService/TaskQueuePop",
            request,
            TaskQueuePopResponse,
        )
