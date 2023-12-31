# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: gateway.proto
# plugin: python-betterproto
from dataclasses import dataclass
from typing import AsyncGenerator, Optional

import betterproto
import grpclib


@dataclass
class AuthorizeRequest(betterproto.Message):
    pass


@dataclass
class AuthorizeResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    context_id: str = betterproto.string_field(2)
    new_token: str = betterproto.string_field(3)
    error_msg: str = betterproto.string_field(4)


@dataclass
class ObjectMetadata(betterproto.Message):
    name: str = betterproto.string_field(1)
    size: int = betterproto.int64_field(2)


@dataclass
class HeadObjectRequest(betterproto.Message):
    hash: str = betterproto.string_field(1)


@dataclass
class HeadObjectResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    exists: bool = betterproto.bool_field(2)
    object_id: str = betterproto.string_field(3)
    object_metadata: "ObjectMetadata" = betterproto.message_field(4)
    error_msg: str = betterproto.string_field(5)


@dataclass
class PutObjectRequest(betterproto.Message):
    object_content: bytes = betterproto.bytes_field(1)
    object_metadata: "ObjectMetadata" = betterproto.message_field(2)
    hash: str = betterproto.string_field(3)
    overwrite: bool = betterproto.bool_field(4)


@dataclass
class PutObjectResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    object_id: str = betterproto.string_field(2)
    error_msg: str = betterproto.string_field(3)


@dataclass
class GetNextTaskRequest(betterproto.Message):
    """Task queue messages"""

    queue_name: str = betterproto.string_field(1)
    container_id: str = betterproto.string_field(2)
    s2s_token: str = betterproto.string_field(3)


@dataclass
class GetNextTaskResponse(betterproto.Message):
    task: bytes = betterproto.bytes_field(1)
    task_available: bool = betterproto.bool_field(2)


@dataclass
class GetTaskStreamRequest(betterproto.Message):
    queue_name: str = betterproto.string_field(1)
    container_id: str = betterproto.string_field(2)
    s2s_token: str = betterproto.string_field(3)


@dataclass
class TaskStreamResponse(betterproto.Message):
    task: bytes = betterproto.bytes_field(1)


@dataclass
class StartTaskRequest(betterproto.Message):
    task_id: str = betterproto.string_field(1)
    container_id: str = betterproto.string_field(2)


@dataclass
class StartTaskResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)


@dataclass
class EndTaskRequest(betterproto.Message):
    task_id: str = betterproto.string_field(1)
    task_duration: float = betterproto.float_field(2)
    task_status: str = betterproto.string_field(3)
    container_id: str = betterproto.string_field(4)
    container_hostname: str = betterproto.string_field(5)
    scale_down_delay: float = betterproto.float_field(6)


@dataclass
class EndTaskResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)


@dataclass
class MonitorTaskRequest(betterproto.Message):
    task_id: str = betterproto.string_field(1)
    queue_name: str = betterproto.string_field(2)
    container_id: str = betterproto.string_field(3)
    s2s_token: str = betterproto.string_field(4)


@dataclass
class MonitorTaskResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    canceled: bool = betterproto.bool_field(2)
    complete: bool = betterproto.bool_field(3)
    timed_out: bool = betterproto.bool_field(4)


@dataclass
class GetOrCreateStubRequest(betterproto.Message):
    object_id: str = betterproto.string_field(1)
    image_id: str = betterproto.string_field(2)
    stub_type: str = betterproto.string_field(3)
    name: str = betterproto.string_field(4)
    python_version: str = betterproto.string_field(5)
    cpu: int = betterproto.int64_field(6)
    memory: int = betterproto.int64_field(7)
    gpu: str = betterproto.string_field(8)


@dataclass
class GetOrCreateStubResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    stub_id: str = betterproto.string_field(2)


class GatewayServiceStub(betterproto.ServiceStub):
    async def authorize(self) -> AuthorizeResponse:
        request = AuthorizeRequest()

        return await self._unary_unary(
            "/gateway.GatewayService/Authorize",
            request,
            AuthorizeResponse,
        )

    async def head_object(self, *, hash: str = "") -> HeadObjectResponse:
        request = HeadObjectRequest()
        request.hash = hash

        return await self._unary_unary(
            "/gateway.GatewayService/HeadObject",
            request,
            HeadObjectResponse,
        )

    async def put_object(
        self,
        *,
        object_content: bytes = b"",
        object_metadata: Optional["ObjectMetadata"] = None,
        hash: str = "",
        overwrite: bool = False,
    ) -> PutObjectResponse:
        request = PutObjectRequest()
        request.object_content = object_content
        if object_metadata is not None:
            request.object_metadata = object_metadata
        request.hash = hash
        request.overwrite = overwrite

        return await self._unary_unary(
            "/gateway.GatewayService/PutObject",
            request,
            PutObjectResponse,
        )

    async def get_task_stream(
        self, *, queue_name: str = "", container_id: str = "", s2s_token: str = ""
    ) -> AsyncGenerator[TaskStreamResponse, None]:
        request = GetTaskStreamRequest()
        request.queue_name = queue_name
        request.container_id = container_id
        request.s2s_token = s2s_token

        async for response in self._unary_stream(
            "/gateway.GatewayService/GetTaskStream",
            request,
            TaskStreamResponse,
        ):
            yield response

    async def get_next_task(
        self, *, queue_name: str = "", container_id: str = "", s2s_token: str = ""
    ) -> GetNextTaskResponse:
        request = GetNextTaskRequest()
        request.queue_name = queue_name
        request.container_id = container_id
        request.s2s_token = s2s_token

        return await self._unary_unary(
            "/gateway.GatewayService/GetNextTask",
            request,
            GetNextTaskResponse,
        )

    async def start_task(
        self, *, task_id: str = "", container_id: str = ""
    ) -> StartTaskResponse:
        request = StartTaskRequest()
        request.task_id = task_id
        request.container_id = container_id

        return await self._unary_unary(
            "/gateway.GatewayService/StartTask",
            request,
            StartTaskResponse,
        )

    async def end_task(
        self,
        *,
        task_id: str = "",
        task_duration: float = 0,
        task_status: str = "",
        container_id: str = "",
        container_hostname: str = "",
        scale_down_delay: float = 0,
    ) -> EndTaskResponse:
        request = EndTaskRequest()
        request.task_id = task_id
        request.task_duration = task_duration
        request.task_status = task_status
        request.container_id = container_id
        request.container_hostname = container_hostname
        request.scale_down_delay = scale_down_delay

        return await self._unary_unary(
            "/gateway.GatewayService/EndTask",
            request,
            EndTaskResponse,
        )

    async def monitor_task(
        self,
        *,
        task_id: str = "",
        queue_name: str = "",
        container_id: str = "",
        s2s_token: str = "",
    ) -> AsyncGenerator[MonitorTaskResponse, None]:
        request = MonitorTaskRequest()
        request.task_id = task_id
        request.queue_name = queue_name
        request.container_id = container_id
        request.s2s_token = s2s_token

        async for response in self._unary_stream(
            "/gateway.GatewayService/MonitorTask",
            request,
            MonitorTaskResponse,
        ):
            yield response

    async def get_or_create_stub(
        self,
        *,
        object_id: str = "",
        image_id: str = "",
        stub_type: str = "",
        name: str = "",
        python_version: str = "",
        cpu: int = 0,
        memory: int = 0,
        gpu: str = "",
    ) -> GetOrCreateStubResponse:
        request = GetOrCreateStubRequest()
        request.object_id = object_id
        request.image_id = image_id
        request.stub_type = stub_type
        request.name = name
        request.python_version = python_version
        request.cpu = cpu
        request.memory = memory
        request.gpu = gpu

        return await self._unary_unary(
            "/gateway.GatewayService/GetOrCreateStub",
            request,
            GetOrCreateStubResponse,
        )
