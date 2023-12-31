# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: function.proto
# plugin: python-betterproto
from dataclasses import dataclass
from typing import AsyncGenerator

import betterproto
import grpclib


@dataclass
class FunctionInvokeRequest(betterproto.Message):
    object_id: str = betterproto.string_field(1)
    image_id: str = betterproto.string_field(2)
    stub_id: str = betterproto.string_field(3)
    args: bytes = betterproto.bytes_field(4)
    handler: str = betterproto.string_field(5)
    python_version: str = betterproto.string_field(6)
    cpu: int = betterproto.int64_field(7)
    memory: int = betterproto.int64_field(8)
    gpu: str = betterproto.string_field(9)


@dataclass
class FunctionInvokeResponse(betterproto.Message):
    output: str = betterproto.string_field(1)
    done: bool = betterproto.bool_field(2)
    exit_code: int = betterproto.uint32_field(3)
    result: bytes = betterproto.bytes_field(4)


@dataclass
class FunctionGetArgsRequest(betterproto.Message):
    task_id: str = betterproto.string_field(1)


@dataclass
class FunctionGetArgsResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    args: bytes = betterproto.bytes_field(2)


@dataclass
class FunctionSetResultRequest(betterproto.Message):
    task_id: str = betterproto.string_field(1)
    result: bytes = betterproto.bytes_field(2)


@dataclass
class FunctionSetResultResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)


class FunctionServiceStub(betterproto.ServiceStub):
    async def function_invoke(
        self,
        *,
        object_id: str = "",
        image_id: str = "",
        stub_id: str = "",
        args: bytes = b"",
        handler: str = "",
        python_version: str = "",
        cpu: int = 0,
        memory: int = 0,
        gpu: str = "",
    ) -> AsyncGenerator[FunctionInvokeResponse, None]:
        request = FunctionInvokeRequest()
        request.object_id = object_id
        request.image_id = image_id
        request.stub_id = stub_id
        request.args = args
        request.handler = handler
        request.python_version = python_version
        request.cpu = cpu
        request.memory = memory
        request.gpu = gpu

        async for response in self._unary_stream(
            "/function.FunctionService/FunctionInvoke",
            request,
            FunctionInvokeResponse,
        ):
            yield response

    async def function_get_args(self, *, task_id: str = "") -> FunctionGetArgsResponse:
        request = FunctionGetArgsRequest()
        request.task_id = task_id

        return await self._unary_unary(
            "/function.FunctionService/FunctionGetArgs",
            request,
            FunctionGetArgsResponse,
        )

    async def function_set_result(
        self, *, task_id: str = "", result: bytes = b""
    ) -> FunctionSetResultResponse:
        request = FunctionSetResultRequest()
        request.task_id = task_id
        request.result = result

        return await self._unary_unary(
            "/function.FunctionService/FunctionSetResult",
            request,
            FunctionSetResultResponse,
        )
