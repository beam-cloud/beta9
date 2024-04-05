# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: function.proto
# plugin: python-betterproto
from dataclasses import dataclass
from typing import AsyncGenerator

import betterproto
import grpclib


@dataclass
class FunctionInvokeRequest(betterproto.Message):
    stub_id: str = betterproto.string_field(1)
    args: bytes = betterproto.bytes_field(2)


@dataclass
class FunctionInvokeResponse(betterproto.Message):
    task_id: str = betterproto.string_field(1)
    output: str = betterproto.string_field(2)
    done: bool = betterproto.bool_field(3)
    exit_code: int = betterproto.int32_field(4)
    result: bytes = betterproto.bytes_field(5)


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
    result: bytes = betterproto.bytes_field(3)


@dataclass
class FunctionSetResultResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)


class FunctionServiceStub(betterproto.ServiceStub):
    async def function_invoke(
        self, *, stub_id: str = "", args: bytes = b""
    ) -> AsyncGenerator[FunctionInvokeResponse, None]:
        request = FunctionInvokeRequest()
        request.stub_id = stub_id
        request.args = args

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
