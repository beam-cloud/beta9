# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: queue.proto
# plugin: python-betterproto
# This file has been @generated

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Dict,
    Optional,
)

import betterproto
import grpc
from betterproto.grpcstub.grpcio_client import SyncServiceStub
from betterproto.grpcstub.grpclib_server import ServiceBase


if TYPE_CHECKING:
    import grpclib.server
    from betterproto.grpcstub.grpclib_client import MetadataLike
    from grpclib.metadata import Deadline


@dataclass(eq=False, repr=False)
class SimpleQueuePutRequest(betterproto.Message):
    name: str = betterproto.string_field(1)
    value: bytes = betterproto.bytes_field(2)


@dataclass(eq=False, repr=False)
class SimpleQueuePutResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)


@dataclass(eq=False, repr=False)
class SimpleQueuePopRequest(betterproto.Message):
    name: str = betterproto.string_field(1)
    value: bytes = betterproto.bytes_field(2)


@dataclass(eq=False, repr=False)
class SimpleQueuePopResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    value: bytes = betterproto.bytes_field(2)


@dataclass(eq=False, repr=False)
class SimpleQueuePeekResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    value: bytes = betterproto.bytes_field(2)


@dataclass(eq=False, repr=False)
class SimpleQueueEmptyResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    empty: bool = betterproto.bool_field(2)


@dataclass(eq=False, repr=False)
class SimpleQueueSizeResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    size: int = betterproto.uint64_field(2)


@dataclass(eq=False, repr=False)
class SimpleQueueRequest(betterproto.Message):
    name: str = betterproto.string_field(1)


class SimpleQueueServiceStub(SyncServiceStub):
    def simple_queue_put(
        self, simple_queue_put_request: "SimpleQueuePutRequest"
    ) -> "SimpleQueuePutResponse":
        return self._unary_unary(
            "/simplequeue.SimpleQueueService/SimpleQueuePut",
            SimpleQueuePutRequest,
            SimpleQueuePutResponse,
        )(simple_queue_put_request)

    def simple_queue_pop(
        self, simple_queue_pop_request: "SimpleQueuePopRequest"
    ) -> "SimpleQueuePopResponse":
        return self._unary_unary(
            "/simplequeue.SimpleQueueService/SimpleQueuePop",
            SimpleQueuePopRequest,
            SimpleQueuePopResponse,
        )(simple_queue_pop_request)

    def simple_queue_peek(
        self, simple_queue_request: "SimpleQueueRequest"
    ) -> "SimpleQueuePeekResponse":
        return self._unary_unary(
            "/simplequeue.SimpleQueueService/SimpleQueuePeek",
            SimpleQueueRequest,
            SimpleQueuePeekResponse,
        )(simple_queue_request)

    def simple_queue_empty(
        self, simple_queue_request: "SimpleQueueRequest"
    ) -> "SimpleQueueEmptyResponse":
        return self._unary_unary(
            "/simplequeue.SimpleQueueService/SimpleQueueEmpty",
            SimpleQueueRequest,
            SimpleQueueEmptyResponse,
        )(simple_queue_request)

    def simple_queue_size(
        self, simple_queue_request: "SimpleQueueRequest"
    ) -> "SimpleQueueSizeResponse":
        return self._unary_unary(
            "/simplequeue.SimpleQueueService/SimpleQueueSize",
            SimpleQueueRequest,
            SimpleQueueSizeResponse,
        )(simple_queue_request)
