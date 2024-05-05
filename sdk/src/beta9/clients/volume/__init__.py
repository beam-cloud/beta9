# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: volume.proto
# plugin: python-betterproto
# This file has been @generated

from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    AsyncIterable,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Union,
)

import betterproto
import grpclib
from betterproto.grpc.grpclib_server import ServiceBase


if TYPE_CHECKING:
    import grpclib.server
    from betterproto.grpc.grpclib_client import MetadataLike
    from grpclib.metadata import Deadline


@dataclass(eq=False, repr=False)
class VolumeInstance(betterproto.Message):
    id: str = betterproto.string_field(1)
    name: str = betterproto.string_field(2)
    size: int = betterproto.uint64_field(3)
    created_at: datetime = betterproto.message_field(4)
    updated_at: datetime = betterproto.message_field(5)
    workspace_id: str = betterproto.string_field(6)
    workspace_name: str = betterproto.string_field(7)


@dataclass(eq=False, repr=False)
class GetOrCreateVolumeRequest(betterproto.Message):
    name: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class GetOrCreateVolumeResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    volume: "VolumeInstance" = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class PathInfo(betterproto.Message):
    path: str = betterproto.string_field(1)
    size: int = betterproto.uint64_field(2)
    mod_time: datetime = betterproto.message_field(3)
    is_dir: bool = betterproto.bool_field(4)


@dataclass(eq=False, repr=False)
class ListPathRequest(betterproto.Message):
    path: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class ListPathResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    path_infos: List["PathInfo"] = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class DeletePathRequest(betterproto.Message):
    path: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class DeletePathResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    deleted: List[str] = betterproto.string_field(3)


@dataclass(eq=False, repr=False)
class CopyPathRequest(betterproto.Message):
    path: str = betterproto.string_field(1)
    content: bytes = betterproto.bytes_field(2)


@dataclass(eq=False, repr=False)
class CopyPathResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    object_id: str = betterproto.string_field(2)
    err_msg: str = betterproto.string_field(3)


@dataclass(eq=False, repr=False)
class ListVolumesRequest(betterproto.Message):
    pass


@dataclass(eq=False, repr=False)
class ListVolumesResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    volumes: List["VolumeInstance"] = betterproto.message_field(3)


class VolumeServiceStub(betterproto.ServiceStub):
    async def get_or_create_volume(
        self,
        get_or_create_volume_request: "GetOrCreateVolumeRequest",
        *,
        timeout: Optional[float] = None,
        deadline: Optional["Deadline"] = None,
        metadata: Optional["MetadataLike"] = None
    ) -> "GetOrCreateVolumeResponse":
        return await self._unary_unary(
            "/volume.VolumeService/GetOrCreateVolume",
            get_or_create_volume_request,
            GetOrCreateVolumeResponse,
            timeout=timeout,
            deadline=deadline,
            metadata=metadata,
        )

    async def list_volumes(
        self,
        list_volumes_request: "ListVolumesRequest",
        *,
        timeout: Optional[float] = None,
        deadline: Optional["Deadline"] = None,
        metadata: Optional["MetadataLike"] = None
    ) -> "ListVolumesResponse":
        return await self._unary_unary(
            "/volume.VolumeService/ListVolumes",
            list_volumes_request,
            ListVolumesResponse,
            timeout=timeout,
            deadline=deadline,
            metadata=metadata,
        )

    async def list_path(
        self,
        list_path_request: "ListPathRequest",
        *,
        timeout: Optional[float] = None,
        deadline: Optional["Deadline"] = None,
        metadata: Optional["MetadataLike"] = None
    ) -> "ListPathResponse":
        return await self._unary_unary(
            "/volume.VolumeService/ListPath",
            list_path_request,
            ListPathResponse,
            timeout=timeout,
            deadline=deadline,
            metadata=metadata,
        )

    async def delete_path(
        self,
        delete_path_request: "DeletePathRequest",
        *,
        timeout: Optional[float] = None,
        deadline: Optional["Deadline"] = None,
        metadata: Optional["MetadataLike"] = None
    ) -> "DeletePathResponse":
        return await self._unary_unary(
            "/volume.VolumeService/DeletePath",
            delete_path_request,
            DeletePathResponse,
            timeout=timeout,
            deadline=deadline,
            metadata=metadata,
        )

    async def copy_path_stream(
        self,
        copy_path_request_iterator: Union[
            AsyncIterable["CopyPathRequest"], Iterable["CopyPathRequest"]
        ],
        *,
        timeout: Optional[float] = None,
        deadline: Optional["Deadline"] = None,
        metadata: Optional["MetadataLike"] = None
    ) -> "CopyPathResponse":
        return await self._stream_unary(
            "/volume.VolumeService/CopyPathStream",
            copy_path_request_iterator,
            CopyPathRequest,
            CopyPathResponse,
            timeout=timeout,
            deadline=deadline,
            metadata=metadata,
        )


class VolumeServiceBase(ServiceBase):

    async def get_or_create_volume(
        self, get_or_create_volume_request: "GetOrCreateVolumeRequest"
    ) -> "GetOrCreateVolumeResponse":
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def list_volumes(
        self, list_volumes_request: "ListVolumesRequest"
    ) -> "ListVolumesResponse":
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def list_path(
        self, list_path_request: "ListPathRequest"
    ) -> "ListPathResponse":
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def delete_path(
        self, delete_path_request: "DeletePathRequest"
    ) -> "DeletePathResponse":
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def copy_path_stream(
        self, copy_path_request_iterator: AsyncIterator["CopyPathRequest"]
    ) -> "CopyPathResponse":
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def __rpc_get_or_create_volume(
        self,
        stream: "grpclib.server.Stream[GetOrCreateVolumeRequest, GetOrCreateVolumeResponse]",
    ) -> None:
        request = await stream.recv_message()
        response = await self.get_or_create_volume(request)
        await stream.send_message(response)

    async def __rpc_list_volumes(
        self, stream: "grpclib.server.Stream[ListVolumesRequest, ListVolumesResponse]"
    ) -> None:
        request = await stream.recv_message()
        response = await self.list_volumes(request)
        await stream.send_message(response)

    async def __rpc_list_path(
        self, stream: "grpclib.server.Stream[ListPathRequest, ListPathResponse]"
    ) -> None:
        request = await stream.recv_message()
        response = await self.list_path(request)
        await stream.send_message(response)

    async def __rpc_delete_path(
        self, stream: "grpclib.server.Stream[DeletePathRequest, DeletePathResponse]"
    ) -> None:
        request = await stream.recv_message()
        response = await self.delete_path(request)
        await stream.send_message(response)

    async def __rpc_copy_path_stream(
        self, stream: "grpclib.server.Stream[CopyPathRequest, CopyPathResponse]"
    ) -> None:
        request = stream.__aiter__()
        response = await self.copy_path_stream(request)
        await stream.send_message(response)

    def __mapping__(self) -> Dict[str, grpclib.const.Handler]:
        return {
            "/volume.VolumeService/GetOrCreateVolume": grpclib.const.Handler(
                self.__rpc_get_or_create_volume,
                grpclib.const.Cardinality.UNARY_UNARY,
                GetOrCreateVolumeRequest,
                GetOrCreateVolumeResponse,
            ),
            "/volume.VolumeService/ListVolumes": grpclib.const.Handler(
                self.__rpc_list_volumes,
                grpclib.const.Cardinality.UNARY_UNARY,
                ListVolumesRequest,
                ListVolumesResponse,
            ),
            "/volume.VolumeService/ListPath": grpclib.const.Handler(
                self.__rpc_list_path,
                grpclib.const.Cardinality.UNARY_UNARY,
                ListPathRequest,
                ListPathResponse,
            ),
            "/volume.VolumeService/DeletePath": grpclib.const.Handler(
                self.__rpc_delete_path,
                grpclib.const.Cardinality.UNARY_UNARY,
                DeletePathRequest,
                DeletePathResponse,
            ),
            "/volume.VolumeService/CopyPathStream": grpclib.const.Handler(
                self.__rpc_copy_path_stream,
                grpclib.const.Cardinality.STREAM_UNARY,
                CopyPathRequest,
                CopyPathResponse,
            ),
        }
