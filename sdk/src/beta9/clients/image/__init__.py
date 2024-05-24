# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: image.proto
# plugin: python-betterproto
# This file has been @generated

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Dict,
    Iterator,
    List,
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
class VerifyImageBuildRequest(betterproto.Message):
    python_version: str = betterproto.string_field(1)
    python_packages: List[str] = betterproto.string_field(2)
    commands: List[str] = betterproto.string_field(3)
    force_rebuild: bool = betterproto.bool_field(4)
    existing_image_uri: str = betterproto.string_field(5)


@dataclass(eq=False, repr=False)
class VerifyImageBuildResponse(betterproto.Message):
    image_id: str = betterproto.string_field(1)
    valid: bool = betterproto.bool_field(2)
    exists: bool = betterproto.bool_field(3)


@dataclass(eq=False, repr=False)
class BuildImageRequest(betterproto.Message):
    python_version: str = betterproto.string_field(1)
    """These parameters are used for a "beta9" managed image"""

    python_packages: List[str] = betterproto.string_field(2)
    commands: List[str] = betterproto.string_field(3)
    existing_image_uri: str = betterproto.string_field(4)
    """These parameters are used for an existing image"""

    existing_image_creds: str = betterproto.string_field(5)


@dataclass(eq=False, repr=False)
class BuildImageResponse(betterproto.Message):
    image_id: str = betterproto.string_field(1)
    msg: str = betterproto.string_field(2)
    done: bool = betterproto.bool_field(3)
    success: bool = betterproto.bool_field(4)


class ImageServiceStub(SyncServiceStub):
    def verify_image_build(
        self, verify_image_build_request: "VerifyImageBuildRequest"
    ) -> "VerifyImageBuildResponse":
        return self._unary_unary(
            "/image.ImageService/VerifyImageBuild",
            VerifyImageBuildRequest,
            VerifyImageBuildResponse,
        )(verify_image_build_request)

    def build_image(
        self, build_image_request: "BuildImageRequest"
    ) -> Iterator["BuildImageResponse"]:
        for response in self._unary_stream(
            "/image.ImageService/BuildImage",
            BuildImageRequest,
            BuildImageResponse,
        )(build_image_request):
            yield response
