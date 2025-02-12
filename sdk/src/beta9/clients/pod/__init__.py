# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: pod.proto
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
class RunPodRequest(betterproto.Message):
    stub_id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class RunPodResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    container_id: str = betterproto.string_field(2)


class PodServiceStub(SyncServiceStub):
    def run_pod(self, run_pod_request: "RunPodRequest") -> "RunPodResponse":
        return self._unary_unary(
            "/pod.PodService/RunPod",
            RunPodRequest,
            RunPodResponse,
        )(run_pod_request)
