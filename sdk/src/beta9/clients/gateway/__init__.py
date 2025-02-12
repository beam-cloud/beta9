# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: gateway.proto
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
    Iterator,
    List,
    Optional,
    Union,
)

import betterproto
import grpc
from betterproto.grpcstub.grpcio_client import SyncServiceStub
from betterproto.grpcstub.grpclib_server import ServiceBase

from .. import types as _types__


if TYPE_CHECKING:
    import grpclib.server
    from betterproto.grpcstub.grpclib_client import MetadataLike
    from grpclib.metadata import Deadline


class ReplaceObjectContentOperation(betterproto.Enum):
    WRITE = 0
    DELETE = 1
    MOVED = 2


@dataclass(eq=False, repr=False)
class AuthorizeRequest(betterproto.Message):
    pass


@dataclass(eq=False, repr=False)
class AuthorizeResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    workspace_id: str = betterproto.string_field(2)
    new_token: str = betterproto.string_field(3)
    error_msg: str = betterproto.string_field(4)


@dataclass(eq=False, repr=False)
class SignPayloadRequest(betterproto.Message):
    payload: bytes = betterproto.bytes_field(1)


@dataclass(eq=False, repr=False)
class SignPayloadResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    signature: str = betterproto.string_field(2)
    timestamp: int = betterproto.int64_field(3)
    error_msg: str = betterproto.string_field(4)


@dataclass(eq=False, repr=False)
class ObjectMetadata(betterproto.Message):
    name: str = betterproto.string_field(1)
    size: int = betterproto.int64_field(2)


@dataclass(eq=False, repr=False)
class HeadObjectRequest(betterproto.Message):
    hash: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class HeadObjectResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    exists: bool = betterproto.bool_field(2)
    object_id: str = betterproto.string_field(3)
    object_metadata: "ObjectMetadata" = betterproto.message_field(4)
    error_msg: str = betterproto.string_field(5)


@dataclass(eq=False, repr=False)
class PutObjectRequest(betterproto.Message):
    object_content: bytes = betterproto.bytes_field(1)
    object_metadata: "ObjectMetadata" = betterproto.message_field(2)
    hash: str = betterproto.string_field(3)
    overwrite: bool = betterproto.bool_field(4)


@dataclass(eq=False, repr=False)
class PutObjectResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    object_id: str = betterproto.string_field(2)
    error_msg: str = betterproto.string_field(3)


@dataclass(eq=False, repr=False)
class ReplaceObjectContentRequest(betterproto.Message):
    object_id: str = betterproto.string_field(1)
    path: str = betterproto.string_field(2)
    new_path: str = betterproto.string_field(3)
    is_dir: bool = betterproto.bool_field(4)
    data: bytes = betterproto.bytes_field(5)
    op: "ReplaceObjectContentOperation" = betterproto.enum_field(6)


@dataclass(eq=False, repr=False)
class ReplaceObjectContentResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)


@dataclass(eq=False, repr=False)
class ListContainersRequest(betterproto.Message):
    pass


@dataclass(eq=False, repr=False)
class ListContainersResponse(betterproto.Message):
    containers: List["_types__.Container"] = betterproto.message_field(1)
    ok: bool = betterproto.bool_field(2)
    error_msg: str = betterproto.string_field(3)


@dataclass(eq=False, repr=False)
class StopContainerRequest(betterproto.Message):
    container_id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class StopContainerResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    error_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class StartTaskRequest(betterproto.Message):
    """Task messages"""

    task_id: str = betterproto.string_field(1)
    container_id: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class StartTaskResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)


@dataclass(eq=False, repr=False)
class EndTaskRequest(betterproto.Message):
    task_id: str = betterproto.string_field(1)
    task_duration: float = betterproto.float_field(2)
    task_status: str = betterproto.string_field(3)
    container_id: str = betterproto.string_field(4)
    container_hostname: str = betterproto.string_field(5)
    keep_warm_seconds: float = betterproto.float_field(6)


@dataclass(eq=False, repr=False)
class EndTaskResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)


@dataclass(eq=False, repr=False)
class StringList(betterproto.Message):
    values: List[str] = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class ListTasksRequest(betterproto.Message):
    filters: Dict[str, "StringList"] = betterproto.map_field(
        1, betterproto.TYPE_STRING, betterproto.TYPE_MESSAGE
    )
    limit: int = betterproto.uint32_field(2)


@dataclass(eq=False, repr=False)
class Task(betterproto.Message):
    id: str = betterproto.string_field(2)
    status: str = betterproto.string_field(3)
    container_id: str = betterproto.string_field(4)
    started_at: datetime = betterproto.message_field(5)
    ended_at: datetime = betterproto.message_field(6)
    stub_id: str = betterproto.string_field(7)
    stub_name: str = betterproto.string_field(8)
    workspace_id: str = betterproto.string_field(9)
    workspace_name: str = betterproto.string_field(10)
    created_at: datetime = betterproto.message_field(11)
    updated_at: datetime = betterproto.message_field(12)


@dataclass(eq=False, repr=False)
class ListTasksResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    tasks: List["Task"] = betterproto.message_field(3)
    total: int = betterproto.int32_field(4)


@dataclass(eq=False, repr=False)
class StopTasksRequest(betterproto.Message):
    task_ids: List[str] = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class StopTasksResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class Volume(betterproto.Message):
    id: str = betterproto.string_field(1)
    mount_path: str = betterproto.string_field(2)
    config: Optional["_types__.MountPointConfig"] = betterproto.message_field(
        3, optional=True
    )


@dataclass(eq=False, repr=False)
class SecretVar(betterproto.Message):
    name: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class Autoscaler(betterproto.Message):
    type: str = betterproto.string_field(1)
    max_containers: int = betterproto.uint32_field(2)
    tasks_per_container: int = betterproto.uint32_field(3)
    min_containers: int = betterproto.uint32_field(4)


@dataclass(eq=False, repr=False)
class TaskPolicy(betterproto.Message):
    timeout: int = betterproto.int64_field(1)
    max_retries: int = betterproto.uint32_field(2)
    ttl: int = betterproto.uint32_field(3)


@dataclass(eq=False, repr=False)
class GetOrCreateStubRequest(betterproto.Message):
    object_id: str = betterproto.string_field(1)
    image_id: str = betterproto.string_field(2)
    stub_type: str = betterproto.string_field(3)
    name: str = betterproto.string_field(4)
    python_version: str = betterproto.string_field(5)
    cpu: int = betterproto.int64_field(6)
    memory: int = betterproto.int64_field(7)
    gpu: str = betterproto.string_field(8)
    handler: str = betterproto.string_field(9)
    retries: int = betterproto.uint32_field(10)
    timeout: int = betterproto.int64_field(11)
    keep_warm_seconds: float = betterproto.float_field(12)
    workers: int = betterproto.uint32_field(13)
    max_pending_tasks: int = betterproto.uint32_field(15)
    volumes: List["Volume"] = betterproto.message_field(16)
    force_create: bool = betterproto.bool_field(17)
    on_start: str = betterproto.string_field(18)
    callback_url: str = betterproto.string_field(19)
    authorized: bool = betterproto.bool_field(20)
    secrets: List["SecretVar"] = betterproto.message_field(21)
    autoscaler: "Autoscaler" = betterproto.message_field(22)
    task_policy: "TaskPolicy" = betterproto.message_field(23)
    concurrent_requests: int = betterproto.uint32_field(24)
    extra: str = betterproto.string_field(25)
    checkpoint_enabled: bool = betterproto.bool_field(26)
    gpu_count: int = betterproto.uint32_field(27)
    on_deploy: str = betterproto.string_field(28)
    on_deploy_stub_id: str = betterproto.string_field(29)
    entrypoint: List[str] = betterproto.string_field(30)


@dataclass(eq=False, repr=False)
class GetOrCreateStubResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    stub_id: str = betterproto.string_field(2)
    err_msg: str = betterproto.string_field(3)
    warn_msg: str = betterproto.string_field(4)


@dataclass(eq=False, repr=False)
class DeployStubRequest(betterproto.Message):
    stub_id: str = betterproto.string_field(1)
    name: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class DeployStubResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    deployment_id: str = betterproto.string_field(2)
    version: int = betterproto.uint32_field(3)
    invoke_url: str = betterproto.string_field(4)


@dataclass(eq=False, repr=False)
class Deployment(betterproto.Message):
    id: str = betterproto.string_field(1)
    name: str = betterproto.string_field(2)
    active: bool = betterproto.bool_field(3)
    stub_id: str = betterproto.string_field(4)
    stub_type: str = betterproto.string_field(5)
    stub_name: str = betterproto.string_field(6)
    version: int = betterproto.uint32_field(7)
    workspace_id: str = betterproto.string_field(8)
    workspace_name: str = betterproto.string_field(9)
    created_at: datetime = betterproto.message_field(10)
    updated_at: datetime = betterproto.message_field(11)


@dataclass(eq=False, repr=False)
class ListDeploymentsRequest(betterproto.Message):
    filters: Dict[str, "StringList"] = betterproto.map_field(
        1, betterproto.TYPE_STRING, betterproto.TYPE_MESSAGE
    )
    limit: int = betterproto.uint32_field(2)


@dataclass(eq=False, repr=False)
class ListDeploymentsResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    deployments: List["Deployment"] = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class StopDeploymentRequest(betterproto.Message):
    id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class StopDeploymentResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class StartDeploymentRequest(betterproto.Message):
    id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class StartDeploymentResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class DeleteDeploymentRequest(betterproto.Message):
    id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class DeleteDeploymentResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class Pool(betterproto.Message):
    name: str = betterproto.string_field(2)
    active: bool = betterproto.bool_field(3)
    gpu: str = betterproto.string_field(4)
    min_free_gpu: str = betterproto.string_field(5)
    min_free_cpu: str = betterproto.string_field(6)
    min_free_memory: str = betterproto.string_field(7)
    default_worker_cpu: str = betterproto.string_field(8)
    default_worker_memory: str = betterproto.string_field(9)
    default_worker_gpu_count: str = betterproto.string_field(10)
    state: "_types__.WorkerPoolState" = betterproto.message_field(11)


@dataclass(eq=False, repr=False)
class ListPoolsRequest(betterproto.Message):
    filters: Dict[str, "StringList"] = betterproto.map_field(
        1, betterproto.TYPE_STRING, betterproto.TYPE_MESSAGE
    )
    limit: int = betterproto.uint32_field(2)


@dataclass(eq=False, repr=False)
class ListPoolsResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    pools: List["Pool"] = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class Machine(betterproto.Message):
    id: str = betterproto.string_field(1)
    cpu: int = betterproto.int64_field(2)
    memory: int = betterproto.int64_field(3)
    gpu: str = betterproto.string_field(4)
    gpu_count: int = betterproto.uint32_field(5)
    status: str = betterproto.string_field(6)
    pool_name: str = betterproto.string_field(7)
    provider_name: str = betterproto.string_field(8)
    registration_token: str = betterproto.string_field(9)
    tailscale_url: str = betterproto.string_field(10)
    tailscale_auth: str = betterproto.string_field(11)
    last_keepalive: str = betterproto.string_field(12)
    created: str = betterproto.string_field(13)
    agent_version: str = betterproto.string_field(14)
    machine_metrics: "MachineMetrics" = betterproto.message_field(15)
    user_data: str = betterproto.string_field(16)


@dataclass(eq=False, repr=False)
class MachineMetrics(betterproto.Message):
    total_cpu_available: int = betterproto.int32_field(1)
    total_memory_available: int = betterproto.int32_field(2)
    cpu_utilization_pct: float = betterproto.float_field(3)
    memory_utilization_pct: float = betterproto.float_field(4)
    worker_count: int = betterproto.int32_field(5)
    container_count: int = betterproto.int32_field(6)
    free_gpu_count: int = betterproto.int32_field(7)
    cache_usage_pct: float = betterproto.float_field(8)
    cache_capacity: int = betterproto.int32_field(9)
    cache_memory_usage: int = betterproto.int32_field(10)
    cache_cpu_usage: float = betterproto.float_field(11)


@dataclass(eq=False, repr=False)
class ListMachinesRequest(betterproto.Message):
    pool_name: str = betterproto.string_field(1)
    limit: int = betterproto.uint32_field(2)


@dataclass(eq=False, repr=False)
class ListMachinesResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    machines: List["Machine"] = betterproto.message_field(3)
    gpus: Dict[str, bool] = betterproto.map_field(
        4, betterproto.TYPE_STRING, betterproto.TYPE_BOOL
    )


@dataclass(eq=False, repr=False)
class CreateMachineRequest(betterproto.Message):
    pool_name: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class CreateMachineResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    machine: "Machine" = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class DeleteMachineRequest(betterproto.Message):
    machine_id: str = betterproto.string_field(1)
    pool_name: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class DeleteMachineResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class Token(betterproto.Message):
    token_id: str = betterproto.string_field(1)
    key: str = betterproto.string_field(2)
    active: bool = betterproto.bool_field(3)
    reusable: bool = betterproto.bool_field(4)
    workspace_id: Optional[int] = betterproto.uint32_field(5, optional=True)
    token_type: str = betterproto.string_field(6)
    created_at: datetime = betterproto.message_field(7)
    updated_at: datetime = betterproto.message_field(8)


@dataclass(eq=False, repr=False)
class ListTokensRequest(betterproto.Message):
    pass


@dataclass(eq=False, repr=False)
class ListTokensResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    tokens: List["Token"] = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class CreateTokenRequest(betterproto.Message):
    pass


@dataclass(eq=False, repr=False)
class CreateTokenResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    token: "Token" = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class ToggleTokenRequest(betterproto.Message):
    token_id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class ToggleTokenResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    token: "Token" = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class DeleteTokenRequest(betterproto.Message):
    token_id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class DeleteTokenResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class GetUrlRequest(betterproto.Message):
    stub_id: str = betterproto.string_field(1)
    deployment_id: str = betterproto.string_field(2)
    url_type: str = betterproto.string_field(3)


@dataclass(eq=False, repr=False)
class GetUrlResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    url: str = betterproto.string_field(3)


@dataclass(eq=False, repr=False)
class ListWorkersRequest(betterproto.Message):
    pass


@dataclass(eq=False, repr=False)
class ListWorkersResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)
    workers: List["_types__.Worker"] = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class CordonWorkerRequest(betterproto.Message):
    worker_id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class CordonWorkerResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class UncordonWorkerRequest(betterproto.Message):
    worker_id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class UncordonWorkerResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class DrainWorkerRequest(betterproto.Message):
    worker_id: str = betterproto.string_field(1)


@dataclass(eq=False, repr=False)
class DrainWorkerResponse(betterproto.Message):
    ok: bool = betterproto.bool_field(1)
    err_msg: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class ExportWorkspaceConfigRequest(betterproto.Message):
    pass


@dataclass(eq=False, repr=False)
class ExportWorkspaceConfigResponse(betterproto.Message):
    gateway_http_url: str = betterproto.string_field(1)
    gateway_http_port: int = betterproto.int32_field(2)
    gateway_grpc_url: str = betterproto.string_field(3)
    gateway_grpc_port: int = betterproto.int32_field(4)
    workspace_id: str = betterproto.string_field(5)


class GatewayServiceStub(SyncServiceStub):
    def authorize(self, authorize_request: "AuthorizeRequest") -> "AuthorizeResponse":
        return self._unary_unary(
            "/gateway.GatewayService/Authorize",
            AuthorizeRequest,
            AuthorizeResponse,
        )(authorize_request)

    def sign_payload(
        self, sign_payload_request: "SignPayloadRequest"
    ) -> "SignPayloadResponse":
        return self._unary_unary(
            "/gateway.GatewayService/SignPayload",
            SignPayloadRequest,
            SignPayloadResponse,
        )(sign_payload_request)

    def head_object(
        self, head_object_request: "HeadObjectRequest"
    ) -> "HeadObjectResponse":
        return self._unary_unary(
            "/gateway.GatewayService/HeadObject",
            HeadObjectRequest,
            HeadObjectResponse,
        )(head_object_request)

    def put_object(self, put_object_request: "PutObjectRequest") -> "PutObjectResponse":
        return self._unary_unary(
            "/gateway.GatewayService/PutObject",
            PutObjectRequest,
            PutObjectResponse,
        )(put_object_request)

    def put_object_stream(
        self, put_object_request_iterator: Iterable["PutObjectRequest"]
    ) -> "PutObjectResponse":
        return (
            self._stream_unary(
                "/gateway.GatewayService/PutObjectStream",
                PutObjectRequest,
                PutObjectResponse,
            )
            .future(put_object_request_iterator)
            .result()
        )

    def replace_object_content(
        self, replace_object_content_request: "ReplaceObjectContentRequest"
    ) -> "ReplaceObjectContentResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ReplaceObjectContent",
            ReplaceObjectContentRequest,
            ReplaceObjectContentResponse,
        )(replace_object_content_request)

    def list_containers(
        self, list_containers_request: "ListContainersRequest"
    ) -> "ListContainersResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ListContainers",
            ListContainersRequest,
            ListContainersResponse,
        )(list_containers_request)

    def stop_container(
        self, stop_container_request: "StopContainerRequest"
    ) -> "StopContainerResponse":
        return self._unary_unary(
            "/gateway.GatewayService/StopContainer",
            StopContainerRequest,
            StopContainerResponse,
        )(stop_container_request)

    def start_task(self, start_task_request: "StartTaskRequest") -> "StartTaskResponse":
        return self._unary_unary(
            "/gateway.GatewayService/StartTask",
            StartTaskRequest,
            StartTaskResponse,
        )(start_task_request)

    def end_task(self, end_task_request: "EndTaskRequest") -> "EndTaskResponse":
        return self._unary_unary(
            "/gateway.GatewayService/EndTask",
            EndTaskRequest,
            EndTaskResponse,
        )(end_task_request)

    def stop_tasks(self, stop_tasks_request: "StopTasksRequest") -> "StopTasksResponse":
        return self._unary_unary(
            "/gateway.GatewayService/StopTasks",
            StopTasksRequest,
            StopTasksResponse,
        )(stop_tasks_request)

    def list_tasks(self, list_tasks_request: "ListTasksRequest") -> "ListTasksResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ListTasks",
            ListTasksRequest,
            ListTasksResponse,
        )(list_tasks_request)

    def get_or_create_stub(
        self, get_or_create_stub_request: "GetOrCreateStubRequest"
    ) -> "GetOrCreateStubResponse":
        return self._unary_unary(
            "/gateway.GatewayService/GetOrCreateStub",
            GetOrCreateStubRequest,
            GetOrCreateStubResponse,
        )(get_or_create_stub_request)

    def deploy_stub(
        self, deploy_stub_request: "DeployStubRequest"
    ) -> "DeployStubResponse":
        return self._unary_unary(
            "/gateway.GatewayService/DeployStub",
            DeployStubRequest,
            DeployStubResponse,
        )(deploy_stub_request)

    def get_url(self, get_url_request: "GetUrlRequest") -> "GetUrlResponse":
        return self._unary_unary(
            "/gateway.GatewayService/GetURL",
            GetUrlRequest,
            GetUrlResponse,
        )(get_url_request)

    def list_deployments(
        self, list_deployments_request: "ListDeploymentsRequest"
    ) -> "ListDeploymentsResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ListDeployments",
            ListDeploymentsRequest,
            ListDeploymentsResponse,
        )(list_deployments_request)

    def stop_deployment(
        self, stop_deployment_request: "StopDeploymentRequest"
    ) -> "StopDeploymentResponse":
        return self._unary_unary(
            "/gateway.GatewayService/StopDeployment",
            StopDeploymentRequest,
            StopDeploymentResponse,
        )(stop_deployment_request)

    def start_deployment(
        self, start_deployment_request: "StartDeploymentRequest"
    ) -> "StartDeploymentResponse":
        return self._unary_unary(
            "/gateway.GatewayService/StartDeployment",
            StartDeploymentRequest,
            StartDeploymentResponse,
        )(start_deployment_request)

    def delete_deployment(
        self, delete_deployment_request: "DeleteDeploymentRequest"
    ) -> "DeleteDeploymentResponse":
        return self._unary_unary(
            "/gateway.GatewayService/DeleteDeployment",
            DeleteDeploymentRequest,
            DeleteDeploymentResponse,
        )(delete_deployment_request)

    def list_pools(self, list_pools_request: "ListPoolsRequest") -> "ListPoolsResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ListPools",
            ListPoolsRequest,
            ListPoolsResponse,
        )(list_pools_request)

    def list_machines(
        self, list_machines_request: "ListMachinesRequest"
    ) -> "ListMachinesResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ListMachines",
            ListMachinesRequest,
            ListMachinesResponse,
        )(list_machines_request)

    def create_machine(
        self, create_machine_request: "CreateMachineRequest"
    ) -> "CreateMachineResponse":
        return self._unary_unary(
            "/gateway.GatewayService/CreateMachine",
            CreateMachineRequest,
            CreateMachineResponse,
        )(create_machine_request)

    def delete_machine(
        self, delete_machine_request: "DeleteMachineRequest"
    ) -> "DeleteMachineResponse":
        return self._unary_unary(
            "/gateway.GatewayService/DeleteMachine",
            DeleteMachineRequest,
            DeleteMachineResponse,
        )(delete_machine_request)

    def list_tokens(
        self, list_tokens_request: "ListTokensRequest"
    ) -> "ListTokensResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ListTokens",
            ListTokensRequest,
            ListTokensResponse,
        )(list_tokens_request)

    def create_token(
        self, create_token_request: "CreateTokenRequest"
    ) -> "CreateTokenResponse":
        return self._unary_unary(
            "/gateway.GatewayService/CreateToken",
            CreateTokenRequest,
            CreateTokenResponse,
        )(create_token_request)

    def toggle_token(
        self, toggle_token_request: "ToggleTokenRequest"
    ) -> "ToggleTokenResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ToggleToken",
            ToggleTokenRequest,
            ToggleTokenResponse,
        )(toggle_token_request)

    def delete_token(
        self, delete_token_request: "DeleteTokenRequest"
    ) -> "DeleteTokenResponse":
        return self._unary_unary(
            "/gateway.GatewayService/DeleteToken",
            DeleteTokenRequest,
            DeleteTokenResponse,
        )(delete_token_request)

    def list_workers(
        self, list_workers_request: "ListWorkersRequest"
    ) -> "ListWorkersResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ListWorkers",
            ListWorkersRequest,
            ListWorkersResponse,
        )(list_workers_request)

    def cordon_worker(
        self, cordon_worker_request: "CordonWorkerRequest"
    ) -> "CordonWorkerResponse":
        return self._unary_unary(
            "/gateway.GatewayService/CordonWorker",
            CordonWorkerRequest,
            CordonWorkerResponse,
        )(cordon_worker_request)

    def uncordon_worker(
        self, uncordon_worker_request: "UncordonWorkerRequest"
    ) -> "UncordonWorkerResponse":
        return self._unary_unary(
            "/gateway.GatewayService/UncordonWorker",
            UncordonWorkerRequest,
            UncordonWorkerResponse,
        )(uncordon_worker_request)

    def drain_worker(
        self, drain_worker_request: "DrainWorkerRequest"
    ) -> "DrainWorkerResponse":
        return self._unary_unary(
            "/gateway.GatewayService/DrainWorker",
            DrainWorkerRequest,
            DrainWorkerResponse,
        )(drain_worker_request)

    def export_workspace_config(
        self, export_workspace_config_request: "ExportWorkspaceConfigRequest"
    ) -> "ExportWorkspaceConfigResponse":
        return self._unary_unary(
            "/gateway.GatewayService/ExportWorkspaceConfig",
            ExportWorkspaceConfigRequest,
            ExportWorkspaceConfigResponse,
        )(export_workspace_config_request)
