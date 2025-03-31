# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: types.proto
# plugin: python-betterproto
# This file has been @generated

from dataclasses import dataclass
from datetime import datetime
from typing import List

import betterproto


@dataclass(eq=False, repr=False)
class App(betterproto.Message):
    id: int = betterproto.uint32_field(1)
    external_id: str = betterproto.string_field(2)
    name: str = betterproto.string_field(3)
    description: str = betterproto.string_field(4)
    workspace_id: int = betterproto.uint32_field(5)
    created_at: datetime = betterproto.message_field(6)
    updated_at: datetime = betterproto.message_field(7)


@dataclass(eq=False, repr=False)
class BuildOptions(betterproto.Message):
    source_image: str = betterproto.string_field(1)
    dockerfile: str = betterproto.string_field(2)
    build_ctx_object: str = betterproto.string_field(3)
    source_image_creds: str = betterproto.string_field(4)
    build_secrets: List[str] = betterproto.string_field(5)


@dataclass(eq=False, repr=False)
class CheckpointState(betterproto.Message):
    stub_id: str = betterproto.string_field(1)
    container_id: str = betterproto.string_field(2)
    status: str = betterproto.string_field(3)
    remote_key: str = betterproto.string_field(4)


@dataclass(eq=False, repr=False)
class ConcurrencyLimit(betterproto.Message):
    id: int = betterproto.uint32_field(1)
    external_id: str = betterproto.string_field(2)
    gpu_limit: int = betterproto.uint32_field(3)
    cpu_millicore_limit: int = betterproto.uint32_field(4)
    created_at: datetime = betterproto.message_field(5)
    updated_at: datetime = betterproto.message_field(6)


@dataclass(eq=False, repr=False)
class Container(betterproto.Message):
    container_id: str = betterproto.string_field(1)
    stub_id: str = betterproto.string_field(2)
    status: str = betterproto.string_field(3)
    scheduled_at: datetime = betterproto.message_field(4)
    started_at: datetime = betterproto.message_field(5)
    workspace_id: str = betterproto.string_field(6)
    worker_id: str = betterproto.string_field(7)
    machine_id: str = betterproto.string_field(8)
    deployment_id: str = betterproto.string_field(9)


@dataclass(eq=False, repr=False)
class ContainerRequest(betterproto.Message):
    container_id: str = betterproto.string_field(1)
    entry_point: List[str] = betterproto.string_field(2)
    env: List[str] = betterproto.string_field(3)
    cpu: int = betterproto.int64_field(4)
    memory: int = betterproto.int64_field(5)
    gpu: str = betterproto.string_field(6)
    gpu_request: List[str] = betterproto.string_field(7)
    gpu_count: int = betterproto.uint32_field(8)
    image_id: str = betterproto.string_field(9)
    stub_id: str = betterproto.string_field(10)
    workspace_id: str = betterproto.string_field(11)
    workspace: "Workspace" = betterproto.message_field(12)
    stub: "StubWithRelated" = betterproto.message_field(13)
    timestamp: datetime = betterproto.message_field(14)
    mounts: List["Mount"] = betterproto.message_field(15)
    retry_count: int = betterproto.int64_field(16)
    pool_selector: str = betterproto.string_field(17)
    preemptable: bool = betterproto.bool_field(18)
    checkpoint_enabled: bool = betterproto.bool_field(19)
    build_options: "BuildOptions" = betterproto.message_field(20)
    ports: List[int] = betterproto.uint32_field(21)
    cost_per_ms: float = betterproto.double_field(22)


@dataclass(eq=False, repr=False)
class ContainerState(betterproto.Message):
    container_id: str = betterproto.string_field(1)
    stub_id: str = betterproto.string_field(2)
    status: str = betterproto.string_field(3)
    scheduled_at: int = betterproto.int64_field(4)
    workspace_id: str = betterproto.string_field(5)
    gpu: str = betterproto.string_field(6)
    gpu_count: int = betterproto.uint32_field(7)
    cpu: int = betterproto.int64_field(8)
    memory: int = betterproto.int64_field(9)
    started_at: int = betterproto.int64_field(10)


@dataclass(eq=False, repr=False)
class Mount(betterproto.Message):
    local_path: str = betterproto.string_field(1)
    mount_path: str = betterproto.string_field(2)
    link_path: str = betterproto.string_field(3)
    read_only: bool = betterproto.bool_field(4)
    mount_type: str = betterproto.string_field(5)
    mount_point_config: "MountPointConfig" = betterproto.message_field(6)


@dataclass(eq=False, repr=False)
class MountPointConfig(betterproto.Message):
    bucket_name: str = betterproto.string_field(1)
    access_key: str = betterproto.string_field(2)
    secret_key: str = betterproto.string_field(3)
    endpoint_url: str = betterproto.string_field(4)
    region: str = betterproto.string_field(5)
    read_only: bool = betterproto.bool_field(6)


@dataclass(eq=False, repr=False)
class Object(betterproto.Message):
    id: int = betterproto.uint32_field(1)
    external_id: str = betterproto.string_field(2)
    hash: str = betterproto.string_field(3)
    size: int = betterproto.int64_field(4)
    workspace_id: int = betterproto.uint32_field(5)
    created_at: datetime = betterproto.message_field(6)


@dataclass(eq=False, repr=False)
class Stub(betterproto.Message):
    id: int = betterproto.uint32_field(1)
    external_id: str = betterproto.string_field(2)
    name: str = betterproto.string_field(3)
    type: str = betterproto.string_field(4)
    config: str = betterproto.string_field(5)
    config_version: int = betterproto.uint32_field(6)
    object_id: int = betterproto.uint32_field(7)
    workspace_id: int = betterproto.uint32_field(8)
    created_at: datetime = betterproto.message_field(9)
    updated_at: datetime = betterproto.message_field(10)
    public: bool = betterproto.bool_field(11)
    app_id: int = betterproto.uint32_field(12)


@dataclass(eq=False, repr=False)
class StubWithRelated(betterproto.Message):
    stub: "Stub" = betterproto.message_field(1)
    workspace: "Workspace" = betterproto.message_field(2)
    object: "Object" = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class Worker(betterproto.Message):
    id: str = betterproto.string_field(1)
    status: str = betterproto.string_field(2)
    total_cpu: int = betterproto.int64_field(3)
    total_memory: int = betterproto.int64_field(4)
    total_gpu_count: int = betterproto.uint32_field(5)
    free_cpu: int = betterproto.int64_field(6)
    free_memory: int = betterproto.int64_field(7)
    free_gpu_count: int = betterproto.uint32_field(8)
    gpu: str = betterproto.string_field(9)
    pool_name: str = betterproto.string_field(10)
    machine_id: str = betterproto.string_field(11)
    resource_version: int = betterproto.int64_field(12)
    requires_pool_selector: bool = betterproto.bool_field(13)
    priority: int = betterproto.int32_field(14)
    preemptable: bool = betterproto.bool_field(15)
    build_version: str = betterproto.string_field(16)
    active_containers: List["Container"] = betterproto.message_field(17)


@dataclass(eq=False, repr=False)
class WorkerPoolState(betterproto.Message):
    status: str = betterproto.string_field(1)
    scheduling_latency: int = betterproto.int64_field(2)
    free_gpu: int = betterproto.uint32_field(3)
    free_cpu: int = betterproto.int64_field(4)
    free_memory: int = betterproto.int64_field(5)
    pending_workers: int = betterproto.int64_field(6)
    available_workers: int = betterproto.int64_field(7)
    pending_containers: int = betterproto.int64_field(8)
    running_containers: int = betterproto.int64_field(9)
    registered_machines: int = betterproto.int64_field(10)
    pending_machines: int = betterproto.int64_field(11)


@dataclass(eq=False, repr=False)
class Workspace(betterproto.Message):
    id: int = betterproto.uint32_field(1)
    external_id: str = betterproto.string_field(2)
    name: str = betterproto.string_field(3)
    created_at: datetime = betterproto.message_field(4)
    updated_at: datetime = betterproto.message_field(5)
    signing_key: str = betterproto.string_field(6)
    volume_cache_enabled: bool = betterproto.bool_field(7)
    multi_gpu_enabled: bool = betterproto.bool_field(8)
    concurrency_limit_id: int = betterproto.uint32_field(9)
    concurrency_limit: "ConcurrencyLimit" = betterproto.message_field(10)
