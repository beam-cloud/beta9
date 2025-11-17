"""Meta-config schema and infrastructure model."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Any

from pydantic import BaseModel, Field, field_validator


class CloudProvider(str, Enum):
    """Supported cloud providers."""

    AWS = "aws"
    GCP = "gcp"


class ClusterProfile(str, Enum):
    """Cluster size profiles."""

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class DatabaseSize(str, Enum):
    """Database instance sizes."""

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class RedisSize(str, Enum):
    """Redis instance sizes."""

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


# Pydantic models for API input validation
class CloudConfig(BaseModel):
    """Cloud provider configuration."""

    provider: CloudProvider
    region: str
    account_id: str = Field(..., description="AWS account ID or GCP project ID")
    role_arn: Optional[str] = Field(
        None, description="AWS IAM role ARN to assume (required for AWS)"
    )
    service_account: Optional[str] = Field(
        None, description="GCP service account email (required for GCP)"
    )
    external_id: Optional[str] = Field(None, description="AWS STS external ID")

    @field_validator("role_arn")
    @classmethod
    def validate_aws_role(cls, v: Optional[str], info: Any) -> Optional[str]:
        """Validate that AWS provider has role_arn."""
        if info.data.get("provider") == CloudProvider.AWS and not v:
            raise ValueError("role_arn is required for AWS provider")
        return v

    @field_validator("service_account")
    @classmethod
    def validate_gcp_sa(cls, v: Optional[str], info: Any) -> Optional[str]:
        """Validate that GCP provider has service_account."""
        if info.data.get("provider") == CloudProvider.GCP and not v:
            raise ValueError("service_account is required for GCP provider")
        return v


class ClusterConfig(BaseModel):
    """Kubernetes cluster configuration."""

    profile: ClusterProfile = ClusterProfile.SMALL
    use_existing: bool = False
    existing_cluster_name: Optional[str] = None
    node_labels: Dict[str, str] = Field(default_factory=dict)
    node_taints: list[str] = Field(default_factory=list)


class PostgresConfig(BaseModel):
    """PostgreSQL database configuration."""

    size: DatabaseSize = DatabaseSize.SMALL
    multi_az: bool = True
    backup_retention_days: int = 7
    storage_gb: Optional[int] = None  # Auto-sized based on 'size' if None


class RedisConfig(BaseModel):
    """Redis cache configuration."""

    size: RedisSize = RedisSize.SMALL
    ha: bool = True  # High availability (multi-AZ)
    num_replicas: Optional[int] = None  # Auto-sized if None


class ObjectStoreConfig(BaseModel):
    """Object storage configuration."""

    bucket_prefix: str = "beta9"
    versioning: bool = True
    lifecycle_days: int = 90  # Archive after 90 days


class DataConfig(BaseModel):
    """Data layer configuration."""

    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    object_store: ObjectStoreConfig = Field(default_factory=ObjectStoreConfig)


class WorkersConfig(BaseModel):
    """Beta9 workers configuration."""

    gpu_enabled: bool = False
    gpu_type: Optional[str] = None  # e.g., "nvidia-tesla-t4"
    min_workers: int = 1
    max_workers: int = 10


class Beta9Config(BaseModel):
    """Beta9-specific configuration."""

    domain: str
    workers: WorkersConfig = Field(default_factory=WorkersConfig)
    version: str = "latest"
    enable_metrics: bool = True
    enable_logging: bool = True


class MetaConfig(BaseModel):
    """Top-level meta-configuration for environment."""

    name: str = Field(..., description="Environment name")
    cloud: CloudConfig
    cluster: ClusterConfig = Field(default_factory=ClusterConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    beta9: Beta9Config


# Internal infrastructure model (converted from MetaConfig)
@dataclass
class NodeGroupSpec:
    """Node group specification."""

    name: str
    instance_type: str
    min_size: int
    max_size: int
    desired_size: int
    labels: Dict[str, str] = field(default_factory=dict)
    taints: list[str] = field(default_factory=list)


@dataclass
class NetworkSpec:
    """Network specification."""

    vpc_cidr: str
    availability_zones: list[str]
    public_subnets: list[str]
    private_subnets: list[str]
    enable_nat_gateway: bool = True
    enable_vpn_gateway: bool = False


@dataclass
class DatabaseSpec:
    """Database specification."""

    instance_class: str
    allocated_storage_gb: int
    multi_az: bool
    backup_retention_days: int
    engine_version: str = "15.4"  # PostgreSQL version


@dataclass
class CacheSpec:
    """Cache specification."""

    node_type: str
    num_cache_nodes: int
    multi_az: bool
    engine_version: str = "7.0"  # Redis version


@dataclass
class StorageSpec:
    """Object storage specification."""

    bucket_name: str
    versioning: bool
    lifecycle_rules: Dict[str, Any]


@dataclass
class InfraModel:
    """
    Internal infrastructure model.

    This is the normalized, cloud-agnostic model that the CDKTF stacks consume.
    Converted from user's MetaConfig.
    """

    # Identity
    environment_name: str
    environment_id: str

    # Cloud
    cloud_provider: CloudProvider
    region: str
    account_id: str
    role_arn: Optional[str] = None
    service_account: Optional[str] = None
    external_id: Optional[str] = None

    # Network
    network: NetworkSpec = field(default_factory=lambda: NetworkSpec(
        vpc_cidr="10.0.0.0/16",
        availability_zones=[],
        public_subnets=[],
        private_subnets=[],
    ))

    # Cluster
    cluster_name: str = ""
    use_existing_cluster: bool = False
    node_groups: list[NodeGroupSpec] = field(default_factory=list)

    # Data layer
    database: DatabaseSpec = field(default_factory=lambda: DatabaseSpec(
        instance_class="db.t3.small",
        allocated_storage_gb=20,
        multi_az=True,
        backup_retention_days=7,
    ))
    cache: CacheSpec = field(default_factory=lambda: CacheSpec(
        node_type="cache.t3.small",
        num_cache_nodes=2,
        multi_az=True,
    ))
    storage: StorageSpec = field(default_factory=lambda: StorageSpec(
        bucket_name="",
        versioning=True,
        lifecycle_rules={},
    ))

    # Beta9
    beta9_domain: str = ""
    beta9_version: str = "latest"
    gpu_enabled: bool = False
    gpu_type: Optional[str] = None

    # State backend
    state_backend_bucket: str = ""
    state_backend_key: str = ""
    state_backend_region: str = ""

    # Tags
    tags: Dict[str, str] = field(default_factory=dict)


def from_meta_config(
    meta: MetaConfig, environment_id: str, state_bucket: Optional[str] = None
) -> InfraModel:
    """
    Convert user's meta-config into internal InfraModel.

    Args:
        meta: User's meta-configuration
        environment_id: Unique environment identifier
        state_bucket: Optional override for state bucket (defaults to auto-generated)

    Returns:
        InfraModel ready for CDKTF stack
    """
    # Determine availability zones (simplified - use first 3 AZs in region)
    azs = [f"{meta.cloud.region}a", f"{meta.cloud.region}b", f"{meta.cloud.region}c"]

    # Network configuration
    network = NetworkSpec(
        vpc_cidr="10.0.0.0/16",
        availability_zones=azs[:2],  # Use 2 AZs for redundancy
        public_subnets=["10.0.1.0/24", "10.0.2.0/24"],
        private_subnets=["10.0.10.0/24", "10.0.11.0/24"],
        enable_nat_gateway=True,
    )

    # Node groups based on profile
    node_groups = _build_node_groups(meta.cluster.profile, meta.beta9.workers, meta.cluster)

    # Database spec based on size
    database = _build_database_spec(meta.data.postgres)

    # Cache spec based on size
    cache = _build_cache_spec(meta.data.redis)

    # Storage spec
    bucket_name = f"{meta.data.object_store.bucket_prefix}-{meta.name}-{environment_id[:8]}"
    storage = StorageSpec(
        bucket_name=bucket_name,
        versioning=meta.data.object_store.versioning,
        lifecycle_rules={
            "archive_after_days": meta.data.object_store.lifecycle_days,
        },
    )

    # State backend
    if not state_bucket:
        state_bucket = f"beta9-tf-state-{meta.cloud.account_id}-{meta.cloud.region}"

    state_key = f"environments/{environment_id}/terraform.tfstate"

    # Build tags
    tags = {
        "Environment": meta.name,
        "EnvironmentId": environment_id,
        "ManagedBy": "Beta9Provisioning",
        "Beta9Domain": meta.beta9.domain,
    }

    return InfraModel(
        environment_name=meta.name,
        environment_id=environment_id,
        cloud_provider=meta.cloud.provider,
        region=meta.cloud.region,
        account_id=meta.cloud.account_id,
        role_arn=meta.cloud.role_arn,
        service_account=meta.cloud.service_account,
        external_id=meta.cloud.external_id,
        network=network,
        cluster_name=f"beta9-{meta.name}",
        use_existing_cluster=meta.cluster.use_existing,
        node_groups=node_groups,
        database=database,
        cache=cache,
        storage=storage,
        beta9_domain=meta.beta9.domain,
        beta9_version=meta.beta9.version,
        gpu_enabled=meta.beta9.workers.gpu_enabled,
        gpu_type=meta.beta9.workers.gpu_type,
        state_backend_bucket=state_bucket,
        state_backend_key=state_key,
        state_backend_region=meta.cloud.region,
        tags=tags,
    )


def _build_node_groups(
    profile: ClusterProfile, workers: WorkersConfig, cluster: ClusterConfig
) -> list[NodeGroupSpec]:
    """Build node group specifications based on profile."""
    # Instance types by profile
    instance_map = {
        ClusterProfile.SMALL: "t3.large",  # 2 vCPU, 8 GB RAM
        ClusterProfile.MEDIUM: "t3.xlarge",  # 4 vCPU, 16 GB RAM
        ClusterProfile.LARGE: "t3.2xlarge",  # 8 vCPU, 32 GB RAM
    }

    # Node counts by profile
    size_map = {
        ClusterProfile.SMALL: (1, 3, 2),  # min, max, desired
        ClusterProfile.MEDIUM: (2, 6, 3),
        ClusterProfile.LARGE: (3, 10, 5),
    }

    min_size, max_size, desired_size = size_map[profile]
    instance_type = instance_map[profile]

    node_groups = [
        NodeGroupSpec(
            name="system",
            instance_type=instance_type,
            min_size=min_size,
            max_size=max_size,
            desired_size=desired_size,
            labels={"beta9.io/node-type": "system", **cluster.node_labels},
            taints=cluster.node_taints,
        )
    ]

    # Add GPU node group if enabled
    if workers.gpu_enabled:
        gpu_instance_type = "g4dn.xlarge"  # NVIDIA T4 GPU
        if workers.gpu_type == "nvidia-tesla-v100":
            gpu_instance_type = "p3.2xlarge"

        node_groups.append(
            NodeGroupSpec(
                name="gpu-workers",
                instance_type=gpu_instance_type,
                min_size=workers.min_workers,
                max_size=workers.max_workers,
                desired_size=workers.min_workers,
                labels={
                    "beta9.io/node-type": "gpu-worker",
                    "beta9.io/gpu-type": workers.gpu_type or "nvidia-tesla-t4",
                },
                taints=["nvidia.com/gpu=true:NoSchedule"],
            )
        )

    return node_groups


def _build_database_spec(postgres: PostgresConfig) -> DatabaseSpec:
    """Build database specification based on size."""
    instance_class_map = {
        DatabaseSize.SMALL: "db.t3.small",  # 2 vCPU, 2 GB RAM
        DatabaseSize.MEDIUM: "db.t3.large",  # 2 vCPU, 8 GB RAM
        DatabaseSize.LARGE: "db.r5.xlarge",  # 4 vCPU, 32 GB RAM
    }

    storage_map = {
        DatabaseSize.SMALL: 20,
        DatabaseSize.MEDIUM: 100,
        DatabaseSize.LARGE: 500,
    }

    return DatabaseSpec(
        instance_class=instance_class_map[postgres.size],
        allocated_storage_gb=postgres.storage_gb or storage_map[postgres.size],
        multi_az=postgres.multi_az,
        backup_retention_days=postgres.backup_retention_days,
    )


def _build_cache_spec(redis: RedisConfig) -> CacheSpec:
    """Build cache specification based on size."""
    node_type_map = {
        RedisSize.SMALL: "cache.t3.small",  # 2 vCPU, 1.55 GB RAM
        RedisSize.MEDIUM: "cache.t3.medium",  # 2 vCPU, 3.09 GB RAM
        RedisSize.LARGE: "cache.r5.large",  # 2 vCPU, 13.07 GB RAM
    }

    num_replicas = redis.num_replicas if redis.num_replicas is not None else (2 if redis.ha else 1)

    return CacheSpec(
        node_type=node_type_map[redis.size],
        num_cache_nodes=num_replicas,
        multi_az=redis.ha,
    )
