"""Unit tests for configuration models."""

import pytest
from pydantic import ValidationError

from models.config import (
    CloudConfig,
    CloudProvider,
    ClusterProfile,
    DatabaseSize,
    MetaConfig,
    from_meta_config,
)


class TestCloudConfig:
    """Test CloudConfig validation."""

    def test_aws_config_valid(self):
        """Test valid AWS configuration."""
        config = CloudConfig(
            provider=CloudProvider.AWS,
            region="us-east-1",
            account_id="123456789012",
            role_arn="arn:aws:iam::123456789012:role/Beta9Provisioner",
        )

        assert config.provider == CloudProvider.AWS
        assert config.region == "us-east-1"
        assert config.role_arn == "arn:aws:iam::123456789012:role/Beta9Provisioner"

    def test_aws_config_missing_role_arn(self):
        """Test AWS configuration without required role_arn."""
        with pytest.raises(ValidationError) as exc_info:
            CloudConfig(
                provider=CloudProvider.AWS,
                region="us-east-1",
                account_id="123456789012",
            )

        assert "role_arn is required for AWS provider" in str(exc_info.value)

    def test_gcp_config_valid(self):
        """Test valid GCP configuration."""
        config = CloudConfig(
            provider=CloudProvider.GCP,
            region="us-central1",
            account_id="my-project-id",
            service_account="terraform@my-project.iam.gserviceaccount.com",
        )

        assert config.provider == CloudProvider.GCP
        assert config.service_account == "terraform@my-project.iam.gserviceaccount.com"


class TestMetaConfig:
    """Test MetaConfig parsing and validation."""

    def test_minimal_config(self):
        """Test minimal valid configuration."""
        config = MetaConfig(
            name="test-env",
            cloud={
                "provider": "aws",
                "region": "us-east-1",
                "account_id": "123456789012",
                "role_arn": "arn:aws:iam::123456789012:role/Beta9",
            },
            beta9={"domain": "beta9.example.com"},
        )

        assert config.name == "test-env"
        assert config.cloud.provider == CloudProvider.AWS
        assert config.cluster.profile == ClusterProfile.SMALL  # Default
        assert config.data.postgres.size == DatabaseSize.SMALL  # Default

    def test_full_config(self):
        """Test full configuration with all options."""
        config = MetaConfig(
            name="prod-env",
            cloud={
                "provider": "aws",
                "region": "us-west-2",
                "account_id": "123456789012",
                "role_arn": "arn:aws:iam::123456789012:role/Beta9",
                "external_id": "my-external-id",
            },
            cluster={
                "profile": "large",
                "use_existing": False,
                "node_labels": {"app": "beta9"},
            },
            data={
                "postgres": {
                    "size": "large",
                    "multi_az": True,
                    "backup_retention_days": 14,
                },
                "redis": {"size": "medium", "ha": True},
                "object_store": {
                    "bucket_prefix": "my-beta9",
                    "versioning": True,
                },
            },
            beta9={
                "domain": "beta9.production.com",
                "workers": {
                    "gpu_enabled": True,
                    "gpu_type": "nvidia-tesla-v100",
                    "min_workers": 2,
                    "max_workers": 20,
                },
                "version": "v1.2.3",
            },
        )

        assert config.cluster.profile == ClusterProfile.LARGE
        assert config.data.postgres.size == DatabaseSize.LARGE
        assert config.beta9.workers.gpu_enabled is True
        assert config.beta9.workers.gpu_type == "nvidia-tesla-v100"


class TestFromMetaConfig:
    """Test meta-config to infra-model conversion."""

    def test_small_profile_conversion(self):
        """Test conversion of small profile."""
        meta = MetaConfig(
            name="test-small",
            cloud={
                "provider": "aws",
                "region": "us-east-1",
                "account_id": "123456789012",
                "role_arn": "arn:aws:iam::123456789012:role/Beta9",
            },
            cluster={"profile": "small"},
            beta9={"domain": "beta9.example.com"},
        )

        infra = from_meta_config(meta, "env_abc123")

        assert infra.environment_id == "env_abc123"
        assert infra.environment_name == "test-small"
        assert infra.cluster_name == "beta9-test-small"
        assert len(infra.node_groups) == 1

        # Check node group specs
        node_group = infra.node_groups[0]
        assert node_group.name == "system"
        assert node_group.instance_type == "t3.large"
        assert node_group.min_size == 1
        assert node_group.max_size == 3
        assert node_group.desired_size == 2

        # Check database specs
        assert infra.database.instance_class == "db.t3.small"
        assert infra.database.allocated_storage_gb == 20

        # Check cache specs
        assert infra.cache.node_type == "cache.t3.small"

    def test_large_profile_with_gpu(self):
        """Test conversion of large profile with GPU workers."""
        meta = MetaConfig(
            name="test-large",
            cloud={
                "provider": "aws",
                "region": "us-west-2",
                "account_id": "123456789012",
                "role_arn": "arn:aws:iam::123456789012:role/Beta9",
            },
            cluster={"profile": "large"},
            beta9={
                "domain": "beta9.example.com",
                "workers": {
                    "gpu_enabled": True,
                    "gpu_type": "nvidia-tesla-v100",
                    "min_workers": 2,
                    "max_workers": 10,
                },
            },
        )

        infra = from_meta_config(meta, "env_xyz789")

        # Should have 2 node groups: system + gpu-workers
        assert len(infra.node_groups) == 2

        system_ng = infra.node_groups[0]
        assert system_ng.name == "system"
        assert system_ng.instance_type == "t3.2xlarge"  # Large profile

        gpu_ng = infra.node_groups[1]
        assert gpu_ng.name == "gpu-workers"
        assert gpu_ng.instance_type == "p3.2xlarge"  # V100 GPU
        assert gpu_ng.min_size == 2
        assert gpu_ng.max_size == 10
        assert "nvidia.com/gpu=true:NoSchedule" in gpu_ng.taints

    def test_network_configuration(self):
        """Test network configuration."""
        meta = MetaConfig(
            name="test-network",
            cloud={
                "provider": "aws",
                "region": "eu-west-1",
                "account_id": "123456789012",
                "role_arn": "arn:aws:iam::123456789012:role/Beta9",
            },
            beta9={"domain": "beta9.example.com"},
        )

        infra = from_meta_config(meta, "env_net123")

        # Check network specs
        assert infra.network.vpc_cidr == "10.0.0.0/16"
        assert len(infra.network.availability_zones) == 2
        assert infra.network.availability_zones[0] == "eu-west-1a"
        assert infra.network.availability_zones[1] == "eu-west-1b"
        assert len(infra.network.public_subnets) == 2
        assert len(infra.network.private_subnets) == 2
        assert infra.network.enable_nat_gateway is True

    def test_storage_configuration(self):
        """Test storage configuration."""
        meta = MetaConfig(
            name="test-storage",
            cloud={
                "provider": "aws",
                "region": "us-east-1",
                "account_id": "123456789012",
                "role_arn": "arn:aws:iam::123456789012:role/Beta9",
            },
            data={
                "object_store": {
                    "bucket_prefix": "my-beta9",
                    "versioning": True,
                    "lifecycle_days": 60,
                }
            },
            beta9={"domain": "beta9.example.com"},
        )

        infra = from_meta_config(meta, "env_str456")

        # Check storage specs
        assert infra.storage.bucket_name.startswith("my-beta9-test-storage-")
        assert infra.storage.versioning is True
        assert infra.storage.lifecycle_rules["archive_after_days"] == 60

    def test_state_backend_configuration(self):
        """Test state backend configuration."""
        meta = MetaConfig(
            name="test-state",
            cloud={
                "provider": "aws",
                "region": "ap-southeast-1",
                "account_id": "987654321098",
                "role_arn": "arn:aws:iam::987654321098:role/Beta9",
            },
            beta9={"domain": "beta9.example.com"},
        )

        infra = from_meta_config(meta, "env_state789")

        # Check state backend
        assert infra.state_backend_bucket == "beta9-tf-state-987654321098-ap-southeast-1"
        assert infra.state_backend_key == "environments/env_state789/terraform.tfstate"
        assert infra.state_backend_region == "ap-southeast-1"

    def test_tags(self):
        """Test resource tags."""
        meta = MetaConfig(
            name="test-tags",
            cloud={
                "provider": "aws",
                "region": "us-east-1",
                "account_id": "123456789012",
                "role_arn": "arn:aws:iam::123456789012:role/Beta9",
            },
            beta9={"domain": "beta9.example.com"},
        )

        infra = from_meta_config(meta, "env_tags123")

        # Check tags
        assert infra.tags["Environment"] == "test-tags"
        assert infra.tags["EnvironmentId"] == "env_tags123"
        assert infra.tags["ManagedBy"] == "Beta9Provisioning"
        assert infra.tags["Beta9Domain"] == "beta9.example.com"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
