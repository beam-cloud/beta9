"""Integration tests for infrastructure provisioning.

WARNING: These tests provision real infrastructure and may incur costs!
Run only in sandbox AWS accounts with appropriate cleanup.
"""

import os
import tempfile
from pathlib import Path

import pytest

from models.config import (
    MetaConfig,
    CloudProvider,
    ClusterProfile,
    from_meta_config,
)
from engine.executor import apply_infra, destroy_infra


# Skip if not in testing environment
pytestmark = pytest.mark.skipif(
    not os.getenv("BETA9_INTEGRATION_TESTS"),
    reason="Integration tests disabled. Set BETA9_INTEGRATION_TESTS=1 to run.",
)


@pytest.fixture
def test_meta_config():
    """Create a test meta-config for AWS."""
    # These should be set via environment variables
    aws_account_id = os.getenv("AWS_ACCOUNT_ID")
    aws_role_arn = os.getenv("AWS_ROLE_ARN")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    if not aws_account_id or not aws_role_arn:
        pytest.skip("AWS credentials not configured")

    return MetaConfig(
        name="integration-test",
        cloud={
            "provider": "aws",
            "region": aws_region,
            "account_id": aws_account_id,
            "role_arn": aws_role_arn,
        },
        cluster={"profile": "small"},  # Use smallest profile for cost
        data={
            "postgres": {"size": "small", "multi_az": False},  # Single AZ for cost
            "redis": {"size": "small", "ha": False},  # No HA for cost
        },
        beta9={"domain": "test.beta9.dev"},
    )


@pytest.fixture
def test_infra(test_meta_config):
    """Create test infrastructure model."""
    environment_id = f"test_{int(os.urandom(4).hex(), 16)}"
    return from_meta_config(test_meta_config, environment_id)


@pytest.fixture
def workdir():
    """Create temporary working directory."""
    tmpdir = Path(tempfile.mkdtemp(prefix="beta9-test-"))
    yield tmpdir

    # Cleanup
    import shutil

    if tmpdir.exists():
        shutil.rmtree(tmpdir)


class TestInfrastructureLifecycle:
    """Test full infrastructure lifecycle."""

    @pytest.mark.slow
    @pytest.mark.timeout(3600)  # 1 hour timeout
    def test_apply_and_destroy(self, test_infra, workdir):
        """
        Test full apply and destroy cycle.

        This is an expensive test that provisions real infrastructure!
        """
        # Apply infrastructure
        print(f"\n🚀 Applying infrastructure for {test_infra.environment_id}...")

        apply_result = apply_infra(test_infra, workdir)

        assert apply_result.success, f"Apply failed: {apply_result.error}"
        assert apply_result.outputs is not None
        assert "vpc_id" in apply_result.outputs
        assert "cluster_name" in apply_result.outputs
        assert "rds_endpoint" in apply_result.outputs
        assert "redis_endpoint" in apply_result.outputs
        assert "bucket_name" in apply_result.outputs

        print(f"✅ Infrastructure provisioned successfully")
        print(f"   VPC: {apply_result.outputs['vpc_id']}")
        print(f"   Cluster: {apply_result.outputs['cluster_name']}")
        print(f"   RDS: {apply_result.outputs['rds_endpoint']}")
        print(f"   Redis: {apply_result.outputs['redis_endpoint']}")
        print(f"   Bucket: {apply_result.outputs['bucket_name']}")

        # Destroy infrastructure
        print(f"\n💣 Destroying infrastructure for {test_infra.environment_id}...")

        destroy_result = destroy_infra(test_infra, workdir)

        assert destroy_result.success, f"Destroy failed: {destroy_result.error}"

        print(f"✅ Infrastructure destroyed successfully")


class TestInfrastructureOutputs:
    """Test infrastructure outputs."""

    @pytest.mark.slow
    def test_outputs_format(self, test_infra, workdir):
        """Test that outputs are in correct format."""
        apply_result = apply_infra(test_infra, workdir)

        if not apply_result.success:
            pytest.skip(f"Apply failed: {apply_result.error}")

        try:
            outputs = apply_result.outputs

            # Check VPC
            assert isinstance(outputs["vpc_id"], str)
            assert outputs["vpc_id"].startswith("vpc-")

            # Check EKS
            assert isinstance(outputs["cluster_name"], str)
            assert outputs["cluster_name"] == test_infra.cluster_name
            assert isinstance(outputs["cluster_endpoint"], str)
            assert outputs["cluster_endpoint"].startswith("https://")

            # Check RDS
            assert isinstance(outputs["rds_endpoint"], str)
            assert ".rds.amazonaws.com" in outputs["rds_endpoint"]
            assert isinstance(outputs["rds_master_user_secret_arn"], str)
            assert outputs["rds_master_user_secret_arn"].startswith("arn:aws:secretsmanager:")

            # Check Redis
            assert isinstance(outputs["redis_endpoint"], str)
            assert isinstance(outputs["redis_port"], int)
            assert outputs["redis_port"] == 6379

            # Check S3
            assert isinstance(outputs["bucket_name"], str)
            assert outputs["bucket_name"] == test_infra.storage.bucket_name

        finally:
            # Cleanup
            destroy_infra(test_infra, workdir)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
