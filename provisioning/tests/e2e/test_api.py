"""End-to-end API tests.

WARNING: These tests provision real infrastructure and may incur costs!
"""

import os
import time

import pytest
import httpx


# Skip if not in testing environment
pytestmark = pytest.mark.skipif(
    not os.getenv("BETA9_E2E_TESTS"),
    reason="E2E tests disabled. Set BETA9_E2E_TESTS=1 to run.",
)


@pytest.fixture
def api_url():
    """Get API URL from environment."""
    url = os.getenv("BETA9_API_URL", "http://localhost:8000")
    return url


@pytest.fixture
def test_environment_config():
    """Create test environment configuration."""
    aws_account_id = os.getenv("AWS_ACCOUNT_ID")
    aws_role_arn = os.getenv("AWS_ROLE_ARN")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    if not aws_account_id or not aws_role_arn:
        pytest.skip("AWS credentials not configured")

    return {
        "name": f"e2e-test-{int(time.time())}",
        "cloud": {
            "provider": "aws",
            "region": aws_region,
            "account_id": aws_account_id,
            "role_arn": aws_role_arn,
        },
        "cluster": {"profile": "small"},
        "data": {
            "postgres": {"size": "small", "multi_az": False},
            "redis": {"size": "small", "ha": False},
        },
        "beta9": {"domain": "test.beta9.dev"},
    }


class TestEnvironmentLifecycle:
    """Test full environment lifecycle via API."""

    @pytest.mark.slow
    @pytest.mark.timeout(7200)  # 2 hour timeout
    def test_create_provision_destroy(self, api_url, test_environment_config):
        """
        Test complete environment lifecycle:
        1. Create environment
        2. Wait for provisioning
        3. Verify infrastructure
        4. Destroy environment
        5. Verify destruction
        """
        client = httpx.Client(base_url=api_url, timeout=30.0)

        # Step 1: Create environment
        print("\n🚀 Creating environment...")

        response = client.post("/api/v1/environments", json=test_environment_config)

        assert response.status_code == 200, f"Create failed: {response.text}"

        data = response.json()
        environment_id = data["environment_id"]
        assert data["status"] == "queued"

        print(f"✅ Environment created: {environment_id}")

        # Step 2: Wait for provisioning
        print("\n⏳ Waiting for provisioning...")

        max_wait = 3600  # 1 hour
        start_time = time.time()
        status = "queued"

        while time.time() - start_time < max_wait:
            response = client.get(f"/api/v1/environments/{environment_id}")
            assert response.status_code == 200

            data = response.json()
            status = data["status"]

            print(f"   Status: {status}")

            if status == "ready":
                break
            elif status == "failed":
                pytest.fail(f"Provisioning failed: {data.get('error_message')}")

            time.sleep(30)  # Check every 30 seconds

        assert status == "ready", f"Provisioning did not complete in {max_wait}s"

        print(f"✅ Environment ready")

        # Step 3: Verify infrastructure outputs
        print("\n🔍 Verifying infrastructure...")

        outputs = data["outputs"]
        assert "cluster_name" in outputs
        assert "cluster_endpoint" in outputs
        assert "rds_endpoint" in outputs
        assert "redis_endpoint" in outputs
        assert "bucket_name" in outputs

        print(f"   Cluster: {outputs['cluster_name']}")
        print(f"   RDS: {outputs['rds_endpoint']}")
        print(f"   Redis: {outputs['redis_endpoint']}")
        print(f"   Bucket: {outputs['bucket_name']}")

        # TODO: Verify Beta9 is accessible
        # beta9_endpoint = f"https://{test_environment_config['beta9']['domain']}"
        # response = client.get(f"{beta9_endpoint}/health")
        # assert response.status_code == 200

        # Step 4: Destroy environment
        print("\n💣 Destroying environment...")

        response = client.delete(f"/api/v1/environments/{environment_id}")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "destroy_queued"

        # Step 5: Wait for destruction
        print("\n⏳ Waiting for destruction...")

        max_wait = 1800  # 30 minutes
        start_time = time.time()

        while time.time() - start_time < max_wait:
            response = client.get(f"/api/v1/environments/{environment_id}")
            assert response.status_code == 200

            data = response.json()
            status = data["status"]

            print(f"   Status: {status}")

            if status == "destroyed":
                break
            elif status == "failed":
                pytest.fail(f"Destruction failed: {data.get('error_message')}")

            time.sleep(30)

        assert status == "destroyed", f"Destruction did not complete in {max_wait}s"

        print(f"✅ Environment destroyed")


class TestAPIValidation:
    """Test API validation and error handling."""

    def test_create_invalid_config(self, api_url):
        """Test creating environment with invalid configuration."""
        client = httpx.Client(base_url=api_url)

        # Missing required fields
        response = client.post(
            "/api/v1/environments",
            json={
                "name": "test",
                # Missing cloud and beta9
            },
        )

        assert response.status_code == 422  # Validation error

    def test_get_nonexistent_environment(self, api_url):
        """Test getting non-existent environment."""
        client = httpx.Client(base_url=api_url)

        response = client.get("/api/v1/environments/env_nonexistent")

        assert response.status_code == 404

    def test_delete_nonexistent_environment(self, api_url):
        """Test deleting non-existent environment."""
        client = httpx.Client(base_url=api_url)

        response = client.delete("/api/v1/environments/env_nonexistent")

        assert response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
