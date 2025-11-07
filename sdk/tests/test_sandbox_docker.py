from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from beta9.abstractions.sandbox import Sandbox, SandboxInstance, SandboxDockerManager


class TestSandboxDockerManager(TestCase):
    def setUp(self):
        """Set up a mock sandbox instance for testing."""
        self.mock_sandbox_instance = MagicMock(spec=SandboxInstance)
        self.mock_sandbox_instance.process = MagicMock()
        
        # Mock the exec method to return a process with wait() that returns 0
        mock_process = MagicMock()
        mock_process.wait = MagicMock(return_value=0)
        self.mock_sandbox_instance.process.exec = MagicMock(return_value=mock_process)
        
        # Create Docker manager with auto_wait disabled for faster tests
        self.docker_manager = SandboxDockerManager(
            self.mock_sandbox_instance,
            auto_wait_for_daemon=False
        )

    def test_docker_manager_init(self):
        """Test Docker manager initialization."""
        self.assertIsInstance(self.docker_manager, SandboxDockerManager)
        self.assertEqual(self.docker_manager.sandbox_instance, self.mock_sandbox_instance)

    def test_docker_build_basic(self):
        """Test basic docker build command."""
        self.docker_manager.build("my-app:v1")
        
        self.mock_sandbox_instance.process.exec.assert_called_once_with(
            "docker", "build", "-t", "my-app:v1", "."
        )

    def test_docker_build_with_options(self):
        """Test docker build with additional options."""
        self.docker_manager.build(
            tag="my-app:v2",
            context="./app",
            dockerfile="./app/Dockerfile.prod",
            build_args={"VERSION": "2.0", "ENV": "production"},
            no_cache=True
        )
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("build", call_args)
        self.assertIn("-t", call_args)
        self.assertIn("my-app:v2", call_args)
        self.assertIn("-f", call_args)
        self.assertIn("./app/Dockerfile.prod", call_args)
        self.assertIn("--build-arg", call_args)
        self.assertIn("--no-cache", call_args)
        self.assertIn("./app", call_args)

    def test_docker_run_basic(self):
        """Test basic docker run command."""
        self.docker_manager.run("nginx:latest")
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("run", call_args)
        self.assertIn("nginx:latest", call_args)

    def test_docker_run_with_ports(self):
        """Test docker run with port mappings."""
        self.docker_manager.run(
            "nginx:latest",
            detach=True,
            ports={"80": "8080", "443": "8443"},
            name="my-nginx"
        )
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("-d", call_args)
        self.assertIn("--name", call_args)
        self.assertIn("my-nginx", call_args)
        self.assertIn("-p", call_args)
        self.assertIn("nginx:latest", call_args)

    def test_docker_run_with_volumes_and_env(self):
        """Test docker run with volumes and environment variables."""
        self.docker_manager.run(
            "postgres:15",
            detach=True,
            env={"POSTGRES_PASSWORD": "mysecret", "POSTGRES_USER": "admin"},
            volumes={"/data": "/var/lib/postgresql/data"}
        )
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("-d", call_args)
        self.assertIn("-e", call_args)
        self.assertIn("-v", call_args)
        self.assertIn("postgres:15", call_args)

    def test_docker_ps(self):
        """Test docker ps command."""
        self.docker_manager.ps(all=True, quiet=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("ps", call_args)
        self.assertIn("-a", call_args)
        self.assertIn("-q", call_args)

    def test_docker_stop(self):
        """Test docker stop command."""
        self.docker_manager.stop("my-container", timeout=30)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("stop", call_args)
        self.assertIn("-t", call_args)
        self.assertIn("30", call_args)
        self.assertIn("my-container", call_args)

    def test_docker_logs(self):
        """Test docker logs command."""
        self.docker_manager.logs("my-container", tail=100, timestamps=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("logs", call_args)
        self.assertIn("--tail", call_args)
        self.assertIn("100", call_args)
        self.assertIn("-t", call_args)
        self.assertIn("my-container", call_args)

    def test_docker_exec(self):
        """Test docker exec command."""
        self.docker_manager.exec(
            "my-container",
            "ls -la",
            env={"ENV": "production"},
            workdir="/app"
        )
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("exec", call_args)
        self.assertIn("-e", call_args)
        self.assertIn("-w", call_args)
        self.assertIn("/app", call_args)
        self.assertIn("my-container", call_args)
        self.assertIn("ls -la", call_args)

    def test_docker_pull(self):
        """Test docker pull command."""
        self.docker_manager.pull("nginx:latest", quiet=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("pull", call_args)
        self.assertIn("-q", call_args)
        self.assertIn("nginx:latest", call_args)

    def test_docker_images(self):
        """Test docker images command."""
        self.docker_manager.images(all=True, quiet=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("images", call_args)
        self.assertIn("-a", call_args)
        self.assertIn("-q", call_args)

    def test_docker_rmi(self):
        """Test docker rmi command."""
        self.docker_manager.rmi("my-image:v1", force=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("rmi", call_args)
        self.assertIn("-f", call_args)
        self.assertIn("my-image:v1", call_args)

    def test_compose_up(self):
        """Test docker-compose up command."""
        self.docker_manager.compose_up(
            file="docker-compose.prod.yml",
            detach=True,
            build=True
        )
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker-compose", call_args)
        self.assertIn("-f", call_args)
        self.assertIn("docker-compose.prod.yml", call_args)
        self.assertIn("up", call_args)
        self.assertIn("-d", call_args)
        self.assertIn("--build", call_args)

    def test_compose_down(self):
        """Test docker-compose down command."""
        self.docker_manager.compose_down(volumes=True, remove_orphans=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker-compose", call_args)
        self.assertIn("down", call_args)
        self.assertIn("-v", call_args)
        self.assertIn("--remove-orphans", call_args)

    def test_compose_logs(self):
        """Test docker-compose logs command."""
        self.docker_manager.compose_logs(service="web", follow=True, tail=50)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker-compose", call_args)
        self.assertIn("logs", call_args)
        self.assertIn("-f", call_args)
        self.assertIn("--tail", call_args)
        self.assertIn("50", call_args)
        self.assertIn("web", call_args)

    def test_compose_ps(self):
        """Test docker-compose ps command."""
        self.docker_manager.compose_ps(quiet=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker-compose", call_args)
        self.assertIn("ps", call_args)
        self.assertIn("-q", call_args)

    def test_network_create(self):
        """Test docker network create command."""
        self.docker_manager.network_create("my-network", driver="bridge")
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("network", call_args)
        self.assertIn("create", call_args)
        self.assertIn("--driver", call_args)
        self.assertIn("bridge", call_args)
        self.assertIn("my-network", call_args)

    def test_network_rm(self):
        """Test docker network rm command."""
        self.docker_manager.network_rm("my-network")
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("network", call_args)
        self.assertIn("rm", call_args)
        self.assertIn("my-network", call_args)

    def test_network_ls(self):
        """Test docker network ls command."""
        self.docker_manager.network_ls(quiet=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("network", call_args)
        self.assertIn("ls", call_args)
        self.assertIn("-q", call_args)

    def test_volume_create(self):
        """Test docker volume create command."""
        self.docker_manager.volume_create("my-volume")
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("volume", call_args)
        self.assertIn("create", call_args)
        self.assertIn("my-volume", call_args)

    def test_volume_rm(self):
        """Test docker volume rm command."""
        self.docker_manager.volume_rm("my-volume", force=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("volume", call_args)
        self.assertIn("rm", call_args)
        self.assertIn("-f", call_args)
        self.assertIn("my-volume", call_args)

    def test_volume_ls(self):
        """Test docker volume ls command."""
        self.docker_manager.volume_ls(quiet=True)
        
        call_args = self.mock_sandbox_instance.process.exec.call_args[0]
        self.assertIn("docker", call_args)
        self.assertIn("volume", call_args)
        self.assertIn("ls", call_args)
        self.assertIn("-q", call_args)


class TestDockerDaemonWaiting(TestCase):
    def setUp(self):
        """Set up mock sandbox instance for daemon waiting tests."""
        self.mock_sandbox_instance = MagicMock(spec=SandboxInstance)
        self.mock_sandbox_instance.process = MagicMock()

    def test_wait_for_daemon_success(self):
        """Test successful daemon wait."""
        # Mock docker info to succeed immediately
        mock_process = MagicMock()
        mock_process.wait = MagicMock(return_value=0)
        self.mock_sandbox_instance.process.exec = MagicMock(return_value=mock_process)
        
        docker_manager = SandboxDockerManager(
            self.mock_sandbox_instance,
            auto_wait_for_daemon=False,
            daemon_timeout=5
        )
        
        result = docker_manager.wait_for_daemon(timeout=5)
        self.assertTrue(result)
        self.assertTrue(docker_manager._docker_ready)

    def test_wait_for_daemon_timeout(self):
        """Test daemon wait timeout."""
        # Mock docker info to always fail
        mock_process = MagicMock()
        mock_process.wait = MagicMock(return_value=1)
        self.mock_sandbox_instance.process.exec = MagicMock(return_value=mock_process)
        
        docker_manager = SandboxDockerManager(
            self.mock_sandbox_instance,
            auto_wait_for_daemon=False,
            daemon_timeout=1
        )
        
        result = docker_manager.wait_for_daemon(timeout=1)
        self.assertFalse(result)
        self.assertTrue(docker_manager._docker_ready_checked)

    def test_is_daemon_ready(self):
        """Test is_daemon_ready check."""
        # Mock docker info to succeed
        mock_process = MagicMock()
        mock_process.wait = MagicMock(return_value=0)
        self.mock_sandbox_instance.process.exec = MagicMock(return_value=mock_process)
        
        docker_manager = SandboxDockerManager(
            self.mock_sandbox_instance,
            auto_wait_for_daemon=False
        )
        
        result = docker_manager.is_daemon_ready()
        self.assertTrue(result)
        self.assertTrue(docker_manager._docker_ready)

    def test_auto_wait_for_daemon(self):
        """Test automatic daemon waiting on first command."""
        # Mock docker info to succeed
        mock_process = MagicMock()
        mock_process.wait = MagicMock(return_value=0)
        self.mock_sandbox_instance.process.exec = MagicMock(return_value=mock_process)
        
        docker_manager = SandboxDockerManager(
            self.mock_sandbox_instance,
            auto_wait_for_daemon=True,
            daemon_timeout=5
        )
        
        # First command should trigger daemon wait
        self.assertFalse(docker_manager._docker_ready_checked)
        docker_manager.ps()
        
        # Should have checked daemon readiness
        self.assertTrue(docker_manager._docker_ready_checked)
        
        # docker info should have been called at least once for the wait
        self.mock_sandbox_instance.process.exec.assert_any_call("docker", "info")

    def test_no_auto_wait_when_disabled(self):
        """Test that auto-wait doesn't happen when disabled."""
        mock_process = MagicMock()
        mock_process.wait = MagicMock(return_value=0)
        self.mock_sandbox_instance.process.exec = MagicMock(return_value=mock_process)
        
        docker_manager = SandboxDockerManager(
            self.mock_sandbox_instance,
            auto_wait_for_daemon=False
        )
        
        # Should not check daemon when disabled
        docker_manager.ps()
        self.assertFalse(docker_manager._docker_ready_checked)


class TestSandboxWithDocker(TestCase):
    def test_sandbox_docker_enabled_parameter(self):
        """Test that Sandbox accepts docker_enabled parameter."""
        sandbox = Sandbox(docker_enabled=True)
        # The docker_enabled parameter should be passed to the parent Pod class
        self.assertTrue(hasattr(sandbox, 'docker_enabled'))

    @patch('beta9.abstractions.sandbox.GatewayServiceStub')
    @patch('beta9.abstractions.sandbox.PodServiceStub')
    def test_sandbox_instance_has_docker_manager(self, mock_pod_stub, mock_gateway_stub):
        """Test that SandboxInstance has a docker attribute."""
        # Create a minimal SandboxInstance
        instance = SandboxInstance(
            container_id="test-123",
            stub_id="stub-456",
            ok=True,
            error_msg=""
        )
        
        # Check that the docker manager is initialized
        self.assertIsInstance(instance.docker, SandboxDockerManager)
        self.assertEqual(instance.docker.sandbox_instance, instance)
        
        # Check that auto_wait is enabled by default
        self.assertTrue(instance.docker.auto_wait_for_daemon)
        self.assertEqual(instance.docker.daemon_timeout, 30)
