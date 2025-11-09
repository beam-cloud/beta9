import os
import tempfile
import pytest
from beta9 import Image, Sandbox


@pytest.mark.skipif(
    os.environ.get("BETA9_GATEWAY_HOST") is None,
    reason="Requires Beta9 gateway connection",
)
def test_sandbox_upload_file_visible_to_exec():
    """
    Test that files uploaded via fs.upload_file are visible to processes run via process.exec.
    This is a regression test for the gVisor rootfs overlay issue where files written to the
    host overlay path were not visible inside the sandbox.
    """
    # Create a sandbox with docker enabled
    sandbox = Sandbox(
        docker_enabled=True,
        image=Image().with_docker()
    ).create()
    
    try:
        # Create a test file locally
        test_content = "Hello from upload_file test!\n"
        test_path = "/tmp/test-upload-file.txt"
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp.write(test_content)
            local_path = tmp.name
        
        try:
            # Upload the file to the sandbox
            sandbox.fs.upload_file(local_path, test_path)
            
            # Verify the file exists using ls
            p = sandbox.process.exec("ls", "-la", test_path)
            p.wait()
            assert p.exit_code == 0, f"ls failed: {p.stderr()}"
            
            # Verify we can read the file using cat
            p = sandbox.process.exec("cat", test_path)
            p.wait()
            assert p.exit_code == 0, f"cat failed: {p.stderr()}"
            assert p.stdout() == test_content, f"File content mismatch: expected '{test_content}', got '{p.stdout()}'"
            
            # Verify we can stat the file using the fs API
            stat_info = sandbox.fs.stat_file(test_path)
            assert stat_info.size == len(test_content.encode()), f"Size mismatch: expected {len(test_content.encode())}, got {stat_info.size}"
            
        finally:
            os.unlink(local_path)
            
    finally:
        sandbox.terminate()


@pytest.mark.skipif(
    os.environ.get("BETA9_GATEWAY_HOST") is None,
    reason="Requires Beta9 gateway connection",
)
def test_sandbox_docker_compose_with_override():
    """
    Test that docker-compose can read override files uploaded via fs.upload_file.
    This is the original bug report - docker-compose couldn't find the override file.
    """
    # Create a sandbox with docker enabled
    sandbox = Sandbox(
        docker_enabled=True,
        image=Image().with_docker()
    ).create()
    
    try:
        # Create a simple docker-compose file
        compose_content = """services:
  test:
    image: alpine:latest
    command: echo "hello"
"""
        compose_path = "/tmp/docker-compose.yml"
        
        # Create an override file
        override_content = """services:
  test:
    command: echo "override"
"""
        override_path = "/tmp/docker-compose.override.yml"
        
        # Write both files to temp files and upload
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp:
            tmp.write(compose_content)
            local_compose = tmp.name
            
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp:
            tmp.write(override_content)
            local_override = tmp.name
        
        try:
            sandbox.fs.upload_file(local_compose, compose_path)
            sandbox.fs.upload_file(local_override, override_path)
            
            # Verify both files are readable
            p = sandbox.process.exec("cat", compose_path)
            p.wait()
            assert p.exit_code == 0, f"Failed to read compose file: {p.stderr()}"
            
            p = sandbox.process.exec("cat", override_path)
            p.wait()
            assert p.exit_code == 0, f"Failed to read override file: {p.stderr()}"
            
            # Verify docker-compose can see both files
            # (We use config command to validate without running containers)
            p = sandbox.process.exec("docker-compose", "-f", compose_path, "-f", override_path, "config", cwd="/tmp")
            p.wait()
            
            # docker-compose config should succeed and show the merged config
            assert p.exit_code == 0, f"docker-compose config failed: {p.stderr()}"
            output = p.stdout()
            assert "override" in output, f"Override not applied, output: {output}"
            
        finally:
            os.unlink(local_compose)
            os.unlink(local_override)
            
    finally:
        sandbox.terminate()


if __name__ == "__main__":
    # Allow running tests directly
    test_sandbox_upload_file_visible_to_exec()
    print("✓ test_sandbox_upload_file_visible_to_exec passed")
    
    test_sandbox_docker_compose_with_override()
    print("✓ test_sandbox_docker_compose_with_override passed")
    
    print("\nAll tests passed!")
