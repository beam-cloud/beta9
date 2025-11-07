"""
Example demonstrating Docker operations with log streaming.
"""

from beta9 import Image, Sandbox
import tempfile
import os


def example_build_with_logs():
    """Build a Docker image and stream the build logs."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Create a Dockerfile locally and upload it
    with tempfile.TemporaryDirectory() as tmpdir:
        dockerfile_path = os.path.join(tmpdir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write("""FROM python:3.11-slim
RUN echo "Installing dependencies..."
RUN pip install --no-cache-dir flask
RUN echo "Build complete!"
CMD ["python", "-c", "print('Hello from Docker!')"]
""")
        sandbox.fs.upload_file(dockerfile_path, "/workspace/Dockerfile")
    
    print("Building image with streaming logs:")
    print("=" * 50)
    
    # Build and stream logs in real-time
    result = sandbox.docker.build(tag="myapp:v1", context="/workspace")
    for line in result.logs():
        if line.strip():  # Skip empty lines
            print(f"  {line.strip()}")
    
    print("=" * 50)
    print(f"Build {'succeeded' if result.success else 'failed'}!")
    
    sandbox.terminate()


def example_pull_with_progress():
    """Pull an image and see the progress."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    print("Pulling nginx image:")
    print("=" * 50)
    
    # Pull and stream progress
    result = sandbox.docker.pull("nginx:alpine")
    for line in result.logs():
        print(f"  {line.strip()}")
    
    print("=" * 50)
    print(f"Pull {'succeeded' if result.success else 'failed'}!")
    
    sandbox.terminate()


def example_run_with_output():
    """Run a container and capture output."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Run a container and get output
    print("Running container:")
    result = sandbox.docker.run(
        "alpine",
        command=["sh", "-c", "echo 'Hello from Alpine!' && ls -la /"]
    )
    
    # Stream logs as they come
    print("Container output:")
    print("=" * 50)
    for line in result.logs():
        print(f"  {line.strip()}")
    
    print("=" * 50)
    print(f"Container exit code: {'0 (success)' if result.success else 'non-zero (failed)'}")
    
    sandbox.terminate()


def example_detached_container():
    """Run a detached container and get its ID."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Pull nginx first
    print("Pulling nginx...")
    pull_result = sandbox.docker.pull("nginx:alpine")
    pull_result.wait()
    
    # Run detached container
    print("Starting nginx container in background...")
    result = sandbox.docker.run(
        "nginx:alpine",
        name="web-server",
        ports={"80": "8080"},
        detach=True
    )
    
    # Get container ID (auto-waits)
    container_id = result.output
    print(f"Container ID: {container_id}")
    
    # Check it's running
    containers = sandbox.docker.ps()
    print(f"\nRunning containers:\n{containers}")
    
    # Stop and remove
    print("\nCleaning up...")
    sandbox.docker.stop("web-server")
    sandbox.docker.rm("web-server")
    
    sandbox.terminate()


def example_build_failure_handling():
    """Handle build failures gracefully."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Create a Dockerfile that will fail locally and upload
    with tempfile.TemporaryDirectory() as tmpdir:
        dockerfile_path = os.path.join(tmpdir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write("""FROM python:3.11-slim
RUN pip install nonexistent-package-12345
""")
        sandbox.fs.upload_file(dockerfile_path, "/workspace/Dockerfile")
    
    print("Attempting build that will fail:")
    print("=" * 50)
    
    result = sandbox.docker.build(tag="myapp:bad", context="/workspace")
    
    # Stream logs even on failure
    for line in result.logs():
        if line.strip():  # Skip empty lines
            print(f"  {line.strip()}")
    
    print("=" * 50)
    
    if not result.success:
        print("Build failed as expected!")
        print(f"Error output:\n{result.stderr}")
    
    sandbox.terminate()


def example_multiple_operations():
    """Chain multiple Docker operations."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Pull base image
    print("1. Pulling base image...")
    result = sandbox.docker.pull("python:3.11-slim")
    result.wait()
    print(f"   ✓ Pull {'succeeded' if result.success else 'failed'}")
    
    # Create Dockerfile locally and upload
    with tempfile.TemporaryDirectory() as tmpdir:
        dockerfile_path = os.path.join(tmpdir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write("""FROM python:3.11-slim
RUN pip install --no-cache-dir requests
CMD ["python", "-c", "import requests; print('Requests version:', requests.__version__)"]
""")
        sandbox.fs.upload_file(dockerfile_path, "/workspace/Dockerfile")
    
    # Build image
    print("\n2. Building custom image...")
    result = sandbox.docker.build(tag="myapp:latest", context="/workspace")
    result.wait()
    print(f"   ✓ Build {'succeeded' if result.success else 'failed'}")
    
    # Run container
    print("\n3. Running container...")
    result = sandbox.docker.run("myapp:latest")
    print(f"   Output: {result.stdout.strip()}")
    print(f"   ✓ Run {'succeeded' if result.success else 'failed'}")
    
    # List images
    print("\n4. Listing images...")
    images = sandbox.docker.images()
    print(f"   Images:\n{images}")
    
    sandbox.terminate()


if __name__ == "__main__":
    print("=== Docker with Log Streaming Examples ===\n")
    
    print("1. Build with Streaming Logs")
    print("-" * 50)
    example_build_with_logs()
    
    print("\n2. Pull with Progress")
    print("-" * 50)
    example_pull_with_progress()
    
    print("\n3. Run with Output")
    print("-" * 50)
    example_run_with_output()
    
    print("\n4. Detached Container")
    print("-" * 50)
    example_detached_container()
    
    print("\n5. Build Failure Handling")
    print("-" * 50)
    example_build_failure_handling()
    
    print("\n6. Multiple Operations")
    print("-" * 50)
    example_multiple_operations()
    
    print("\n" + "=" * 50)
    print("All examples completed! ✓")
