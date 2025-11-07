"""
Example demonstrating the Docker manager in sandboxes with streaming logs.

The Docker manager now returns DockerResult objects that provide:
- Streaming logs via .logs()
- Success checking via .success property
- Output access via .output property
- Automatic waiting when accessing properties
"""

from beta9 import Image, Sandbox
import tempfile
import os


def example_docker_basics():
    """Basic Docker operations with the new API."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()

    # Pull an image - stream the progress
    print("Pulling nginx...")
    result = sandbox.docker.pull("nginx:alpine")
    for line in result.logs():
        if "Pull" in line or "Download" in line:
            print(f"  {line.strip()}")
    print(f"✓ Pull {'succeeded' if result.success else 'failed'}")
    
    # Run a detached container - get container ID
    print("\nStarting nginx container...")
    result = sandbox.docker.run(
        "nginx:alpine",
        name="web-server",
        ports={"80": "8080"},
        detach=True,
    )
    container_id = result.output  # Auto-waits and returns ID
    print(f"Container ID: {container_id[:12]}...")
    
    # List containers
    print("\nRunning containers:")
    print(sandbox.docker.ps())
    
    # Stop and remove
    print("\nCleaning up...")
    sandbox.docker.stop("web-server")
    sandbox.docker.rm("web-server")
    
    sandbox.terminate()
    print("✓ Example complete")


def example_docker_build():
    """Build and run a custom image with streaming logs."""
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
WORKDIR /app
RUN echo "Installing dependencies..." && pip install --no-cache-dir flask
CMD ["python", "-c", "print('Hello from Docker!')"]
""")
        
        # Upload Dockerfile to sandbox
        sandbox.fs.upload_file(dockerfile_path, "/workspace/Dockerfile")
    
    # Build the image with streaming logs
    print("Building image (streaming logs):")
    print("=" * 50)
    result = sandbox.docker.build(tag="myapp:v1", context="/workspace")
    for line in result.logs():
        print(f"  {line.strip()}")
    print("=" * 50)
    print(f"✓ Build {'succeeded' if result.success else 'failed'}")
    
    if result.success:
        # Run the built image
        print("\nRunning built image:")
        run_result = sandbox.docker.run("myapp:v1")
        print(f"Output: {run_result.stdout.strip()}")
    
    sandbox.terminate()
    print("✓ Build example complete")


def example_docker_compose():
    """Use Docker Compose."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Create docker-compose.yml locally and upload
    with tempfile.TemporaryDirectory() as tmpdir:
        compose_path = os.path.join(tmpdir, "docker-compose.yml")
        with open(compose_path, "w") as f:
            f.write("""version: '3'
services:
  web:
    image: nginx:alpine
    ports:
      - "8080:80"
  redis:
    image: redis:alpine
""")
        sandbox.fs.upload_file(compose_path, "/workspace/docker-compose.yml")
    
    # Pull images first
    print("Pulling images...")
    for image in ["nginx:alpine", "redis:alpine"]:
        result = sandbox.docker.pull(image, quiet=True)
        result.wait()
        print(f"  ✓ {image}")
    
    # Start services
    print("\nStarting compose services...")
    result = sandbox.docker.compose_up("/workspace/docker-compose.yml")
    for line in result.logs():
        if "Started" in line or "Creating" in line:
            print(f"  {line.strip()}")
    
    # List services
    print("\nRunning services:")
    services = sandbox.docker.compose_ps("/workspace/docker-compose.yml")
    print(services)
    
    # Stop services
    print("\nStopping services...")
    sandbox.docker.compose_down("/workspace/docker-compose.yml", volumes=True)
    
    sandbox.terminate()
    print("✓ Compose example complete")


def example_error_handling():
    """Handle Docker errors gracefully."""
    from beta9.exceptions import DockerCommandError
    
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Try to pull a non-existent image
    print("Attempting to pull non-existent image...")
    result = sandbox.docker.pull("nonexistent:image12345")
    
    # Check result
    if not result.success:
        print(f"✓ Pull failed as expected")
        print(f"Error: {result.stderr[:100]}...")
    
    # Try to run a container that exits with error
    print("\nRunning container that will fail...")
    result = sandbox.docker.run("alpine", command=["sh", "-c", "exit 1"])
    
    if not result.success:
        print("✓ Container failed as expected")
    
    sandbox.terminate()
    print("✓ Error handling example complete")


def example_streaming_vs_blocking():
    """Compare streaming logs vs blocking wait."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Create Dockerfile locally and upload
    with tempfile.TemporaryDirectory() as tmpdir:
        dockerfile_path = os.path.join(tmpdir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write("""FROM alpine
RUN echo "Step 1: Setting up..."
RUN sleep 1 && echo "Step 2: Installing packages..."
RUN sleep 1 && echo "Step 3: Configuring..."
RUN sleep 1 && echo "Step 4: Done!"
""")
        sandbox.fs.upload_file(dockerfile_path, "/workspace/Dockerfile")
    
    print("Method 1: Streaming logs (see progress in real-time)")
    print("=" * 50)
    result = sandbox.docker.build(tag="streaming:v1", context="/workspace")
    for line in result.logs():
        if "Step" in line or "RUN" in line or "#" in line:
            print(f"  {line.strip()}")
    print("=" * 50)
    print(f"✓ Build {'succeeded' if result.success else 'failed'}\n")
    
    print("Method 2: Blocking wait (no output until done)")
    print("=" * 50)
    result = sandbox.docker.build(tag="blocking:v1", context="/workspace")
    result.wait()  # Blocks until complete
    print("✓ Build complete (waited for completion)")
    print(f"Success: {result.success}")
    
    sandbox.terminate()
    print("\n✓ Comparison complete")


if __name__ == "__main__":
    print("=== Docker Sandbox Examples ===\n")
    
    print("1. Basic Docker Operations")
    print("-" * 50)
    example_docker_basics()
    
    print("\n2. Building Custom Images")
    print("-" * 50)
    example_docker_build()
    
    print("\n3. Docker Compose")
    print("-" * 50)
    example_docker_compose()
    
    print("\n4. Error Handling")
    print("-" * 50)
    example_error_handling()
    
    print("\n5. Streaming vs Blocking")
    print("-" * 50)
    example_streaming_vs_blocking()
    
    print("\n" + "=" * 50)
    print("All examples completed successfully! ✓")
