"""
Example demonstrating the simplified Docker manager in sandboxes.

The new Docker manager provides a cleaner API where:
- Query commands return data directly (strings/lists)
- Action commands return bool or raise exceptions
- Container operations are intuitive and Pythonic
"""

from beta9 import Image, Sandbox


def example_docker_basics():
    """Basic Docker operations with the simplified API."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()

    # Pull an image
    print("Pulling nginx...")
    sandbox.docker.pull("nginx:alpine")
    
    # Run a detached container - returns container ID
    print("Starting nginx container...")
    container_id = sandbox.docker.run(
        "nginx:alpine",
        name="web-server",
        ports={"80": "8080"},
        detach=True,
    )
    print(f"Container ID: {container_id}")
    
    # List containers - returns formatted output
    print("\nRunning containers:")
    print(sandbox.docker.ps())
    
    # Get container IDs only - returns list
    container_ids = sandbox.docker.ps(quiet=True)
    print(f"\nContainer IDs: {container_ids}")
    
    # View logs
    print("\nContainer logs:")
    logs = sandbox.docker.logs("web-server", tail=10)
    logs.wait()
    print(logs.stdout.read())
    
    # Stop and remove
    print("\nCleaning up...")
    sandbox.docker.stop("web-server")
    sandbox.docker.rm("web-server")
    
    sandbox.terminate()
    print("✓ Example complete")


def example_docker_build():
    """Build and run a custom image."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Create a simple Dockerfile
    sandbox.files.write(
        "/app/Dockerfile",
        """FROM python:3.11-slim
RUN pip install flask
CMD ["python", "-c", "print('Hello from Docker!')"]
"""
    )
    
    # Build the image
    print("Building image...")
    build_process = sandbox.docker.build(
        tag="myapp:v1",
        context="/app",
    )
    build_process.wait()
    print("Build output:")
    print(build_process.stdout.read())
    
    # Run the built image
    print("\nRunning built image...")
    run_process = sandbox.docker.run("myapp:v1")
    run_process.wait()
    print(run_process.stdout.read())
    
    # List images - returns formatted output
    print("\nAvailable images:")
    print(sandbox.docker.images())
    
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
    
    # Create docker-compose.yml
    sandbox.files.write(
        "/app/docker-compose.yml",
        """version: '3'
services:
  web:
    image: nginx:alpine
    ports:
      - "8080:80"
  redis:
    image: redis:alpine
"""
    )
    
    # Start services
    print("Starting compose services...")
    sandbox.docker.compose_up("/app/docker-compose.yml")
    
    # List services
    print("\nRunning services:")
    print(sandbox.docker.compose_ps("/app/docker-compose.yml"))
    
    # View logs
    print("\nService logs:")
    logs = sandbox.docker.compose_logs("/app/docker-compose.yml")
    logs.wait()
    print(logs.stdout.read())
    
    # Stop services
    print("\nStopping services...")
    sandbox.docker.compose_down("/app/docker-compose.yml", volumes=True)
    
    sandbox.terminate()
    print("✓ Compose example complete")


def example_docker_advanced():
    """Advanced Docker operations."""
    sandbox = Sandbox(
        cpu=2,
        memory="4Gi",
        image=Image.from_registry("ubuntu:22.04").with_docker(),
        docker_enabled=True,
    ).create()
    
    # Create custom network
    print("Creating network...")
    sandbox.docker.network_create("mynet", driver="bridge")
    
    # Create volume
    print("Creating volume...")
    sandbox.docker.volume_create("mydata")
    
    # Run container with network and volume
    print("Running container with network and volume...")
    container_id = sandbox.docker.run(
        "alpine",
        command=["sh", "-c", "echo 'data' > /data/file.txt && cat /data/file.txt"],
        network="mynet",
        volumes={"mydata": "/data"},
        name="worker",
    )
    
    # Wait and show output
    container_id_process = sandbox.sandbox_instance.process.get(int(container_id.split()[0])) if not isinstance(container_id, str) else None
    
    # List networks
    print("\nNetworks:")
    print(sandbox.docker.network_ls())
    
    # List volumes  
    print("\nVolumes:")
    print(sandbox.docker.volume_ls())
    
    # Cleanup
    print("\nCleaning up...")
    sandbox.docker.rm("worker", force=True)
    sandbox.docker.volume_rm("mydata")
    sandbox.docker.network_rm("mynet")
    
    sandbox.terminate()
    print("✓ Advanced example complete")


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
    
    print("\n4. Advanced Operations")
    print("-" * 50)
    example_docker_advanced()
    
    print("\n" + "=" * 50)
    print("All examples completed successfully! ✓")
