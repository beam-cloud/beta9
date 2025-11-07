"""
Example demonstrating how to use Docker within a Beta9 Sandbox.

This example shows various Docker operations that can be performed
within a sandbox using the SandboxDockerManager.
"""

from beta9 import Sandbox, Image


def example_basic_docker_usage():
    """Basic example of using Docker in a sandbox."""
    # Create a sandbox with Docker enabled
    sandbox = Sandbox(
        docker_enabled=True,
        cpu=2.0,
        memory="4Gi",
        keep_warm_seconds=1800,  # 30 minutes
    )
    
    instance = sandbox.create()
    
    try:
        # Note: The Docker manager automatically waits for the Docker daemon to be ready
        # before executing commands. This usually takes a few seconds after sandbox creation.
        
        # Pull a Docker image
        print("Pulling nginx image...")
        print("(Docker manager will wait for daemon to be ready if needed...)")
        process = instance.docker.pull("nginx:latest")
        process.wait()
        print("Image pulled successfully!")
        
        # List available images
        print("\nListing Docker images:")
        process = instance.docker.images()
        process.wait()
        print(process.stdout.read())
        
        # Run a container in detached mode
        print("\nRunning nginx container...")
        process = instance.docker.run(
            "nginx:latest",
            detach=True,
            name="my-nginx",
            ports={"80": "8080"}
        )
        process.wait()
        
        # List running containers
        print("\nListing running containers:")
        process = instance.docker.ps()
        process.wait()
        print(process.stdout.read())
        
        # Get logs from the container
        print("\nFetching container logs:")
        process = instance.docker.logs("my-nginx", tail=20)
        process.wait()
        print(process.stdout.read())
        
        # Execute a command in the running container
        print("\nExecuting command in container:")
        process = instance.docker.exec("my-nginx", ["ls", "-la", "/usr/share/nginx/html"])
        process.wait()
        print(process.stdout.read())
        
        # Stop the container
        print("\nStopping container...")
        process = instance.docker.stop("my-nginx")
        process.wait()
        
        # Remove the container
        print("\nRemoving container...")
        process = instance.docker.rm("my-nginx")
        process.wait()
        
        print("\nDocker operations completed successfully!")
        
    finally:
        # Clean up
        instance.terminate()


def example_docker_build():
    """Example of building a Docker image in a sandbox."""
    sandbox = Sandbox(
        docker_enabled=True,
        cpu=2.0,
        memory="4Gi",
    )
    
    instance = sandbox.create()
    
    try:
        # First, create a simple Dockerfile in the sandbox
        dockerfile_content = """
FROM python:3.11-slim
WORKDIR /app
RUN pip install flask
COPY app.py /app/
CMD ["python", "app.py"]
"""
        
        app_content = """
from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello from Docker in Beta9 Sandbox!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
"""
        
        # Upload files to sandbox
        print("Creating application files in sandbox...")
        instance.process.exec("mkdir", "-p", "/workspace/myapp").wait()
        
        # Write Dockerfile and app.py using process.exec with echo
        instance.process.exec(
            "sh", "-c", 
            f"echo '{dockerfile_content}' > /workspace/myapp/Dockerfile"
        ).wait()
        
        instance.process.exec(
            "sh", "-c",
            f"echo '{app_content}' > /workspace/myapp/app.py"
        ).wait()
        
        # Build the Docker image
        print("\nBuilding Docker image...")
        process = instance.docker.build(
            tag="my-flask-app:v1",
            context="/workspace/myapp",
            dockerfile="/workspace/myapp/Dockerfile"
        )
        process.wait()
        print("Image built successfully!")
        
        # List images to verify
        print("\nVerifying image was built:")
        process = instance.docker.images()
        process.wait()
        print(process.stdout.read())
        
        # Run the built image
        print("\nRunning the Flask application...")
        process = instance.docker.run(
            "my-flask-app:v1",
            detach=True,
            name="flask-app",
            ports={"5000": "5000"}
        )
        process.wait()
        
        print("\nFlask app is running in Docker container!")
        
        # Give it a moment to start
        import time
        time.sleep(2)
        
        # Check logs
        print("\nApplication logs:")
        process = instance.docker.logs("flask-app", tail=50)
        process.wait()
        print(process.stdout.read())
        
        # Cleanup
        print("\nCleaning up...")
        instance.docker.stop("flask-app").wait()
        instance.docker.rm("flask-app").wait()
        instance.docker.rmi("my-flask-app:v1", force=True).wait()
        
        print("\nBuild example completed!")
        
    finally:
        instance.terminate()


def example_docker_compose():
    """Example of using docker-compose in a sandbox."""
    sandbox = Sandbox(
        docker_enabled=True,
        cpu=2.0,
        memory="4Gi",
    )
    
    instance = sandbox.create()
    
    try:
        # Create a docker-compose.yml file
        compose_content = """
version: '3.8'

services:
  web:
    image: nginx:latest
    ports:
      - "8080:80"
    volumes:
      - ./html:/usr/share/nginx/html
  
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
"""
        
        html_content = "<h1>Hello from Docker Compose in Beta9!</h1>"
        
        # Create directory structure
        print("Setting up docker-compose project...")
        instance.process.exec("mkdir", "-p", "/workspace/compose-demo/html").wait()
        
        # Write files
        instance.process.exec(
            "sh", "-c",
            f"echo '{compose_content}' > /workspace/compose-demo/docker-compose.yml"
        ).wait()
        
        instance.process.exec(
            "sh", "-c",
            f"echo '{html_content}' > /workspace/compose-demo/html/index.html"
        ).wait()
        
        # Change to the compose directory and start services
        print("\nStarting services with docker-compose...")
        process = instance.docker.compose_up(
            file="/workspace/compose-demo/docker-compose.yml",
            detach=True
        )
        process.wait()
        
        print("\nServices started successfully!")
        
        # List running compose services
        print("\nListing compose services:")
        process = instance.docker.compose_ps(
            file="/workspace/compose-demo/docker-compose.yml"
        )
        process.wait()
        print(process.stdout.read())
        
        # View logs
        print("\nViewing compose logs:")
        process = instance.docker.compose_logs(
            file="/workspace/compose-demo/docker-compose.yml",
            tail=20
        )
        process.wait()
        print(process.stdout.read())
        
        # Stop services
        print("\nStopping compose services...")
        process = instance.docker.compose_down(
            file="/workspace/compose-demo/docker-compose.yml",
            volumes=True
        )
        process.wait()
        
        print("\nDocker Compose example completed!")
        
    finally:
        instance.terminate()


def example_docker_network_and_volumes():
    """Example of managing Docker networks and volumes."""
    sandbox = Sandbox(
        docker_enabled=True,
        cpu=2.0,
        memory="4Gi",
    )
    
    instance = sandbox.create()
    
    try:
        # Create a network
        print("Creating Docker network...")
        instance.docker.network_create("my-network", driver="bridge").wait()
        
        # List networks
        print("\nListing networks:")
        process = instance.docker.network_ls()
        process.wait()
        print(process.stdout.read())
        
        # Create a volume
        print("\nCreating Docker volume...")
        instance.docker.volume_create("my-volume").wait()
        
        # List volumes
        print("\nListing volumes:")
        process = instance.docker.volume_ls()
        process.wait()
        print(process.stdout.read())
        
        # Run containers using the network and volume
        print("\nRunning containers with custom network and volume...")
        instance.docker.run(
            "alpine:latest",
            command=["sh", "-c", "echo 'Hello' > /data/test.txt && sleep 300"],
            detach=True,
            name="alpine-container",
            network="my-network",
            volumes={"my-volume": "/data"}
        ).wait()
        
        # Verify
        print("\nVerifying container is running:")
        process = instance.docker.ps()
        process.wait()
        print(process.stdout.read())
        
        # Cleanup
        print("\nCleaning up...")
        instance.docker.stop("alpine-container").wait()
        instance.docker.rm("alpine-container").wait()
        instance.docker.volume_rm("my-volume", force=True).wait()
        instance.docker.network_rm("my-network").wait()
        
        print("\nNetwork and volume example completed!")
        
    finally:
        instance.terminate()


def example_docker_daemon_waiting():
    """Example of controlling Docker daemon waiting behavior."""
    sandbox = Sandbox(
        docker_enabled=True,
        cpu=2.0,
        memory="4Gi",
    )
    
    instance = sandbox.create()
    
    try:
        # Example 1: Default behavior (auto-wait enabled)
        print("Using default behavior (auto-wait for daemon)...")
        print("The first Docker command will automatically wait for the daemon.")
        process = instance.docker.ps()
        process.wait()
        print("✓ Docker daemon was ready!")
        
        # Example 2: Manually check daemon readiness
        print("\nManually checking daemon readiness...")
        if instance.docker.is_daemon_ready():
            print("✓ Daemon is ready!")
        else:
            print("✗ Daemon is not ready yet")
        
        # Example 3: Manually wait with custom timeout
        print("\nManually waiting for daemon with 60s timeout...")
        if instance.docker.wait_for_daemon(timeout=60):
            print("✓ Daemon became ready within timeout!")
        else:
            print("✗ Daemon did not become ready within timeout")
        
        # Example 4: Access the configuration
        print(f"\nDocker manager configuration:")
        print(f"  - auto_wait_for_daemon: {instance.docker.auto_wait_for_daemon}")
        print(f"  - daemon_timeout: {instance.docker.daemon_timeout}s")
        print(f"  - daemon ready: {instance.docker._docker_ready}")
        
    finally:
        instance.terminate()


if __name__ == "__main__":
    print("=" * 60)
    print("Beta9 Sandbox Docker Manager Examples")
    print("=" * 60)
    
    # Uncomment the examples you want to run:
    
    # Example 1: Basic Docker usage
    # print("\n\n### Example 1: Basic Docker Usage ###\n")
    # example_basic_docker_usage()
    
    # Example 2: Building Docker images
    # print("\n\n### Example 2: Building Docker Images ###\n")
    # example_docker_build()
    
    # Example 3: Docker Compose
    # print("\n\n### Example 3: Docker Compose ###\n")
    # example_docker_compose()
    
    # Example 4: Networks and Volumes
    # print("\n\n### Example 4: Networks and Volumes ###\n")
    # example_docker_network_and_volumes()
    
    # Example 5: Docker Daemon Waiting
    # print("\n\n### Example 5: Docker Daemon Waiting ###\n")
    # example_docker_daemon_waiting()
    
    print("\n\nTo run these examples, uncomment them in the __main__ block.")
    print("Make sure you have a Beta9 workspace configured!")
