# Docker Manager for Beta9 Sandboxes

This document describes the Docker manager functionality for Beta9 Sandboxes, which allows you to run Docker commands and manage Docker containers within isolated sandbox environments.

## Overview

The Docker manager provides a high-level Python interface for executing Docker commands within a Beta9 Sandbox. It uses `sandbox.process.exec` under the hood to run Docker CLI commands, offering a clean and intuitive API for Docker operations.

### Automatic Docker Daemon Waiting

The Docker daemon takes a few seconds to start when a sandbox is created. The Docker manager **automatically waits** for the daemon to be ready before executing commands, ensuring your Docker operations succeed without manual waiting or error handling.

## Prerequisites

To use Docker within a sandbox, you must:

1. Enable Docker support by setting `docker_enabled=True` when creating the Sandbox
2. Ensure the sandbox has sufficient resources (CPU and memory) for your Docker workloads

## Basic Usage

```python
from beta9 import Sandbox

# Create a sandbox with Docker enabled
sandbox = Sandbox(docker_enabled=True, cpu=2.0, memory="4Gi")
instance = sandbox.create()

# Use Docker operations
instance.docker.pull("nginx:latest")
instance.docker.run("nginx:latest", detach=True, ports={"80": "8080"})
instance.docker.ps()

# Clean up
instance.terminate()
```

## Docker Daemon Management

### Automatic Waiting (Default Behavior)

By default, the Docker manager automatically waits for the Docker daemon to be ready before executing any command. The first time you run a Docker command, the manager will:

1. Poll the Docker daemon with `docker info`
2. Wait up to 30 seconds (configurable) for the daemon to respond
3. Once ready, all subsequent commands execute immediately

```python
# No special code needed - just use Docker commands directly!
instance = sandbox.create()
instance.docker.pull("nginx:latest")  # Automatically waits for daemon
```

### Manual Daemon Control

If you need more control over daemon readiness:

```python
# Check if daemon is ready (non-blocking)
if instance.docker.is_daemon_ready():
    print("Ready to use Docker!")

# Manually wait with custom timeout
if instance.docker.wait_for_daemon(timeout=60):
    print("Daemon ready!")
else:
    print("Daemon failed to start in time")

# Disable auto-wait (for advanced use cases)
# Note: You'll need to manually check readiness
instance.docker.auto_wait_for_daemon = False
```

### Configuration

You can configure daemon waiting behavior when the Docker manager is created:

- `auto_wait_for_daemon` (bool, default=True): Enable automatic waiting
- `daemon_timeout` (int, default=30): Maximum seconds to wait for daemon

## API Reference

The `SandboxDockerManager` is accessible via `instance.docker` and provides the following methods:

### Daemon Management

#### `is_daemon_ready()`
Check if the Docker daemon is ready without waiting.

**Returns:** bool - True if daemon is ready

**Example:**
```python
if instance.docker.is_daemon_ready():
    print("Docker is ready!")
```

#### `wait_for_daemon(timeout=None)`
Manually wait for the Docker daemon to be ready.

**Parameters:**
- `timeout` (int, optional): Maximum seconds to wait

**Returns:** bool - True if daemon became ready

**Example:**
```python
if instance.docker.wait_for_daemon(timeout=60):
    print("Docker is ready!")
```

### Container Management

#### `build(tag, context=".", dockerfile=None, build_args=None, no_cache=False, quiet=False)`
Build a Docker image from a Dockerfile.

**Parameters:**
- `tag` (str): Image tag (e.g., "my-app:v1")
- `context` (str): Build context path
- `dockerfile` (str, optional): Path to Dockerfile
- `build_args` (dict, optional): Build-time variables
- `no_cache` (bool): Don't use cache when building
- `quiet` (bool): Suppress build output

**Example:**
```python
instance.docker.build(
    tag="my-app:v1",
    context="/workspace/app",
    build_args={"VERSION": "1.0"}
).wait()
```

#### `run(image, command=None, detach=False, remove=False, name=None, ports=None, volumes=None, env=None, network=None, extra_args=None)`
Run a Docker container.

**Parameters:**
- `image` (str): Docker image to run
- `command` (str | list, optional): Command to run in container
- `detach` (bool): Run in background
- `remove` (bool): Auto-remove container when it exits
- `name` (str, optional): Container name
- `ports` (dict, optional): Port mappings {"container_port": "host_port"}
- `volumes` (dict, optional): Volume mappings {"host_path": "container_path"}
- `env` (dict, optional): Environment variables
- `network` (str, optional): Network to connect to
- `extra_args` (list, optional): Additional docker run arguments

**Example:**
```python
instance.docker.run(
    "nginx:latest",
    detach=True,
    name="my-nginx",
    ports={"80": "8080"},
    env={"ENV": "production"}
).wait()
```

#### `ps(all=False, quiet=False, filters=None)`
List Docker containers.

**Example:**
```python
process = instance.docker.ps(all=True)
process.wait()
print(process.stdout.read())
```

#### `stop(container, timeout=None)`
Stop a running container.

**Example:**
```python
instance.docker.stop("my-nginx").wait()
```

#### `start(container)`
Start a stopped container.

**Example:**
```python
instance.docker.start("my-nginx").wait()
```

#### `restart(container, timeout=None)`
Restart a container.

**Example:**
```python
instance.docker.restart("my-nginx").wait()
```

#### `rm(container, force=False, volumes=False)`
Remove a container.

**Example:**
```python
instance.docker.rm("my-nginx", force=True).wait()
```

#### `logs(container, follow=False, tail=None, timestamps=False)`
Fetch container logs.

**Example:**
```python
process = instance.docker.logs("my-nginx", tail=100)
process.wait()
print(process.stdout.read())
```

#### `exec(container, command, detach=False, interactive=False, tty=False, env=None, workdir=None)`
Execute a command in a running container.

**Example:**
```python
process = instance.docker.exec("my-nginx", "ls -la /app")
process.wait()
print(process.stdout.read())
```

### Image Management

#### `pull(image, quiet=False)`
Pull an image from a registry.

**Example:**
```python
instance.docker.pull("postgres:15").wait()
```

#### `images(all=False, quiet=False, filters=None)`
List Docker images.

**Example:**
```python
process = instance.docker.images()
process.wait()
print(process.stdout.read())
```

#### `rmi(image, force=False)`
Remove an image.

**Example:**
```python
instance.docker.rmi("my-old-image:v1", force=True).wait()
```

### Docker Compose

#### `compose_up(file="docker-compose.yml", detach=True, build=False, extra_args=None)`
Start services defined in a docker-compose file.

**Example:**
```python
instance.docker.compose_up(
    file="/workspace/docker-compose.yml",
    build=True
).wait()
```

#### `compose_down(file="docker-compose.yml", volumes=False, remove_orphans=False)`
Stop and remove containers from a docker-compose file.

**Example:**
```python
instance.docker.compose_down(volumes=True).wait()
```

#### `compose_logs(file="docker-compose.yml", follow=False, tail=None, service=None)`
View logs from compose services.

**Example:**
```python
process = instance.docker.compose_logs(service="web", tail=50)
process.wait()
print(process.stdout.read())
```

#### `compose_ps(file="docker-compose.yml", quiet=False)`
List containers for compose services.

### Network Management

#### `network_create(name, driver=None)`
Create a Docker network.

**Example:**
```python
instance.docker.network_create("my-network", driver="bridge").wait()
```

#### `network_rm(name)`
Remove a Docker network.

**Example:**
```python
instance.docker.network_rm("my-network").wait()
```

#### `network_ls(quiet=False)`
List Docker networks.

**Example:**
```python
process = instance.docker.network_ls()
process.wait()
print(process.stdout.read())
```

### Volume Management

#### `volume_create(name)`
Create a Docker volume.

**Example:**
```python
instance.docker.volume_create("my-volume").wait()
```

#### `volume_rm(name, force=False)`
Remove a Docker volume.

**Example:**
```python
instance.docker.volume_rm("my-volume", force=True).wait()
```

#### `volume_ls(quiet=False)`
List Docker volumes.

**Example:**
```python
process = instance.docker.volume_ls()
process.wait()
print(process.stdout.read())
```

## Complete Examples

See `sdk/examples/docker_sandbox_example.py` for complete working examples including:

1. **Basic Docker Usage** - Pull images, run containers, view logs
2. **Building Docker Images** - Create and build custom images
3. **Docker Compose** - Multi-container applications
4. **Networks and Volumes** - Advanced Docker networking and storage

## Important Notes

1. **Automatic Daemon Waiting**: The Docker manager automatically waits for the Docker daemon to be ready (up to 30 seconds by default) before executing your first Docker command. No manual waiting required!

2. **Resource Requirements**: Docker-in-Docker operations require more resources. Allocate sufficient CPU and memory to your sandbox.

3. **Security**: The `docker_enabled=True` flag grants elevated capabilities to run Docker within the sandbox. Only enable this for trusted workloads.

4. **Process Management**: All Docker commands return a `SandboxProcess` object. Remember to call `.wait()` to wait for command completion.

5. **Output Handling**: Use `process.stdout.read()` and `process.stderr.read()` to access command output.

6. **Cleanup**: Always terminate your sandbox when done to free up resources:
   ```python
   instance.terminate()
   ```

## Architecture

The Docker manager is implemented as a helper class that wraps Docker CLI commands using the sandbox's process execution capabilities. Each method:

1. Constructs the appropriate Docker command with flags and arguments
2. Executes the command via `sandbox.process.exec()`
3. Returns a `SandboxProcess` object for output streaming and status monitoring

This design keeps the implementation clean and maintainable while providing full access to Docker functionality.

## Testing

Unit tests are available in `sdk/tests/test_sandbox_docker.py`. These tests verify that:

- Docker commands are constructed correctly
- Parameters are properly passed to the process executor
- The Docker manager integrates correctly with SandboxInstance

## Future Enhancements

Potential future improvements:

- Higher-level abstractions for common Docker patterns
- Built-in helpers for popular container registries
- Kubernetes integration
- Docker build caching optimizations
- Container lifecycle management helpers

## Contributing

When adding new Docker commands to the manager:

1. Follow the existing method naming conventions
2. Document all parameters with types and examples
3. Return a `SandboxProcess` object for consistency
4. Add corresponding unit tests
5. Update this README with the new functionality

## Support

For issues or questions about the Docker manager:

1. Check the examples in `sdk/examples/docker_sandbox_example.py`
2. Review the unit tests in `sdk/tests/test_sandbox_docker.py`
3. Consult the main Beta9 documentation
4. Open an issue on the repository
