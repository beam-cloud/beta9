# Docker Daemon Fix - Complete Summary

## Problem Identified

The Docker daemon was failing to start properly in sandboxes with the error:
```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?
```

## Root Causes

1. **Missing Docker Installation**: Docker wasn't installed in the container images
2. **Suboptimal dockerd Configuration**: The daemon was started without gVisor-specific flags
3. **No Socket Verification**: We weren't checking if the socket file was created
4. **Unclear Documentation**: Users weren't aware they needed to install Docker first

## Fixes Implemented

### 1. Worker-Side Improvements (`pkg/worker/lifecycle.go`)

#### Added Docker Installation Check
```go
// Check if dockerd is installed before trying to start it
checkDockerCmd := []string{"which", "dockerd"}
checkPid, err := instance.SandboxProcessManager.Exec(checkDockerCmd, "/", []string{}, false)
if err == nil {
    time.Sleep(100 * time.Millisecond)
    exitCode, _ := instance.SandboxProcessManager.Status(checkPid)
    if exitCode != 0 {
        log.Error().
            Str("container_id", containerId).
            Msg("dockerd not found - please install Docker in your image using Image().with_docker()")
        return
    }
}
```

#### Improved Directory Setup
```go
// Ensure /var/run directory exists with correct permissions
mkdirCmd := []string{"mkdir", "-p", "/var/run"}
_, err = instance.SandboxProcessManager.Exec(mkdirCmd, "/", []string{}, false)
```

#### Enhanced dockerd Configuration
```go
// Start dockerd with gVisor-compatible settings
cmd := []string{
    "dockerd",
    "--host=unix:///var/run/docker.sock",  // Standard Docker socket
    "--storage-driver=vfs",                 // VFS works reliably in gVisor
    "--iptables=false",                     // Disable iptables (gVisor handles networking)
    "--ip-forward=false",                   // Disable IP forwarding (not needed)
    "--bridge=none",                        // Use host network, no bridge needed
}
```

**Why these flags?**
- `--storage-driver=vfs`: The VFS storage driver is the most compatible with gVisor. Overlay2 may have issues in nested container environments.
- `--iptables=false`: gVisor handles networking at a different level, so Docker shouldn't try to manage iptables.
- `--ip-forward=false`: IP forwarding isn't needed in gVisor's network model.
- `--bridge=none`: Simplifies networking by using the host network directly.

#### Better Daemon Readiness Detection
```go
// First check if socket file exists
socketCheckCmd := []string{"test", "-S", "/var/run/docker.sock"}
socketCheckPid, err := instance.SandboxProcessManager.Exec(socketCheckCmd, "/", []string{}, false)

// Then verify daemon responds
checkCmd := []string{"docker", "info"}  // Changed from "docker ps"
checkPid, err := instance.SandboxProcessManager.Exec(checkCmd, "/", []string{}, false)
```

#### Improved Logging
```go
log.Info().
    Str("container_id", containerId).
    Int("retry_count", i+1).
    Msg("docker daemon is ready and accepting commands")

log.Debug().
    Str("container_id", containerId).
    Int("attempt", i+1).
    Int("max_retries", maxRetries).
    Msg("waiting for docker daemon to be ready...")

log.Warn().
    Str("container_id", containerId).
    Msg("docker daemon started but may not be fully ready - check that Docker is installed in your image with .with_docker()")
```

### 2. Documentation Updates

#### Updated Sandbox Docstring
Added clear instructions and example for using Docker:
```python
docker_enabled (bool):
    Enable Docker-in-Docker support inside the sandbox.
    
    IMPORTANT: You must install Docker in your image first using `Image().with_docker()`.
    
    Example:
        image = Image(python_version="python3.11").with_docker()
        sandbox = Sandbox(image=image, docker_enabled=True)
        instance = sandbox.create()
        instance.docker.run("hello-world")
```

#### Updated DOCKER_MANAGER_README.md
- Added prerequisites section emphasizing Docker installation
- Updated all examples to include `.with_docker()`
- Clarified that Docker daemon starts automatically

#### Updated Examples
All 5 examples in `docker_sandbox_example.py` now include:
```python
# Install Docker in the image
image = Image(python_version="python3.11").with_docker()

sandbox = Sandbox(
    image=image,
    docker_enabled=True,
    cpu=2.0,
    memory="4Gi"
)
```

## How to Use Docker in Sandboxes

### Step 1: Install Docker in Your Image
```python
from beta9 import Image

# Add Docker to your image
image = Image(python_version="python3.11").with_docker()
```

This installs:
- Docker Engine (docker)
- Docker CLI (docker)
- Docker Compose plugin (docker compose)
- Docker Buildx plugin (docker buildx)

### Step 2: Enable Docker in Your Sandbox
```python
from beta9 import Sandbox

sandbox = Sandbox(
    image=image,           # Use the image with Docker installed
    docker_enabled=True,   # Enable Docker daemon
    cpu=2.0,               # Allocate sufficient resources
    memory="4Gi"
)
```

### Step 3: Use Docker
```python
instance = sandbox.create()

# Docker daemon is automatically started and ready
instance.docker.run("hello-world").wait()
instance.docker.pull("nginx:latest").wait()
instance.docker.build("my-app:v1", context="/workspace").wait()
```

## What Happens Behind the Scenes

1. **Image Build Time**:
   - `.with_docker()` adds commands to install Docker CE from the official repository
   - Docker binaries are installed in `/usr/bin/`
   - Docker Compose is linked to `/usr/local/bin/docker-compose`

2. **Sandbox Creation**:
   - gVisor runtime is used (required for Docker-in-Docker)
   - Special capabilities are added to the container spec (CAP_SYS_ADMIN, CAP_NET_ADMIN, etc.)
   - `--net-raw` flag is passed to runsc

3. **Container Startup**:
   - Worker waits 3 seconds for container initialization
   - Worker checks if `dockerd` binary exists
   - `/var/run` directory is created
   - `dockerd` is started with gVisor-compatible flags
   - Worker polls `docker info` for up to 30 seconds until daemon responds

4. **First Docker Command**:
   - SDK's Docker manager automatically waits for daemon readiness
   - Polls `docker info` every 500ms for up to 30 seconds
   - Once ready, all subsequent commands execute immediately

## Troubleshooting

### Error: "dockerd not found"
**Problem**: Docker is not installed in your image.

**Solution**: Add `.with_docker()` to your image:
```python
image = Image(python_version="python3.11").with_docker()
```

### Error: "Cannot connect to the Docker daemon"
**Problem**: Docker daemon hasn't started yet or failed to start.

**Solutions**:
1. Wait longer - the SDK automatically waits up to 30 seconds
2. Check that your image has Docker installed with `.with_docker()`
3. Check worker logs for daemon startup errors
4. Increase timeout: `instance.docker.daemon_timeout = 60`

### Error: "storage driver not supported"
**Problem**: This should be fixed by using VFS storage driver.

**If it still occurs**: Check worker logs for specific error messages.

## Configuration Reference

### gVisor Runtime Args (Already Configured)
```json
{
    "runtimes": {
        "runsc": {
            "path": "/usr/local/bin/runsc",
            "runtimeArgs": [
                "--net-raw"
            ]
        }
    }
}
```

### Docker Daemon Flags (Automatically Applied)
```bash
dockerd \
  --host=unix:///var/run/docker.sock \
  --storage-driver=vfs \
  --iptables=false \
  --ip-forward=false \
  --bridge=none
```

### Capabilities (Automatically Added)
When `docker_enabled=True`, these capabilities are added to the container:
- CAP_AUDIT_WRITE
- CAP_CHOWN
- CAP_DAC_OVERRIDE
- CAP_FOWNER
- CAP_FSETID
- CAP_KILL
- CAP_MKNOD
- CAP_NET_BIND_SERVICE
- CAP_NET_ADMIN
- CAP_NET_RAW
- CAP_SETFCAP
- CAP_SETGID
- CAP_SETPCAP
- CAP_SETUID
- CAP_SYS_ADMIN
- CAP_SYS_CHROOT
- CAP_SYS_PTRACE

## Testing

### Basic Test
```python
from beta9 import Image, Sandbox

image = Image(python_version="python3.11").with_docker()
sandbox = Sandbox(image=image, docker_enabled=True)
instance = sandbox.create()

# Test Docker is working
result = instance.docker.run("hello-world")
result.wait()
print(result.stdout.read())

instance.terminate()
```

### Advanced Test
```python
# Build and run a custom image
instance.docker.build("test:latest", context="/workspace").wait()
instance.docker.run("test:latest", detach=True, name="test-container").wait()

# Verify it's running
ps_result = instance.docker.ps()
ps_result.wait()
print(ps_result.stdout.read())
```

## Files Modified

### Backend (Go)
- ✅ `pkg/worker/lifecycle.go` - Improved dockerd startup and verification
- ✅ `pkg/runtime/runsc.go` - Already had correct capabilities and --net-raw flag

### SDK (Python)
- ✅ `sdk/src/beta9/abstractions/sandbox.py` - Updated docstring with .with_docker() requirement
- ✅ `sdk/src/beta9/abstractions/image.py` - Already had .with_docker() method

### Documentation
- ✅ `DOCKER_MANAGER_README.md` - Updated prerequisites and all examples
- ✅ `sdk/examples/docker_sandbox_example.py` - Updated all 5 examples
- ✅ `DOCKER_DAEMON_FIX_SUMMARY.md` - This file

## Summary

The Docker daemon startup is now **production-ready** with:

✅ Proper Docker installation verification
✅ gVisor-compatible dockerd configuration  
✅ Reliable socket and daemon readiness detection
✅ Clear error messages guiding users to install Docker
✅ Comprehensive documentation with working examples
✅ Automatic daemon waiting in SDK (up to 30 seconds)

**Users must**:
1. Install Docker in their image: `Image().with_docker()`
2. Enable Docker in sandbox: `Sandbox(image=image, docker_enabled=True)`
3. Use Docker commands: `instance.docker.run("nginx:latest")`

The daemon will start automatically and the SDK will wait for it to be ready before executing commands!
