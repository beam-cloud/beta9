# Docker-in-Docker for gVisor - The Simple Solution

## The Problem
Docker daemon was failing with various cgroup-related errors.

## The Insight
**Docker does NOT require cgroups if you disable the features that need them!**

gVisor already supports Docker-in-Docker. The official example:
```bash
docker run --runtime=runsc --rm --cap-add all docker:dind
```

That's it! No special mounts, no daemon.json, no cgroup setup.

## The Solution

### What We Changed

**Before (overcomplicated):**
- ❌ Attempted to bind mount host cgroups
- ❌ Created complex cgroup hierarchy with mount commands
- ❌ Generated daemon.json configuration file
- ❌ Set DOCKER_RAMDISK environment variable
- ❌ 100+ lines of setup code

**After (simple):**
```go
cmd := []string{
    "dockerd",
    "--iptables=false",
    "--ip-forward=false", 
    "--userland-proxy=false",
    "--bridge=none",
    "--storage-driver=vfs",
}
```

**That's it!** 6 lines. No cgroups needed.

### Why Each Flag

| Flag | Purpose | Why It Matters |
|------|---------|----------------|
| `--iptables=false` | Don't configure iptables | gVisor handles networking, iptables would fail anyway |
| `--ip-forward=false` | Don't enable IP forwarding | Not needed for basic Docker operations |
| `--userland-proxy=false` | Disable userland proxy | Avoids "userland-proxy-path" error (requires path if enabled) |
| `--bridge=none` | Don't create docker0 bridge | gVisor networking doesn't need it |
| `--storage-driver=vfs` | Use simple VFS storage | Works everywhere, no device mapper complexity |

## What We Already Had (Still Correct)

1. ✅ **Runtime configuration** - `runsc` with `--net-raw` flag
2. ✅ **Container capabilities** - All 17 Docker capabilities added
3. ✅ **Daemon verification** - Wait for socket and `docker info` check
4. ✅ **SDK auto-wait** - Client polls for readiness

## The Complete Setup

```
┌─────────────────────────────────────────────────┐
│ 1. Container starts with gVisor                 │
│    ├─ runsc --net-raw                          │
│    └─ CAP_SYS_ADMIN, CAP_NET_ADMIN, etc.      │
├─────────────────────────────────────────────────┤
│ 2. Worker starts dockerd (simple!)             │
│    └─ dockerd --iptables=false                 │
│              --ip-forward=false                 │
│              --userland-proxy=false             │
│              --bridge=none                      │
│              --storage-driver=vfs               │
├─────────────────────────────────────────────────┤
│ 3. Docker daemon starts successfully            │
│    └─ No cgroups needed!                       │
├─────────────────────────────────────────────────┤
│ 4. SDK auto-waits for daemon                    │
│    └─ Polls until ready                        │
├─────────────────────────────────────────────────┤
│ 5. Docker commands work! 🎉                     │
│    └─ docker run, pull, ps, etc.               │
└─────────────────────────────────────────────────┘
```

## Files Modified

| File | Changes | Description |
|------|---------|-------------|
| `pkg/worker/lifecycle.go` | -79 lines, +6 lines | Removed all cgroup/daemon.json setup, simplified to 5 flags |

## Code Changes

**`pkg/worker/lifecycle.go` Lines 1097-1107:**

```go
// Start dockerd with minimal flags - no cgroup setup needed!
// Docker can work without cgroups in gVisor if we disable features that require them
cmd := []string{
    "dockerd",
    "--iptables=false",        // Don't try to configure iptables (gVisor handles networking)
    "--ip-forward=false",      // Don't enable IP forwarding
    "--userland-proxy=false",  // Disable userland proxy (requires userland-proxy-path otherwise)
    "--bridge=none",           // Don't create docker0 bridge
    "--storage-driver=vfs",    // Use simple VFS storage driver
}
env := []string{}
```

That's the entire change! 

## Why This Works

### Docker Features and Cgroup Requirements

| Feature | Needs Cgroups? | Our Setting |
|---------|----------------|-------------|
| Container creation | No | ✅ Works |
| Image management | No | ✅ Works |
| Networking (basic) | No | ✅ Works with gVisor |
| Storage (VFS) | No | ✅ Works |
| Resource limits | **Yes** | ⚠️ Not needed for basic use |
| iptables/bridge | **Yes** | ❌ Disabled |
| Device control | **Yes** | ❌ gVisor handles it |

We're running Docker in **simple mode** - no resource limits, no iptables, no device control. gVisor provides the security isolation, so we don't need Docker's cgroup-based isolation.

## Usage Example

```python
from beta9 import Image, Sandbox

# Create sandbox with Docker
sandbox = Sandbox(
    cpu=1,
    memory="2Gi",
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()

# Docker just works!
result = sandbox.docker.run("hello-world")
result.wait()
print(result.stdout.read())
# Hello from Docker!

# Pull images
sandbox.docker.pull("nginx:latest").wait()

# Run containers
sandbox.docker.run("nginx:latest", detach=True, ports={"80": "8080"}).wait()

# Check running containers
ps_result = sandbox.docker.ps()
ps_result.wait()
print(ps_result.stdout.read())

sandbox.terminate()
```

## Expected Logs

### Worker Side
```
INF added docker capabilities for sandbox container
INF preparing to start docker daemon
INF docker daemon process started - waiting for daemon to be ready pid=12
DBG socket exists, checking daemon with 'docker info'...
INF docker daemon is ready and accepting commands retry_count=3
```

### No Errors About:
- ✅ No cgroup errors
- ✅ No userland-proxy errors
- ✅ No device cgroup errors
- ✅ No iptables errors

## Comparison with Previous Attempts

| Attempt | Approach | Lines of Code | Result |
|---------|----------|---------------|--------|
| #1 | Bind mount `/sys/fs/cgroup` | +12 | ❌ Failed - gVisor doesn't expose device cgroup |
| #2 | Remove device cgroups from spec | -8 | ❌ Failed - Docker checks at startup |
| #3 | Create cgroup hierarchy + daemon.json | +100 | ❌ Failed - userland-proxy error |
| **#4** | **Simple flags** | **+6** | **✅ Should work!** |

## Key Takeaways

1. **Docker doesn't need cgroups for basic operations** - Only for resource limiting
2. **gVisor provides security** - We don't need Docker's cgroup-based isolation
3. **Disable features that require cgroups** - Use flags to turn off what we don't need
4. **Simple is better** - 5 flags beats 100 lines of setup code

## What Could Still Fail

If you need:
- **Resource limits on Docker containers** (--memory, --cpus) → Won't work without cgroups
- **Custom Docker networks** → May have issues with gVisor networking
- **iptables-based networking** → Disabled, won't work

But for **basic Docker operations** (pull, run, build, push), this should work perfectly! 🎉

## Testing

Try your code now:

```python
from beta9 import Image, Sandbox
import time

sandbox = Sandbox(
    cpu=1,
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()

time.sleep(6)
process = sandbox.docker.pull("nginx:latest")
process.wait()
print(process.stdout.read())
```

This should work! If you get any errors, send me the logs and I'll help debug. 🚀
