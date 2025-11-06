# Troubleshooting: runsc list Shows No Containers

## Problem

When running `runsc list` manually, no containers are shown even though gVisor containers are running:

```bash
root@worker:/# runsc list
ID          PID         STATUS      BUNDLE      CREATED     OWNER
# Empty - no containers listed
```

But you can see runsc processes running:
```bash
root@worker:/# ps aux | grep runsc
root       113     1  0 15:00 ?   00:00:00 runsc --root /run/gvisor --platform systrap run ...
```

## Root Cause

The `runsc list` command requires the **same `--root` flag** that was used when creating the containers.

When Beta9 creates containers, it uses:
```bash
runsc --root /run/gvisor --platform systrap run --bundle <path> <container-id>
```

But when you run `runsc list` without flags, it uses the default root:
```bash
runsc list
# Equivalent to: runsc --root /var/run/runsc list
# ❌ Wrong root directory!
```

## Solution

Use the same `--root` flag that Beta9 uses:

```bash
# Correct way to list gVisor containers
runsc --root /run/gvisor list

# With formatting
runsc --root /run/gvisor list --format=json | jq

# With specific platform
runsc --root /run/gvisor --platform systrap list
```

## Understanding the --root Flag

The `--root` flag specifies where runsc stores container state information:

| Runtime | Default Root | Beta9 Configuration |
|---------|-------------|---------------------|
| runc | `/run/runc` | `/run/runc` (default) |
| runsc | `/var/run/runsc` | `/run/gvisor` (configured) |

Each `--root` directory is independent, so containers created with one root won't appear when listing with a different root.

## Verify Your Configuration

### Check Beta9 Configuration

```yaml
# In your config file
worker:
  pools:
    default:
      containerRuntime: gvisor
      containerRuntimeConfig:
        gvisorRoot: /run/gvisor  # ← This is your --root value
```

### Check Worker Logs

```bash
kubectl logs <worker-pod> | grep "gVisor runtime initialized"
```

Should show:
```
"gVisor runtime initialized successfully" platform="systrap" root="/run/gvisor"
```

### List Containers with Correct Root

```bash
# Get the root from config
GVISOR_ROOT="/run/gvisor"

# List containers
runsc --root $GVISOR_ROOT list

# Example output:
ID                                           PID    STATUS    BUNDLE
taskqueue-0c9ab734-01b2-4fae-aaaa-82c2c7...  123    running   /tmp/...
```

## Common Commands

### List All gVisor Containers

```bash
runsc --root /run/gvisor list
```

### Get Container State

```bash
runsc --root /run/gvisor state <container-id>
```

### Get Container Events

```bash
runsc --root /run/gvisor events <container-id>
```

### Debug a Container

```bash
runsc --root /run/gvisor --debug ps <container-id>
```

### Kill a Container

```bash
runsc --root /run/gvisor kill <container-id>
```

### Delete a Container

```bash
runsc --root /run/gvisor delete <container-id>
```

## Helper Script

Create a helper script for convenience:

```bash
#!/bin/bash
# Save as /usr/local/bin/runsc-beta9

GVISOR_ROOT="/run/gvisor"
PLATFORM="systrap"

runsc --root $GVISOR_ROOT --platform $PLATFORM "$@"
```

Usage:
```bash
chmod +x /usr/local/bin/runsc-beta9
runsc-beta9 list
runsc-beta9 state <container-id>
```

## Programmatic Access

To list containers from Go code:

```go
import "github.com/beam-cloud/beta9/pkg/runtime"

// Create runtime with same config as worker
rt, err := runtime.New(runtime.Config{
    Type:          "gvisor",
    RunscPath:     "runsc",
    RunscRoot:     "/run/gvisor",
    RunscPlatform: "systrap",
})

// List containers
containers, err := rt.(*runtime.Runsc).List(context.Background())
for _, c := range containers {
    fmt.Printf("Container: %s, PID: %d, Status: %s\n", c.ID, c.Pid, c.Status)
}
```

## Debugging

### Check if Containers Exist in Filesystem

```bash
# gVisor stores container metadata in the root directory
ls -la /run/gvisor/

# Should show container directories
drwx------ 2 root root  80 Nov  5 15:00 taskqueue-0c9ab734-...
```

### Verify runsc Version

```bash
runsc --version
```

Expected:
```
runsc version release-20250407.0
spec: 1.1.0
```

### Check Running Processes

```bash
# See all runsc processes
ps aux | grep runsc

# Should show:
# - runsc run (main process)
# - runsc-gofer (I/O gofer)
# - runsc-sandbox (sandbox process)
```

## Why Different Root Directories?

Beta9 uses `/run/gvisor` instead of the default `/var/run/runsc` because:

1. **Isolation**: Separate from system runsc usage
2. **Configuration**: Easy to customize per pool
3. **Cleanup**: Easy to wipe all Beta9 gVisor state
4. **Multi-tenancy**: Different pools can use different roots

## Related Documentation

- [Container Runtime Configuration](./container-runtime-configuration.md)
- [gVisor Platforms](./gvisor-platforms.md)
- [Runtime Configuration Explained](./runtime-configuration-explained.md)

## Quick Reference

```bash
# Always use the same root as your config
GVISOR_ROOT="/run/gvisor"

# List containers
runsc --root $GVISOR_ROOT list

# Get detailed state
runsc --root $GVISOR_ROOT state <container-id>

# Debug mode
runsc --root $GVISOR_ROOT --debug list

# With specific platform
runsc --root $GVISOR_ROOT --platform systrap list
```

Remember: **Always use `--root /run/gvisor`** when working with Beta9 gVisor containers!
