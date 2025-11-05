# Container Runtime Configuration

This document describes how to configure container runtimes (runc and gVisor) at the worker pool level.

## Overview

Beta9 now supports multiple container runtimes with configuration at the worker pool level. Each pool can specify its own runtime, allowing you to:

- Run GPU workloads on runc pools
- Run sandboxed workloads on gVisor pools
- Mix different runtimes across different worker pools

## Supported Runtimes

### runc (default)
- Full OCI compatibility
- CRIU checkpoint/restore support
- GPU passthrough via CDI
- Device access
- Best performance

### gVisor (runsc)
- Enhanced security isolation
- Userspace kernel
- No GPU support
- No checkpoint/restore
- Slightly reduced performance for syscall-heavy workloads

## Configuration

### Global Default (Optional)

Set a global default runtime in the worker configuration:

```yaml
worker:
  containerRuntime: "runc"  # or "gvisor"
```

### Per-Pool Configuration (Recommended)

Configure runtime at the worker pool level for fine-grained control:

```yaml
worker:
  pools:
    # GPU pool - must use runc
    gpu-pool:
      gpuType: "nvidia-a100"
      runtime: "runc"
      criuEnabled: true
      # ... other pool settings
    
    # Sandbox pool - use gVisor for isolation
    sandbox-pool:
      runtime: "gvisor"
      runtimeConfig:
        gvisorPlatform: "kvm"      # or "ptrace" (optional)
        gvisorRoot: "/run/gvisor"  # optional, defaults to /run/gvisor
      # ... other pool settings
    
    # Standard pool - uses global default or runc
    standard-pool:
      # runtime not specified, falls back to global default
      # ... other pool settings
```

## Runtime Selection Priority

The system selects runtimes in the following priority order:

1. **Pool-level configuration** (`pools.<pool-name>.runtime`)
2. **Global worker configuration** (`worker.containerRuntime`)
3. **Default fallback** (`runc`)

## Automatic Runtime Selection

Regardless of pool configuration, certain workload types will automatically use runc:

- **GPU workloads**: Always routed to runc (gVisor doesn't support GPU)
- **Checkpoint/restore workloads**: Always routed to runc (gVisor doesn't support CRIU)

## gVisor Configuration Options

### Platform

The `gvisorPlatform` option controls the isolation mechanism:

```yaml
runtimeConfig:
  gvisorPlatform: "kvm"  # Hardware virtualization (best security, requires KVM)
  # OR
  gvisorPlatform: "ptrace"  # Software isolation (no KVM required)
```

**Recommendations:**
- Use `kvm` for production (better security and performance)
- Use `ptrace` for development or when KVM is unavailable
- Leave empty to use gVisor's default

### Root Directory

The `gvisorRoot` option specifies where gVisor stores runtime state:

```yaml
runtimeConfig:
  gvisorRoot: "/run/gvisor"  # default
```

## Features by Runtime

| Feature | runc | gVisor |
|---------|------|--------|
| GPU Support | ✅ | ❌ |
| CDI (Device Injection) | ✅ | ❌ |
| Checkpoint/Restore (CRIU) | ✅ | ❌ |
| OOM Detection | ✅ (cgroup v2) | ✅ (cgroup v2) |
| Network Namespaces | ✅ | ✅ |
| Overlay Filesystem | ✅ | ✅ |
| Bind Mounts | ✅ | ✅ |
| Security Isolation | Standard | Enhanced |

## Example Configurations

### Mixed Runtime Environment

```yaml
worker:
  containerRuntime: "runc"  # Global default
  
  pools:
    # High-security sandbox workloads
    secure-sandbox:
      runtime: "gvisor"
      runtimeConfig:
        gvisorPlatform: "kvm"
      mode: "local"
      # Standard containers, checkpointing available
    
    standard:
      runtime: "runc"
      criuEnabled: true
      mode: "local"
    
    # GPU workloads (must be runc)
    gpu-a100:
      runtime: "runc"
      gpuType: "nvidia-a100"
      criuEnabled: false
      mode: "external"
```

### All-gVisor Environment

```yaml
worker:
  containerRuntime: "gvisor"  # Global default
  
  pools:
    sandbox-ptrace:
      # Uses global gVisor default
      runtimeConfig:
        gvisorPlatform: "ptrace"
    
    sandbox-kvm:
      runtime: "gvisor"
      runtimeConfig:
        gvisorPlatform: "kvm"
```

## Monitoring and Debugging

### Runtime Selection Logging

The worker logs which runtime is selected for each pool at startup:

```
INFO  initializing container runtime for worker pool pool=sandbox-pool runtime=gvisor
INFO  gVisor runtime initialized successfully platform=kvm root=/run/gvisor
```

Per-container runtime selection is also logged:

```
DEBUG selected runtime for container container_id=abc123 runtime=gvisor
```

### OOM Detection

OOM detection works identically across runtimes using cgroup v2 memory events:

```
INFO  OOM kill detected via cgroup watcher container_id=abc123
```

## Migration Guide

### From runc-only to Mixed Runtime

1. **Identify workload types** in your pools
2. **Add runtime configuration** to appropriate pools:
   - GPU pools: Keep `runtime: "runc"`
   - Checkpoint pools: Keep `runtime: "runc"`
   - Sandbox pools: Set `runtime: "gvisor"`
3. **Test gradually** by adding one gVisor pool at a time
4. **Monitor performance** and adjust platform settings

### Rollback

To disable gVisor and revert to runc:

```yaml
worker:
  pools:
    my-pool:
      runtime: "runc"  # Change from "gvisor"
      # Remove runtimeConfig section
```

Or remove the `runtime` field entirely to use the global default.

## Troubleshooting

### gVisor Not Available

**Error**: `failed to create gvisor runtime, falling back to runc`

**Solutions:**
- Install runsc: `apt-get install runsc` or download from https://github.com/google/gvisor
- Check runsc is in PATH: `which runsc`
- Verify runsc works: `runsc --version`

### KVM Platform Issues

**Error**: `platform initialization failed`

**Solutions:**
- Verify KVM is available: `ls /dev/kvm`
- Check KVM permissions: `sudo chmod 666 /dev/kvm`
- Fall back to ptrace: `runtimeConfig.gvisorPlatform: "ptrace"`

### GPU Not Working in gVisor

**Expected behavior**: GPU workloads are automatically routed to runc even if the pool specifies gVisor.

Check logs for:
```
DEBUG using runc for GPU workload container_id=abc123
```

## Best Practices

1. **Use runc for GPU workloads** - Always configure GPU pools with `runtime: "runc"`
2. **Use gVisor for untrusted code** - Sandbox user workloads in gVisor pools
3. **Enable CRIU only on runc pools** - Set `criuEnabled: true` only for runc
4. **Use KVM platform in production** - Better security and performance
5. **Test before deploying** - Validate gVisor works in your environment
6. **Monitor resource usage** - gVisor may use slightly more memory

## See Also

- [Worker Pool Configuration](worker-pools.md)
- [gVisor Documentation](https://gvisor.dev/)
- [OCI Runtime Spec](https://github.com/opencontainers/runtime-spec)
