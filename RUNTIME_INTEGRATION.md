# Firecracker Runtime Integration

## Summary

Added Firecracker runtime integration to the worker initialization logic in `pkg/worker/worker.go`.

## Changes Made

### 1. Added Firecracker Case to Runtime Switch

In the worker initialization, added a new `case "firecracker"` to handle Firecracker runtime selection:

```go
case "firecracker":
    // Get Firecracker configuration
    firecrackerBin := "firecracker"
    firecrackerRoot := "/var/lib/beta9/microvm"
    kernelImage := "/var/lib/beta9/vmlinux"
    defaultCPUs := 1
    defaultMemMiB := 512

    firecrackerRuntime, err := runtime.New(runtime.Config{
        Type:           "firecracker",
        FirecrackerBin: firecrackerBin,
        MicroVMRoot:    firecrackerRoot,
        KernelImage:    kernelImage,
        DefaultCPUs:    defaultCPUs,
        DefaultMemMiB:  defaultMemMiB,
        Debug:          config.DebugMode,
    })
    if err != nil {
        log.Warn().Err(err).Msg("failed to create firecracker runtime, falling back to runc")
        defaultRuntime = runcRuntime
    } else {
        defaultRuntime = firecrackerRuntime
        log.Info().
            Str("bin", firecrackerBin).
            Str("root", firecrackerRoot).
            Str("kernel", kernelImage).
            Int("cpus", defaultCPUs).
            Int("mem_mib", defaultMemMiB).
            Msg("Firecracker runtime initialized successfully")
    }
```

### 2. Added firecrackerRuntime Field to Worker Struct

Added `firecrackerRuntime` field to store the Firecracker runtime instance:

```go
type Worker struct {
    // ... other fields ...
    runtime                 runtime.Runtime
    runcRuntime             runtime.Runtime
    gvisorRuntime           runtime.Runtime
    firecrackerRuntime      runtime.Runtime  // Added this
    // ... other fields ...
}
```

### 3. Initialize and Store Firecracker Runtime

Updated worker initialization to declare and store the Firecracker runtime:

```go
var defaultRuntime runtime.Runtime
var gvisorRuntime runtime.Runtime
var firecrackerRuntime runtime.Runtime  // Added this
```

And in the Worker struct initialization:

```go
worker := &Worker{
    // ... other fields ...
    runtime:            defaultRuntime,
    runcRuntime:        runcRuntime,
    gvisorRuntime:      gvisorRuntime,
    firecrackerRuntime: firecrackerRuntime,  // Added this
    // ... other fields ...
}
```

## Configuration

### Setting Firecracker as Runtime

To use Firecracker, set the container runtime in your pool configuration or worker config:

#### Option 1: Pool Configuration

```yaml
worker:
  pools:
    my-pool:
      container_runtime: "firecracker"  # Use Firecracker for this pool
```

#### Option 2: Global Worker Configuration

```yaml
worker:
  container_runtime: "firecracker"  # Use Firecracker for all pools by default
```

#### Option 3: Environment Variable

```bash
export CONTAINER_RUNTIME=firecracker
```

### Default Configuration Values

When Firecracker is selected, the following defaults are used:

| Setting | Default Value | Description |
|---------|---------------|-------------|
| `FirecrackerBin` | `firecracker` | Binary name (must be in PATH or absolute path) |
| `MicroVMRoot` | `/var/lib/beta9/microvm` | Directory for microVM state |
| `KernelImage` | `/var/lib/beta9/vmlinux` | Path to kernel image |
| `DefaultCPUs` | `1` | Default CPU count for VMs |
| `DefaultMemMiB` | `512` | Default memory in MiB for VMs |

These paths match the Docker image configuration where:
- Firecracker binary is installed to `/usr/local/bin/firecracker` (in PATH)
- Kernel is installed to `/var/lib/beta9/vmlinux`
- Runtime directory is created at `/var/lib/beta9/microvm`

## Runtime Selection Logic

The runtime is selected with this precedence:

1. **Pool-specific configuration**: `pools[pool_name].container_runtime`
2. **Global worker configuration**: `worker.container_runtime`
3. **Default**: `"runc"`

Example:
```go
// Get runtime type from pool config, fall back to global config
runtimeType := poolConfig.ContainerRuntime
if runtimeType == "" {
    runtimeType = config.Worker.ContainerRuntime
}
if runtimeType == "" {
    runtimeType = "runc"
}
```

## Error Handling

If Firecracker runtime initialization fails (e.g., binary not found, kernel missing):
1. A warning is logged: `"failed to create firecracker runtime, falling back to runc"`
2. The worker automatically falls back to `runc` runtime
3. Containers continue to work with `runc` instead

This ensures graceful degradation and prevents worker startup failures.

## Verification

### Check Runtime Selection in Logs

When a worker starts with Firecracker configured, you should see:

```
[INFO] initializing container runtime for worker pool pool=default runtime=firecracker
[INFO] Firecracker runtime initialized successfully bin=firecracker root=/var/lib/beta9/microvm kernel=/var/lib/beta9/vmlinux cpus=1 mem_mib=512
```

### Check Runtime in Use

```bash
# In worker logs, look for:
grep "Firecracker runtime initialized" /var/log/worker.log

# Or check runtime capabilities:
# Firecracker should report:
# - CheckpointRestore: false
# - GPU: false  
# - JoinExistingNetNS: true
# - OOMEvents: false
# - CDI: false
```

### Test Firecracker is Actually Used

Run a container and check:

```bash
# List Firecracker processes
ps aux | grep firecracker

# Check for microVM directories
ls -la /var/lib/beta9/microvm/

# Check for TAP devices (Firecracker networking)
ip link show | grep b9fc_
```

## Future Enhancements

### Add Pool-Specific Firecracker Config

Currently using hardcoded defaults. Future enhancement:

```go
// Add to types.ContainerRuntimeConfig:
type ContainerRuntimeConfig struct {
    GVisorPlatform       string `key:"gvisorPlatform" json:"gvisor_platform"`
    GVisorRoot           string `key:"gvisorRoot" json:"gvisor_root"`
    
    // Add Firecracker-specific config:
    FirecrackerCPUs      int    `key:"firecrackerCpus" json:"firecracker_cpus"`
    FirecrackerMemMiB    int    `key:"firecrackerMemMib" json:"firecracker_mem_mib"`
    FirecrackerKernel    string `key:"firecrackerKernel" json:"firecracker_kernel"`
}
```

Then in worker.go:

```go
case "firecracker":
    cpus := poolConfig.ContainerRuntimeConfig.FirecrackerCPUs
    if cpus == 0 {
        cpus = 1
    }
    
    memMiB := poolConfig.ContainerRuntimeConfig.FirecrackerMemMiB
    if memMiB == 0 {
        memMiB = 512
    }
    
    kernel := poolConfig.ContainerRuntimeConfig.FirecrackerKernel
    if kernel == "" {
        kernel = "/var/lib/beta9/vmlinux"
    }
    
    firecrackerRuntime, err := runtime.New(runtime.Config{
        Type:           "firecracker",
        FirecrackerBin: "firecracker",
        MicroVMRoot:    "/var/lib/beta9/microvm",
        KernelImage:    kernel,
        DefaultCPUs:    cpus,
        DefaultMemMiB:  memMiB,
        Debug:          config.DebugMode,
    })
    // ...
```

## Deployment

### Enable Firecracker in Production

1. **Update worker configuration**:
   ```yaml
   worker:
     pools:
       firecracker-pool:
         container_runtime: "firecracker"
         # Other pool settings...
   ```

2. **Deploy new worker image** (with Firecracker v1.13.1 + kernel)

3. **Monitor startup logs** for runtime initialization

4. **Verify containers use Firecracker**:
   ```bash
   kubectl logs <worker-pod> | grep "Firecracker runtime initialized"
   ```

### Gradual Rollout

Use pool-specific configuration for gradual rollout:

```yaml
worker:
  pools:
    # Existing pools continue using runc
    default:
      container_runtime: "runc"
    
    # New pool uses Firecracker
    firecracker-test:
      container_runtime: "firecracker"
```

This allows testing Firecracker with specific workloads before full rollout.

## Troubleshooting

### Issue: "failed to create firecracker runtime"

**Possible causes**:
1. Firecracker binary not in PATH
2. Kernel image missing
3. Insufficient permissions (needs root for KVM)

**Check**:
```bash
which firecracker
ls -la /var/lib/beta9/vmlinux
ls -la /dev/kvm
```

### Issue: Containers still using runc

**Possible causes**:
1. Configuration not applied
2. Firecracker initialization failed (check logs)
3. Worker not restarted after config change

**Fix**:
```bash
# Check config
kubectl get configmap worker-config -o yaml

# Check worker logs
kubectl logs <worker-pod> | grep -i runtime

# Restart worker
kubectl rollout restart deployment/worker
```

### Issue: microVM fails to start

**Check**:
```bash
# Worker logs
kubectl logs <worker-pod> | grep firecracker

# Inside worker pod
ls -la /var/lib/beta9/microvm/<container-id>/
cat /var/lib/beta9/microvm/<container-id>/firecracker.log
```

## Related Documentation

- `SQUASHFS_IMPLEMENTATION.md` - Rootfs preparation details
- `FUSE_ROOTFS_APPROACH.md` - Design decisions
- `FINAL_IMPLEMENTATION_SUMMARY.md` - Complete overview
- `pkg/runtime/FIRECRACKER.md` - Runtime interface details

---

**Implementation Status**: ✅ Complete and tested
**Ready for Production**: Yes (with monitoring)
