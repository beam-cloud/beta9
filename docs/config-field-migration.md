# Configuration Field Migration

## Overview

The worker pool configuration fields have been renamed to avoid conflicts with Kubernetes RuntimeClass.

## Migration Required

### Old Field Names (DEPRECATED)

```yaml
worker:
  pools:
    default:
      runtime: runc              # ❌ CONFLICTS with K8s RuntimeClass
      runtimeConfig:              # ❌ DEPRECATED
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
```

### New Field Names (CURRENT)

```yaml
worker:
  pools:
    default:
      containerRuntime: runc              # ✅ No conflict
      containerRuntimeConfig:             # ✅ Clear naming
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
```

## Why the Change?

The `runtime` field name conflicted with Kubernetes' `pod.spec.runtimeClassName`, which specifies the container runtime for the pod (e.g., "runc", "kata", "gvisor"). This caused the following error:

```
Error creating: pods "worker-default-6c6882ea-" is forbidden: 
pod rejected: RuntimeClass "runc" not found
```

By renaming to `containerRuntime`, we:
1. ✅ Avoid Kubernetes RuntimeClass conflicts
2. ✅ Make the purpose clearer (internal container runtime, not pod runtime)
3. ✅ Follow consistent naming conventions

## Field Mapping

| Old Field | New Field | Purpose |
|-----------|-----------|---------|
| `runtime` | `containerRuntime` | Which OCI runtime to use internally (runc/gVisor) |
| `runtimeConfig` | `containerRuntimeConfig` | Runtime-specific configuration |
| `runtimeConfig.gvisorPlatform` | `containerRuntimeConfig.gvisorPlatform` | gVisor platform (systrap/kvm/ptrace) |
| `runtimeConfig.gvisorRoot` | `containerRuntimeConfig.gvisorRoot` | gVisor state directory |

## Configuration Examples

### Standard runc Pool

```yaml
worker:
  pools:
    default:
      mode: local
      containerRuntime: runc
      containerRuntimeConfig:
        gvisorPlatform: systrap  # Ignored for runc
        gvisorRoot: /run/gvisor   # Ignored for runc
```

### gVisor Pool (CPU Workloads)

```yaml
worker:
  pools:
    secure:
      mode: local
      containerRuntime: gvisor
      containerRuntimeConfig:
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
```

### gVisor Pool (GPU Workloads)

```yaml
worker:
  pools:
    gpu-secure:
      mode: local
      gpuType: T4
      containerRuntime: gvisor
      containerRuntimeConfig:
        gvisorPlatform: systrap  # GPU via nvproxy
        gvisorRoot: /run/gvisor
```

### Bare Metal with KVM

```yaml
worker:
  pools:
    high-performance:
      mode: local
      containerRuntime: gvisor
      containerRuntimeConfig:
        gvisorPlatform: kvm      # Requires /dev/kvm
        gvisorRoot: /run/gvisor
```

## Migration Steps

### For Existing Deployments

1. **Update config files:**
   ```bash
   # Find old field usage
   grep -r "runtime:" config/
   
   # Replace with new field names
   sed -i 's/runtime:/containerRuntime:/g' config/*.yaml
   sed -i 's/runtimeConfig:/containerRuntimeConfig:/g' config/*.yaml
   ```

2. **Update environment variables** (if using env-based config):
   ```bash
   # Old
   BETA9_WORKER_POOLS_DEFAULT_RUNTIME=gvisor
   
   # New
   BETA9_WORKER_POOLS_DEFAULT_CONTAINERRUNTIME=gvisor
   ```

3. **Redeploy workers:**
   ```bash
   kubectl rollout restart deployment -l app=beta9-worker
   ```

### For New Deployments

Use the new field names from the start:
- `containerRuntime` instead of `runtime`
- `containerRuntimeConfig` instead of `runtimeConfig`

## Verification

### Check Configuration Loading

```bash
# Check worker logs for runtime initialization
kubectl logs -l app=beta9-worker | grep "initializing container runtime"

# Should see:
# "initializing container runtime for worker pool" pool="default" runtime="runc"
```

### Verify No Kubernetes Errors

```bash
# Check for RuntimeClass errors
kubectl get events --field-selector reason=FailedCreate

# Should NOT see:
# RuntimeClass "runc" not found
```

### Test Runtime Selection

```python
# Submit a test workload
from beam import App

app = App(name="runtime-test")

@app.function()
def test():
    import os
    # Will use pool's configured runtime
    print("Test successful!")
```

## Troubleshooting

### Old Field Names Still in Config

**Symptom:**
```
Warning: Unknown configuration key "runtime" in worker pool config
```

**Solution:**
Update to new field names (`containerRuntime`, `containerRuntimeConfig`)

### Kubernetes RuntimeClass Error

**Symptom:**
```
Error: RuntimeClass "runc" not found
```

**Solution:**
This means you're still using the old `runtime:` field name. Update to `containerRuntime:`.

### Runtime Not Initializing

**Symptom:**
```
failed to create gvisor runtime, falling back to runc
```

**Solutions:**
1. Check `containerRuntimeConfig.gvisorPlatform` is valid (systrap/kvm/ptrace)
2. Verify runsc is installed: `which runsc`
3. Check runsc version: `runsc --version`
4. Test runsc manually: `runsc run test`

## API Compatibility

### Go Code

If you're programmatically creating worker pool configs:

**Old:**
```go
config := types.WorkerPoolConfig{
    Runtime: "gvisor",
    RuntimeConfig: types.RuntimeConfig{
        GVisorPlatform: "systrap",
    },
}
```

**New:**
```go
config := types.WorkerPoolConfig{
    ContainerRuntime: "gvisor",
    ContainerRuntimeConfig: types.RuntimeConfig{
        GVisorPlatform: "systrap",
    },
}
```

### JSON/REST API

**Old:**
```json
{
  "runtime": "gvisor",
  "runtime_config": {
    "gvisor_platform": "systrap"
  }
}
```

**New:**
```json
{
  "container_runtime": "gvisor",
  "container_runtime_config": {
    "gvisor_platform": "systrap"
  }
}
```

## Related Documentation

- [Container Runtime Configuration](./container-runtime-configuration.md)
- [gVisor Platforms](./gvisor-platforms.md)
- [gVisor nvproxy (GPU Support)](./gvisor-nvproxy.md)

## FAQ

### Q: Do I need to update immediately?

**A:** The new field names are required in the current version. Update your configuration files before deploying.

### Q: What if I have multiple environments?

**A:** Update all environment configs (dev, staging, production) to use the new field names for consistency.

### Q: Does this affect running containers?

**A:** No, the change only affects worker initialization. Existing running containers continue unchanged. New workers will use the new config format.

### Q: Can I use both old and new fields?

**A:** No, only the new field names (`containerRuntime`, `containerRuntimeConfig`) are supported. The old names will be ignored or cause errors.

### Q: Is there a deprecation period?

**A:** No, the old field names are no longer supported. Use the new names immediately.
