# gVisor GPU Support - Final Solution

## Summary

After extensive research of gVisor's official documentation, here's the correct implementation for GPU support with nvproxy.

## What We Changed

### 1. Runtime Preparation (`pkg/runtime/runsc.go`)

**What we do:**
- Enable nvproxy when GPU devices or CDI annotations are detected
- Preserve GPU device entries exactly as CDI provides them
- Clear devices only for non-GPU workloads
- Do NOT filter or modify device entries

**Why this is correct:**
- gVisor's nvproxy reads `spec.Linux.Devices` to know which GPU devices to virtualize
- CDI only injects GPU-related devices (`/dev/nvidia*`, `/dev/nvidiactl`, `/dev/nvidia-uvm`)
- These are all explicitly supported by gVisor (per official docs)
- No filtering is needed because CDI is already GPU-specific

### 2. CDI Integration (`pkg/worker/lifecycle.go`)

**What we do:**
- Use CDI to inject GPU devices, mounts, and environment variables
- Add debug logging to see what CDI actually injects
- Rely entirely on CDI for GPU configuration

**What CDI should provide:**
- Device entries: `/dev/nvidia0`, `/dev/nvidia1`, `/dev/nvidiactl`, `/dev/nvidia-uvm`
- Environment variables: `NVIDIA_VISIBLE_DEVICES`, `NVIDIA_DRIVER_CAPABILITIES`, etc.
- Library mounts: NVIDIA driver libraries from host
- Annotations: CDI tracking annotations

## The Remaining Issue

**Problem:** Container starts but `libnvidia-ml.so` not found

**Root Cause:** CDI is not injecting library mounts properly

**Why:** The CDI spec file (`/etc/cdi/nvidia.yaml`) likely doesn't include library mount specifications

**Solution:** Need to investigate CDI generation:
1. Check what `nvidia-ctk cdi generate` produces
2. Verify `/etc/cdi/nvidia.yaml` includes library mounts
3. Possibly need to configure CDI generation differently

## Debug Logging Added

The code now logs what CDI injects:
```
log.Info().
    Str("container_id", request.ContainerId).
    Strs("devices", devicePaths).           // e.g. ["/dev/nvidia0", "/dev/nvidiactl"]
    Strs("gpu_mounts", mountSources).       // e.g. ["/usr/lib/.../libnvidia-ml.so"]
    Strs("gpu_env_vars", envVars).          // e.g. ["NVIDIA_VISIBLE_DEVICES=0"]
    Interface("cdi_annotations", spec.Annotations).
    Msg("CDI injection complete - inspect what was injected")
```

Check these logs to see if CDI is providing library mounts.

## Next Steps

1. **Run a GPU container** and collect the debug logs
2. **Check the CDI spec:** `cat /etc/cdi/nvidia.yaml` on a worker node  
3. **Verify library paths:** Ensure NVIDIA libraries exist on the host where CDI expects them
4. **If CDI is missing mounts:** Fix the CDI generation in `pkg/worker/nvidia.go:44`

## How gVisor nvproxy Works (Per Official Docs)

1. Container runtime (via CDI) adds GPU device entries to OCI spec
2. Runtime starts container with `--nvproxy=true` flag
3. gVisor reads `spec.Linux.Devices` to see which GPUs are requested
4. gVisor creates **virtual** `/dev/nvidia*` devices inside the sandbox
5. Application opens `/dev/nvidia0` - gVisor intercepts this
6. gVisor's nvproxy forwards GPU ioctl calls to real host driver
7. Libraries (libnvidia-ml.so, etc.) must be mounted from host via CDI

## Multi-GPU Support

Works identically for any GPU count:
- `gpu_count=1`: CDI injects `/dev/nvidia0`
- `gpu_count=2`: CDI injects `/dev/nvidia0` and `/dev/nvidia1`
- `gpu_count=4`: CDI injects `/dev/nvidia0` through `/dev/nvidia3`

All device entries are preserved and passed to gVisor, which virtualizes them all.

## References

- [gVisor GPU Support Docs](https://gvisor.dev/docs/user_guide/gpu/)
- [NVIDIA CDI Documentation](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/cdi-support.html)
- [CDI Specification](https://github.com/cncf-tags/container-device-interface)
