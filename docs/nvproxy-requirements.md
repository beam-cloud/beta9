# nvproxy Requirements - What's Needed?

## TL;DR - Answer to Your Question

**For nvproxy to work, the worker image needs:**

✅ **runsc binary with nvproxy support** (we have this - version 20250407.0)
✅ **Access to /dev/nvidia\* devices** (provided by Kubernetes device plugin, not the image)

**That's it!** nvproxy is built into runsc and doesn't require any additional libraries or tools in the worker image.

---

## Detailed Breakdown

### What We Already Have ✅

Our current `Dockerfile.worker` includes:

```dockerfile
# gVisor with nvproxy support
ARG GVISOR_VERSION=20250407.0  # Latest version, includes nvproxy
RUN curl -fsSL .../runsc ...

# NVIDIA base image (for runc GPU support and CRIU)
FROM nvidia/cuda:12.8.0-base-ubuntu22.04

# nvidia-container-toolkit (for runc GPU support)
RUN apt-get install -y nvidia-container-toolkit
```

### What's Actually Needed for nvproxy

| Component | Needed? | Where It Comes From |
|-----------|---------|---------------------|
| **runsc binary with nvproxy** | ✅ Required | Dockerfile.worker |
| **/dev/nvidia\* device files** | ✅ Required | K8s device plugin (runtime) |
| **NVIDIA kernel driver** | ✅ Required | Host node |
| **NVIDIA user libraries** | ❌ Not needed in worker | Inner container images |
| **nvidia-container-toolkit** | ❌ Not needed for nvproxy | (Needed for runc GPU support) |
| **CUDA base image** | ❌ Not needed for nvproxy | (Needed for CRIU GPU checkpoints) |

### Why the Worker Image Has Extra NVIDIA Stuff

The worker image is based on `nvidia/cuda` and includes `nvidia-container-toolkit`, but these are **NOT for nvproxy**. They're for:

1. **runc GPU support** (direct device passthrough)
2. **CRIU GPU checkpoints** (requires CUDA libraries)

For nvproxy specifically, **only runsc with nvproxy support is needed**.

---

## How nvproxy Works

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Inner Container (User Workload)                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Python + PyTorch + CUDA libraries (libcuda.so)      │   │
│  │  ↓ Makes GPU API calls (ioctl)                      │   │
│  └──────────────────────┬──────────────────────────────┘   │
│                         │                                    │
│  ┌──────────────────────▼──────────────────────────────┐   │
│  │ gVisor Sentry (Userspace Kernel)                    │   │
│  │  ┌──────────────────────────────────────────────┐  │   │
│  │  │ nvproxy - Built into runsc                   │  │   │
│  │  │ Intercepts GPU ioctls                        │  │   │
│  │  └───────────────┬──────────────────────────────┘  │   │
│  └──────────────────┼─────────────────────────────────┘   │
└───────────────────────┼──────────────────────────────────────┘
                        │ Proxied ioctl to /dev/nvidia*
┌───────────────────────▼──────────────────────────────────────┐
│ Host System (Worker Node)                                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ /dev/nvidia0, /dev/nvidiactl, /dev/nvidia-uvm       │   │
│  │ ↓ Provided by NVIDIA device plugin to worker pod    │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                    │
│  ┌──────────────────────▼───────────────────────────────┐   │
│  │ NVIDIA Kernel Driver (nvidia.ko)                     │   │
│  └──────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
```

### Key Points

1. **nvproxy is built INTO runsc** - it's not a separate binary or library
2. **nvproxy runs in the sentry process** - the gVisor userspace kernel
3. **Worker pod needs GPU devices** - from Kubernetes device plugin
4. **Inner containers need NVIDIA libraries** - from their own base images
5. **Worker image doesn't need NVIDIA libraries for nvproxy** - the interception happens at the syscall level

---

## What Each Layer Needs

### 1. Worker Image (`Dockerfile.worker`)

**For nvproxy only:**
- ✅ `runsc` binary (version with nvproxy support)

**What we have (for other features):**
- ✅ `runsc` binary 20250407.0
- ✅ NVIDIA CUDA base image (for runc GPU + CRIU)
- ✅ nvidia-container-toolkit (for runc GPU)

**Verdict:** ✅ Nothing additional needed for nvproxy!

### 2. Worker Pod (Kubernetes Deployment)

**Required for nvproxy:**
```yaml
spec:
  runtimeClassName: nvidia  # Gets GPU devices from device plugin
  containers:
  - name: worker
    resources:
      limits:
        nvidia.com/gpu: 1  # Request GPU from device plugin
```

**What this provides:**
- `/dev/nvidia0`, `/dev/nvidiactl`, `/dev/nvidia-uvm` device files
- Access to NVIDIA kernel driver on host
- GPU scheduling and isolation

### 3. User Container Images (Workloads)

**Required:**
- NVIDIA libraries: `libcuda.so`, `libcudnn.so`, `libnvidia-ml.so`, etc.

**Typically from:**
```dockerfile
FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04
# or
FROM pytorch/pytorch:2.0.1-cuda11.7-cudnn8-runtime
```

**Why:** These libraries make GPU API calls that nvproxy intercepts.

---

## Verification Checklist

### Worker Image ✅
```bash
# Check runsc version
docker run <worker-image> runsc --version
# Output: runsc version release-20250407.0

# Check nvproxy available
docker run <worker-image> runsc help 2>&1 | grep nvproxy
# Should show --nvproxy flag documentation
```

### Worker Pod (at runtime)
```bash
# Check GPU devices available
kubectl exec -it <worker-pod> -- ls -la /dev/nvidia*
# Should show: /dev/nvidia0, /dev/nvidiactl, /dev/nvidia-uvm

# Check NVIDIA driver version
kubectl exec -it <worker-pod> -- cat /proc/driver/nvidia/version
# Should show driver version (e.g., 535.129.03)
```

### User Container (when running)
```bash
# Inside a GPU workload container
python3 -c "import torch; print(torch.cuda.is_available())"
# Should print: True

# Check CUDA libraries present
ldconfig -p | grep libcuda
# Should show: libcuda.so.1 => /usr/local/cuda/lib64/libcuda.so.1
```

---

## Common Misconceptions

### ❌ "Worker image needs NVIDIA libraries for nvproxy"
**False.** nvproxy intercepts syscalls at the kernel boundary. It doesn't need CUDA libraries.

### ❌ "Worker image needs nvidia-container-toolkit for nvproxy"
**False.** nvidia-container-toolkit is for runc GPU support (device injection). nvproxy handles GPU access internally.

### ❌ "Need to install nvproxy separately"
**False.** nvproxy is built into runsc. Just use `runsc --nvproxy=true`.

### ✅ "Worker pod needs GPU devices from device plugin"
**True!** The worker pod must have RuntimeClass: nvidia to get /dev/nvidia* devices.

### ✅ "User containers need NVIDIA libraries"
**True!** The workload containers need libcuda.so etc. to make GPU API calls.

---

## Testing Without Changing Worker Image

Our current worker image already has everything needed! To test:

### 1. Ensure Worker Pod Has GPU Access

```yaml
# In your worker pod spec
apiVersion: v1
kind: Pod
metadata:
  name: worker
spec:
  runtimeClassName: nvidia  # ← Critical for nvproxy!
  containers:
  - name: worker
    image: <worker-image>
    resources:
      limits:
        nvidia.com/gpu: 1  # ← Request GPU
```

### 2. Configure Worker Pool for gVisor

```yaml
# In config
worker:
  pools:
    gpu:
      runtime: nvidia              # Pod RuntimeClass
      containerRuntime: gvisor     # Use gVisor for inner containers
      containerRuntimeConfig:
        gvisorPlatform: systrap
```

### 3. Run GPU Workload

```python
# test_gpu.py - User provides this in their container
import torch
print(f"CUDA: {torch.cuda.is_available()}")
print(f"GPU: {torch.cuda.get_device_name(0)}")

x = torch.rand(1000, 1000).cuda()
print(f"✅ GPU working: {x.device}")
```

### 4. Verify nvproxy Active

```bash
# Check worker logs
kubectl logs <worker-pod> | grep nvproxy

# Check runsc process
kubectl exec -it <worker-pod> -- ps aux | grep -- --nvproxy=true

# Should see:
# runsc --root /run/gvisor --platform systrap --nvproxy=true run ...
```

---

## Summary

### What's Already in Worker Image ✅

| Component | Purpose | Needed for nvproxy? |
|-----------|---------|---------------------|
| runsc 20250407.0 | gVisor runtime | ✅ Yes |
| nvidia/cuda base | CRIU GPU support | ❌ No (but useful for runc) |
| nvidia-container-toolkit | runc GPU support | ❌ No (but useful for runc) |

### What's Needed at Runtime ✅

| Component | Provided By | Status |
|-----------|-------------|--------|
| /dev/nvidia* devices | K8s device plugin | ✅ When RuntimeClass: nvidia |
| NVIDIA kernel driver | Host node | ✅ On GPU nodes |
| NVIDIA libraries | User container image | ✅ User responsibility |

### Answer: No Additional Installation Needed! ✅

The worker image already has:
- ✅ runsc with nvproxy support (version 20250407.0)
- ✅ Everything else is handled at runtime by Kubernetes

**You're ready to test GPU workloads with gVisor + nvproxy right now!** 🚀

---

## References

- [gVisor GPU Support (nvproxy)](https://gvisor.dev/docs/user_guide/gpu/)
- [NVIDIA Device Plugin](https://github.com/NVIDIA/k8s-device-plugin)
- [Kubernetes RuntimeClass](https://kubernetes.io/docs/concepts/containers/runtime-class/)
