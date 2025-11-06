# Runtime Configuration Explained

## Two Types of Runtime Configuration

Beta9 has **two separate runtime configurations** that serve different purposes:

### 1. Pod Runtime (`runtime`)

**What it configures:** The Kubernetes RuntimeClass for the worker **pod itself**

**Used by:** Kubernetes scheduler

**Purpose:** Configures how Kubernetes runs the worker pod

**Common values:**
- `nvidia` - For GPU-enabled pods (NVIDIA Container Runtime)
- `kata` - For Kata Containers isolation
- Empty/unset - Use default Kubernetes runtime (typically containerd or CRI-O)

**Example:**
```yaml
worker:
  pools:
    gpu-pool:
      runtime: nvidia  # ← Pod runs with NVIDIA Container Runtime
```

**Technical detail:** This sets `pod.spec.runtimeClassName` in Kubernetes

---

### 2. Container Runtime (`containerRuntime`)

**What it configures:** The OCI runtime for **containers spawned inside the worker**

**Used by:** Beta9 worker process

**Purpose:** Configures how the worker spawns user containers

**Valid values:**
- `runc` - Standard OCI runtime (default)
- `gvisor` - Google's sandboxed runtime (via runsc)

**Example:**
```yaml
worker:
  pools:
    secure-pool:
      containerRuntime: gvisor  # ← User containers run in gVisor sandbox
```

**Technical detail:** This determines which `pkg/runtime.Runtime` implementation is used

---

## Configuration Structure

```yaml
worker:
  pools:
    my-pool:
      # ========================================
      # POD RUNTIME (Kubernetes level)
      # ========================================
      runtime: nvidia  # Optional: RuntimeClass for the pod
      
      # ========================================
      # CONTAINER RUNTIME (Worker level)
      # ========================================
      containerRuntime: gvisor  # Required: runc or gvisor
      containerRuntimeConfig:
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
```

## Common Configurations

### Standard CPU Pool (runc)

```yaml
worker:
  pools:
    default:
      mode: local
      # No pod runtime specified (use K8s default)
      containerRuntime: runc  # User containers use runc
      containerRuntimeConfig:
        gvisorPlatform: systrap  # Ignored for runc
        gvisorRoot: /run/gvisor   # Ignored for runc
```

**Result:**
- Pod: Runs with default Kubernetes runtime
- User containers: Run with runc

---

### Secure CPU Pool (gVisor)

```yaml
worker:
  pools:
    secure:
      mode: local
      # No pod runtime specified (use K8s default)
      containerRuntime: gvisor  # User containers use gVisor
      containerRuntimeConfig:
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
```

**Result:**
- Pod: Runs with default Kubernetes runtime
- User containers: Run in gVisor sandbox

---

### GPU Pool with NVIDIA Runtime + runc

```yaml
worker:
  pools:
    gpu-runc:
      mode: local
      gpuType: T4
      runtime: nvidia  # Pod needs GPU access
      containerRuntime: runc  # User containers use runc
      containerRuntimeConfig:
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
```

**Result:**
- Pod: Runs with NVIDIA Container Runtime (GPU access)
- User containers: Run with runc (direct GPU passthrough)

**Use case:** Maximum GPU performance

---

### GPU Pool with NVIDIA Runtime + gVisor

```yaml
worker:
  pools:
    gpu-gvisor:
      mode: local
      gpuType: T4
      runtime: nvidia  # Pod needs GPU access
      containerRuntime: gvisor  # User containers use gVisor
      containerRuntimeConfig:
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
```

**Result:**
- Pod: Runs with NVIDIA Container Runtime (GPU access)
- User containers: Run in gVisor sandbox (GPU via nvproxy)

**Use case:** GPU workloads with strong isolation

---

## Decision Tree

```
┌─────────────────────────────────────┐
│ Does the POOL need GPU access?      │
└─────────────┬───────────────────────┘
              │
        ┌─────┴─────┐
       Yes          No
        │            │
        ▼            ▼
   runtime: nvidia   (no pod runtime)
        │            │
        │            │
        ▼            ▼
┌───────────────────────────────────────┐
│ Do USER containers need isolation?   │
└───────────────┬───────────────────────┘
                │
          ┌─────┴─────┐
         Yes          No
          │            │
          ▼            ▼
  containerRuntime:    containerRuntime:
      gvisor              runc
```

## Comparison Matrix

| Pod Runtime | Container Runtime | GPU | Isolation | Performance | Use Case |
|-------------|-------------------|-----|-----------|-------------|----------|
| (default) | runc | ❌ | Low | ⭐⭐⭐⭐⭐ | Standard workloads |
| (default) | gvisor | ❌ | High | ⭐⭐⭐⭐ | Secure workloads |
| nvidia | runc | ✅ | Low | ⭐⭐⭐⭐⭐ | Max GPU performance |
| nvidia | gvisor | ✅ | High | ⭐⭐⭐⭐ | Secure GPU workloads |

## Key Differences

| Aspect | Pod Runtime | Container Runtime |
|--------|-------------|-------------------|
| **Scope** | Worker pod | User containers inside worker |
| **Configured by** | Kubernetes | Beta9 worker |
| **Controls** | Pod execution environment | Container execution environment |
| **Examples** | nvidia, kata | runc, gvisor |
| **Required?** | No (optional) | Yes (defaults to runc) |
| **When set** | Pod creation | Worker initialization |
| **Affects** | 1 pod | N containers |

## Examples in Practice

### Example 1: Multi-Tenant Platform

**Goal:** Isolate untrusted user code

```yaml
worker:
  pools:
    public:
      mode: local
      # No special pod runtime needed
      containerRuntime: gvisor  # Strong isolation
      containerRuntimeConfig:
        gvisorPlatform: systrap
```

### Example 2: GPU Training Farm

**Goal:** Maximum GPU performance

```yaml
worker:
  pools:
    gpu-training:
      mode: local
      gpuType: A100
      runtime: nvidia  # Pod needs GPU
      containerRuntime: runc  # Max performance
```

### Example 3: Secure GPU Inference

**Goal:** GPU access with strong isolation

```yaml
worker:
  pools:
    gpu-inference:
      mode: local
      gpuType: T4
      runtime: nvidia  # Pod needs GPU
      containerRuntime: gvisor  # User isolation
      containerRuntimeConfig:
        gvisorPlatform: systrap
```

### Example 4: Mixed Environment

**Goal:** Different pools for different needs

```yaml
worker:
  pools:
    # Fast, direct execution
    performance:
      containerRuntime: runc
    
    # Secure, sandboxed execution
    sandbox:
      containerRuntime: gvisor
      containerRuntimeConfig:
        gvisorPlatform: systrap
    
    # GPU with max performance
    gpu-fast:
      runtime: nvidia
      containerRuntime: runc
    
    # GPU with security
    gpu-secure:
      runtime: nvidia
      containerRuntime: gvisor
      containerRuntimeConfig:
        gvisorPlatform: systrap
```

## Troubleshooting

### Error: "RuntimeClass 'nvidia' not found"

**Cause:** `runtime: nvidia` is set but K8s doesn't have this RuntimeClass

**Solution:**
1. Install NVIDIA GPU Operator
2. Or remove `runtime: nvidia` if not needed
3. Or create the RuntimeClass manually

### Error: "failed to create gvisor runtime"

**Cause:** `containerRuntime: gvisor` but runsc not available

**Solution:**
1. Verify runsc in worker image: `docker run worker:latest which runsc`
2. Check worker logs for initialization errors
3. Fall back to `containerRuntime: runc`

### Containers not getting GPU

**Cause:** Missing `runtime: nvidia` at pod level

**Solution:**
```yaml
worker:
  pools:
    gpu:
      runtime: nvidia  # ← Add this for pod GPU access
      containerRuntime: gvisor
```

## FAQ

### Q: Do I need both runtime and containerRuntime?

**A:** No, only `containerRuntime` is required. `runtime` is optional and only needed for special pod requirements (like GPU access).

### Q: Can I use gVisor as the pod runtime?

**A:** That's different! `runtime: gvisor` would be a K8s RuntimeClass. `containerRuntime: gvisor` is for containers inside the worker.

### Q: What's the default if I don't specify?

**A:**
- `runtime`: Empty (uses K8s default, typically containerd)
- `containerRuntime`: `runc` (if not specified)

### Q: Can different pools use different configurations?

**A:** Yes! Each pool is independent:
```yaml
pools:
  pool-a:
    runtime: nvidia
    containerRuntime: runc
  pool-b:
    # no pod runtime
    containerRuntime: gvisor
```

### Q: Does containerRuntime affect the worker pod?

**A:** No, `containerRuntime` only affects containers spawned **inside** the worker. The worker pod itself uses `runtime` (if set) or the K8s default.

## Related Documentation

- [Container Runtime Configuration](./container-runtime-configuration.md)
- [gVisor Platforms](./gvisor-platforms.md)
- [gVisor nvproxy (GPU)](./gvisor-nvproxy.md)
- [Config Field Migration](./config-field-migration.md)
