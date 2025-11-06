# gVisor GPU Support via nvproxy

## Overview

gVisor supports GPU acceleration through **nvproxy**, a userspace driver proxy that allows containers to access NVIDIA GPUs without direct device passthrough. This provides GPU functionality while maintaining gVisor's strong security isolation.

## How nvproxy Works

nvproxy intercepts NVIDIA driver calls from the containerized application and proxies them to the host's NVIDIA driver, providing:

1. **Security Isolation**: No direct device access; all calls go through controlled proxy
2. **GPU Virtualization**: Multiple gVisor containers can safely share GPUs
3. **Compatibility**: Works with CUDA, cuDNN, TensorFlow, PyTorch, and other GPU frameworks

## Architecture

```
┌─────────────────────────────────────────┐
│ Container (gVisor sandbox)              │
│ ┌─────────────────────────────────────┐ │
│ │ Application (CUDA/TensorFlow/PyTorch)││
│ └──────────────┬──────────────────────┘ │
│                │ NVIDIA API calls       │
│ ┌──────────────▼──────────────────────┐ │
│ │ nvproxy (userspace driver proxy)    │ │
│ └──────────────┬──────────────────────┘ │
└────────────────┼────────────────────────┘
                 │ Proxied driver calls
┌────────────────▼────────────────────────┐
│ Host NVIDIA Driver                      │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│ GPU Hardware                            │
└─────────────────────────────────────────┘
```

## Configuration

### Basic Configuration

Enable gVisor with GPU support in your worker pool config:

```yaml
pools:
  gpu-pool:
    runtime: gvisor
    gpuType: T4
    runtimeConfig:
      gvisorPlatform: kvm  # or ptrace
      gvisorRoot: /run/gvisor
```

### CDI Integration

gVisor nvproxy works with CDI (Container Device Interface) for GPU injection:

```yaml
pools:
  gpu-pool:
    runtime: gvisor
    gpuType: A100
    runtimeConfig:
      gvisorPlatform: kvm
```

## Usage Examples

### Basic GPU Workload

```python
from beam import App, Runtime, Image

app = App(
    name="gvisor-gpu-demo",
    runtime=Runtime(
        gpu="T4",
        cpu=4,
        memory="16Gi",
    ),
)

@app.function()
def train_model():
    import torch
    
    # GPU available through nvproxy
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")
    
    # Your GPU code here
    model = YourModel().to(device)
    # ...
```

### Machine Learning Training

```python
@app.function()
def train_with_tensorflow():
    import tensorflow as tf
    
    # Check GPU availability
    gpus = tf.config.list_physical_devices('GPU')
    print(f"GPUs available: {len(gpus)}")
    
    # Train model with GPU acceleration
    model = tf.keras.models.Sequential([...])
    model.compile(optimizer='adam', loss='sparse_categorical_crossentropy')
    model.fit(train_data, epochs=10)
```

## Performance Considerations

### nvproxy vs. Direct Passthrough (runc)

| Metric | runc (passthrough) | gVisor (nvproxy) | Notes |
|--------|-------------------|------------------|-------|
| **Throughput** | 100% | 95-98% | Small overhead for driver proxying |
| **Latency** | Baseline | +1-3% | Minimal added latency |
| **Memory** | Baseline | +50-100MB | nvproxy overhead |
| **Security** | Lower | Higher | Strong isolation with gVisor |

### Best Use Cases for nvproxy

✅ **Recommended:**
- Multi-tenant GPU workloads
- Untrusted code execution
- ML inference services
- Batch training jobs

⚠️ **Consider runc instead:**
- Ultra low-latency requirements (<1ms)
- Maximum throughput critical applications
- Single-tenant dedicated GPU scenarios

## Compatibility

### Supported

✅ NVIDIA GPUs (Compute Capability 3.5+)
✅ CUDA 11.x, 12.x
✅ cuDNN
✅ TensorFlow
✅ PyTorch
✅ JAX
✅ ONNX Runtime
✅ Triton Inference Server

### Not Supported

❌ AMD GPUs (no AMD driver proxy)
❌ Intel GPUs (no Intel driver proxy)
❌ Direct `/dev/nvidia*` device access
❌ NVIDIA Fabric Manager (multi-GPU NVLink)

## Requirements

### Host Requirements

1. **NVIDIA Driver**: Version 525.60.13 or newer
2. **Kernel Modules**: nvidia.ko, nvidia-uvm.ko loaded
3. **nvidia-container-toolkit**: Installed for CDI support
4. **gVisor**: Installed with nvproxy support (included in worker image)

### Runtime Requirements

```dockerfile
# Already included in worker Dockerfile
COPY --from=gvisor /usr/local/bin/runsc /usr/local/bin/runsc
```

### Verification

Check if nvproxy is available:

```bash
# On the worker node
runsc help | grep -i nvproxy
```

Expected output:
```
--nvproxy=true: Enable nvproxy GPU support
```

## Troubleshooting

### GPU Not Detected

**Symptoms:**
```python
torch.cuda.is_available()  # Returns False
```

**Solutions:**

1. **Verify host driver:**
   ```bash
   nvidia-smi
   ```

2. **Check CDI devices:**
   ```bash
   ls /etc/cdi/*.yaml
   ```

3. **Verify runsc nvproxy:**
   ```bash
   runsc help | grep nvproxy
   ```

4. **Check worker pool config:**
   ```yaml
   pools:
     default:
       runtime: gvisor
       gpuType: T4  # Must be set
   ```

### Driver Version Mismatch

**Error:**
```
CUDA driver version is insufficient for CUDA runtime version
```

**Solution:**
Update host NVIDIA driver to match CUDA requirements:
```bash
# Check required version
nvidia-smi

# Update driver if needed
apt-get update
apt-get install -y nvidia-driver-535
```

### Permission Errors

**Error:**
```
Failed to initialize NVML: Insufficient Permissions
```

**Cause:**
Container doesn't have GPU devices injected.

**Solution:**
Ensure CDI is working and devices are injected. Check container spec includes CDI device references.

### Performance Issues

**Symptoms:**
Slower than expected GPU performance.

**Solutions:**

1. **Use KVM platform** (if available):
   ```yaml
   runtimeConfig:
     gvisorPlatform: kvm  # Faster than ptrace
   ```

2. **Check GPU utilization:**
   ```bash
   nvidia-smi dmon
   ```

3. **Profile your application:**
   ```python
   import torch.profiler as profiler
   
   with profiler.profile(
       activities=[profiler.ProfilerActivity.CPU, profiler.ProfilerActivity.CUDA],
       record_shapes=True
   ) as prof:
       # Your GPU code
       model(input_data)
   
   print(prof.key_averages().table())
   ```

## Limitations

1. **Single GPU per container**: nvproxy currently supports one GPU per sandbox
2. **No MIG support**: NVIDIA Multi-Instance GPU not yet supported
3. **No GPUDirect**: Direct GPU-to-GPU communication not supported
4. **No CUDA Graphs**: Some advanced CUDA features may not work

## Security Benefits

nvproxy provides several security advantages:

1. **Syscall Filtering**: All GPU-related syscalls go through gVisor's seccomp filter
2. **Memory Isolation**: GPU memory access is controlled and isolated
3. **Driver Protection**: Host NVIDIA driver protected from direct container access
4. **Resource Limits**: GPU usage can be monitored and limited
5. **No Privileged Containers**: No need for `--privileged` flag

## Comparison: nvproxy vs. Kata Containers vs. Bare Metal

| Feature | gVisor + nvproxy | Kata Containers | Bare Metal (runc) |
|---------|-----------------|-----------------|-------------------|
| **Security** | High | Very High | Low |
| **Performance** | 95-98% | 90-95% | 100% |
| **Startup Time** | Fast (~100ms) | Slow (~1s) | Fast (~50ms) |
| **Memory Overhead** | Low (~50MB) | High (~150MB) | Minimal |
| **GPU Support** | Via proxy | Via passthrough | Direct |
| **Compatibility** | Good | Excellent | Excellent |

## References

- [gVisor GPU Documentation](https://gvisor.dev/docs/user_guide/gpu/)
- [nvproxy Design Doc](https://gvisor.dev/docs/architecture_guide/nvproxy/)
- [NVIDIA Container Toolkit](https://github.com/NVIDIA/nvidia-container-toolkit)
- [CDI Specification](https://github.com/cncf-tags/container-device-interface)

## Future Enhancements

Planned improvements for nvproxy:

- Multi-GPU support
- MIG (Multi-Instance GPU) support
- GPUDirect RDMA support
- AMD GPU support (via similar proxy mechanism)
- Enhanced performance monitoring and profiling
