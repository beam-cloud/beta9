# gVisor Platform Configuration

## Overview

gVisor supports multiple platforms for executing sandboxed applications. Each platform provides different trade-offs between performance, compatibility, and security isolation.

## Available Platforms

### 1. systrap (Recommended Default)

**Best for**: General-purpose workloads, cloud environments, nested virtualization

```yaml
runtimeConfig:
  gvisorPlatform: systrap
```

**Pros:**
- ✅ Works without KVM hardware support
- ✅ Better performance than ptrace (~20-30% faster for most workloads)
- ✅ Works in nested virtualization (VMs, containers)
- ✅ No special kernel modules required
- ✅ More stable than experimental platforms
- ✅ Good CPU and memory efficiency

**Cons:**
- ⚠️ Slightly slower than KVM (~10-20% overhead vs KVM)
- ⚠️ Requires kernel 4.12+ for optimal performance

**Use Cases:**
- Cloud deployments (AWS, GCP, Azure)
- CI/CD environments
- Development and testing
- Multi-tenant platforms
- Containers running in VMs

### 2. kvm (Highest Performance)

**Best for**: Bare metal servers with KVM support

```yaml
runtimeConfig:
  gvisorPlatform: kvm
```

**Pros:**
- ✅ Best performance (~5-10% overhead)
- ✅ Native hardware virtualization
- ✅ Strong isolation via VM technology
- ✅ Good for CPU-intensive workloads

**Cons:**
- ❌ Requires `/dev/kvm` access
- ❌ Needs KVM kernel module loaded
- ❌ Doesn't work in nested virtualization
- ❌ Not available in most cloud environments
- ❌ Requires privileged container or host access

**Use Cases:**
- Bare metal deployments
- High-performance computing
- Latency-sensitive applications
- Dedicated GPU workloads

### 3. ptrace (Maximum Compatibility)

**Best for**: Legacy systems, debugging

```yaml
runtimeConfig:
  gvisorPlatform: ptrace
```

**Pros:**
- ✅ Works everywhere (no special requirements)
- ✅ Good for debugging
- ✅ Maximum compatibility
- ✅ No kernel version requirements

**Cons:**
- ❌ Slowest performance (~50-100% overhead)
- ❌ High CPU usage
- ❌ Poor scalability
- ❌ Not recommended for production

**Use Cases:**
- Debugging gVisor issues
- Very old kernel versions
- Proof of concept testing
- Last resort fallback

## Platform Comparison

| Platform | Performance | Compatibility | Requirements | Recommended |
|----------|------------|---------------|--------------|-------------|
| **systrap** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Kernel 4.12+ | ✅ **Yes** |
| **kvm** | ⭐⭐⭐⭐⭐ | ⭐⭐ | KVM support | For bare metal |
| **ptrace** | ⭐⭐ | ⭐⭐⭐⭐⭐ | None | For debug only |

## Why systrap is the Default

We've chosen `systrap` as the default platform for Beta9 because:

1. **Cloud Native**: Works in all cloud environments (AWS, GCP, Azure) without special permissions
2. **Good Performance**: Only 10-20% slower than KVM, much faster than ptrace
3. **Portability**: Works in nested virtualization scenarios
4. **Reliability**: Stable and well-tested in production
5. **No Setup**: Doesn't require KVM modules or `/dev/kvm` access
6. **Developer Friendly**: Works in local development with Docker/Podman

## Performance Benchmarks

### Syscall Overhead

| Platform | Overhead | Relative |
|----------|----------|----------|
| Native | 0% | 1.0x |
| kvm | 5-10% | 1.05-1.10x |
| systrap | 10-20% | 1.10-1.20x |
| ptrace | 50-100% | 1.50-2.00x |

### Network Throughput

| Platform | Throughput | Relative |
|----------|------------|----------|
| Native | 100% | 1.0x |
| kvm | 95-98% | 0.95-0.98x |
| systrap | 90-95% | 0.90-0.95x |
| ptrace | 50-70% | 0.50-0.70x |

### Memory Overhead

| Platform | Per-Container | Base |
|----------|---------------|------|
| runc | ~5 MB | 0 MB |
| kvm | ~50 MB | ~20 MB |
| systrap | ~30 MB | ~10 MB |
| ptrace | ~40 MB | ~10 MB |

## Configuration Examples

### Production Setup (Recommended)

```yaml
worker:
  pools:
    default:
      runtime: gvisor
      runtimeConfig:
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor

    gpu-pool:
      runtime: gvisor
      gpuType: T4
      runtimeConfig:
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
```

### Bare Metal with KVM

```yaml
worker:
  pools:
    high-performance:
      runtime: gvisor
      runtimeConfig:
        gvisorPlatform: kvm
        gvisorRoot: /run/gvisor
```

**Prerequisites:**
```bash
# Verify KVM is available
ls -l /dev/kvm

# Ensure KVM module is loaded
lsmod | grep kvm

# Grant container access (if running in Docker)
docker run --device /dev/kvm ...
```

### Development/Testing

```yaml
worker:
  pools:
    dev:
      runtime: gvisor
      runtimeConfig:
        gvisorPlatform: systrap  # Works in Docker Desktop, etc.
        gvisorRoot: /tmp/gvisor
```

## Switching Platforms

### At Runtime

Platforms are configured per worker pool and cannot be changed without restarting the worker.

### Testing Different Platforms

Create multiple pools for comparison:

```yaml
worker:
  pools:
    systrap-pool:
      runtime: gvisor
      runtimeConfig:
        gvisorPlatform: systrap
    
    kvm-pool:
      runtime: gvisor
      runtimeConfig:
        gvisorPlatform: kvm
    
    runc-pool:
      runtime: runc
```

## Troubleshooting

### systrap Not Working

**Error:**
```
platform systrap is not supported
```

**Solutions:**
1. Update kernel to 4.12+
2. Verify runsc version supports systrap: `runsc --version`
3. Check runsc help: `runsc help | grep systrap`

### KVM Not Available

**Error:**
```
cannot open /dev/kvm: No such file or directory
```

**Solutions:**
1. Load KVM module: `modprobe kvm kvm_intel` (or `kvm_amd`)
2. Install KVM: `apt-get install qemu-kvm`
3. Verify hardware support: `egrep -c '(vmx|svm)' /proc/cpuinfo`
4. Fall back to systrap if KVM not available

### Performance Issues

If experiencing poor performance:

1. **Check platform**: Ensure not using ptrace
2. **Try KVM**: If on bare metal with KVM support
3. **Monitor overhead**: Use `runsc debug` commands
4. **Compare with runc**: Run same workload on runc pool

## Best Practices

1. **Use systrap by default** unless you have specific needs
2. **Reserve KVM for bare metal** high-performance scenarios
3. **Avoid ptrace** except for debugging
4. **Test your workload** on different platforms if performance-critical
5. **Monitor metrics** to understand actual overhead
6. **Document platform choice** in your deployment configs

## References

- [gVisor Platform Guide](https://gvisor.dev/docs/architecture_guide/platforms/)
- [Performance Guide](https://gvisor.dev/docs/architecture_guide/performance/)
- [Platform Selection](https://gvisor.dev/docs/user_guide/platforms/)

## FAQ

### Q: Why not use KVM everywhere?

**A:** KVM requires hardware virtualization support and `/dev/kvm` access, which isn't available in:
- Cloud VMs (AWS EC2, GCP GCE, etc.)
- Docker containers
- Nested virtualization scenarios
- CI/CD environments

### Q: Is systrap production-ready?

**A:** Yes, systrap is stable and recommended for production use. It's been extensively tested and is used in many production deployments.

### Q: Can I mix platforms in the same cluster?

**A:** Yes! You can have different worker pools using different platforms. For example:
- Bare metal workers use KVM
- Cloud workers use systrap
- Development workers use systrap

### Q: What about nvproxy (GPU) performance?

**A:** GPU virtualization via nvproxy works with all platforms. The platform choice affects CPU syscall overhead, not GPU performance. Use systrap or KVM for best GPU workload performance.

### Q: When should I use runc instead of gVisor?

**A:** Use runc when you need:
- Checkpoint/restore (CRIU)
- Absolute minimum overhead (<5%)
- Legacy applications with special requirements
- Maximum kernel feature compatibility
