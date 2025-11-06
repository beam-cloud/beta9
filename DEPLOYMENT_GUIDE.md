# Firecracker Runtime - Deployment Guide

This guide covers deploying the Firecracker microVM runtime in production beta9 environments.

## What's Included in the Worker

The worker Docker image now includes everything needed for Firecracker:

### Binaries

- ✅ **firecracker** (v1.9.2) - The microVM hypervisor
- ✅ **jailer** (v1.9.2) - Security sandboxing for Firecracker
- ✅ **beta9-vm-init** - Guest VM init system (built from source)
- ✅ **vmlinux** (5.10.217) - Linux kernel for microVMs

### Directories

- `/usr/local/bin/firecracker` - Firecracker binary
- `/usr/local/bin/jailer` - Jailer binary
- `/usr/local/bin/beta9-vm-init` - VM init binary
- `/var/lib/beta9/vmlinux` - Kernel image
- `/var/lib/beta9/microvm/` - Runtime directory for VMs

## Dockerfile Changes

The following changes were made to `docker/Dockerfile.worker`:

### 1. Firecracker Stage

```dockerfile
# Firecracker
# ========================
FROM debian:bookworm AS firecracker

ARG TARGETARCH
ARG FIRECRACKER_VERSION=v1.9.2

# Downloads and installs:
# - Firecracker binary
# - Jailer binary  
# - Kernel image (vmlinux-5.10.217)
```

### 2. VM Init Build

```dockerfile
# In worker stage
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -extldflags=-static" \
    -o /usr/local/bin/beta9-vm-init \
    ./cmd/vm-init/main.go
```

### 3. Binary Installation

```dockerfile
# Copy from build stages
COPY --from=firecracker /usr/local/bin/firecracker /usr/local/bin/firecracker
COPY --from=firecracker /usr/local/bin/jailer /usr/local/bin/jailer
COPY --from=firecracker /var/lib/beta9/vmlinux /var/lib/beta9/vmlinux
COPY --from=worker /usr/local/bin/beta9-vm-init /usr/local/bin/beta9-vm-init
```

### 4. Runtime Directories

```dockerfile
# Create Firecracker runtime directories
RUN mkdir -p /var/lib/beta9/microvm
```

## Building the Worker Image

### Standard Build

```bash
# Build with Firecracker support (automatically included)
docker build -f docker/Dockerfile.worker -t beta9-worker:latest .
```

### Multi-arch Build

```bash
# Build for both amd64 and arm64
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -f docker/Dockerfile.worker \
    -t beta9-worker:latest \
    --push .
```

### Custom Firecracker Version

```bash
# Build with specific Firecracker version
docker build \
    --build-arg FIRECRACKER_VERSION=v1.10.0 \
    -f docker/Dockerfile.worker \
    -t beta9-worker:fc-v1.10.0 .
```

## Deployment

### Kubernetes

The worker pod needs additional capabilities:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: beta9-worker
spec:
  containers:
  - name: worker
    image: beta9-worker:latest
    securityContext:
      privileged: true  # Required for KVM access
      capabilities:
        add:
        - NET_ADMIN     # For TAP devices
        - SYS_ADMIN     # For mounting
    volumeMounts:
    - name: dev-kvm
      mountPath: /dev/kvm
    - name: microvm-root
      mountPath: /var/lib/beta9/microvm
  volumes:
  - name: dev-kvm
    hostPath:
      path: /dev/kvm
      type: CharDevice
  - name: microvm-root
    emptyDir: {}
```

### Docker Compose

```yaml
version: '3.8'

services:
  worker:
    image: beta9-worker:latest
    privileged: true
    devices:
      - /dev/kvm:/dev/kvm
    cap_add:
      - NET_ADMIN
      - SYS_ADMIN
    volumes:
      - microvm-data:/var/lib/beta9/microvm
    environment:
      - RUNTIME_TYPE=firecracker
      - FIRECRACKER_KERNEL=/var/lib/beta9/vmlinux

volumes:
  microvm-data:
```

### Docker Run

```bash
docker run -d \
    --name beta9-worker \
    --privileged \
    --device=/dev/kvm \
    --cap-add=NET_ADMIN \
    --cap-add=SYS_ADMIN \
    -v /var/lib/beta9/microvm:/var/lib/beta9/microvm \
    -e RUNTIME_TYPE=firecracker \
    beta9-worker:latest
```

## Configuration

### Worker Configuration

Configure the runtime in worker config:

```yaml
# config.yaml
runtime:
  type: firecracker
  firecracker_bin: /usr/local/bin/firecracker
  kernel_image: /var/lib/beta9/vmlinux
  microvm_root: /var/lib/beta9/microvm
  default_cpus: 1
  default_mem_mib: 512
  debug: false
```

Or via environment variables:

```bash
export RUNTIME_TYPE=firecracker
export FIRECRACKER_BIN=/usr/local/bin/firecracker
export FIRECRACKER_KERNEL=/var/lib/beta9/vmlinux
export MICROVM_ROOT=/var/lib/beta9/microvm
export DEFAULT_CPUS=1
export DEFAULT_MEM_MIB=512
```

### Container Images

Container images that will run in microVMs must include `beta9-vm-init`:

```dockerfile
# In your container Dockerfile
COPY beta9-vm-init /sbin/beta9-vm-init
RUN chmod +x /sbin/beta9-vm-init
```

The worker image already includes the init, so you can copy it:

```dockerfile
FROM beta9-worker:latest AS init-source

FROM your-base-image
COPY --from=init-source /usr/local/bin/beta9-vm-init /sbin/beta9-vm-init
RUN chmod +x /sbin/beta9-vm-init
```

## Verification

### Check Installation

```bash
# Inside worker container
docker exec -it beta9-worker bash

# Verify binaries
which firecracker
firecracker --version

which jailer
jailer --version

ls -la /var/lib/beta9/vmlinux
ls -la /usr/local/bin/beta9-vm-init

# Check KVM access
ls -la /dev/kvm
```

### Test Runtime

```bash
# Inside worker container
cd /workspace

# Run unit tests
go test -v ./pkg/runtime -run TestFirecracker

# Quick smoke test
cat > /tmp/test.go <<'EOF'
package main
import (
    "context"
    "github.com/beam-cloud/beta9/pkg/runtime"
)
func main() {
    cfg := runtime.Config{
        Type: "firecracker",
        FirecrackerBin: "/usr/local/bin/firecracker",
        KernelImage: "/var/lib/beta9/vmlinux",
        MicroVMRoot: "/tmp/test-vms",
    }
    rt, _ := runtime.New(cfg)
    defer rt.Close()
    println("Firecracker runtime OK!")
}
EOF

go run /tmp/test.go
```

## Local Testing

### Quick Test

```bash
# On your local machine (Linux with KVM)
git clone https://github.com/beam-cloud/beta9.git
cd beta9

# Run comprehensive test suite
sudo ./bin/test_firecracker_local.sh all
```

See [LOCAL_TESTING.md](LOCAL_TESTING.md) for detailed testing instructions.

### Test Harness Features

- ✅ Automatic Firecracker download and installation
- ✅ Kernel image download
- ✅ VM init build
- ✅ Test bundle creation
- ✅ Unit tests
- ✅ Integration tests
- ✅ Manual end-to-end tests
- ✅ Automatic cleanup

## Production Checklist

### Pre-deployment

- [ ] **KVM Enabled**: Verify `/dev/kvm` exists on worker nodes
- [ ] **Kernel Modules**: Ensure `kvm`, `kvm_intel`/`kvm_amd` loaded
- [ ] **Privileges**: Worker pods have `privileged: true` and `NET_ADMIN`
- [ ] **Storage**: Adequate disk space for VM images (`/var/lib/beta9/microvm`)
- [ ] **Testing**: Run test harness successfully
- [ ] **Images**: Container images include `beta9-vm-init`

### Post-deployment

- [ ] **Verify Installation**: Check binaries in worker pods
- [ ] **Run Smoke Tests**: Test basic VM creation
- [ ] **Monitor Resources**: Watch CPU, memory, disk usage
- [ ] **Check Logs**: Review worker and Firecracker logs
- [ ] **Performance**: Benchmark vs baseline (runc)

## Monitoring

### Metrics to Track

```bash
# VM count
ls /var/lib/beta9/microvm | wc -l

# Disk usage
du -sh /var/lib/beta9/microvm

# Firecracker processes
ps aux | grep firecracker | wc -l

# Memory usage per VM
for vm in /var/lib/beta9/microvm/*; do
    vmid=$(basename $vm)
    ps aux | grep $vmid | awk '{print $6}'
done
```

### Log Locations

- **Worker logs**: stdout/stderr of worker process
- **Firecracker logs**: `$MICROVM_ROOT/$containerID/firecracker.log` (if debug enabled)
- **Guest logs**: Forwarded via vsock to worker

### Health Checks

```bash
# Check Firecracker binary
firecracker --version || echo "FAIL: Firecracker not available"

# Check kernel
[ -f /var/lib/beta9/vmlinux ] || echo "FAIL: Kernel missing"

# Check KVM
[ -e /dev/kvm ] || echo "FAIL: KVM not available"

# Check init
[ -f /usr/local/bin/beta9-vm-init ] || echo "FAIL: VM init missing"
```

## Troubleshooting

### Firecracker not found

```bash
# Check if binary exists
ls -la /usr/local/bin/firecracker

# Rebuild worker image if missing
docker build -f docker/Dockerfile.worker -t beta9-worker:latest .
```

### KVM not accessible

```bash
# Check device exists
ls -la /dev/kvm

# Check permissions
# Should be accessible by worker process

# In Kubernetes, ensure pod has:
# - privileged: true
# - /dev/kvm mounted
```

### VM init not found in container

```bash
# Container images need beta9-vm-init at /sbin/beta9-vm-init
# Add to your Dockerfile:
COPY --from=beta9-worker /usr/local/bin/beta9-vm-init /sbin/beta9-vm-init
RUN chmod +x /sbin/beta9-vm-init
```

### TAP device creation fails

```bash
# Worker needs NET_ADMIN capability
# In Kubernetes:
securityContext:
  capabilities:
    add:
    - NET_ADMIN
```

## Rollback

If you need to revert to runc:

```yaml
# config.yaml
runtime:
  type: runc  # Change from firecracker to runc
```

Or:

```bash
export RUNTIME_TYPE=runc
```

No changes needed to the worker image - it includes all runtimes.

## Performance Tuning

### Resource Limits

```yaml
# Adjust per workload
runtime:
  default_cpus: 2        # More vCPUs
  default_mem_mib: 1024  # More memory
```

### Storage

```bash
# Use faster storage for microVM root
# tmpfs for testing:
mount -t tmpfs -o size=10G tmpfs /var/lib/beta9/microvm
```

### Kernel Parameters

```bash
# Optimize for many VMs
sysctl -w vm.max_map_count=262144
sysctl -w net.ipv4.ip_forward=1
```

## Security

### Isolation

Firecracker provides hardware-level isolation:
- Separate kernel per VM
- No shared resources
- Attack surface: ~50KB of Rust code

### Additional Hardening

```yaml
# Use jailer for extra sandboxing (Phase 2)
runtime:
  jailer_bin: /usr/local/bin/jailer
  use_jailer: true
```

### Network Policies

```yaml
# Limit VM network access
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: microvm-policy
spec:
  podSelector:
    matchLabels:
      app: beta9-worker
  policyTypes:
  - Egress
  egress:
  - to:
    - podSelector: {}
```

## Cost Optimization

### Right-sizing

- Start with minimal resources (1 CPU, 512MB)
- Monitor actual usage
- Adjust defaults based on workloads

### Resource Sharing

- Multiple small VMs per worker node
- Overcommit memory (with care)
- Share base kernel image across VMs

## Support

For issues or questions:

1. Check [LOCAL_TESTING.md](LOCAL_TESTING.md)
2. Review [FIRECRACKER_IMPLEMENTATION.md](FIRECRACKER_IMPLEMENTATION.md)
3. See [pkg/runtime/FIRECRACKER.md](pkg/runtime/FIRECRACKER.md)
4. Run local test harness for debugging
5. Open GitHub issue with logs and config

## Next Steps

1. **Test locally**: `sudo ./bin/test_firecracker_local.sh all`
2. **Build worker**: `docker build -f docker/Dockerfile.worker -t beta9-worker:fc .`
3. **Deploy to staging**: Test with real workloads
4. **Monitor performance**: Compare vs baseline
5. **Roll out gradually**: Start with subset of workers
6. **Plan Phase 2**: Lazy block device integration

---

**Deployment Status**: ✅ Ready for Testing
**Production Status**: ⚠️ Staging Evaluation Recommended
