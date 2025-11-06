# Local Testing Guide for Firecracker Runtime

This guide explains how to test the Firecracker microVM runtime locally on your development machine.

## Prerequisites

### System Requirements

- **Operating System**: Linux (Ubuntu 20.04+ or Debian Bookworm recommended)
- **Architecture**: x86_64 or aarch64
- **Virtualization**: KVM support (check with `ls /dev/kvm`)
- **Privileges**: Root access (for KVM and TAP devices)

### Required Tools

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
    curl \
    tar \
    golang \
    e2fsprogs \
    iproute2 \
    git \
    make
```

### Enable KVM

```bash
# Check if KVM is available
ls -la /dev/kvm

# If not available, enable virtualization:
# 1. Enable VT-x/AMD-V in BIOS
# 2. Load KVM modules
sudo modprobe kvm
sudo modprobe kvm_intel  # For Intel
# OR
sudo modprobe kvm_amd    # For AMD
```

## Quick Start: Automated Test Harness

The easiest way to test is using our automated test harness:

```bash
# Clone the repository
git clone https://github.com/beam-cloud/beta9.git
cd beta9

# Run all tests (automatically sets up everything)
sudo ./bin/test_firecracker_local.sh all
```

That's it! The test harness will:
1. ✅ Check prerequisites
2. ✅ Download Firecracker v1.9.2
3. ✅ Download kernel image
4. ✅ Build beta9-vm-init
5. ✅ Create test bundles
6. ✅ Run unit tests
7. ✅ Run integration tests
8. ✅ Run manual end-to-end test
9. ✅ Clean up automatically

## Test Harness Commands

### Full Test Suite

```bash
sudo ./bin/test_firecracker_local.sh all
```

### Individual Components

```bash
# Just setup (download Firecracker, kernel, build init)
sudo ./bin/test_firecracker_local.sh setup

# Unit tests only
sudo ./bin/test_firecracker_local.sh unit

# Integration tests only
sudo ./bin/test_firecracker_local.sh integration

# Manual end-to-end test
sudo ./bin/test_firecracker_local.sh manual

# Clean up test environment
sudo ./bin/test_firecracker_local.sh clean
```

### Help

```bash
./bin/test_firecracker_local.sh --help
```

## Manual Testing

If you prefer to set up and test manually:

### 1. Install Firecracker

```bash
# Detect architecture
ARCH="$(uname -m)"
if [ "$ARCH" = "x86_64" ]; then
    FC_ARCH="x86_64"
elif [ "$ARCH" = "aarch64" ]; then
    FC_ARCH="aarch64"
fi

# Download and install
RELEASE_URL="https://github.com/firecracker-microvm/firecracker/releases"
VERSION="v1.9.2"
curl -fsSL ${RELEASE_URL}/download/${VERSION}/firecracker-${VERSION}-${FC_ARCH}.tgz -o firecracker.tgz
tar -xzf firecracker.tgz
sudo mv release-${VERSION}-${FC_ARCH}/firecracker-${VERSION}-${FC_ARCH} /usr/local/bin/firecracker
sudo chmod +x /usr/local/bin/firecracker

# Verify
firecracker --version
```

### 2. Download Kernel Image

```bash
# Create directory
sudo mkdir -p /var/lib/beta9

# Download kernel (matches your architecture)
sudo curl -fsSL \
    https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.9/${FC_ARCH}/vmlinux-5.10.217 \
    -o /var/lib/beta9/vmlinux

sudo chmod 644 /var/lib/beta9/vmlinux
```

### 3. Build VM Init

```bash
cd /path/to/beta9
make vm-init-static
```

This creates `bin/beta9-vm-init`.

### 4. Run Unit Tests

```bash
cd /path/to/beta9

# Run Firecracker-specific tests
go test -v ./pkg/runtime -run TestFirecracker

# Or run all runtime tests
make test-runtime
```

### 5. Run Integration Tests

```bash
cd /path/to/beta9

# Set kernel path
export FIRECRACKER_KERNEL=/var/lib/beta9/vmlinux

# Run integration tests (requires root)
sudo -E make test-runtime-integration

# Or directly
sudo -E go test -v -tags=integration ./pkg/runtime -run TestFirecracker -timeout 5m
```

## Testing in Docker Worker

To test the full integration in the worker container:

### 1. Build Worker Image

```bash
cd /path/to/beta9

# Build worker with Firecracker support
docker build -f docker/Dockerfile.worker -t beta9-worker:firecracker .
```

### 2. Run Worker Container

```bash
# Run with KVM access
docker run -it --rm \
    --privileged \
    --device=/dev/kvm \
    --cap-add=NET_ADMIN \
    beta9-worker:firecracker \
    bash

# Inside container, verify installation
firecracker --version
ls -la /var/lib/beta9/vmlinux
ls -la /usr/local/bin/beta9-vm-init
```

### 3. Test Runtime in Container

```bash
# Inside the worker container
cd /workspace

# Run unit tests
go test -v ./pkg/runtime -run TestFirecracker

# Run integration tests
go test -v -tags=integration ./pkg/runtime -run TestFirecracker -timeout 5m
```

## Creating Custom Test Bundles

To test with your own container images:

### 1. Create OCI Bundle

```bash
# Create bundle structure
mkdir -p /tmp/test-bundle/rootfs

# Export container to rootfs
docker export $(docker create alpine:latest) | tar -C /tmp/test-bundle/rootfs -xf -

# Add beta9-vm-init
cp bin/beta9-vm-init /tmp/test-bundle/rootfs/sbin/
chmod +x /tmp/test-bundle/rootfs/sbin/beta9-vm-init

# Create config.json (see example in test harness)
```

### 2. Run with Runtime

```go
package main

import (
    "context"
    "github.com/beam-cloud/beta9/pkg/runtime"
    "time"
)

func main() {
    cfg := runtime.Config{
        Type:           "firecracker",
        FirecrackerBin: "/usr/local/bin/firecracker",
        KernelImage:    "/var/lib/beta9/vmlinux",
        MicroVMRoot:    "/tmp/microvms",
        DefaultCPUs:    1,
        DefaultMemMiB:  512,
        Debug:          true,
    }
    
    rt, _ := runtime.New(cfg)
    defer rt.Close()
    
    ctx := context.Background()
    opts := &runtime.RunOpts{}
    
    rt.Run(ctx, "test-vm-001", "/tmp/test-bundle", opts)
}
```

## Troubleshooting

### "Permission denied" on /dev/kvm

```bash
# Add your user to kvm group
sudo usermod -a -G kvm $USER

# Or run with sudo
sudo ./bin/test_firecracker_local.sh all
```

### "Firecracker binary not found"

The test harness downloads Firecracker automatically. If testing manually:

```bash
# Check if installed
which firecracker

# Install if missing
sudo ./bin/test_firecracker_local.sh setup
```

### "Kernel image not found"

```bash
# Check if exists
ls -la /var/lib/beta9/vmlinux

# Download if missing
sudo mkdir -p /var/lib/beta9
ARCH=$(uname -m)
sudo curl -fsSL \
    https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.9/${ARCH}/vmlinux-5.10.217 \
    -o /var/lib/beta9/vmlinux
```

### "Failed to create TAP device"

TAP device creation requires root:

```bash
# Run with sudo
sudo ./bin/test_firecracker_local.sh all

# Or grant CAP_NET_ADMIN
sudo setcap cap_net_admin+ep $(which firecracker)
```

### Tests timeout

Increase timeout in integration tests:

```bash
go test -v -tags=integration ./pkg/runtime -timeout 10m
```

### Debug mode

Enable debug logging:

```go
cfg := runtime.Config{
    // ...
    Debug: true,  // Enable debug logs
}
```

Logs will be in `$MicroVMRoot/$containerID/firecracker.log`.

## Performance Testing

### Measure Boot Time

```bash
# Run manual test and observe timing
sudo ./bin/test_firecracker_local.sh manual
```

### Benchmark

```bash
cd /path/to/beta9
go test -v ./pkg/runtime -bench=. -run=^$ -benchtime=10s
```

## Continuous Testing

### Watch Mode

```bash
# Install entr or similar
sudo apt-get install entr

# Watch for changes and re-run tests
find pkg/runtime cmd/vm-init -name "*.go" | entr -c sudo make test-runtime
```

### Pre-commit Hook

```bash
# Add to .git/hooks/pre-commit
#!/bin/bash
make test-runtime || exit 1
```

## CI/CD Integration

Example GitHub Actions workflow:

```yaml
name: Firecracker Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.22'
      
      - name: Enable KVM
        run: |
          echo 'KERNEL=="kvm", GROUP="kvm", MODE="0666", OPTIONS+="static_node=kvm"' | sudo tee /etc/udev/rules.d/99-kvm4all.rules
          sudo udevadm control --reload-rules
          sudo udevadm trigger --name-match=kvm
      
      - name: Run tests
        run: sudo ./bin/test_firecracker_local.sh all
```

## Next Steps

After successful local testing:

1. **Deploy to staging**: Test in beta9 worker environment
2. **Performance benchmarks**: Compare vs runc/gVisor
3. **Load testing**: Multiple concurrent VMs
4. **Security audit**: Review isolation and attack surface
5. **Production deployment**: Roll out gradually

## Resources

- [Firecracker Documentation](https://github.com/firecracker-microvm/firecracker/tree/main/docs)
- [Implementation Guide](FIRECRACKER_IMPLEMENTATION.md)
- [Detailed Documentation](pkg/runtime/FIRECRACKER.md)
- [Test Harness Source](bin/test_firecracker_local.sh)

## Getting Help

If you encounter issues:

1. Check [Troubleshooting](#troubleshooting) section
2. Review logs in `$MicroVMRoot/$containerID/firecracker.log`
3. Run with debug mode enabled
4. Check Firecracker logs: `dmesg | grep firecracker`
5. Verify KVM: `ls -la /dev/kvm`

For bugs or feature requests, please open an issue on GitHub.
