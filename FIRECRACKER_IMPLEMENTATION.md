# Firecracker microVM Runtime Implementation

This document provides an overview of the Firecracker microVM runtime implementation for beta9, following the design principle: **smart worker, dumb guest**.

## Summary

The Firecracker runtime enables beta9 to run workloads in lightweight microVMs instead of containers, providing hardware-level isolation while maintaining near-container performance. All orchestration logic lives in the worker on the host side, with a minimal init shim (~200 lines) running inside each microVM.

## Implementation Status

✅ **Phase 1 Complete** - All core functionality implemented and tested:

- ✅ Runtime interface implementation (`firecracker.go`)
- ✅ Rootfs preparation using ext4 block devices (`firecracker_rootfs.go`)
- ✅ TAP networking support (`firecracker_network.go`)
- ✅ Guest VM init shim (`cmd/vm-init/main.go`)
- ✅ vsock protocol for host-guest communication
- ✅ Comprehensive unit tests
- ✅ Integration tests
- ✅ Documentation

## Key Files

```
pkg/runtime/
├── runtime.go                        # Runtime interface (extended for Firecracker)
├── firecracker.go                    # Main Firecracker runtime implementation
├── firecracker_rootfs.go             # Rootfs preparation (Phase 1: ext4)
├── firecracker_network.go            # TAP device networking
├── firecracker_test.go               # Unit tests
├── firecracker_integration_test.go   # Integration tests
└── FIRECRACKER.md                    # Detailed documentation

cmd/vm-init/
└── main.go                           # Guest VM init shim

Makefile                              # Added build targets for vm-init
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                      Worker (Host)                      │
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │            FirecrackerRuntime                     │ │
│  │  - Prepares rootfs (ext4 block device)           │ │
│  │  - Creates TAP network device                    │ │
│  │  - Spawns Firecracker process                    │ │
│  │  - Manages lifecycle (Run/Kill/Delete/State)     │ │
│  │  - Handles vsock communication                   │ │
│  └───────────────────────┬───────────────────────────┘ │
│                          │                             │
│                     vsock (JSON)                        │
│                          │                             │
└──────────────────────────┼─────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────┐
│                    Guest microVM                       │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │         beta9-vm-init (PID 1)                    │ │
│  │  - Mounts filesystems (proc, sys, dev)          │ │
│  │  - Sets up networking                            │ │
│  │  - Connects to host via vsock                   │ │
│  │  - Receives process specification               │ │
│  │  - Spawns workload                              │ │
│  │  - Forwards stdout/stderr                       │ │
│  │  - Reports exit status                          │ │
│  └──────────────────────┬───────────────────────────┘ │
│                         │                              │
│  ┌──────────────────────▼───────────────────────────┐ │
│  │           User Workload Process                  │ │
│  └──────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Prerequisites

Install Firecracker and prepare a kernel:

```bash
# Install Firecracker
ARCH="$(uname -m)"
RELEASE_URL="https://github.com/firecracker-microvm/firecracker/releases"
LATEST=$(basename $(curl -fsSLI -o /dev/null -w  %{url_effective} ${RELEASE_URL}/latest))
curl -L ${RELEASE_URL}/download/${LATEST}/firecracker-${LATEST}-${ARCH}.tgz | tar -xz
sudo mv release-${LATEST}-${ARCH}/firecracker-${LATEST}-${ARCH} /usr/local/bin/firecracker

# Download a kernel (or build your own)
curl -fsSL -o /var/lib/beta9/vmlinux \
  https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.9/x86_64/vmlinux-5.10.217
```

### 2. Build the Guest Init

```bash
make vm-init-static
# This creates bin/beta9-vm-init
```

### 3. Ensure Init in Container Images

The `beta9-vm-init` binary must be present in container images at `/sbin/beta9-vm-init`:

```dockerfile
# In your Dockerfile
COPY bin/beta9-vm-init /sbin/beta9-vm-init
RUN chmod +x /sbin/beta9-vm-init
```

### 4. Configure Runtime

```yaml
# config.yaml
runtime:
  type: firecracker
  firecracker_bin: /usr/local/bin/firecracker
  microvm_root: /var/lib/beta9/microvm
  kernel_image: /var/lib/beta9/vmlinux
  default_cpus: 1
  default_mem_mib: 512
```

Or in code:

```go
import "github.com/beam-cloud/beta9/pkg/runtime"

cfg := runtime.Config{
    Type:           "firecracker",
    FirecrackerBin: "/usr/local/bin/firecracker",
    MicroVMRoot:    "/var/lib/beta9/microvm",
    KernelImage:    "/var/lib/beta9/vmlinux",
    DefaultCPUs:    1,
    DefaultMemMiB:  512,
    Debug:          false,
}

rt, err := runtime.New(cfg)
if err != nil {
    log.Fatal(err)
}
defer rt.Close()
```

### 5. Run a Workload

```go
ctx := context.Background()
containerID := "my-microvm-001"
bundlePath := "/path/to/oci/bundle"  // Standard OCI bundle

started := make(chan int)
opts := &runtime.RunOpts{
    Started:      started,
    OutputWriter: os.Stdout,
}

// Run the microVM (blocks until exit)
go rt.Run(ctx, containerID, bundlePath, opts)

// Wait for start
pid := <-started
fmt.Printf("MicroVM started with PID %d\n", pid)

// Check state
state, _ := rt.State(ctx, containerID)
fmt.Printf("Status: %s\n", state.Status)

// Kill after some time
time.Sleep(30 * time.Second)
rt.Kill(ctx, containerID, syscall.SIGTERM, &runtime.KillOpts{All: true})

// Cleanup
rt.Delete(ctx, containerID, &runtime.DeleteOpts{Force: true})
```

## Testing

### Quick Test with Local Harness

We provide a comprehensive test harness that sets up everything needed and runs all tests:

```bash
# Run all tests (setup + unit + integration + manual)
sudo ./bin/test_firecracker_local.sh all

# Or run individual test suites
sudo ./bin/test_firecracker_local.sh setup        # Just setup environment
sudo ./bin/test_firecracker_local.sh unit         # Unit tests only
sudo ./bin/test_firecracker_local.sh integration  # Integration tests only
sudo ./bin/test_firecracker_local.sh manual       # Manual end-to-end test

# Clean up test environment
sudo ./bin/test_firecracker_local.sh clean
```

The test harness will:
- ✅ Check all prerequisites (KVM, root, tools)
- ✅ Download and install Firecracker
- ✅ Download kernel image
- ✅ Build beta9-vm-init
- ✅ Create test OCI bundles
- ✅ Run all tests
- ✅ Clean up automatically

### Manual Testing

#### Run Unit Tests

```bash
make test-runtime
```

#### Run Integration Tests

Requires root privileges and Firecracker installed:

```bash
export FIRECRACKER_KERNEL=/var/lib/beta9/vmlinux
sudo make test-runtime-integration
```

#### Set Up Local Environment Manually

If you prefer to set up manually:

```bash
# 1. Install Firecracker
ARCH="$(uname -m)"
RELEASE_URL="https://github.com/firecracker-microvm/firecracker/releases"
LATEST="v1.9.2"
curl -L ${RELEASE_URL}/download/${LATEST}/firecracker-${LATEST}-${ARCH}.tgz | tar -xz
sudo mv release-${LATEST}-${ARCH}/firecracker-${LATEST}-${ARCH} /usr/local/bin/firecracker

# 2. Download kernel
sudo mkdir -p /var/lib/beta9
sudo curl -fsSL -o /var/lib/beta9/vmlinux \
  https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.9/${ARCH}/vmlinux-5.10.217

# 3. Build vm-init
make vm-init-static

# 4. Run tests
export FIRECRACKER_KERNEL=/var/lib/beta9/vmlinux
sudo make test-runtime-integration
```

## Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `FirecrackerBin` | No | `firecracker` | Path to firecracker binary |
| `MicroVMRoot` | No | `/var/lib/beta9/microvm` | Root directory for VM state |
| `KernelImage` | **Yes** | - | Path to Linux kernel image |
| `InitrdImage` | No | - | Path to initrd (optional) |
| `DefaultCPUs` | No | `1` | Default vCPU count |
| `DefaultMemMiB` | No | `512` | Default memory in MiB |
| `Debug` | No | `false` | Enable debug logging |

## Runtime Capabilities

| Capability | Supported | Notes |
|------------|-----------|-------|
| `CheckpointRestore` | ❌ | Phase 2 feature |
| `GPU` | ❌ | Not supported by microVMs |
| `OOMEvents` | ❌ | Use cgroup poller fallback |
| `JoinExistingNetNS` | ✅ | Via TAP in pod netns |
| `CDI` | ❌ | Not applicable |

## How It Works

### 1. Runtime Preparation

When `Prepare()` is called, the runtime:
- Strips out host-specific configs (namespaces, cgroups, seccomp)
- Filters mounts to only essential ones
- Validates the spec for microVM compatibility

### 2. Rootfs Setup (Phase 1)

When `Run()` is called:
1. Calculate rootfs size from OCI bundle
2. Create sparse ext4 image file
3. Format with `mkfs.ext4`
4. Mount, copy contents, unmount
5. Attach to Firecracker as root block device

### 3. Networking

For each microVM:
1. Create TAP device: `tap-{containerID}`
2. Attach to pod's network namespace
3. Configure in Firecracker
4. Guest uses DHCP to obtain IP

### 4. VM Boot

1. Start Firecracker with config
2. Wait for API socket
3. VM boots with kernel and rootfs
4. `beta9-vm-init` starts as PID 1
5. Init connects to host via vsock

### 5. Process Execution

1. Host sends process spec via vsock
2. Guest spawns process
3. Guest forwards stdout/stderr
4. Guest reports exit code

### 6. Cleanup

1. Kill Firecracker process
2. Remove TAP device
3. Delete rootfs image
4. Remove VM directory

## vsock Protocol

Simple JSON messages over vsock CID 2 (host), port determined per-VM.

**Host → Guest:**

```json
{
  "type": "run",
  "process": {
    "args": ["/bin/sh", "-c", "echo hello"],
    "env": ["PATH=/bin"],
    "cwd": "/work",
    "terminal": false
  }
}
```

**Guest → Host:**

```json
{"type": "output", "output": "hello\n"}
{"type": "exit", "exit_code": 0}
{"type": "error", "error": "failed to start"}
```

## Design Decisions

### Why "Smart Worker, Dumb Guest"?

1. **Simplicity**: Guest init is ~200 lines, easy to maintain
2. **Flexibility**: All logic in worker can evolve without guest changes
3. **Security**: Less attack surface inside VM
4. **Debugging**: Easier to debug issues in worker than in guest

### Why Phase 1 Uses ext4?

1. **Reliability**: ext4 is battle-tested and well-understood
2. **Simplicity**: Easy to implement and debug
3. **Compatibility**: Works everywhere with standard tools
4. **Baseline**: Establishes working system before optimization

Phase 2 will add lazy block device support for faster startup.

### Why TAP Networking?

1. **Integration**: Works with existing pod networking
2. **Compatibility**: Standard approach for VM networking
3. **Flexibility**: Can bridge, route, or isolate as needed
4. **Performance**: Near-native network performance

## Performance Characteristics

Based on Firecracker benchmarks:

- **Boot time**: ~125-200ms (cold start)
- **Memory overhead**: ~5MB per VM (excluding workload)
- **Startup latency**: ~1-2s (includes rootfs preparation)
- **Runtime performance**: ~95-99% native (KVM)
- **Network throughput**: ~10Gbps with TAP
- **Disk I/O**: ~1GB/s sequential read/write

## Limitations & Future Work

### Current Limitations

1. **Slower startup**: Rootfs copy adds 1-2s vs containers
2. **No checkpoint/restore**: CRIU-style snapshots not implemented
3. **No GPU**: Hardware passthrough not supported
4. **Root required**: TAP device management needs privileges
5. **Linux/x86_64 only**: Limited platform support

### Phase 2: Lazy Block Device

Next phase will integrate NBD for lazy loading:
- Share base layers across VMs
- Load blocks on-demand
- Faster startup (~200ms total)
- Better disk utilization

### Phase 3: Advanced Features

- Snapshot and restore
- Live migration
- Memory ballooning
- Better TTY/exec support

## Troubleshooting

### "firecracker binary not found"

```bash
# Check firecracker is installed
which firecracker

# Install if missing
# (see Prerequisites above)
```

### "kernel image not found"

```bash
# Check kernel exists
ls -lh /var/lib/beta9/vmlinux

# Download if missing
curl -fsSL -o /var/lib/beta9/vmlinux \
  https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.9/x86_64/vmlinux-5.10.217
```

### "guest not connecting"

```bash
# Check beta9-vm-init is in rootfs
docker run --rm your-image ls -l /sbin/beta9-vm-init

# Add if missing (in Dockerfile)
COPY bin/beta9-vm-init /sbin/beta9-vm-init
RUN chmod +x /sbin/beta9-vm-init
```

### Debug mode

Enable debug logging:

```go
cfg.Debug = true
```

Logs will be written to `$MicroVMRoot/$containerID/firecracker.log`

## References

- [Detailed Documentation](pkg/runtime/FIRECRACKER.md)
- [Firecracker Docs](https://github.com/firecracker-microvm/firecracker/tree/main/docs)
- [OCI Runtime Spec](https://github.com/opencontainers/runtime-spec)
- [Design Discussion](https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md)

## Contributing

To extend or modify the Firecracker runtime:

1. Read the detailed documentation: `pkg/runtime/FIRECRACKER.md`
2. Run tests: `make test-runtime`
3. Add integration tests for new features
4. Update documentation

## License

Same as beta9 project license.
