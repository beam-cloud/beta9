# Firecracker microVM Runtime

This document describes the Firecracker microVM runtime implementation for beta9.

## Overview

The Firecracker runtime provides strong isolation using microVMs (micro virtual machines) powered by [Firecracker](https://firecracker-microvm.github.io/). Unlike container runtimes (runc, gVisor), each workload runs in its own lightweight VM with a dedicated kernel, providing hardware-level isolation while maintaining near-container performance.

### Key Characteristics

- **Isolation**: Each workload runs in a separate microVM with its own kernel
- **Performance**: Fast boot times (~125ms) and low memory overhead
- **Security**: Hardware-level isolation through virtualization
- **Simplicity**: All orchestration logic lives in the worker; guest only runs a tiny init shim

## Architecture

```
┌─────────────────────────────────────────┐
│           Worker (Host)                 │
│  ┌────────────────────────────────────┐ │
│  │   FirecrackerRuntime               │ │
│  │   - Spawns Firecracker             │ │
│  │   - Drives API                     │ │
│  │   - Manages lifecycle              │ │
│  │   - Handles vsock communication    │ │
│  └────────────┬───────────────────────┘ │
│               │ vsock                    │
│  ┌────────────▼───────────────────────┐ │
│  │   Firecracker Process              │ │
│  │   - KVM virtualization             │ │
│  │   - Network (TAP)                  │ │
│  │   - Block device (rootfs)          │ │
│  └────────────┬───────────────────────┘ │
└───────────────┼─────────────────────────┘
                │
┌───────────────▼─────────────────────────┐
│         Guest VM (microVM)              │
│  ┌────────────────────────────────────┐ │
│  │   beta9-vm-init (PID 1)            │ │
│  │   - Connects to host via vsock     │ │
│  │   - Receives process spec          │ │
│  │   - Spawns workload                │ │
│  │   - Forwards I/O                   │ │
│  │   - Reports exit status            │ │
│  └────────────┬───────────────────────┘ │
│               │                          │
│  ┌────────────▼───────────────────────┐ │
│  │   User Workload Process            │ │
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

## Components

### 1. FirecrackerRuntime (Host)

Located in `firecracker.go`, this implements the `Runtime` interface and manages microVM lifecycle:

- **Preparation**: Converts OCI spec to microVM configuration
- **Rootfs Management**: Creates ext4 block device from container rootfs
- **Networking**: Sets up TAP devices for VM networking
- **Lifecycle**: Start, stop, kill, delete operations
- **Communication**: vsock protocol for guest interaction

### 2. beta9-vm-init (Guest)

Located in `cmd/vm-init/main.go`, this is a minimal init system that runs inside each microVM:

- Boots as PID 1
- Mounts essential filesystems (proc, sys, dev)
- Connects to host via vsock
- Receives process specification via JSON
- Spawns and monitors the workload
- Forwards stdout/stderr to host
- Reports exit status

**Design Philosophy**: The init is intentionally simple (~200 lines). All orchestration logic lives in the worker on the host side. The guest is a "dumb shim" that just spawns processes and forwards I/O.

## Configuration

### Runtime Config

```go
cfg := runtime.Config{
    Type:          "firecracker",
    FirecrackerBin: "/usr/bin/firecracker",  // Path to firecracker binary
    MicroVMRoot:   "/var/lib/beta9/microvm",  // Root for VM state
    KernelImage:   "/var/lib/beta9/vmlinux",  // Linux kernel image
    InitrdImage:   "",                        // Optional initrd
    DefaultCPUs:   1,                         // Default vCPU count
    DefaultMemMiB: 512,                       // Default memory in MiB
    Debug:        false,                      // Enable debug logging
}

runtime, err := runtime.New(cfg)
```

### Prerequisites

1. **Firecracker binary**: Install from https://github.com/firecracker-microvm/firecracker/releases
2. **Kernel image**: A Linux kernel compiled for virtualization
3. **KVM support**: Host must support KVM (`/dev/kvm` must exist)
4. **Root privileges**: Required for TAP devices and network setup

### Building the Guest Init

```bash
# Build static binary for guest VM
make vm-init-static

# This creates bin/beta9-vm-init which should be included in VM rootfs at /sbin/beta9-vm-init
```

## Rootfs Preparation

### Phase 1: Simple ext4 (Current Implementation)

The runtime creates a sparse ext4 image for each microVM:

1. Calculate rootfs size from OCI bundle
2. Create sparse file with 20% overhead
3. Format as ext4
4. Mount and copy rootfs contents
5. Ensure beta9-vm-init is present and executable
6. Unmount and attach to Firecracker

**Pros**: Simple, reliable, easy to debug
**Cons**: Slower startup (need to copy entire rootfs)

### Phase 2: Lazy Block Device (Future)

Future enhancement will use NBD (Network Block Device) for lazy loading:

1. Start block device service in worker
2. Expose rootfs from CAS/FUSE as NBD device
3. Attach NBD device to Firecracker
4. Load blocks on-demand as VM reads them

**Pros**: Faster startup, shared storage, efficient use of disk
**Cons**: More complex, requires additional daemon

## Networking

Each microVM gets a TAP device in the host network namespace:

```
┌──────────────────────────────────────┐
│  Worker Pod (Host Network NS)       │
│                                      │
│  ┌────────────────┐                 │
│  │  tap-abc123    │◄─────┐          │
│  └────────────────┘      │          │
│         │                │          │
│    ┌────▼─────┐          │          │
│    │  Bridge  │          │          │
│    │  or CNI  │    Firecracker      │
│    └──────────┘          │          │
│                          │          │
└──────────────────────────┼──────────┘
                           │
                   ┌───────▼────────┐
                   │   Guest VM     │
                   │    eth0        │
                   └────────────────┘
```

The TAP device is attached to the pod's network, allowing the microVM to communicate as if it were another container in the same pod.

## vsock Protocol

Communication between host and guest uses a simple JSON-based protocol over vsock:

### Message Types

#### Host → Guest

**Run Process**:
```json
{
  "type": "run",
  "process": {
    "args": ["/bin/sh", "-c", "echo hello"],
    "env": ["PATH=/bin:/usr/bin"],
    "cwd": "/work",
    "terminal": false
  }
}
```

**Execute Additional Process**:
```json
{
  "type": "exec",
  "process": { ... }
}
```

**Kill Process**:
```json
{
  "type": "kill",
  "signal": 15
}
```

#### Guest → Host

**Output**:
```json
{
  "type": "output",
  "output": "hello\n"
}
```

**Exit**:
```json
{
  "type": "exit",
  "exit_code": 0
}
```

**Error**:
```json
{
  "type": "error",
  "error": "failed to start process"
}
```

## Usage Example

```go
package main

import (
    "context"
    "github.com/beam-cloud/beta9/pkg/runtime"
)

func main() {
    // Create runtime
    cfg := runtime.Config{
        Type:           "firecracker",
        FirecrackerBin: "firecracker",
        MicroVMRoot:    "/tmp/microvms",
        KernelImage:    "/path/to/vmlinux",
        DefaultCPUs:    1,
        DefaultMemMiB:  256,
    }
    
    rt, err := runtime.New(cfg)
    if err != nil {
        panic(err)
    }
    defer rt.Close()
    
    // Run a container
    ctx := context.Background()
    containerID := "my-microvm-001"
    bundlePath := "/path/to/oci/bundle"
    
    started := make(chan int)
    opts := &runtime.RunOpts{
        Started: started,
    }
    
    go rt.Run(ctx, containerID, bundlePath, opts)
    
    // Wait for start
    pid := <-started
    fmt.Printf("MicroVM started with PID %d\n", pid)
    
    // Check state
    state, _ := rt.State(ctx, containerID)
    fmt.Printf("Status: %s\n", state.Status)
    
    // Kill after 10 seconds
    time.Sleep(10 * time.Second)
    rt.Kill(ctx, containerID, syscall.SIGTERM, &runtime.KillOpts{All: true})
    
    // Cleanup
    rt.Delete(ctx, containerID, &runtime.DeleteOpts{Force: true})
}
```

## Testing

### Unit Tests

```bash
make test-runtime
```

### Integration Tests

Integration tests require:
- Root privileges
- Firecracker installed
- Kernel image available

```bash
# Set kernel path
export FIRECRACKER_KERNEL=/path/to/vmlinux

# Run integration tests
sudo make test-runtime-integration
```

## Capabilities

The Firecracker runtime reports the following capabilities:

| Capability | Supported | Notes |
|------------|-----------|-------|
| CheckpointRestore | ❌ | Phase 1: not implemented; future enhancement |
| GPU | ❌ | microVMs don't support GPU passthrough |
| OOMEvents | ❌ | Use cgroup poller on host as fallback |
| JoinExistingNetNS | ✅ | Via TAP device in pod network namespace |
| CDI | ❌ | Not applicable for microVMs |

## Performance Characteristics

- **Boot time**: ~125-200ms (cold start)
- **Memory overhead**: ~5-10MB per VM (excluding workload)
- **Startup latency**: Higher than containers due to rootfs preparation
- **Runtime performance**: Near-native (KVM virtualization)

## Limitations

### Phase 1 Limitations

1. **No checkpoint/restore**: CRIU-style checkpointing not implemented
2. **No GPU support**: Hardware passthrough not supported
3. **Slower startup**: Rootfs must be fully prepared before boot
4. **Exec limitations**: Basic exec support, may not handle all edge cases

### General Limitations

1. **Requires KVM**: Host must support hardware virtualization
2. **Linux only**: Firecracker only runs on Linux hosts
3. **x86_64/aarch64**: Limited architecture support
4. **Root required**: TAP device management requires privileges

## Future Enhancements

### Phase 2: Lazy Block Device

- Integrate with NBD for lazy rootfs loading
- Share base layers across multiple VMs
- Faster startup times

### Phase 3: Advanced Features

- Snapshot and restore support
- Live migration between workers
- Memory ballooning for better resource utilization
- Improved exec support with TTY handling

## Troubleshooting

### Firecracker won't start

- Check `/dev/kvm` exists and is accessible
- Verify firecracker binary is in PATH
- Check kernel image is valid
- Review debug logs in `$MicroVMRoot/$containerID/firecracker.log`

### Guest not connecting

- Verify beta9-vm-init is in guest rootfs at `/sbin/beta9-vm-init`
- Check vsock support in kernel
- Review guest logs (requires console output capture)

### Network issues

- Verify TAP device was created: `ip link show`
- Check network namespace configuration
- Ensure guest has DHCP client or static IP configured

### Rootfs errors

- Verify sufficient disk space
- Check permissions on MicroVMRoot directory
- Ensure mkfs.ext4 is installed
- Review mount errors in debug logs

## References

- [Firecracker Documentation](https://github.com/firecracker-microvm/firecracker/tree/main/docs)
- [Firecracker Design](https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md)
- [OCI Runtime Specification](https://github.com/opencontainers/runtime-spec)
- [vsock Overview](https://man7.org/linux/man-pages/man7/vsock.7.html)
