# gVisor CUDA Checkpoint/Restore

## Overview

CUDA checkpoint/restore for gVisor works by:
1. Auto-mounting `cuda-checkpoint` from host into container
2. Running `cuda-checkpoint` inside the container via `runsc exec`
3. Freezing GPU state before checkpoint, unfreezing after restore

## Implementation

### Auto-Mount (in `Prepare()`)

When GPU devices are detected, `cuda-checkpoint` is automatically bind-mounted:

```go
spec.Mounts = append(spec.Mounts, specs.Mount{
    Destination: "/usr/local/bin/cuda-checkpoint",
    Source:      "/usr/bin/cuda-checkpoint", // from host
    Type:        "bind",
    Options:     []string{"bind", "ro"},
})
```

### Checkpoint Flow

```
1. Find CUDA processes in container
2. For each PID: runsc exec container-id /usr/local/bin/cuda-checkpoint checkpoint <pid>
3. Run runsc checkpoint
```

### Restore Flow

```
1. Run runsc restore
2. Find CUDA processes in restored container
3. For each PID: runsc exec container-id /usr/local/bin/cuda-checkpoint restore <pid>
```

## Requirements

- `cuda-checkpoint` binary at `/usr/bin/cuda-checkpoint` on worker
- NVIDIA driver >= 570 (recommended)
- GPU devices in container spec (CDI or direct)

## Key Points

- cuda-checkpoint runs **inside** the container (not from host)
- Uses **container PIDs** (not host PIDs)
- Process detection: checks `/proc` for nvidia device file descriptors
- Fallback: defaults to PID 1 if detection fails
- Non-blocking: errors in CUDA checkpoint don't fail the entire checkpoint

## Code

See `pkg/runtime/runsc.go` for implementation:
- `mountCudaCheckpoint()` - Mounts binary
- `findCUDAProcesses()` - Detects CUDA processes
- `cudaCheckpointProcesses()` - Freeze/unfreeze GPU state
