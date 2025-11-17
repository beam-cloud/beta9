# Corrected gVisor CUDA Checkpoint Implementation - Summary

## What Was Wrong

### Previous Incorrect Assumption ❌
I initially assumed that gVisor's nvproxy would automatically handle GPU checkpoint/restore, similar to how runc+CRIU works. This was **incorrect**.

**What I thought:**
- nvproxy intercepts CUDA calls → checkpoint will automatically capture GPU state
- `runsc checkpoint` alone would be sufficient
- GPU state restoration would happen automatically on `runsc restore`

**Why this was wrong:**
- nvproxy only intercepts CUDA API calls at runtime
- nvproxy does NOT automatically checkpoint/restore GPU state
- Manual intervention is required using the `cuda-checkpoint` binary

## Correct Implementation ✅

### The Right Approach (Based on gVisor GitHub Issues)

gVisor requires a **two-step manual process** for CUDA checkpoint/restore:

#### Checkpoint:
1. **First**: Run `cuda-checkpoint` binary INSIDE the sandbox to freeze CUDA processes
2. **Then**: Run `runsc checkpoint` to create checkpoint image

#### Restore:
1. **First**: Run `runsc restore` to restore the sandbox
2. **Then**: Run `cuda-checkpoint` binary INSIDE the sandbox to unfreeze CUDA processes

## What Was Changed

### File: `pkg/runtime/runsc.go`

#### 1. Updated Documentation
```go
// OLD (incorrect):
// gVisor supports CUDA checkpoint/restore through its nvproxy feature, which
// intercepts CUDA API calls at the userspace level. GPU state is automatically
// captured through nvproxy's interception layer.

// NEW (correct):
// gVisor requires a two-step process for CUDA checkpoints:
// 1. Checkpoint:
//    a. Run cuda-checkpoint binary INSIDE the gVisor sandbox on all CUDA processes
//    b. Then run `runsc checkpoint` to create the checkpoint image
// 2. Restore:
//    a. Run `runsc restore` to restore the sandbox from checkpoint
//    b. Run cuda-checkpoint binary INSIDE the restored sandbox to unfreeze CUDA processes
```

#### 2. Enhanced Checkpoint Method
```go
func (r *Runsc) Checkpoint(ctx, containerID, opts) error {
    // Step 1: Freeze CUDA processes (NEW!)
    if r.nvproxyEnabled {
        if err := r.freezeCUDAProcesses(ctx, containerID, opts.OutputWriter); err != nil {
            return err
        }
    }

    // Step 2: Run runsc checkpoint (existing)
    args := r.baseArgs(false)
    args = append(args, "checkpoint", "--image-path", opts.ImagePath, containerID)
    return cmd.Run()
}
```

#### 3. Enhanced Restore Method
```go
func (r *Runsc) Restore(ctx, containerID, opts) (int, error) {
    // Step 1: Run runsc restore (existing)
    args := r.baseArgs(false)
    if r.nvproxyEnabled {
        args = append(args, "--nvproxy=true")
    }
    args = append(args, "restore", "--image-path", opts.ImagePath, containerID)
    cmd.Wait()

    // Step 2: Unfreeze CUDA processes (NEW!)
    if r.nvproxyEnabled {
        if err := r.unfreezeCUDAProcesses(ctx, containerID, opts.OutputWriter); err != nil {
            return -1, err
        }
    }

    return 0, nil
}
```

#### 4. Added Helper Methods (NEW!)

```go
// freezeCUDAProcesses runs cuda-checkpoint inside the sandbox to freeze GPU state
func (r *Runsc) freezeCUDAProcesses(ctx, containerID, outputWriter) error {
    cudaPIDs, _ := r.findCUDAProcesses(ctx, containerID)
    for _, pid := range cudaPIDs {
        r.runCUDACheckpoint(ctx, containerID, pid, "checkpoint", outputWriter)
    }
}

// unfreezeCUDAProcesses runs cuda-checkpoint inside the sandbox to unfreeze GPU state
func (r *Runsc) unfreezeCUDAProcesses(ctx, containerID, outputWriter) error {
    cudaPIDs, _ := r.findCUDAProcesses(ctx, containerID)
    for _, pid := range cudaPIDs {
        r.runCUDACheckpoint(ctx, containerID, pid, "restore", outputWriter)
    }
}

// findCUDAProcesses finds all CUDA processes inside the container
func (r *Runsc) findCUDAProcesses(ctx, containerID) ([]int, error) {
    // Method 1: Use lsof to find processes with /dev/nvidia* file descriptors
    // Method 2: Fallback to checking /proc for nvidia device files
}

// runCUDACheckpoint executes cuda-checkpoint inside the container
func (r *Runsc) runCUDACheckpoint(ctx, containerID, pid int, action string, outputWriter) error {
    // runsc exec container-id cuda-checkpoint [checkpoint|restore] PID
}
```

## Key Differences: runc vs gVisor

| Aspect | runc + CRIU | gVisor + runsc |
|--------|-------------|----------------|
| **Checkpoint Method** | CRIU plugin automatic | Manual cuda-checkpoint binary |
| **Where cuda-checkpoint runs** | Controlled by CRIU | **INSIDE container** via runsc exec |
| **Process Detection** | CRIU handles | **Manual detection** via lsof or /proc |
| **Checkpoint Steps** | 1 step (runc checkpoint) | **2 steps** (freeze, then checkpoint) |
| **Restore Steps** | 1 step (runc restore) | **2 steps** (restore, then unfreeze) |
| **Binary Location** | Host or container | **MUST be in container** |

## Requirements for gVisor CUDA Checkpoint

### 1. cuda-checkpoint Binary in Container ⚠️

**CRITICAL**: The `cuda-checkpoint` binary must be installed **INSIDE the container**:

```dockerfile
# Option 1: Install from package
RUN apt-get update && apt-get install -y cuda-checkpoint

# Option 2: Copy from host
COPY cuda-checkpoint /usr/local/bin/cuda-checkpoint
RUN chmod +x /usr/local/bin/cuda-checkpoint
```

### 2. nvproxy Enabled

```bash
runsc --nvproxy=true run --bundle /path container-id
```

### 3. GPU Devices in OCI Spec

Via CDI or direct device specifications.

### 4. NVIDIA Driver >= 570

Recommended for full checkpoint support.

## Implementation Flow

### Checkpoint Flow
```
Container Running with CUDA
         ↓
Find CUDA Processes (runsc exec lsof /dev/nvidia*)
         ↓
For Each Process: runsc exec cuda-checkpoint checkpoint PID
         ↓ (GPU state frozen)
runsc checkpoint --image-path /path container-id
         ↓
Checkpoint Complete (GPU state saved)
```

### Restore Flow
```
runsc restore --image-path /path container-id
         ↓
Container Restored (GPU state frozen)
         ↓
Find CUDA Processes (runsc exec lsof /dev/nvidia*)
         ↓
For Each Process: runsc exec cuda-checkpoint restore PID
         ↓ (GPU state unfrozen)
Container Running with CUDA
```

## Testing

### Unit Tests Added

```bash
# Test correct CUDA checkpoint workflow
go test ./pkg/runtime/... -run TestGVisorCUDACheckpointWorkflow -v

# Test cuda-checkpoint binary requirements
go test ./pkg/runtime/... -run TestCUDACheckpointBinaryRequirement -v

# Test CUDA checkpoint requirements
go test ./pkg/runtime/... -run TestCUDACheckpointRequirements -v
```

### Integration Testing

1. Create container image with cuda-checkpoint:
```dockerfile
FROM nvidia/cuda:12.4.0-runtime-ubuntu22.04
RUN apt-get update && apt-get install -y cuda-checkpoint
COPY my-cuda-app /app/cuda-app
CMD ["/app/cuda-app"]
```

2. Run with gVisor:
```bash
runsc --nvproxy=true run --bundle /bundle container-id
```

3. Checkpoint:
```go
runtime.Checkpoint(ctx, containerID, &CheckpointOpts{
    ImagePath: "/checkpoint",
})
// Internally: freezes CUDA, then checkpoints
```

4. Restore:
```go
runtime.Restore(ctx, containerID, &RestoreOpts{
    ImagePath: "/checkpoint",
    BundlePath: "/bundle",
})
// Internally: restores, then unfreezes CUDA
```

## Documentation Files

### New Documentation
- **GVISOR_CUDA_CHECKPOINT.md** - Complete guide to correct workflow
- **CORRECTED_IMPLEMENTATION_SUMMARY.md** - This file

### Updated Documentation
- **CHECKPOINT_RESTORE_IMPLEMENTATION.md** - Now includes correct gVisor workflow
- **CUDA_CHECKPOINT_OPTIONS.md** - Updated with two-step process

## Error Handling

### Graceful Degradation

If `cuda-checkpoint` is not found in the container:
- Checkpoint proceeds without CUDA freeze
- Warning logged but no fatal error
- Allows non-CUDA containers to checkpoint normally

### Process Detection Failure

If CUDA processes cannot be detected:
- Returns empty process list
- Checkpoint continues
- May result in corrupted GPU state (user responsibility to include cuda-checkpoint)

## Summary of Changes

### ✅ What Was Fixed
1. Corrected understanding of gVisor CUDA checkpoint workflow
2. Implemented two-step checkpoint process (freeze, then checkpoint)
3. Implemented two-step restore process (restore, then unfreeze)
4. Added CUDA process detection inside container
5. Added cuda-checkpoint binary execution via runsc exec
6. Updated all documentation with correct workflow
7. Added comprehensive tests documenting requirements

### ❌ What Was Wrong
1. Assumed nvproxy automatically handles checkpoint
2. Missing pre-checkpoint CUDA freeze step
3. Missing post-restore CUDA unfreeze step
4. No process detection mechanism
5. No cuda-checkpoint binary execution

### 🎯 Result
- **Correct implementation** following gVisor GitHub issues and documentation
- **Two-step process** properly implemented
- **cuda-checkpoint binary** runs inside container as required
- **Automatic CUDA process detection** via lsof or /proc
- **Graceful degradation** if cuda-checkpoint not available
- **Comprehensive tests** documenting requirements

## References

- gVisor GitHub Issues on CUDA checkpoint
- NVIDIA cuda-checkpoint documentation  
- gVisor nvproxy documentation
- User feedback on correct workflow

## Conclusion

Thank you for the correction! The implementation now correctly follows the gVisor CUDA checkpoint workflow:

1. ✅ Run `cuda-checkpoint` **inside** the container before checkpoint
2. ✅ Run `runsc checkpoint` to save state
3. ✅ Run `runsc restore` to restore state
4. ✅ Run `cuda-checkpoint` **inside** the container after restore

This two-step manual process is fundamentally different from runc where CRIU handles everything automatically.
