# gVisor CUDA Checkpoint/Restore - Corrected Implementation

## Summary

This implementation has been **corrected** based on user feedback to follow the official gVisor CUDA checkpoint workflow.

## Key Correction ⚠️

**Previous Assumption (Incorrect):**
> nvproxy automatically handles GPU checkpoint/restore like runc+CRIU does.

**Correct Approach:**
> gVisor requires manually running `cuda-checkpoint` binary **inside the sandbox** before/after checkpoint/restore.

## The Correct Workflow

### Checkpoint
```
1. Find CUDA processes → runsc exec lsof /dev/nvidia*
2. Freeze each process → runsc exec cuda-checkpoint checkpoint PID
3. Create checkpoint → runsc checkpoint --image-path /path container-id
```

### Restore
```
1. Restore container → runsc restore --image-path /path container-id
2. Find CUDA processes → runsc exec lsof /dev/nvidia*
3. Unfreeze each process → runsc exec cuda-checkpoint restore PID
```

## Implementation

### File: `pkg/runtime/runsc.go`

#### New Methods Added:
- `freezeCUDAProcesses()` - Freeze CUDA before checkpoint
- `unfreezeCUDAProcesses()` - Unfreeze CUDA after restore
- `findCUDAProcesses()` - Detect CUDA processes via lsof or /proc
- `runCUDACheckpoint()` - Execute cuda-checkpoint inside container

#### Updated Methods:
- `Checkpoint()` - Now freezes CUDA before checkpointing
- `Restore()` - Now unfreezes CUDA after restoring

### Example Usage

```go
// Create runtime
rt, _ := NewRunsc(Config{RunscPath: "runsc"})

// Checkpoint (automatically handles CUDA freeze)
err := rt.Checkpoint(ctx, containerID, &CheckpointOpts{
    ImagePath:    "/checkpoint",
    LeaveRunning: true,
})

// Restore (automatically handles CUDA unfreeze)
exitCode, err := rt.Restore(ctx, containerID, &RestoreOpts{
    ImagePath:  "/checkpoint",
    BundlePath: "/bundle",
})
```

## Critical Requirement

### cuda-checkpoint Binary

⚠️ **MUST be installed INSIDE the container:**

```dockerfile
FROM nvidia/cuda:12.4.0-runtime-ubuntu22.04
RUN apt-get update && apt-get install -y cuda-checkpoint
COPY my-cuda-app /app/
CMD ["/app/my-cuda-app"]
```

## Documentation

### Primary Documentation
1. **GVISOR_CUDA_CHECKPOINT.md** - Complete implementation guide
2. **CORRECTED_IMPLEMENTATION_SUMMARY.md** - What was fixed
3. **GVISOR_CUDA_CHECKPOINT_CHANGES.md** - Change details

### Quick Reference
- **Two-step checkpoint**: freeze CUDA → runsc checkpoint
- **Two-step restore**: runsc restore → unfreeze CUDA
- **Binary location**: INSIDE container (not on host)
- **Process detection**: lsof or /proc fallback
- **Graceful degradation**: Works without cuda-checkpoint (no CUDA support)

## Testing

### Run Tests
```bash
# All runtime tests
go test ./pkg/runtime/... -v

# CUDA-specific tests
go test ./pkg/runtime/... -run TestCUDACheckpoint -v
go test ./pkg/runtime/... -run TestGVisorCUDA -v

# CRIU manager tests
go test ./pkg/worker/... -run TestNvidiaCRIUManager -v
```

### Verification Results
```
✅ All binaries build successfully
✅ All tests pass
✅ Runtime package compiles without errors
✅ Worker package compiles without errors
✅ CUDA checkpoint workflow implemented correctly
```

## Comparison: runc vs gVisor

| Feature | runc + CRIU | gVisor + runsc |
|---------|-------------|----------------|
| CUDA Checkpoint | Automatic | **Manual (2-step)** |
| cuda-checkpoint | Called by CRIU | **Must call manually** |
| Binary Location | Host or container | **INSIDE container only** |
| Process Detection | CRIU handles | **Manual via lsof/proc** |
| Steps | 1 (checkpoint) | **2 (freeze → checkpoint)** |

## Key Differences

### runc (CRIU)
```bash
# Single step - CRIU handles everything
runc checkpoint container-id
```

### gVisor (runsc)
```bash
# Step 1: Freeze CUDA (MANUAL)
runsc exec container-id cuda-checkpoint checkpoint 1234

# Step 2: Checkpoint container
runsc checkpoint container-id
```

## Requirements Checklist

- [x] cuda-checkpoint binary in container image
- [x] nvproxy enabled (--nvproxy=true)
- [x] GPU devices in OCI spec
- [x] NVIDIA driver >= 570 (recommended)
- [x] Two-step checkpoint process implemented
- [x] Two-step restore process implemented
- [x] CUDA process detection implemented
- [x] Graceful degradation if cuda-checkpoint missing

## What Changed

### Code Changes
- ✅ Added CUDA freeze/unfreeze methods
- ✅ Added CUDA process detection (lsof + /proc)
- ✅ Updated Checkpoint() to freeze before checkpoint
- ✅ Updated Restore() to unfreeze after restore
- ✅ Added comprehensive inline documentation

### Tests Added
- ✅ TestGVisorCUDACheckpointWorkflow
- ✅ TestCUDACheckpointBinaryRequirement
- ✅ Updated TestCUDACheckpointRequirements

### Documentation
- ✅ Created GVISOR_CUDA_CHECKPOINT.md
- ✅ Created CORRECTED_IMPLEMENTATION_SUMMARY.md
- ✅ Created GVISOR_CUDA_CHECKPOINT_CHANGES.md
- ✅ Removed incorrect old documentation

## Credits

Implementation corrected based on:
- User feedback pointing to correct gVisor workflow
- gVisor GitHub issues on CUDA checkpoint
- NVIDIA cuda-checkpoint documentation

## Status

🎯 **COMPLETE** - Corrected implementation following official gVisor CUDA checkpoint workflow

All todos completed:
- ✅ Understand correct gVisor CUDA workflow
- ✅ Implement pre-checkpoint CUDA freeze
- ✅ Implement post-restore CUDA unfreeze
- ✅ Update documentation
- ✅ Add comprehensive tests

## Build & Test Status

```
✅ go build ./cmd/worker/...    - SUCCESS
✅ go build ./cmd/gateway/...   - SUCCESS
✅ go test ./pkg/runtime/...    - PASS
✅ go test ./pkg/worker/...     - PASS
```

## Next Steps for Users

1. **Add cuda-checkpoint to container images:**
   ```dockerfile
   RUN apt-get install -y cuda-checkpoint
   ```

2. **Ensure nvproxy is enabled:**
   ```go
   runtime.Prepare(spec) // Auto-detects GPU and enables nvproxy
   ```

3. **Use checkpoint/restore as normal:**
   ```go
   runtime.Checkpoint(ctx, containerID, opts)
   runtime.Restore(ctx, containerID, opts)
   ```

The implementation handles CUDA freeze/unfreeze automatically when nvproxy is enabled!
