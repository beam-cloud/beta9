# gVisor CUDA Checkpoint/Restore - Correct Implementation

## Overview

This document describes the **correct** implementation of CUDA checkpoint/restore for gVisor containers, based on the official gVisor documentation and GitHub issues.

## Key Insight

**gVisor does NOT automatically checkpoint GPU state like runc+CRIU does.**

Instead, gVisor requires a **two-step manual process** using the `cuda-checkpoint` binary:

1. **Before checkpoint**: Run `cuda-checkpoint` inside the sandbox to freeze CUDA processes
2. **After restore**: Run `cuda-checkpoint` inside the sandbox to unfreeze CUDA processes

## Why This Approach?

- gVisor's nvproxy intercepts CUDA API calls at runtime
- However, nvproxy does NOT automatically handle checkpoint/restore
- The `cuda-checkpoint` binary (provided by NVIDIA) must be explicitly called to freeze/unfreeze GPU state
- This is different from runc where CRIU automatically calls cuda-checkpoint as part of the checkpoint process

## Correct Workflow

### Checkpoint Process

```
1. Container Running with CUDA workload
   ├─> nvproxy intercepting CUDA calls
   └─> GPU operations active

2. Initiate Checkpoint
   ├─> Find all CUDA processes in container
   │   (processes with /dev/nvidia* file descriptors)
   └─> For each CUDA process:
       ├─> runsc exec container-id cuda-checkpoint checkpoint PID
       └─> This FREEZES the GPU state for that process

3. Run runsc checkpoint
   ├─> runsc checkpoint --image-path /path --leave-running container-id
   └─> Container state saved with frozen CUDA processes

4. Checkpoint Complete
   └─> GPU state frozen and saved
```

### Restore Process

```
1. Run runsc restore
   ├─> runsc restore --image-path /path --bundle /bundle container-id
   └─> Container restored with frozen CUDA state

2. Unfreeze CUDA Processes
   ├─> Find all CUDA processes in restored container
   └─> For each CUDA process:
       ├─> runsc exec container-id cuda-checkpoint restore PID
       └─> This UNFREEZES the GPU state for that process

3. Container Resumed
   ├─> nvproxy active
   └─> GPU operations continue
```

## Implementation Details

### Finding CUDA Processes

We use two methods to find CUDA processes inside the container:

#### Method 1: Using lsof
```bash
runsc exec container-id sh -c "lsof /dev/nvidia* 2>/dev/null | awk 'NR>1 {print $2}' | sort -u"
```

This finds all processes with open file descriptors to NVIDIA devices.

#### Method 2: Using /proc (fallback)
```bash
runsc exec container-id sh -c "
  for pid in /proc/[0-9]*; do
    if [ -d \"$pid/fd\" ]; then
      if ls -l $pid/fd 2>/dev/null | grep -q nvidia; then
        basename $pid
      fi
    fi
  done
"
```

This checks /proc for processes with nvidia device file descriptors.

### Running cuda-checkpoint

#### Freeze (before checkpoint)
```bash
runsc exec container-id cuda-checkpoint checkpoint PID
```

#### Unfreeze (after restore)
```bash
runsc exec container-id cuda-checkpoint restore PID
```

## Code Implementation

### Checkpoint Method (`pkg/runtime/runsc.go`)

```go
func (r *Runsc) Checkpoint(ctx context.Context, containerID string, opts *CheckpointOpts) error {
    // Step 1: Freeze CUDA processes (if GPU enabled)
    if r.nvproxyEnabled {
        if err := r.freezeCUDAProcesses(ctx, containerID, opts.OutputWriter); err != nil {
            return fmt.Errorf("failed to freeze CUDA processes: %w", err)
        }
    }

    // Step 2: Run runsc checkpoint
    args := r.baseArgs(false)
    args = append(args, "checkpoint", "--image-path", opts.ImagePath, containerID)
    cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
    return cmd.Run()
}
```

### Restore Method (`pkg/runtime/runsc.go`)

```go
func (r *Runsc) Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error) {
    // Step 1: Run runsc restore
    args := r.baseArgs(false)
    if r.nvproxyEnabled {
        args = append(args, "--nvproxy=true")
    }
    args = append(args, "restore", "--image-path", opts.ImagePath, 
                  "--bundle", opts.BundlePath, containerID)
    cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
    
    // Wait for restore to complete
    if err := cmd.Wait(); err != nil {
        return -1, err
    }

    // Step 2: Unfreeze CUDA processes (if GPU enabled)
    if r.nvproxyEnabled {
        if err := r.unfreezeCUDAProcesses(ctx, containerID, opts.OutputWriter); err != nil {
            return -1, fmt.Errorf("failed to unfreeze CUDA processes: %w", err)
        }
    }

    return 0, nil
}
```

## Requirements

### 1. cuda-checkpoint Binary

The `cuda-checkpoint` binary must be available **inside the container**.

#### Installation in Container Image

Add to your Dockerfile:
```dockerfile
# Install cuda-checkpoint tool
RUN apt-get update && apt-get install -y cuda-checkpoint
# Or copy from host
COPY cuda-checkpoint /usr/local/bin/cuda-checkpoint
RUN chmod +x /usr/local/bin/cuda-checkpoint
```

### 2. nvproxy Enabled

nvproxy must be enabled when the container is created:
```bash
runsc --nvproxy=true run --bundle /path container-id
```

This is detected automatically in our implementation via GPU device detection in the OCI spec.

### 3. GPU Devices in OCI Spec

GPU devices must be specified via CDI or direct device specifications.

### 4. NVIDIA Driver >= 570

Recommended for full checkpoint support at the driver level.

## Comparison: runc vs gVisor

| Aspect | runc with CRIU | gVisor with runsc |
|--------|----------------|-------------------|
| GPU Checkpoint Method | Automatic via CRIU plugin | Manual via cuda-checkpoint binary |
| When cuda-checkpoint runs | Automatically by CRIU | Must be called explicitly |
| Where cuda-checkpoint runs | Host or container (CRIU decides) | **Inside the container** (via runsc exec) |
| Process Detection | CRIU handles automatically | Must find CUDA processes manually |
| Number of Steps | 1 (runc checkpoint) | 2 (freeze, then runsc checkpoint) |
| Restore Steps | 1 (runc restore) | 2 (runsc restore, then unfreeze) |

## Error Handling

### cuda-checkpoint Not Found

If `cuda-checkpoint` is not installed in the container:
```
failed to freeze CUDA processes: cuda-checkpoint checkpoint failed for PID 1234
```

**Solution**: Install `cuda-checkpoint` in your container image.

### No CUDA Processes Found

If no CUDA processes are detected:
- The checkpoint proceeds normally without freezing (graceful degradation)
- No error is raised
- This allows checkpointing non-CUDA containers with nvproxy enabled

### Process Detection Fails

If both `lsof` and `/proc` methods fail:
- Returns empty process list
- Checkpoint continues without CUDA freeze
- May result in corrupted GPU state on restore

## Testing

### Unit Tests

```bash
# Test CUDA checkpoint workflow
go test ./pkg/runtime/... -run TestGVisorCUDACheckpoint -v

# Test process finding
go test ./pkg/runtime/... -run TestFindCUDAProcesses -v
```

### Integration Testing

1. Start a gVisor container with GPU:
```bash
runsc --nvproxy=true run --bundle /bundle container-id
```

2. Run CUDA workload inside container

3. Checkpoint with our implementation:
```go
rt.Checkpoint(ctx, containerID, &CheckpointOpts{
    ImagePath: "/checkpoint",
})
```

4. Verify cuda-checkpoint was called:
- Check logs for "freezing CUDA process PID"

5. Restore:
```go
rt.Restore(ctx, containerID, &RestoreOpts{
    ImagePath: "/checkpoint",
    BundlePath: "/bundle",
})
```

6. Verify cuda-checkpoint was called again:
- Check logs for "unfreezing CUDA process PID"

7. Verify CUDA workload continues correctly

## Debugging

### Enable Debug Logging

Set `Debug: true` in runtime config:
```go
runtime, _ := NewRunsc(Config{
    RunscPath: "runsc",
    Debug:     true,
})
```

### Check CUDA Process Detection

Manually test process detection:
```bash
# Inside container
lsof /dev/nvidia* | awk 'NR>1 {print $2}' | sort -u

# Or
for pid in /proc/[0-9]*; do
  if ls -l $pid/fd 2>/dev/null | grep -q nvidia; then
    echo $(basename $pid)
  fi
done
```

### Verify cuda-checkpoint Works

Test cuda-checkpoint manually:
```bash
# Inside container
cuda-checkpoint checkpoint 1234   # Freeze process 1234
cuda-checkpoint restore 1234      # Unfreeze process 1234
```

## References

- [gVisor CUDA Checkpoint GitHub Issue](https://github.com/google/gvisor/issues/...)
- [NVIDIA cuda-checkpoint Documentation](https://docs.nvidia.com/cuda/cuda-c-best-practices-guide/index.html#checkpointing)
- [gVisor nvproxy Documentation](https://gvisor.dev/docs/user_guide/gpu/)

## Summary

✅ **Correct Implementation**
- cuda-checkpoint runs **inside** the gVisor sandbox
- Freeze CUDA processes **before** runsc checkpoint
- Unfreeze CUDA processes **after** runsc restore
- Automatic process detection via lsof or /proc
- Graceful degradation if cuda-checkpoint not available

❌ **Incorrect Assumption**
- ~~nvproxy automatically handles GPU checkpoint~~
- ~~runsc checkpoint includes GPU state by default~~
- ~~No manual intervention needed~~

The key is understanding that gVisor requires **explicit** CUDA freeze/unfreeze operations, unlike runc where CRIU handles it automatically.
