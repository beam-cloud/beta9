# gVisor CUDA Checkpoint/Restore - Final Implementation

## Summary

This implementation enables CUDA checkpoint/restore for gVisor containers by:
1. **Mounting** `cuda-checkpoint` binary from host into container
2. **Running** `cuda-checkpoint` inside the container via `runsc exec`
3. **Using** container PIDs (not host PIDs)

## Why This Approach?

**gVisor provides strong isolation** - processes inside the sandbox are not directly accessible from the host. Therefore:
- ❌ Cannot run cuda-checkpoint from host on sandboxed processes
- ✅ Must run cuda-checkpoint INSIDE the sandbox via `runsc exec`
- ✅ Must use CONTAINER PIDs, not host PIDs

## Implementation Details

### 1. Mounting cuda-checkpoint (in `Prepare()`)

When GPU devices are detected and nvproxy is enabled, we automatically mount the cuda-checkpoint binary:

```go
func (r *Runsc) Prepare(ctx context.Context, spec *specs.Spec) error {
    if r.nvproxyEnabled {
        // Bind mount cuda-checkpoint from host into container
        mount := specs.Mount{
            Destination: "/usr/local/bin/cuda-checkpoint",
            Type:        "bind",
            Source:      "/usr/bin/cuda-checkpoint", // host path
            Options:     []string{"bind", "ro"},     // read-only
        }
        spec.Mounts = append(spec.Mounts, mount)
    }
}
```

**Result**: cuda-checkpoint is available at `/usr/local/bin/cuda-checkpoint` inside the container.

### 2. Finding CUDA Processes (Inside Container)

```go
func (r *Runsc) findCUDAProcesses(ctx context.Context, containerID string) ([]int, error) {
    // Run inside container to find processes with nvidia device FDs
    runsc exec container-id sh -c '
        for pid in /proc/[0-9]*; do
            if ls -l $pid/fd 2>/dev/null | grep -q nvidia; then
                basename $pid
            fi
        done
    '
    
    // Returns CONTAINER PIDs (e.g., 1, 15, 23)
    // Falls back to PID 1 if detection fails
}
```

**Simpler than previous attempts**: No complex nvidia-smi parsing or cgroup checks.

### 3. Running cuda-checkpoint (Inside Container)

```go
func (r *Runsc) runCUDACheckpoint(ctx context.Context, containerID string, 
                                   pid int, action string) error {
    // Execute cuda-checkpoint INSIDE the container
    runsc exec container-id /usr/local/bin/cuda-checkpoint <action> <container_pid>
    
    // Examples:
    // runsc exec my-container /usr/local/bin/cuda-checkpoint checkpoint 1
    // runsc exec my-container /usr/local/bin/cuda-checkpoint restore 1
}
```

### 4. Checkpoint Workflow

```
1. Container created with GPU devices
   ├─> Prepare() detects GPU devices
   ├─> nvproxy enabled
   └─> cuda-checkpoint mounted at /usr/local/bin/cuda-checkpoint

2. Checkpoint initiated
   ├─> findCUDAProcesses(container-id)
   │   └─> Returns: [1, 15] (container PIDs)
   ├─> For each PID:
   │   └─> runsc exec container-id /usr/local/bin/cuda-checkpoint checkpoint 1
   │   └─> runsc exec container-id /usr/local/bin/cuda-checkpoint checkpoint 15
   └─> runsc checkpoint --image-path /path container-id

3. Checkpoint complete
   └─> GPU state frozen and container checkpointed
```

### 5. Restore Workflow

```
1. Restore initiated
   └─> runsc restore --image-path /path --nvproxy=true container-id

2. Container restored with frozen CUDA state
   ├─> findCUDAProcesses(container-id)
   │   └─> Returns: [1, 15] (container PIDs)
   ├─> For each PID:
   │   └─> runsc exec container-id /usr/local/bin/cuda-checkpoint restore 1
   │   └─> runsc exec container-id /usr/local/bin/cuda-checkpoint restore 15
   └─> CUDA operations resume

3. Restore complete
   └─> Container running with unfrozen GPU state
```

## Key Differences from Previous Attempts

| Aspect | Previous (Wrong) | Current (Correct) |
|--------|------------------|-------------------|
| **cuda-checkpoint location** | Tried running from host | Mounted into container |
| **Execution context** | From host | Inside container via runsc exec |
| **PID namespace** | Host PIDs | Container PIDs |
| **Process detection** | nvidia-smi + cgroup filtering | /proc inside container |
| **Complexity** | High (cgroup checks, PID mapping) | Low (simple /proc check) |

## Requirements

### On Worker (Host)
✅ cuda-checkpoint binary at `/usr/bin/cuda-checkpoint`
✅ NVIDIA driver >= 570 (recommended)
✅ runsc (gVisor runtime)

### In Container Spec
✅ GPU devices specified (CDI or direct)
✅ nvproxy will be automatically enabled
✅ cuda-checkpoint will be automatically mounted
❌ No need to install cuda-checkpoint in container images!

## Code Changes

### Files Modified

1. **pkg/runtime/runsc.go**
   - Added `mountCudaCheckpoint()` method
   - Updated `Prepare()` to mount cuda-checkpoint when nvproxy enabled
   - Simplified `findCUDAProcesses()` to work inside container
   - Updated `runCUDACheckpoint()` to use `runsc exec`
   - Updated documentation comments

2. **pkg/runtime/checkpoint_test.go** (will be updated)
   - Tests document the correct approach

## Testing

```bash
# Build
go build ./cmd/worker/...
go build ./cmd/gateway/...

# Test
go test ./pkg/runtime/... -v
go test ./pkg/worker/... -v
```

## Example Usage

```go
// Create runtime
runtime, _ := NewRunsc(Config{
    RunscPath: "runsc",
    RunscRoot: "/var/run/runsc",
})

// Prepare spec (automatically mounts cuda-checkpoint if GPU detected)
runtime.Prepare(ctx, spec)

// Checkpoint (automatically freezes CUDA inside container)
runtime.Checkpoint(ctx, containerID, &CheckpointOpts{
    ImagePath:    "/checkpoint",
    LeaveRunning: true,
})

// What happens internally:
// 1. Find CUDA processes: runsc exec container-id sh -c "find nvidia FDs"
// 2. Freeze each:        runsc exec container-id /usr/local/bin/cuda-checkpoint checkpoint 1
// 3. Checkpoint:         runsc checkpoint container-id

// Restore (automatically unfreezes CUDA inside container)
runtime.Restore(ctx, containerID, &RestoreOpts{
    ImagePath:  "/checkpoint",
    BundlePath: "/bundle",
})

// What happens internally:
// 1. Restore:     runsc restore container-id
// 2. Find PIDs:   runsc exec container-id sh -c "find nvidia FDs"  
// 3. Unfreeze:    runsc exec container-id /usr/local/bin/cuda-checkpoint restore 1
```

## Advantages

### 1. Respects gVisor Isolation
- Works within gVisor's security model
- No attempts to bypass sandbox isolation
- cuda-checkpoint operates on visible processes

### 2. Simpler Process Detection
- No nvidia-smi host queries
- No cgroup parsing
- No PID namespace translation
- Just check /proc for nvidia FDs inside container

### 3. Automatic Setup
- cuda-checkpoint mounted automatically when GPU detected
- No manual configuration needed
- Works with any container image

### 4. Clean Architecture
- Follows the principle from gVisor docs: "run cuda-checkpoint inside the sandbox"
- No complex host-to-container PID mapping
- Straightforward execution model

## Debugging

### Verify cuda-checkpoint is Mounted

```bash
# List container mounts
runsc exec container-id ls -l /usr/local/bin/cuda-checkpoint

# Should show the mounted binary
```

### Check CUDA Processes

```bash
# Inside container
runsc exec container-id sh -c "
for pid in /proc/[0-9]*; do
    if ls -l $pid/fd 2>/dev/null | grep -q nvidia; then
        echo $pid
    fi
done
"
```

### Test cuda-checkpoint Manually

```bash
# Inside container
runsc exec container-id /usr/local/bin/cuda-checkpoint checkpoint 1
runsc exec container-id /usr/local/bin/cuda-checkpoint restore 1
```

## Status

✅ **Implementation Complete**
- cuda-checkpoint mounting implemented
- Process detection simplified
- Execution via runsc exec implemented
- Documentation updated
- Respects gVisor isolation model

## Credits

Thanks to user feedback for pointing out:
1. "cuda-checkpoint is installed in the worker" → mount it into container
2. "need a more elegant way of finding cuda processes" → simple /proc check
3. Questioning whether host execution would work → correct, it won't!

This led to the proper solution: mount + runsc exec with container PIDs.
