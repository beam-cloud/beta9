# gVisor Exit Detection Fix

## Problem

Containers running with gVisor (runsc) that call `os._exit(0)` or similar direct exit calls don't terminate properly. The process exits but the runsc sandbox continues running, leaving the container in a zombie state.

### Symptoms

1. Python code calls `os._exit(0)`
2. gRPC connection closes ("Socket closed" error)
3. Container appears to exit
4. But `runsc` process doesn't exit
5. Container stays in running state
6. Worker can't clean up properly

### Example Log

```
worker INF grpc._channel._InactiveRpcError: <_InactiveRpcError of RPC that terminated with:
worker INF   status = StatusCode.UNAVAILABLE
worker INF   details = "Socket closed"
```

The socket closes (process exited) but the container doesn't shut down.

## Root Cause

gVisor's sandbox has multiple internal processes:
- **Sentry**: The userspace kernel
- **Gofer**: Handles file system operations  
- **Platform process**: Manages syscalls (systrap, kvm, or ptrace)

By default, runsc waits for cgroup cleanup before exiting, even after the init process (PID 1) terminates. This causes the sandbox to hang waiting for cgroup events that may not fire properly when processes call `os._exit()` directly.

## Solution

Add the `--ignore-cgroups` flag to `runsc run`:

```go
args = append(args, "--ignore-cgroups")
args = append(args, "run", "--bundle", bundlePath, containerID)
```

### What `--ignore-cgroups` Does

- **Disables cgroup-based wait**: runsc doesn't wait for cgroup cleanup
- **Exit on init process death**: runsc exits as soon as PID 1 in the container exits
- **Immediate cleanup**: The sandbox tears down immediately when the main process exits

## Implementation

```go
func (r *Runsc) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
    args := r.baseArgs()  // --root, --platform
    
    if r.nvproxyEnabled {
        args = append(args, "--nvproxy=true")
    }
    
    // Critical: ignore cgroups so runsc exits when init process exits
    args = append(args, "--ignore-cgroups")
    
    args = append(args, "run", "--bundle", bundlePath, containerID)
    
    cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
    cmd.Stdout = opts.OutputWriter
    cmd.Stderr = opts.OutputWriter
    
    cmd.Start()
    opts.Started <- cmd.Process.Pid
    
    err := cmd.Wait()  // Now exits promptly when container exits
    // ... handle exit code
}
```

## Behavior Comparison

### Without `--ignore-cgroups` (broken)

```
Container process calls os._exit(0)
    ↓
Process terminates
    ↓
gRPC socket closes
    ↓
runsc waits for cgroup events... ⏳ (hangs here)
    ↓
Timeout or manual cleanup needed
```

### With `--ignore-cgroups` (fixed)

```
Container process calls os._exit(0)
    ↓
Process terminates
    ↓
runsc detects PID 1 exit
    ↓
runsc immediately cleans up sandbox
    ↓
runsc run exits
    ↓
cmd.Wait() returns ✅
```

## Testing

### Test Case 1: Direct Exit

```python
import os
print("Exiting now")
os._exit(0)  # Direct exit without cleanup
```

**Expected**: Container exits immediately, no hanging processes

### Test Case 2: Normal Exit

```python
import sys
print("Exiting normally")
sys.exit(0)  # Normal Python exit
```

**Expected**: Container exits immediately

### Test Case 3: Uncaught Exception

```python
raise RuntimeError("Something went wrong")
```

**Expected**: Container exits with non-zero code immediately

### Test Case 4: Signal

```bash
# Inside container
kill -TERM 1  # Kill init process
```

**Expected**: Container exits immediately

## Verification

### Check Container Exits Properly

```bash
# Start a container with gVisor
# Inside container, run:
python3 -c "import os; os._exit(0)"

# On worker, verify runsc exits:
ps aux | grep runsc
# Should show no runsc processes for that container

# Verify no zombie containers:
runsc --root /run/gvisor list
# Should not show the exited container
```

### Check Cleanup

```bash
# After container exits, verify:
ls /run/gvisor/
# Should not contain state for exited container
```

## Why This Works

The `--ignore-cgroups` flag changes runsc's termination logic:

| Aspect | Without Flag | With Flag |
|--------|-------------|-----------|
| **Wait condition** | Wait for cgroup empty | Wait for PID 1 exit |
| **Exit detection** | Polls cgroup.procs | Monitors init process |
| **Cleanup trigger** | Cgroup events | Process death |
| **Edge cases** | Can hang on direct exits | Handles all exits cleanly |

## Trade-offs

### Pros
- ✅ Containers exit immediately when process terminates
- ✅ Works with `os._exit()`, signals, crashes
- ✅ No zombie containers
- ✅ Simple, predictable behavior

### Cons
- ⚠️ Slightly less cgroup integration
- ⚠️ May not wait for all child processes (but PID 1 should reap them anyway)

**Verdict**: The pros far outweigh the cons. The flag fixes a critical usability issue with minimal downside.

## Alternative Solutions Considered

### 1. Use create+start+wait pattern
**Rejected**: Too complex, caused other issues (stop broken, list hanging)

### 2. Poll for process exit
**Rejected**: Race conditions, inefficient

### 3. Use runsc events API
**Rejected**: Not reliable for all exit scenarios

### 4. Add init system (tini, dumb-init)
**Rejected**: Adds overhead, complexity; should work without it

### 5. Use --ignore-cgroups (chosen)
**Selected**: Simple one-line fix, works reliably

## Impact on Other Features

### OOM Detection
✅ **Still works**: We use cgroup poller in worker, independent of runsc's cgroup handling

### Container Stats
✅ **Still works**: Stats come from cgroup files, not runsc

### Checkpoint/Restore
✅ **N/A**: gVisor doesn't support C/R anyway

### GPU (nvproxy)
✅ **Still works**: nvproxy flag independent of cgroup handling

### Network
✅ **Still works**: Network namespace management unchanged

## Documentation

- Official runsc docs: `runsc help run`
- Flag description: "Ignore cgroup requirements"
- gVisor GitHub: Multiple issues about exit detection fixed by this flag

## Summary

**Problem**: Containers with `os._exit(0)` don't shut down  
**Solution**: Add `--ignore-cgroups` flag to runsc run  
**Result**: Containers exit immediately and cleanly  

One-line fix with huge impact on container lifecycle reliability.
