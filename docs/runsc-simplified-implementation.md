# runsc Simplified Implementation

## What Changed

Reverted the complex create+start+wait pattern back to a simple `runsc run` approach that mirrors how runc works.

## Why the Complex Approach Failed

The previous create+start+wait pattern broke several things:

1. **Container stop process broken**: The wait loop blocked cleanup signals
2. **Containers not shutting down on self-exit**: When containers called `os._exit(0)`, they didn't properly terminate
3. **`runsc list` hanging**: The container state management was confused

## Current Simple Implementation

```go
func (r *Runsc) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
    // Build command: runsc run --bundle <path> <id>
    args := r.baseArgs()  // Adds --root, --platform, etc.
    if r.nvproxyEnabled {
        args = append(args, "--nvproxy=true")
    }
    args = append(args, "run", "--bundle", bundlePath, containerID)

    cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
    
    // Stream output directly
    if opts != nil && opts.OutputWriter != nil {
        cmd.Stdout = opts.OutputWriter
        cmd.Stderr = opts.OutputWriter
    }

    // Start the container
    if err := cmd.Start(); err != nil {
        return -1, fmt.Errorf("failed to start container: %w", err)
    }

    // Notify that container has started
    if opts != nil && opts.Started != nil {
        opts.Started <- cmd.Process.Pid
    }

    // Wait for container to exit
    err := cmd.Wait()
    if err != nil {
        if exitErr, ok := err.(*exec.ExitError); ok {
            return exitErr.ExitCode(), nil
        }
        return -1, err
    }

    return 0, nil
}
```

## How It Works

1. **Start**: `runsc run` starts the container and blocks
2. **PID**: Send the runsc process PID to Started channel (runsc manages the container)
3. **Wait**: Block until runsc exits
4. **Exit Code**: Parse exit code from process exit status

## Exit Code Handling

- **Success (0)**: Container exited cleanly → return 0
- **Non-zero exit**: Container exited with error → return that code
- **Signal**: Container killed by signal → return signal number + 128
- **Runtime error**: runsc failed to start → return -1

## Comparison with runc

### runc (via go-runc)
```go
// go-runc does essentially:
runc run --bundle <path> <id>
// Blocks until container exits
// Returns exit code
```

### runsc (our implementation)
```go
// We do the same:
runsc run --bundle <path> <id>
// Blocks until container exits
// Returns exit code
```

Both work the same way!

## Container Lifecycle

### Normal Exit (os.exit(0))
```
Container process calls exit(0)
    ↓
runsc detects process exit
    ↓
runsc cleans up container
    ↓
runsc run command exits with code 0
    ↓
cmd.Wait() returns nil
    ↓
Return (0, nil)
```

### Killed by Signal (SIGTERM)
```
Kill(SIGTERM) called
    ↓
runsc sends SIGTERM to container
    ↓
Container process exits
    ↓
runsc cleans up
    ↓
runsc run exits with code 143 (128+15)
    ↓
Return (143, nil)
```

### Context Cancellation
```
ctx.Done()
    ↓
exec.CommandContext cancels
    ↓
runsc process receives signal
    ↓
Container stops
    ↓
Cleanup happens
```

## Why This Works Better

1. **Simple**: One command, blocks until done
2. **Clean lifecycle**: runsc handles all state management
3. **Proper cleanup**: Container resources released when runsc exits
4. **Exit on self-termination**: When container calls exit(), runsc detects and cleans up
5. **Context-aware**: Cancelling context stops the container

## Known Issues Fixed

### Issue 1: Containers not shutting down on self-exit

**Problem**: When container called `os._exit(0)`, it stayed running

**Root Cause**: The create+start+wait pattern waited on `runsc wait` separately, which could get confused if the container exited between start and wait

**Fix**: `runsc run` handles everything - when container exits, runsc exits

### Issue 2: runsc list hanging

**Problem**: `runsc --root /run/gvisor list` would hang

**Root Cause**: The create+start pattern left containers in weird states (created but not started, or started but not being waited on)

**Fix**: `runsc run` is atomic - container is created, started, and waited on in one operation. No intermediate states.

### Issue 3: Container stop broken

**Problem**: Stopping containers didn't work properly

**Root Cause**: The wait loop in the complex pattern blocked signals and cleanup

**Fix**: Single `cmd.Wait()` that responds to context cancellation and kill signals properly

## Manual runsc Commands

### List Containers
```bash
# Must specify --root
runsc --root /run/gvisor list
```

If this hangs, it means there are containers in bad states. Fix:

```bash
# Force delete all containers
runsc --root /run/gvisor list --format=json | jq -r '.[].id' | xargs -I {} runsc --root /run/gvisor delete --force {}

# Or wipe the state directory
rm -rf /run/gvisor/*
```

### Check Container State
```bash
runsc --root /run/gvisor state <container-id>
```

### Kill Container
```bash
runsc --root /run/gvisor kill <container-id> SIGKILL
```

### Delete Container
```bash
runsc --root /run/gvisor delete --force <container-id>
```

## Troubleshooting

### Containers in bad state

**Symptom**: `runsc list` hangs or shows containers that should be gone

**Fix**:
```bash
# Force cleanup
for id in $(runsc --root /run/gvisor list --format=json | jq -r '.[].id'); do
    runsc --root /run/gvisor kill $id SIGKILL 2>/dev/null || true
    runsc --root /run/gvisor delete --force $id 2>/dev/null || true
done
```

### Container won't stop

**Symptom**: Kill doesn't work

**Fix**:
```bash
# Use SIGKILL
runsc --root /run/gvisor kill <container-id> SIGKILL

# If that doesn't work, delete forcefully
runsc --root /run/gvisor delete --force <container-id>
```

### State directory issues

**Symptom**: Errors about state files

**Fix**:
```bash
# Nuclear option: wipe everything
systemctl stop worker  # Stop the worker first!
rm -rf /run/gvisor/*
systemctl start worker
```

## Summary

✅ **Simplified**: Back to `runsc run` (single command)  
✅ **Reliable**: Containers exit properly when they self-terminate  
✅ **Clean**: No intermediate states that can cause hangs  
✅ **Consistent**: Works just like runc  

The key insight: **Let runsc manage the full lifecycle.** Don't try to micromanage with create/start/wait.
