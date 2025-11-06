# runsc Exit Code and Cleanup Fix

## Problem

The runsc implementation had issues with exit code handling and cleanup that made it inconsistent with the runc implementation:

1. **Incorrect PID reporting**: The `Started` channel received the PID of the `runsc` wrapper process instead of the actual container/sandbox PID
2. **Inconsistent exit codes**: Using `runsc run` (blocking) made it harder to properly track container lifecycle
3. **Poor cleanup on errors**: Errors during container creation didn't always clean up properly

## Solution

Refactored `runsc.Run()` to use the proper OCI runtime pattern: **create → start → wait**

### Pattern Comparison

#### Before (runsc run - blocking)
```go
// Old approach: runsc run (blocks until container exits)
cmd := exec.Command("runsc", "run", "--bundle", bundlePath, containerID)
cmd.Start()
opts.Started <- cmd.Process.Pid  // ❌ Wrong PID (runsc process, not container)
cmd.Wait()
```

#### After (create + start + wait)
```go
// New approach: matches runc's internal behavior
// 1. Create container (doesn't start yet)
exec.Command("runsc", "create", "--bundle", bundlePath, "--pid-file", pidFile, containerID).Run()
pid := readPidFromFile(pidFile)
opts.Started <- pid  // ✅ Correct PID (actual container sandbox)

// 2. Start container (begins execution)
exec.Command("runsc", "start", containerID).Run()

// 3. Wait for exit
exitCode := exec.Command("runsc", "wait", containerID).Run()
```

### Benefits

1. **Correct PID reporting**: The `Started` channel now receives the actual container/sandbox PID, not the wrapper process
2. **Proper lifecycle control**: Can create, start, and wait independently
3. **Clean error handling**: Failed creates/starts properly clean up via `Delete()`
4. **Consistent with runc**: Matches the pattern used internally by go-runc
5. **Simple exit code handling**: `runsc wait` outputs just the exit code, easy to parse

## Implementation Details

### Step 1: Create Container

```go
createArgs := []string{
    "--root", r.cfg.RunscRoot,
    "--platform", r.cfg.RunscPlatform,
    "--nvproxy=true",  // if GPU detected
    "create",
    "--bundle", bundlePath,
    "--pid-file", pidFile,
    containerID,
}
```

- Creates the container but doesn't start it
- Writes the sandbox PID to pidFile
- Returns immediately

### Step 2: Read and Report PID

```go
pidBytes, _ := os.ReadFile(pidFile)
pid, _ := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
os.Remove(pidFile)

if opts.Started != nil {
    opts.Started <- pid  // Send actual container PID
}
```

- Reads the PID written by runsc
- Sends it to the `Started` channel
- Worker can now monitor the correct process

### Step 3: Start Container

```go
startArgs := []string{
    "--root", r.cfg.RunscRoot,
    "--platform", r.cfg.RunscPlatform,
    "start",
    containerID,
}
```

- Begins container execution
- Returns immediately

### Step 4: Wait for Exit

```go
waitArgs := []string{
    "--root", r.cfg.RunscRoot,
    "--platform", r.cfg.RunscPlatform,
    "wait",
    containerID,
}

output, _ := exec.Command("runsc", waitArgs...).CombinedOutput()
exitCode, _ := strconv.Atoi(strings.TrimSpace(string(output)))
```

- Blocks until container exits
- Outputs just the exit code (e.g., "0", "1", "137")
- Easy to parse

### Error Handling

```go
if err := createCmd.Run(); err != nil {
    return -1, fmt.Errorf("failed to create container: %w", err)
}

if err := readPidFile(); err != nil {
    _ = r.Delete(ctx, containerID, &DeleteOpts{Force: true})  // Clean up
    return -1, fmt.Errorf("failed to read container PID: %w", err)
}

if err := startCmd.Run(); err != nil {
    _ = r.Delete(ctx, containerID, &DeleteOpts{Force: true})  // Clean up
    return -1, fmt.Errorf("failed to start container: %w", err)
}
```

- Any failure in create/start triggers cleanup via `Delete()`
- Prevents orphaned containers
- Consistent error handling

## Exit Code Behavior

### Container Exits Successfully (0)

```
runsc wait outputs: "0"
Return: (0, nil)
```

### Container Exits with Error (e.g., 1)

```
runsc wait outputs: "1"
Return: (1, nil)  // Not a runtime error, just non-zero exit
```

### Container Killed by Signal (e.g., SIGKILL = 137)

```
runsc wait outputs: "137"
Return: (137, nil)
```

### Runtime Error (e.g., failed to create)

```
Return: (-1, error("failed to create container"))
```

This matches runc's behavior:
- Exit codes 0-255: Container process exit codes
- Exit code -1: Runtime/infrastructure error

## Comparison with runc

### runc (via go-runc)
```go
// go-runc internally does:
// 1. runc create --pid-file
// 2. Read PID from file
// 3. runc start
// 4. Wait for process (via PID)
// 5. Get exit code

exitCode, err := runcHandle.Run(ctx, containerID, bundlePath, opts)
```

### runsc (our implementation)
```go
// We explicitly do the same pattern:
// 1. runsc create --pid-file
// 2. Read PID from file
// 3. runsc start
// 4. runsc wait
// 5. Parse exit code from output

exitCode, err := r.Run(ctx, containerID, bundlePath, opts)
```

**Result**: Consistent behavior and exit code handling between runc and runsc!

## Testing

### Successful Container
```bash
# Container runs and exits with 0
runsc create --pid-file /tmp/pid.txt --bundle /bundle container1
# PID file contains: 12345
runsc start container1
runsc wait container1
# Outputs: 0
# Returns: (0, nil)
```

### Failed Container
```bash
# Container runs and exits with 1
runsc create --pid-file /tmp/pid.txt --bundle /bundle container2
# PID file contains: 12346
runsc start container2
runsc wait container2
# Outputs: 1
# Returns: (1, nil)
```

### Runtime Error
```bash
# Invalid bundle
runsc create --pid-file /tmp/pid.txt --bundle /invalid container3
# Error: failed to create container
# Returns: (-1, error)
```

## Benefits Summary

✅ **Correct PID tracking**: Worker monitors actual container process  
✅ **Clean error handling**: Failed operations clean up properly  
✅ **Consistent exit codes**: Matches runc behavior exactly  
✅ **Simple implementation**: Clear create → start → wait flow  
✅ **Reliable cleanup**: Delete() called on all error paths  

## Files Modified

- `pkg/runtime/runsc.go` - Rewrote `Run()` method with create+start+wait pattern

No changes needed to:
- `pkg/worker/lifecycle.go` - Already uses runtime interface correctly
- `pkg/runtime/runc.go` - go-runc already does this internally
- Other files - Runtime interface abstracts the differences

## Migration Notes

**No breaking changes!** The runtime interface remains the same:

```go
type Runtime interface {
    Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error)
    // ... other methods
}
```

Callers don't need any changes. The fix is entirely internal to the runsc implementation.
