# Docker Logging Debug - Worker-Side Improvements

## Issue

Even with `--progress=plain`, Docker commands still show no output. The process exits with code 0 (success) but stdout/stderr are empty.

## Changes Made

### 1. Added Worker-Side Logging

**`pkg/worker/container_server.go`:**

#### Log when processes start (Line 574)
```go
log.Debug().
    Str("container_id", in.ContainerId).
    Int("pid", pid).
    Strs("cmd", cmd).
    Msg("sandbox process started")
```

#### Log when stdout is read (Lines 629-631)
```go
log.Debug().
    Str("container_id", in.ContainerId).
    Int32("pid", in.Pid).
    Int("stdout_len", len(stdout)).
    Msg("read stdout")
```

#### Log when stderr is read (Lines 651-653)
```go
log.Debug().
    Str("container_id", in.ContainerId).
    Int32("pid", in.Pid).
    Int("stderr_len", len(stderr)).
    Msg("read stderr")
```

### 2. Added Debug Logging for Docker Daemon Startup

**`pkg/worker/lifecycle.go` (Line 1199):**
```go
log.Debug().
    Str("container_id", containerId).
    Err(err).
    Msg("failed to execute socket check")
```

## What To Look For in Worker Logs

When running a Docker command like `docker pull nginx:latest`, you should now see:

```
# When the command starts
DBG sandbox process started container_id=sandbox-xxx pid=123 cmd=[docker pull --progress=plain nginx:latest]

# When stdout is read
DBG read stdout container_id=sandbox-xxx pid=123 stdout_len=0

# When stderr is read
DBG read stderr container_id=sandbox-xxx pid=123 stderr_len=2048
```

### Key Metrics

- **`stdout_len`**: How many bytes of stdout were captured
- **`stderr_len`**: How many bytes of stderr were captured
- **`pid`**: The process ID
- **`cmd`**: The exact command that was run

## Possible Issues

### Issue 1: Docker Writes to Stderr, Not Stdout

Docker often writes progress output to **stderr**, not stdout! This is because:
- Stdout is for program output (e.g., image IDs, container IDs)
- Stderr is for human-readable progress/status

**Solution:** Make sure the SDK is reading stderr, not just stdout!

```python
process = sandbox.docker.pull("nginx:latest")
process.wait()

# Try reading stderr instead!
print("STDERR:", process.stderr.read())  # This might have the output!
print("STDOUT:", process.stdout.read())  # This might be empty or just final result
```

### Issue 2: Docker Buffering Output

Docker might buffer output and only flush when the process exits. The `--progress=plain` flag should help but might not be enough.

**Possible solution:** Add `--no-trunc` flag or set `DOCKER_CLI_HINTS=false` environment variable.

### Issue 3: GoProc Not Capturing Output

The `goproc` process manager might not be capturing stdout/stderr properly. This could be a bug in how the process manager redirects file descriptors.

**Solution:** Check the goproc implementation or use a different capture method.

### Issue 4: TTY Detection

Even with `--progress=plain`, Docker might detect it's not in a TTY and suppress all output.

**Solution:** Try setting environment variables:
```go
env := []string{
    "DOCKER_CLI_HINTS=false",
    "BUILDKIT_PROGRESS=plain",
}
```

## Testing Steps

### 1. Enable Debug Logging

Set log level to DEBUG in the worker to see all the debug messages:
```bash
export LOG_LEVEL=debug
```

### 2. Run a Docker Command

```python
from beta9 import Image, Sandbox

sandbox = Sandbox(
    cpu=1,
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()

process = sandbox.docker.pull("nginx:latest")
process.wait()

# Check BOTH stdout and stderr
print("Exit code:", process.exit_code)
print("STDOUT length:", len(process.stdout.read()))
print("STDERR length:", len(process.stderr.read()))
print("STDOUT:", process.stdout.read())
print("STDERR:", process.stderr.read())
```

### 3. Check Worker Logs

Look for these patterns:
```
DBG sandbox process started ... cmd=[docker pull --progress=plain nginx:latest]
DBG read stdout ... stdout_len=0
DBG read stderr ... stderr_len=1234  ← If this is > 0, output is in stderr!
```

## Next Steps

Based on the worker logs:

**If `stderr_len > 0`:**
- Docker is writing to stderr (normal!)
- User should read `process.stderr.read()` instead of `process.stdout.read()`
- Update documentation to mention this

**If both `stdout_len=0` and `stderr_len=0`:**
- Docker is not producing any output at all
- Try adding environment variables to force output
- Check if goproc is capturing file descriptors correctly
- Might need to use `script` command to create a pseudo-TTY

**If logs show command not starting:**
- Issue with sandbox process manager
- Check goproc client connection
- Verify command is being executed at all

## Recommended SDK Fix

Update examples and docs to check stderr:

```python
process = sandbox.docker.pull("nginx:latest")
process.wait()

# Docker writes progress to stderr!
stderr_output = process.stderr.read()
if stderr_output:
    print("Progress:", stderr_output)

# Stdout usually has just the final result
stdout_output = process.stdout.read()
if stdout_output:
    print("Result:", stdout_output)
```

## Files Modified

| File | Changes | Description |
|------|---------|-------------|
| `pkg/worker/container_server.go` | Added debug logging | Log stdout/stderr lengths and process starts |
| `pkg/worker/lifecycle.go` | Added debug logging | Log Docker daemon socket checks |

## Summary

✅ **Added worker-side logging** to debug stdout/stderr capture  
✅ **Log process starts** with full command  
✅ **Log stdout/stderr reads** with byte counts  
⚠️ **Next**: Check if Docker writes to stderr instead of stdout  
⚠️ **Next**: Verify goproc is capturing output correctly  

Run with debug logging enabled to see what's happening! 🔍
