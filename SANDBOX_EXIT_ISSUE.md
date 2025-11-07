# Sandbox Container Exiting Prematurely - Root Cause Analysis

## The Problem

```
worker-default-6e722ab2-6fsmm worker 5:23PM INF container has exited with code: 0
worker-default-6e722ab2-6fsmm worker 5:23PM ERR failed to get stdout error="rpc error: code = Canceled desc = context canceled"
worker-default-6e722ab2-6fsmm worker 5:23PM ERR failed to get stderr error="rpc error: code = Canceled desc = context canceled"
```

**The container exits immediately after a Docker command completes, preventing stdout/stderr from being read.**

## Root Cause

### How Sandboxes Work

1. **Main Process**: The sandbox container's main process is `/usr/bin/goproc` (sandbox process manager binary)
2. **Container Lifecycle**: When the main process exits, the entire container exits
3. **Process Execution**: Commands like `docker pull` are executed VIA the goproc server, not AS the main process

**Code Reference (`pkg/worker/lifecycle.go:818`):**
```go
spec.Process.Args = []string{types.WorkerSandboxProcessManagerContainerPath}
// types.WorkerSandboxProcessManagerContainerPath = "/usr/bin/goproc"
```

### The Issue

**The `goproc` binary is exiting, which causes the container to exit.**

Possible reasons:
1. **goproc crashes on startup** - Check if there are errors in goproc logs
2. **goproc exits after being idle** - No keep-alive mechanism
3. **goproc is not run in server mode** - Wrong flags/args
4. **Signal handling** - goproc receives a signal and exits

## Where the Exit Happens

**`pkg/worker/lifecycle.go:1013`:**
```go
exitCode, err := instance.Runtime.Run(ctx, request.ContainerId, bundlePath, &runtime.RunOpts{
    OutputWriter:  outputWriter,
    Started:       startedChan,
    DockerEnabled: request.DockerEnabled,
})
```

`Runtime.Run()` **blocks until the main process (goproc) exits**. When goproc exits, the container exits, and all child processes (including dockerd) are killed.

## Impact

1. ❌ **Cannot read stdout/stderr** - Container is gone before we can fetch output
2. ❌ **Docker daemon dies** - dockerd is killed when container exits
3. ❌ **All processes terminated** - Everything running in the sandbox is killed
4. ✅ **Exit codes work** - We get the exit code before container dies

## Why Exit Code Works But Logs Don't

**Timeline:**
```
1. User calls: process = sandbox.docker.pull("nginx:latest")
   └─> SDK sends exec request to worker
   └─> Worker exec via goproc: docker pull nginx:latest
   └─> Returns PID 147

2. User calls: process.wait()
   └─> SDK polls for exit status
   └─> Docker pull completes with exit code 0
   └─> goproc somehow exits (WHY?!)
   └─> Container exits

3. User calls: process.stdout.read()
   └─> SDK requests stdout for PID 147
   └─> Worker tries to contact goproc
   └─> ❌ ERROR: "context canceled" - container is dead!
```

## Debugging Steps

### 1. Check if goproc is actually running

Add logging right before starting the container:

```go
log.Info().
    Str("container_id", containerId).
    Str("main_process", types.WorkerSandboxProcessManagerContainerPath).
    Msg("starting sandbox container with goproc as main process")
```

### 2. Check goproc logs/errors

The goproc binary might be writing to stdout/stderr. Capture this:

```go
// After container starts
time.Sleep(1 * time.Second)
state, err := instance.Runtime.State(ctx, containerId)
log.Info().
    Str("container_id", containerId).
    Str("status", state.Status).
    Int("pid", state.Pid).
    Msg("container state after start")
```

### 3. Check if goproc needs special flags

The goproc binary might need to be run with specific flags to stay alive:
- `--server` mode?
- `--listen` flag?
- `--no-exit` flag?

### 4. Add keep-alive mechanism

If goproc exits when idle, we might need to:
- Send periodic health checks
- Run a keep-alive goroutine
- Have goproc listen for signals

## Potential Solutions

### Solution 1: Fix goproc to Stay Alive

If goproc is exiting prematurely, fix the goproc binary itself:
- Add server mode that never exits
- Fix crash/panic that causes exit
- Add proper signal handling

### Solution 2: Wrapper Script

Instead of running goproc directly, use a wrapper:

```go
spec.Process.Args = []string{"/bin/sh", "-c", "/usr/bin/goproc & wait"}
```

This starts goproc in background and waits indefinitely.

### Solution 3: Add Keep-Alive Process

Run a dummy process alongside goproc to keep container alive:

```bash
(/usr/bin/goproc &) && tail -f /dev/null
```

### Solution 4: Change Container Lifecycle

Make the container not depend on a single main process:
- Use a process supervisor (supervisord, systemd)
- Run multiple processes with proper lifecycle management

## Recommended Fix

**Immediate:** Check goproc source code and logs to understand why it's exiting

**Short-term:** Add wrapper script or keep-alive mechanism

**Long-term:** Fix goproc to properly handle server lifecycle

## Testing

After implementing a fix, verify:

```python
from beta9 import Image, Sandbox

sandbox = Sandbox(
    cpu=1,
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()

# Run Docker command
process = sandbox.docker.pull("nginx:latest")
process.wait()

# Check container is STILL ALIVE
print("Exit code:", process.exit_code)  # Should be 0
print("Stdout:", process.stdout.read())  # Should have output!
print("Stderr:", process.stderr.read())  # Should have output!

# Verify sandbox still works
ps_result = sandbox.docker.ps()
ps_result.wait()
print("Docker ps works:", ps_result.exit_code == 0)  # Should be True

sandbox.terminate()
```

## Files to Check

| File/Binary | What to Check |
|-------------|---------------|
| `/usr/local/bin/goproc` (on worker) | Source code, build flags |
| `github.com/beam-cloud/goproc` | Repository (if exists) |
| Worker logs | Startup errors for goproc |
| Container logs | goproc stdout/stderr |

## Summary

✅ **Root Cause**: goproc (sandbox main process) exits prematurely  
✅ **Impact**: Container dies, can't read stdout/stderr  
✅ **Why Exit Code Works**: Retrieved before container dies  
⚠️ **Need**: Investigate goproc binary and add keep-alive  
⚠️ **Fix**: Make goproc stay alive or use wrapper script  

**The goproc binary is the key to fixing this issue!** 🔑
