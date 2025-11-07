# Docker Daemon - Final Fix Following gVisor Documentation

## Issue

dockerd was crashing with exit code 1:
```
ERR dockerd process exited unexpectedly exit_code=1 pid=99
```

But we couldn't see WHY it was crashing.

## Changes Made

### 1. Capture and Log dockerd Output

**Before:** No visibility into why dockerd crashes
**After:** Capture stdout/stderr and log them

```go
// When daemon crashes, get the error output
stdout, _ := instance.SandboxProcessManager.Stdout(pid)
stderr, _ := instance.SandboxProcessManager.Stderr(pid)

log.Error().
    Int("exit_code", earlyExitCode).
    Str("stdout", stdout).
    Str("stderr", stderr).
    Msg("dockerd crashed immediately after starting - see stdout/stderr above")
```

### 2. Follow gVisor Documentation Exactly

**Before:**
```go
cmd := []string{
    "dockerd",
    "--iptables=false",  // Was adding flags
}
```

**After:**
```go
// Following gVisor documentation: just run dockerd with no flags
// The container already has necessary capabilities from AddDockerInDockerCapabilities
// and runsc is started with --net-raw flag
cmd := []string{"dockerd"}
```

**Why?** gVisor documentation shows running `dockerd` with NO arguments:
```bash
$ docker run --runtime runsc -d --rm --cap-add all --name docker-in-gvisor docker-in-gvisor
```

The container setup handles everything:
- ✅ `--net-raw` flag passed to runsc (in `runsc.go` line 310-311)
- ✅ All required capabilities added (in `runsc.go` lines 331-356)
- ✅ Capabilities applied to container (in `lifecycle.go` line 836)

## What Will Happen Now

When you run your code:
```python
from beta9 import Image, Sandbox

sandbox = Sandbox(
    cpu=1,
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()
```

### If dockerd Crashes

You'll see logs like:
```
ERR dockerd process exited unexpectedly
    exit_code=1
    pid=99
    stdout="..."
    stderr="failed to start daemon: Error initializing network controller: ..."
```

**This will tell us exactly what's wrong!**

Common errors you might see:
- **"docker: not found"** → Docker not installed, need `.with_docker()`
- **"permission denied"** → Capabilities issue
- **"address already in use"** → Socket conflict
- **"network controller"** → Networking configuration issue

### If dockerd Starts Successfully

```
INF preparing to start docker daemon container_id=sandbox-...
INF docker daemon process started - waiting for daemon to be ready pid=12
DBG socket exists, checking daemon with 'docker info'...
INF docker daemon is ready and accepting commands retry_count=3
```

## Complete Setup Verification

### 1. runsc Configuration (Already Correct)

```go
// In runsc.go line 308-312
if dockerEnabled {
    args = append(args, "--net-raw")
}
```

### 2. Container Capabilities (Already Correct)

```go
// In runsc.go lines 331-349
dockerCaps := []string{
    "CAP_AUDIT_WRITE",
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_FOWNER",
    "CAP_FSETID",
    "CAP_KILL",
    "CAP_MKNOD",
    "CAP_NET_BIND_SERVICE",
    "CAP_NET_ADMIN",
    "CAP_NET_RAW",
    "CAP_SETFCAP",
    "CAP_SETGID",
    "CAP_SETPCAP",
    "CAP_SETUID",
    "CAP_SYS_ADMIN",
    "CAP_SYS_CHROOT",
    "CAP_SYS_PTRACE",
}
```

### 3. Capability Application (Already Correct)

```go
// In lifecycle.go line 833-839
if request.DockerEnabled && request.Stub.Type.Kind() == types.StubTypeSandbox {
    if runscRuntime, ok := s.runtime.(*runtime.Runsc); ok {
        runscRuntime.AddDockerInDockerCapabilities(spec)
        log.Info().Msg("added docker capabilities for sandbox container")
    }
}
```

### 4. dockerd Startup (Now Fixed)

```go
// In lifecycle.go - now matches gVisor docs exactly
cmd := []string{"dockerd"}  // No flags!
```

## How gVisor Docker-in-Docker Works

```
┌─────────────────────────────────────────────────────────────────┐
│                     Host System                                 │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │          runsc (gVisor runtime)                          │ │
│  │          with --net-raw flag                             │ │
│  │                                                           │ │
│  │  ┌─────────────────────────────────────────────────────┐ │ │
│  │  │    Container (Sandbox)                             │ │ │
│  │  │    with Docker capabilities:                       │ │ │
│  │  │    - CAP_SYS_ADMIN                                │ │ │
│  │  │    - CAP_NET_ADMIN                                │ │ │
│  │  │    - CAP_NET_RAW                                  │ │ │
│  │  │    - etc...                                       │ │ │
│  │  │                                                    │ │ │
│  │  │    ┌────────────────────────────────────────────┐ │ │ │
│  │  │    │   dockerd (no flags needed)               │ │ │ │
│  │  │    │   listening on /var/run/docker.sock       │ │ │ │
│  │  │    │                                            │ │ │ │
│  │  │    │   ┌──────────────────────────────────┐   │ │ │ │
│  │  │    │   │  Nested Docker containers        │   │ │ │ │
│  │  │    │   │  (nginx, postgres, etc.)         │   │ │ │ │
│  │  │    │   └──────────────────────────────────┘   │ │ │ │
│  │  │    └────────────────────────────────────────────┘ │ │ │
│  │  └─────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Key Points:**
1. runsc gets `--net-raw` flag (enables raw socket support)
2. Container gets all Docker capabilities
3. dockerd runs with NO flags - capabilities handle everything
4. gVisor virtualizes the kernel, providing isolation

## Testing

Try your code again. You should now see:

**On Success:**
```
INF docker daemon is ready and accepting commands
```

**On Failure (with detailed error):**
```
ERR dockerd crashed immediately after starting - see stdout/stderr above
    stderr="failed to start daemon: Error initializing network controller: ..."
```

This error message will tell us exactly what to fix!

## Files Changed

| File | Change | Lines |
|------|--------|-------|
| `pkg/worker/lifecycle.go` | - Remove `--iptables=false` flag<br>- Capture stdout/stderr on crash<br>- Log error output | ~30 lines |

## Next Steps

1. **Try your code again** - dockerd will start with no flags (matching gVisor docs)
2. **Check the logs** - if it crashes, you'll see stdout/stderr explaining why
3. **Share the error** - the stderr output will tell us exactly what's wrong

The most likely issues:
- ✅ **Setup is correct** - should work now!
- ❌ **Missing Docker** - stderr will say "docker: not found"
- ❌ **Network issue** - stderr will mention "network controller"
- ❌ **Storage issue** - stderr will mention "storage driver"

With the logs, we can fix any remaining issues immediately! 🎯
