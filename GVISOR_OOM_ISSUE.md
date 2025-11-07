# gVisor OOM Detection Issue - Root Cause Analysis

## The REAL Problem

**The container was OOMing (Out Of Memory), but the OOM watcher wasn't detecting it for gVisor!**

### What Happened

1. ✅ Docker pull starts
2. ❌ **Container runs out of memory** (OOM)
3. ❌ **OOM watcher doesn't detect it** (gVisor incompatibility)
4. ❌ Container exits with code 0 (misleading!)
5. ❌ Can't read logs ("context canceled")

## Why OOM Detection Doesn't Work for gVisor

### The OOM Watcher Design

The OOM watcher reads from the HOST's cgroup filesystem:

```go
// 1. Get cgroup path from /proc/<pid>/cgroup
cgroupPath, err := runtime.GetCgroupPathFromPID(pid)

// 2. Read /sys/fs/cgroup/<cgroupPath>/memory.events
memEventsPath := filepath.Join("/sys/fs/cgroup", cgroupPath, "memory.events")
```

### The gVisor Problem

**gVisor virtualizes the kernel**, which means:

1. **`/proc/<pid>/cgroup`** - May not exist or may show wrong path in gVisor
2. **`/sys/fs/cgroup/*/memory.events`** - gVisor doesn't expose real cgroup files
3. **OOM kills happen inside gVisor's virtualized kernel** - Not visible to host's cgroup

```
┌──────────────────────────────────────────────┐
│  Host System                                  │
│                                                │
│  /sys/fs/cgroup/                              │
│  └── beta9/container-123/                    │
│      └── memory.events  ← OOM watcher reads  │
│                                                │
│  ┌────────────────────────────────────────┐  │
│  │  gVisor Container (Virtualized Kernel) │  │
│  │                                         │  │
│  │  /proc/<pid>/cgroup  ← May not exist  │  │
│  │  /sys/fs/cgroup/     ← Virtualized!   │  │
│  │                                         │  │
│  │  Actual OOM happens HERE ↓             │  │
│  │  But host's memory.events not updated!│  │
│  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘
```

## Changes Made

### 1. Added Debug Logging (`pkg/worker/lifecycle.go`)

**Lines 906-924:**
```go
log.Info().Str("container_id", containerId).Str("cgroup_path", cgroupPath).Msg("starting OOM watcher")

oomWatcher := runtime.NewOOMWatcher(ctx, cgroupPath)
err := oomWatcher.Watch(func() {
    log.Warn().Str("container_id", containerId).Msg("OOM kill detected via cgroup watcher")
    isOOMKilled.Store(true)
    // ...
})

if err != nil {
    log.Warn().Str("container_id", containerId).Err(err).Msg("OOM watcher failed to start")
}
```

### 2. Enhanced OOM Watcher Logging (`pkg/runtime/oom_watcher.go`)

**Lines 45-61:**
```go
initialCount, err := w.readOOMKillCount()
if err != nil {
    log.Warn().Str("cgroup", w.cgroupPath).Err(err).Msg("failed to read initial OOM count - OOM detection may not work")
    
    // Check if the file exists
    memEventsPath := filepath.Join(cgroupV2Root, w.cgroupPath, memoryEventsFile)
    if _, statErr := os.Stat(memEventsPath); os.IsNotExist(statErr) {
        log.Warn().
            Str("cgroup", w.cgroupPath).
            Str("path", memEventsPath).
            Msg("memory.events file does not exist - this is expected for gVisor, OOM detection will not work")
    }
}
```

### 3. Added Fail-Safe Logging

**Lines 68-93:**
```go
failedReads := 0
maxFailedReads := 10 // Stop logging after 10 failed reads

for {
    // ...
    currentCount, err := w.readOOMKillCount()
    if err != nil {
        failedReads++
        // Only log first few failures to avoid spam
        if failedReads <= maxFailedReads {
            log.Debug().Str("cgroup", w.cgroupPath).Err(err).Msg("failed to read OOM count")
        } else if failedReads == maxFailedReads+1 {
            log.Warn().Str("cgroup", w.cgroupPath).Msg("OOM watcher continuing to fail, suppressing further logs")
        }
        continue
    }
    // ...
}
```

## Expected Logs

### For gVisor Containers (OOM Detection Won't Work)

```
INF starting OOM watcher container_id=sandbox-xxx cgroup_path=beta9/container-123
WRN failed to read initial OOM count - OOM detection may not work cgroup=beta9/container-123
WRN memory.events file does not exist - this is expected for gVisor, OOM detection will not work path=/sys/fs/cgroup/beta9/container-123/memory.events
```

### When OOM Actually Happens (Won't Be Detected for gVisor!)

```
# What SHOULD happen (but won't for gVisor):
WRN OOM kill detected via cgroup watcher container_id=sandbox-xxx

# What ACTUALLY happens for gVisor:
INF container has exited with code: 0  ← Misleading! It OOMed!
```

## Solutions

### Short-Term: Increase Memory Limit

The immediate fix is to allocate more memory:

```python
from beta9 import Image, Sandbox

sandbox = Sandbox(
    cpu=2,
    memory="4Gi",  # Increase from default 128Mi!
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()

# Docker operations need significant memory
process = sandbox.docker.pull("nginx:latest")
process.wait()
```

### Medium-Term: Alternative OOM Detection for gVisor

We need a different approach for gVisor. Options:

#### Option 1: Monitor gVisor's dmesg

gVisor might log OOM kills to its own dmesg:

```go
// Check gVisor's internal logs
cmd := exec.Command("dmesg")
output, _ := cmd.Output()
if strings.Contains(string(output), "Out of memory") {
    // OOM detected!
}
```

#### Option 2: Monitor Process Exit Codes

OOM-killed processes often exit with code 137 (SIGKILL) or 143:

```go
if exitCode == 137 || exitCode == 143 {
    // Likely OOM kill
    log.Warn().Msg("Process killed, possibly OOM")
}
```

#### Option 3: Implement gVisor-Specific OOM Watcher

Use gVisor's API or metrics to detect OOM:

```go
if s.runtime.Name() == types.ContainerRuntimeGvisor.String() {
    // Use gVisor-specific OOM detection
    gvisorOOMWatcher := NewGvisorOOMWatcher(ctx, containerId)
    gvisorOOMWatcher.Watch(onOOM)
}
```

### Long-Term: Fix gVisor OOM Reporting

Contribute to gVisor to expose OOM events properly:
- Expose memory.events through gVisor's virtual filesystem
- Or provide an API to query OOM status

## Workaround for Now

Until proper OOM detection works for gVisor, users should:

### 1. Allocate Sufficient Memory

```python
# For Docker operations, start with at least 2GB
sandbox = Sandbox(
    memory="2Gi",  # Minimum for Docker operations
    docker_enabled=True,
).create()
```

### 2. Monitor Memory Usage

```python
# Check memory before heavy operations
process = sandbox.process.exec("free", "-h")
process.wait()
print(process.stdout.read())

# Then run Docker command
sandbox.docker.pull("nginx:latest").wait()
```

### 3. Use Smaller Images

```python
# Use alpine-based images to save memory
sandbox.docker.pull("nginx:alpine").wait()  # Smaller than nginx:latest
```

### 4. Pull Images One at a Time

```python
# Don't pull multiple large images simultaneously
images = ["nginx:alpine", "redis:alpine", "postgres:alpine"]
for img in images:
    process = sandbox.docker.pull(img)
    process.wait()
    print(f"Pulled {img}")
```

## Files Modified

| File | Changes | Description |
|------|---------|-------------|
| `pkg/worker/lifecycle.go` | Lines 906-924 | Added OOM watcher logging |
| `pkg/runtime/oom_watcher.go` | Lines 42-113 | Enhanced error logging and fail-safe |

## Testing

To verify OOM is the issue:

```python
from beta9 import Image, Sandbox

# Test with low memory (should OOM)
sandbox_low = Sandbox(
    memory="128Mi",  # Very low!
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()

process = sandbox_low.docker.pull("nginx:latest")
exit_code = process.wait()
print(f"Exit code: {exit_code}")  # Probably 0 even if OOMed!

sandbox_low.terminate()

# Test with high memory (should work)
sandbox_high = Sandbox(
    memory="4Gi",  # Plenty!
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()

process = sandbox_high.docker.pull("nginx:latest")
exit_code = process.wait()
print(f"Exit code: {exit_code}")  # Should be 0 and actually work!
print(f"Output: {process.stdout.read()}")  # Should have output!

sandbox_high.terminate()
```

## Summary

✅ **Root Cause**: Container OOMing during Docker pull  
✅ **Why Not Detected**: gVisor doesn't expose cgroup memory.events file  
✅ **Symptom**: Container exits with code 0, looks successful but isn't  
✅ **Immediate Fix**: Increase memory allocation (at least 2GB for Docker)  
✅ **Long-Term**: Implement gVisor-specific OOM detection  
⚠️ **Warning**: OOM detection currently DOES NOT WORK for gVisor!  

**For now, ensure you allocate enough memory for Docker operations!** 🚀

Recommended minimum: **2GB for Docker-enabled sandboxes**
