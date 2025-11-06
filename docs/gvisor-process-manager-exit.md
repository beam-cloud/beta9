# gVisor Process Manager Exit Issue

## Problem

Task queue containers don't exit when user code calls `os._exit(0)`.

## Root Cause

For taskqueue containers, the process hierarchy is:

```
PID 1: goproc (process manager)
  └─ PID N: user Python code
```

When user code calls `os._exit(0)`:
1. Child process (PID N) exits
2. But PID 1 (goproc) is still running
3. Container doesn't exit because PID 1 is alive

## Why `--ignore-cgroups` Made It Worse

With `--ignore-cgroups`, runsc **only** monitors PID 1:
- ❌ Ignores all child processes
- ❌ Only exits when PID 1 exits
- ❌ goproc stays running even when all children exit

Without `--ignore-cgroups`, runsc uses cgroup monitoring:
- ✅ Detects when ALL processes in cgroup exit
- ✅ Exits when cgroup is empty (no running processes)
- ✅ Works correctly with process managers like goproc

## Solution

**Remove** the `--ignore-cgroups` flag. Let gVisor use its default cgroup-based process tracking.

### How It Works (Default Behavior)

```
Container starts
  ↓
goproc (PID 1) starts
  ↓
goproc spawns user code (PID N)
  ↓
User code calls os._exit(0)
  ↓
PID N exits
  ↓
goproc reaps child and may exit itself
  ↓
runsc detects cgroup is empty OR PID 1 exited
  ↓
Container exits ✅
```

## Container Types

### Type 1: Direct Execution (simple containers)
```
PID 1: user_process
```
- User process **is** PID 1
- When it exits, container exits immediately
- Works with or without `--ignore-cgroups`

### Type 2: Process Manager (taskqueue)
```
PID 1: goproc
  ├─ PID 2: user_process_1
  └─ PID 3: user_process_2
```
- User process is **not** PID 1
- Need cgroup monitoring to detect when children exit
- **Must not use** `--ignore-cgroups`

## The Real Issue

The actual issue might be that **goproc doesn't exit** when its children exit. Let me check what goproc's behavior is supposed to be.

### Expected goproc Behavior

goproc should:
1. Monitor child processes
2. When a child calls `os._exit()`, reap it
3. Decide whether to exit itself or spawn new children

### Current Behavior

goproc might be configured to:
- Keep running even after children exit
- Wait for new task queue items
- Act as a persistent process manager

## Proper Fix

The issue isn't with runsc - it's with the process manager pattern. We need ONE of:

### Option 1: Make goproc exit when children exit
Configure goproc to terminate when its spawned process exits.

### Option 2: Send signal to PID 1
When user code wants to exit the container, kill PID 1:
```python
os.kill(1, signal.SIGTERM)  # Kill goproc
```

### Option 3: Use direct execution
Don't use goproc for taskqueues - run user code directly as PID 1.

### Option 4: Exit via API
User code signals through goproc API that it wants to exit:
```python
# Instead of os._exit(0)
goproc_client.shutdown()
```

## Recommended Solution

**Option 1 or 2** - The container should exit when the task completes.

For taskqueues, when a task calls `os._exit()`, the container should shut down. The simplest fix:

In the SDK code:
```python
except (grpc.RpcError, OSError):
    print("Failed to retrieve task", traceback.format_exc())
    # Instead of just exiting this process:
    # os._exit(TaskExitCode.Error)
    
    # Exit the entire container by killing PID 1:
    import signal
    os.kill(1, signal.SIGTERM)  # Kill goproc (PID 1)
    os._exit(TaskExitCode.Error)  # Also exit this process
```

This ensures the container exits properly regardless of runtime (runc or gVisor).

## Why This Wasn't an Issue with runc

runc might have different behavior regarding process cleanup, or the timing might mask the issue. But the fundamental problem (PID 1 staying alive) would exist with both runtimes.

## Summary

- ✅ Remove `--ignore-cgroups` flag
- ✅ Let gVisor use cgroup-based monitoring
- ⚠️ The real issue is the process manager pattern
- 🔧 SDK should kill PID 1 when it wants to exit the container

The runtime fix is simple (remove flag), but the proper solution requires SDK changes to explicitly terminate the container.
