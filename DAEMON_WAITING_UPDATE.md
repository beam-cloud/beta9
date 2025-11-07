# Docker Daemon Waiting Feature - Update Documentation

## Problem Statement

When a Beta9 Sandbox is created with Docker enabled, the Docker daemon takes several seconds to start. If Docker commands are executed immediately after sandbox creation, they fail with:

```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?
```

This required users to manually add sleep delays or retry logic, leading to brittle code.

## Solution

The Docker manager now **automatically waits** for the Docker daemon to be ready before executing any command. This is handled transparently, requiring zero changes to user code.

## How It Works

### Automatic Daemon Readiness Check

1. **First Command Triggers Wait**: The first time any Docker command is called, the manager automatically checks if the daemon is ready
2. **Polling with Timeout**: The manager polls the daemon using `docker info` every 500ms
3. **Default Timeout**: Waits up to 30 seconds (configurable) for the daemon to respond
4. **One-Time Check**: Once the daemon is confirmed ready, subsequent commands execute immediately without checking again
5. **User Feedback**: Progress messages are displayed to keep users informed

### Implementation Details

#### New Manager Initialization Parameters

```python
SandboxDockerManager(
    sandbox_instance,
    auto_wait_for_daemon=True,  # Enable automatic waiting (default)
    daemon_timeout=30            # Maximum wait time in seconds (default)
)
```

#### Internal State Tracking

```python
self._docker_ready = False           # Has daemon been confirmed ready?
self._docker_ready_checked = False   # Have we checked at all?
```

#### Key Methods Added

1. **`_wait_for_docker_daemon(timeout)`** - Internal polling loop
   - Polls `docker info` every 500ms
   - Returns True on success, False on timeout
   - Displays progress messages via terminal

2. **`_ensure_docker_ready()`** - Guard for all Docker commands
   - Called automatically before each Docker command
   - Only waits once per sandbox instance
   - Respects `auto_wait_for_daemon` flag

3. **`is_daemon_ready()`** - Non-blocking status check
   - Checks daemon status without waiting
   - Useful for conditional logic

4. **`wait_for_daemon(timeout)`** - Manual wait control
   - Allows explicit waiting with custom timeout
   - Useful when auto-wait is disabled

## Usage Examples

### Default Behavior (No Code Changes Needed)

```python
from beta9 import Sandbox

# Create sandbox with Docker
sandbox = Sandbox(docker_enabled=True)
instance = sandbox.create()

# Just use Docker - waiting happens automatically!
instance.docker.pull("nginx:latest").wait()
instance.docker.run("nginx:latest", detach=True).wait()
```

**Output:**
```
Waiting for Docker daemon to be ready (timeout: 30s)...
Docker daemon is ready (took 3.2s)
```

### Manual Control (Advanced)

```python
# Check readiness without waiting
if instance.docker.is_daemon_ready():
    print("Docker is ready!")
else:
    print("Not ready yet")

# Wait manually with custom timeout
if instance.docker.wait_for_daemon(timeout=60):
    print("Daemon started!")
else:
    print("Timeout - daemon didn't start")

# Disable auto-wait (not recommended)
instance.docker.auto_wait_for_daemon = False
```

### Configuration Options

```python
# Custom timeout for daemon startup
sandbox = Sandbox(docker_enabled=True)
instance = sandbox.create()

# The daemon_timeout can't be set directly during sandbox creation,
# but you can configure it after:
instance.docker.daemon_timeout = 60  # Wait up to 60 seconds

# Or wait manually with your preferred timeout
instance.docker.wait_for_daemon(timeout=120)
```

## Changes Made

### 1. Core Implementation (`sandbox.py`)

**SandboxDockerManager class:**
- Added `__init__` parameters: `auto_wait_for_daemon`, `daemon_timeout`
- Added state tracking: `_docker_ready`, `_docker_ready_checked`
- Added method: `_wait_for_docker_daemon(timeout)` - polling implementation
- Added method: `_ensure_docker_ready()` - guard for commands
- Added method: `is_daemon_ready()` - public status check
- Added method: `wait_for_daemon(timeout)` - public manual wait
- Modified all 21 Docker command methods to call `_ensure_docker_ready()` before execution

### 2. Tests (`test_sandbox_docker.py`)

**Added test class: `TestDockerDaemonWaiting`**
- `test_wait_for_daemon_success` - Successful daemon startup
- `test_wait_for_daemon_timeout` - Timeout handling
- `test_is_daemon_ready` - Status checking
- `test_auto_wait_for_daemon` - Automatic waiting on first command
- `test_no_auto_wait_when_disabled` - Disabled auto-wait behavior

**Updated: `TestSandboxDockerManager`**
- Modified setup to disable auto-wait for faster test execution
- All existing tests continue to pass

### 3. Examples (`docker_sandbox_example.py`)

**Updated: `example_basic_docker_usage()`**
- Added comments explaining automatic daemon waiting

**Added: `example_docker_daemon_waiting()`**
- Demonstrates default auto-wait behavior
- Shows manual readiness checking
- Shows custom timeout configuration
- Displays manager configuration

### 4. Documentation (`DOCKER_MANAGER_README.md`)

**New section: "Docker Daemon Management"**
- Explains automatic waiting behavior
- Documents manual control options
- Shows configuration examples
- Documents new API methods

**Updated: "Important Notes"**
- Added note about automatic daemon waiting as #1
- Emphasizes that no manual waiting is needed

## Behavioral Changes

### Before This Update

```python
instance = sandbox.create()
# Commands would fail immediately if daemon wasn't ready
process = instance.docker.ps()  # ❌ Error: daemon not ready
process.wait()
```

Users had to write:
```python
import time
instance = sandbox.create()
time.sleep(5)  # Manual delay - fragile!
process = instance.docker.ps()
process.wait()
```

### After This Update

```python
instance = sandbox.create()
# Automatically waits for daemon - just works!
process = instance.docker.ps()  # ✅ Waits automatically
process.wait()
```

## Performance Impact

- **First Docker Command**: Adds 0-30 seconds (depends on daemon startup time, typically 2-5 seconds)
- **Subsequent Commands**: Zero overhead (readiness is cached)
- **Network Calls**: One `docker info` call every 500ms until daemon is ready
- **Memory**: Negligible (two boolean flags per instance)

## Backward Compatibility

✅ **Fully backward compatible** - All existing code works without changes:
- Default behavior: Auto-wait enabled (safe, user-friendly)
- Users can disable auto-wait if needed (advanced use case)
- No breaking changes to existing API

## Configuration Best Practices

### Recommended (Default)
```python
# Let the manager handle everything automatically
instance = sandbox.create()
instance.docker.run("nginx:latest", detach=True).wait()
```

### For Long-Running Daemon Startups
```python
# Increase timeout if your setup takes longer
instance = sandbox.create()
instance.docker.daemon_timeout = 60
instance.docker.run("nginx:latest", detach=True).wait()
```

### For Advanced Users
```python
# Manual control with custom logic
instance = sandbox.create()
instance.docker.auto_wait_for_daemon = False

# Implement your own waiting logic
max_attempts = 10
for i in range(max_attempts):
    if instance.docker.is_daemon_ready():
        break
    time.sleep(1)
else:
    raise RuntimeError("Docker daemon failed to start")

# Now safe to use Docker
instance.docker.run("nginx:latest", detach=True).wait()
```

## Error Handling

### Daemon Never Becomes Ready

If the daemon doesn't start within the timeout period:

```
Waiting for Docker daemon to be ready (timeout: 30s)...
WARNING: Docker daemon did not become ready within 30s. 
Docker commands may fail. You can try waiting longer or check the daemon status manually.
```

The manager:
1. Logs a warning
2. Sets `_docker_ready_checked = True` to avoid re-checking
3. Allows subsequent commands to execute (they may fail, but at least won't hang)

### Subsequent Command Failures

If a Docker command fails after daemon was confirmed ready, the error is propagated normally through the `SandboxProcess` exit code and stderr.

## Testing

All daemon waiting functionality is covered by unit tests:
- Successful daemon startup
- Timeout scenarios  
- Manual vs automatic waiting
- Configuration options
- Integration with SandboxInstance

Run tests:
```bash
cd sdk
uv run --with pytest pytest tests/test_sandbox_docker.py -v
```

## Summary

The Docker daemon waiting feature eliminates a common pain point when using Docker in Beta9 Sandboxes. It:

✅ Works automatically with zero code changes
✅ Provides sensible defaults (30s timeout)
✅ Offers manual control for advanced use cases
✅ Includes comprehensive tests and documentation
✅ Has minimal performance impact
✅ Is fully backward compatible

Users can now create sandboxes and immediately use Docker without worrying about daemon startup timing!
