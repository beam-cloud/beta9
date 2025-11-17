# gVisor Checkpoint/Restore with CUDA Support - Implementation Summary

## Overview

This implementation adds full support for checkpoint/restore functionality in gVisor containers, including CUDA workloads. The system now supports runtime-agnostic checkpointing for both runc and gVisor (runsc) containers.

## Key Changes

### 1. Runtime Interface Enhancement (`pkg/runtime/runtime.go`)

Added checkpoint/restore methods to the Runtime interface:

```go
// New types
type CheckpointOpts struct {
    ImagePath    string
    WorkDir      string
    LeaveRunning bool
    AllowOpenTCP bool
    SkipInFlight bool
    LinkRemap    bool
    OutputWriter OutputWriter
}

type RestoreOpts struct {
    ImagePath    string
    WorkDir      string
    BundlePath   string
    OutputWriter OutputWriter
    Started      chan<- int
    TCPClose     bool
}

// New interface methods
Checkpoint(ctx context.Context, containerID string, opts *CheckpointOpts) error
Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error)
```

### 2. Runc Implementation (`pkg/runtime/runc.go`)

Implemented checkpoint/restore using runc's native CRIU support:
- Wraps existing go-runc library checkpoint/restore functionality
- Supports all CRIU options (AllowOpenTCP, LeaveRunning, etc.)
- Full CUDA checkpoint support via NVIDIA driver 570+

### 3. Runsc (gVisor) Implementation (`pkg/runtime/runsc.go`)

Implemented gVisor's native checkpoint/restore:
- Updated `Capabilities()` to report `CheckpointRestore: true`
- Uses `runsc checkpoint --image-path` command
- Uses `runsc restore --image-path --bundle` command
- Supports `--nvproxy=true` flag for GPU workloads
- Properly handles process lifecycle and exit codes

**Key gVisor checkpoint command:**
```bash
runsc --root /run/gvisor checkpoint --image-path /path/to/checkpoint --leave-running container-id
```

**Key gVisor restore command:**
```bash
runsc --root /run/gvisor --nvproxy=true restore --image-path /path/to/checkpoint --bundle /path/to/bundle container-id
```

### 4. CRIU Manager Refactoring (`pkg/worker/criu*.go`)

Updated all CRIU managers to be runtime-agnostic:

**Before:**
```go
CreateCheckpoint(ctx, checkpointId, request) (string, error)
RestoreCheckpoint(ctx, opts) (int, error)
```

**After:**
```go
CreateCheckpoint(ctx, runtime, checkpointId, request) (string, error)
RestoreCheckpoint(ctx, runtime, opts) (int, error)
```

#### Changes in `criu_nvidia.go`:
- Removed direct runc dependency
- Uses runtime interface for checkpoint/restore operations
- Works with both runc and gVisor seamlessly
- Logs runtime name in checkpoint/restore operations

#### Changes in `criu_cedana.go`:
- Added runtime compatibility check
- Explicitly rejects non-runc runtimes (Cedana only supports runc)
- Updated to use new RestoreOpts structure

#### Changes in `criu.go`:
- Updated RestoreOpts structure to use io.Writer instead of runc-specific types
- Modified checkpoint creation flow to pass runtime from container instance
- Enhanced error handling with runtime awareness

### 5. Worker Integration (`pkg/worker/lifecycle.go`)

- Container instances now track which runtime they use
- Checkpoint/restore operations fetch runtime from container instance
- Capability checking ensures only compatible runtimes attempt checkpoint/restore
- GPU support works transparently with both runtimes

## CUDA Checkpoint Support

### For runc (NVIDIA Driver 570+):
- Uses CRIU with NVIDIA cuda-checkpoint plugin
- Requires `crCompatible()` check for driver version >= 570
- Full GPU state preservation and restoration

### For gVisor (NVIDIA nvproxy):
- gVisor's nvproxy intercepts CUDA calls at userspace level
- GPU state is checkpointed through gVisor's native mechanism
- Requires `--nvproxy=true` flag on runsc commands
- GPU devices detected via CDI annotations or device specifications

## Testing

### Runtime Tests (`pkg/runtime/checkpoint_test.go`)

- **TestRuncCheckpointRestore**: Validates runc checkpoint/restore interface
- **TestRunscCheckpointRestore**: Validates gVisor checkpoint/restore interface
- **TestCheckpointRestoreOptions**: Tests option structures
- **TestRuntimeCapabilities**: Verifies both runtimes report checkpoint support
- **TestRuntimeIntegration**: Integration tests for both runtimes
- **TestGVisorNVProxySupport**: Tests GPU detection and nvproxy enablement

### Worker Tests (`pkg/worker/criu_test.go`)

- **TestNvidiaCRIUManagerWithRunc**: Tests NVIDIA CRIU with runc
- **TestNvidiaCRIUManagerWithGVisor**: Tests NVIDIA CRIU with gVisor
- **TestCedanaCRIUManagerRuntimeCheck**: Validates Cedana runtime restrictions
- **TestCheckpointRestoreErrorHandling**: Tests error scenarios
- **TestRuntimeCompatibility**: Tests both runtimes work with same manager

All tests include:
- Mock runtime implementations for unit testing
- Proper error handling validation
- Exit code verification
- CUDA workload testing scenarios

## How It Works

### Checkpoint Flow:

1. Worker receives checkpoint request
2. Worker calls `criuManager.CreateCheckpoint(ctx, runtime, checkpointId, request)`
3. CRIU manager calls `runtime.Checkpoint(ctx, containerId, opts)`
4. Runtime executes checkpoint:
   - **runc**: Uses CRIU library
   - **gVisor**: Executes `runsc checkpoint` command
5. Checkpoint saved to storage (S3 or local)
6. Filesystem state copied separately

### Restore Flow:

1. Worker receives container request with checkpoint
2. Worker waits for checkpoint sync file
3. Worker calls `criuManager.RestoreCheckpoint(ctx, runtime, opts)`
4. CRIU manager calls `runtime.Restore(ctx, containerId, opts)`
5. Runtime executes restore:
   - **runc**: Uses CRIU library with workDir
   - **gVisor**: Executes `runsc restore` command
6. Container resumes from checkpoint state

## Compatibility Matrix

| Feature | runc | gVisor |
|---------|------|--------|
| Checkpoint/Restore | ✅ | ✅ |
| GPU Support | ✅ (CRIU) | ✅ (nvproxy) |
| CUDA Checkpoint | ✅ (Driver 570+) | ✅ (nvproxy) |
| CDI Support | ✅ | ✅ |
| TCP Connections | ✅ | ✅ |
| Leave Running | ✅ | ✅ |

## Cedana Compatibility

Cedana currently only supports runc:
- Runtime check added to prevent gVisor usage
- Clear error messages when incompatible runtime detected
- Future: Could be extended to support gVisor if Cedana adds support

## Configuration

No configuration changes required. The system automatically:
- Detects which runtime is configured
- Uses appropriate checkpoint/restore mechanism
- Enables GPU support when detected
- Handles capability negotiation

## Verification

Run the test suite:
```bash
# Test runtime checkpoint/restore
SKIP_RUNTIME_TESTS=0 go test ./pkg/runtime/... -v

# Test worker integration
SKIP_CRIU_TESTS=0 go test ./pkg/worker/... -v

# Build worker binary
go build -o worker ./cmd/worker/...
```

## Benefits

1. **Unified Interface**: Single API works with both runc and gVisor
2. **CUDA Support**: Full GPU checkpoint/restore for both runtimes
3. **Runtime Flexibility**: Easy to add new runtimes in future
4. **Better Isolation**: gVisor provides enhanced security with checkpoint support
5. **Testing**: Comprehensive test coverage for both runtimes
6. **Maintainability**: Clean abstraction reduces code duplication

## Migration Notes

This is a **backward compatible** change:
- Existing runc checkpoints continue to work
- No changes to checkpoint storage format
- API changes are internal to worker
- Runtime selection happens automatically based on configuration

## Future Enhancements

Potential improvements:
1. Add metrics for checkpoint/restore performance per runtime
2. Implement checkpoint compression for gVisor
3. Add checkpoint verification/validation
4. Support for incremental checkpoints
5. Cross-runtime checkpoint migration (with limitations)
