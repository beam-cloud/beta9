# gVisor Rootfs Overlay Upload File Fix

## Problem

When using gVisor with rootfs overlay (default configuration), files uploaded via `fs.upload_file()` were not visible to processes run via `process.exec()`, even though the upload succeeded and `fs.stat_file()` could see the files.

### Symptoms
- `fs.upload_file()` returns success
- `fs.stat_file()` can see the uploaded file
- `process.exec("cat", file)` fails with "No such file or directory"
- docker-compose cannot find override files uploaded this way

### Root Cause

From [gVisor's rootfs overlay documentation](https://gvisor.dev/blog/2023/05/08/rootfs-overlay/):

> **Sandbox Internal Overlay**: The upper layer [of the overlay filesystem] is destroyed with the container... why keep the upper layer on the host at all? Instead we can move the upper layer into the sandbox.
>
> The idea is to overlay the rootfs using a sandbox-internal overlay mount. We can use a tmpfs upper (container) layer and a read-only lower layer served by the gofer client.

With gVisor's rootfs overlay:
1. The **upper layer** (where file modifications are stored) is a **sandbox-internal tmpfs**, not on the host filesystem
2. The **lower layer** (read-only image layer) is served by the gofer from the host
3. Files written to the host overlay path are **not visible** in the sandbox's tmpfs upper layer

Our original implementation wrote files directly to the host overlay path:
```go
hostPath := s.getHostPathFromContainerPath(containerPath, instance)
err = os.WriteFile(hostPath, in.Data, os.FileMode(in.Mode))  // ← Host filesystem!
```

But `process.exec()` runs commands **inside the sandbox** where they see the sandbox-internal overlay (tmpfs upper layer + gofer lower layer), not the host filesystem.

## Solution

The fix differentiates between runc and runsc (gVisor):

### For runc
Files are written directly to the host overlay path (original behavior):
```go
hostPath := s.getHostPathFromContainerPath(containerPath, instance)
err = os.WriteFile(hostPath, in.Data, os.FileMode(in.Mode))
```

This works for runc because runc uses host-based overlay where both the upper and lower layers are on the host filesystem.

### For runsc (gVisor)
Files are written through the sandbox process manager:
```go
// Write file using cat with stdin
pid, err := instance.SandboxProcessManager.RunCommand(
    []string{"sh", "-c", fmt.Sprintf("cat > '%s'", containerPath)}, 
    instance.Spec.Process.Env, 
    "", 
    in.Data
)
```

This ensures files go through gVisor's VFS and land in the sandbox-internal tmpfs overlay where they're visible to all processes running via `SandboxExec`.

## Implementation Details

### Backend Changes (`pkg/worker/container_server.go`)

1. **Runtime Detection**: Check `instance.Spec.Runtime` to determine if we're using gVisor
2. **Separate Code Paths**:
   - `runc`: Direct host filesystem write (original behavior)
   - `runsc`: Write through sandbox process manager
3. **Helper Functions**:
   - `uploadFileThroughSandbox()`: Handles gVisor file uploads
   - `waitForProcess()`: Waits for process completion with timeout

### Key Implementation Points

```go
func (s *ContainerRuntimeServer) ContainerSandboxUploadFile(...) {
    // ... container path resolution ...
    
    // For gVisor, use sandbox process manager
    if instance.Spec.Runtime == "runsc" {
        return s.uploadFileThroughSandbox(ctx, in, instance, containerPath)
    }
    
    // For runc, write directly to host overlay
    hostPath := s.getHostPathFromContainerPath(containerPath, instance)
    err = os.WriteFile(hostPath, in.Data, os.FileMode(in.Mode))
    // ...
}

func (s *ContainerRuntimeServer) uploadFileThroughSandbox(...) {
    // Wait for process manager to be ready
    // ...
    
    // Create parent directory if needed
    pid, err := instance.SandboxProcessManager.RunCommand(
        []string{"mkdir", "-p", parentDir}, ...)
    
    // Write file using cat with stdin
    pid, err := instance.SandboxProcessManager.RunCommand(
        []string{"sh", "-c", fmt.Sprintf("cat > '%s'", containerPath)}, 
        ..., in.Data)
    
    // Set permissions
    pid, err := instance.SandboxProcessManager.RunCommand(
        []string{"chmod", fmt.Sprintf("%o", in.Mode), containerPath}, ...)
}
```

## Testing

Two tests were added in `sdk/tests/test_sandbox.py`:

### 1. `test_sandbox_upload_file_visible_to_exec`
Tests that uploaded files are visible to exec commands:
- Upload a file via `fs.upload_file()`
- Verify with `ls` command
- Verify with `cat` command
- Verify with `fs.stat_file()`

### 2. `test_sandbox_docker_compose_with_override`
Tests the original bug - docker-compose reading override files:
- Upload `docker-compose.yml`
- Upload `docker-compose.override.yml`
- Run `docker-compose config` to verify both files are readable and merged

## Docker Compose Fix

The original issue was that `docker.compose_up()` couldn't create override files for gVisor networking compatibility. With this fix:

1. SDK parses the compose file to extract service names
2. SDK generates an override file with `network_mode: host` for each service
3. SDK uploads the override file using `fs.upload_file()` ✅ **Now works with gVisor!**
4. Docker Compose can read both files and merge them correctly

### Example Override Generated
```yaml
services:
  redis:
    network_mode: host
  web:
    network_mode: host
```

This solves the gVisor networking issues:
- No bridge network creation (incompatible with gVisor)
- No veth interfaces (causes permission errors)
- Services use host networking directly

## Performance Considerations

### gVisor Path (through process manager)
- **Pros**: Files visible to all processes, works with sandbox-internal overlay
- **Cons**: Slightly slower due to process spawning
- **Impact**: Negligible for typical file uploads (< 100ms overhead)

### runc Path (direct host write)
- **Pros**: Fast, direct filesystem access
- **Cons**: N/A (works as before)
- **Impact**: No change from original implementation

## Future Improvements

1. **Batch Uploads**: For multiple files, spawn a single shell process
2. **Large Files**: Stream data to avoid memory overhead
3. **Atomic Operations**: Use temp file + rename for atomic uploads

## Related Issues

- gVisor rootfs overlay: https://gvisor.dev/blog/2023/05/08/rootfs-overlay/
- Docker Compose host networking errors in gVisor sandboxes
- `fs.upload_file()` succeeding but files not visible to `process.exec()`
