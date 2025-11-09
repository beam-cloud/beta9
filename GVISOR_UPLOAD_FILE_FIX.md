# gVisor Rootfs Overlay Fix

## Problem

Files uploaded via `fs.upload_file()` were not visible to processes run via `process.exec()` in gVisor sandboxes, even though:
- The upload succeeded
- `fs.stat_file()` could see the files (on the host)
- The files were written to the correct host overlay path

### Symptoms
```
ContainerSandboxUploadFile writes to: /tmp/sandbox-.../layer-0/merged/tmp/file.yml
fs.stat_file() sees the file ✓
process.exec("cat", "/tmp/file.yml") → "No such file or directory" ✗
```

## Root Cause

From [gVisor's rootfs overlay documentation](https://gvisor.dev/blog/2023/05/08/rootfs-overlay/):

> **Sandbox Internal Overlay**: Given that the upper layer is destroyed with the container... why keep the upper layer on the host at all? Instead we can move the upper layer into the sandbox.
>
> The idea is to overlay the rootfs using a sandbox-internal overlay mount. We can use a tmpfs upper (container) layer...

By default, gVisor uses `--overlay2=all:memory` which creates a **sandbox-internal tmpfs** for the upper layer. Files written to the host overlay path are not synced to this internal overlay, so they're invisible inside the container.

## Solution

Add the `--overlay2=none` flag to runsc invocation. This **disables the rootfs overlay optimization** entirely, ensuring that files written to the host filesystem are immediately visible inside the container.

From [gVisor's official documentation](https://gvisor.dev/docs/user_guide/filesystem/):
> Self-backed rootfs overlay (`--overlay2=root:self`) is enabled by default in runsc for performance. **If you need to propagate rootfs changes to the host filesystem, then disable it with `--overlay2=none`.**

### Implementation

**File**: `pkg/runtime/runsc.go`

```go
func (r *Runsc) baseArgs(dockerEnabled bool) []string {
    args := []string{
        "--root", r.cfg.RunscRoot,
    }
    
    // ... other args ...
    
    // Disable rootfs overlay to propagate host filesystem changes to the container
    // By default, runsc uses --overlay2=root:self which keeps changes in a sandbox-internal layer.
    // Setting --overlay2=none disables this optimization so files written to the host overlay
    // are immediately visible inside the container.
    args = append(args, "--overlay2=none")
    
    // ... rest of args ...
}
```

## How It Works

### Before (Default: `--overlay2=all:memory`)
```
┌─────────────────────────────────────┐
│  Sandbox                            │
│  ┌─────────────────────────────┐   │
│  │ tmpfs upper layer (memory)  │   │  ← Files here (invisible from host)
│  └─────────────────────────────┘   │
│  ┌─────────────────────────────┐   │
│  │ lower layer (via gofer)     │   │  ← Read-only image layer
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
         ↕ gofer RPC
┌─────────────────────────────────────┐
│  Host                               │
│  /tmp/sandbox-.../layer-0/merged/   │  ← Files written here NOT visible inside
└─────────────────────────────────────┘
```

### After (`--overlay2=none`)
```
┌─────────────────────────────────────┐
│  Sandbox                            │
│  ┌─────────────────────────────┐   │
│  │ Direct filesystem access    │   │  ← No overlay layer
│  │ (via gofer)                 │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
         ↕ gofer RPC
┌─────────────────────────────────────┐
│  Host                               │
│  /tmp/sandbox-.../layer-0/merged/   │  ← Files written here ARE visible inside
└─────────────────────────────────────┘
```

## Benefits

1. **Fixes upload_file**: Files written to host are now visible in container
2. **Fixes docker-compose**: Override files can be uploaded and used
3. **Minimal change**: Single flag, no code restructuring
4. **Maintains compatibility**: Works with existing overlay infrastructure
5. **Still sandboxed**: gVisor security guarantees remain intact

## Trade-offs

### Performance
The default `--overlay2=root:self` mode provides significant performance benefits for filesystem-heavy workloads by keeping changes in memory. With `--overlay2=none`:
- **Slower** filesystem operations (all operations go through gofer)
- No in-memory caching of filesystem changes
- **Required trade-off** for filesystem consistency with host

### Memory
- **Pro**: Files don't consume sandbox memory, won't hit container memory limits
- **Pro**: Better for workloads with large file writes

## Testing

The fix resolves the original issue:

```python
# Upload file
sandbox.fs.upload_file(local_path, "/tmp/file.yml")

# Now visible to exec
p = sandbox.process.exec("cat", "/tmp/file.yml")  # ✓ Works!

# Docker Compose works
process = sandbox.docker.compose_up()  # ✓ Works!
```

## References

- [gVisor Rootfs Overlay Blog Post](https://gvisor.dev/blog/2023/05/08/rootfs-overlay/)
- [gVisor Overlay Modes Documentation](https://gvisor.dev/docs/user_guide/filesystem/)
- runsc flag: `--overlay2=root:self`
