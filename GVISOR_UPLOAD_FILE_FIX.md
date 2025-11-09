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

Add the `--overlay2=root:self` flag to runsc invocation. This tells gVisor to use a **self-backed overlay** for the root filesystem, where the upper layer is stored on the host filesystem instead of in sandbox-internal tmpfs.

### Implementation

**File**: `pkg/runtime/runsc.go`

```go
func (r *Runsc) baseArgs(dockerEnabled bool) []string {
    args := []string{
        "--root", r.cfg.RunscRoot,
    }
    
    // ... other args ...
    
    // Disable rootfs overlay to allow files written to host overlay to be visible inside container
    // By default, gVisor uses a sandbox-internal tmpfs overlay which prevents host filesystem changes
    // from being visible. Setting this to "root:self" makes the root filesystem use a host-backed overlay.
    args = append(args, "--overlay2=root:self")
    
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

### After (`--overlay2=root:self`)
```
┌─────────────────────────────────────┐
│  Sandbox                            │
│  ┌─────────────────────────────┐   │
│  │ self-backed upper layer     │   │  ← Backed by host filestore
│  └─────────────────────────────┘   │
│  ┌─────────────────────────────┐   │
│  │ lower layer (via gofer)     │   │  ← Read-only image layer
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
         ↕ gofer RPC + mmap filestore
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
According to gVisor's own benchmarks, the default `--overlay2=all:memory` mode provides **2x better performance** for filesystem-heavy workloads. With `--overlay2=root:self`:
- **Slightly slower** filesystem operations (still much faster than without overlay)
- File data stored on disk instead of memory
- Acceptable trade-off for correctness

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
