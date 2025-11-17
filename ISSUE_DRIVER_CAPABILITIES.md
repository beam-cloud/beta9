# gVisor GPU Support - THE ACTUAL SOLUTION

## The Real Issue: Driver Capabilities Mismatch

## Problem Found

The container was requesting driver capabilities that weren't allowed by gVisor:

**Container requested (via `NVIDIA_DRIVER_CAPABILITIES`):**
```
compute,utility,graphics,ngx,video
```

**gVisor default allowed capabilities:**
```
compute,utility  (ONLY these two!)
```

## The Error

When a container requests capabilities that aren't in the `--nvproxy-allowed-driver-capabilities` flag, gVisor fails during initialization with:
```
urpc method "containerManager.StartRoot" failed: EOF
```

This is a silent failure - no clear error message about the capability mismatch!

## The Fix

Added `--nvproxy-allowed-driver-capabilities` flag to runsc invocation:

```go
if r.nvproxyEnabled {
    args = append(args, "--nvproxy=true")
    args = append(args, "--nvproxy-allowed-driver-capabilities=compute,utility,graphics,video")
}
```

**Note:** "ngx" is not in gVisor's supported capabilities list, but the container should work without it. gVisor supports:
- compute
- utility  
- graphics
- video

## Why Device Filtering Was Still Needed

Device filtering WAS necessary, but it wasn't the cause of the EOF error. Both issues existed:

1. **Unsupported devices** - Would cause issues if not filtered
2. **Missing capability flags** - Caused the EOF error we saw

## Test It

Run a GPU container now - it should start successfully!
