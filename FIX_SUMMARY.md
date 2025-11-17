# Fix Summary: Exit Status 128 Issue

## Root Cause

Exit status 128 was caused by passing **unsupported flags** to `runsc checkpoint`. gVisor's runsc has different checkpoint flags than runc/CRIU.

## What Was Wrong

Our code was passing runc/CRIU-specific flags that don't exist in gVisor:

```go
// WRONG - These flags don't exist in runsc
--work-dir           // Should be --work-path
--allow-open-tcp     // Not supported by gVisor
--skip-in-flight     // Not supported by gVisor
```

## gVisor Supported Flags

According to `runsc checkpoint --help`:

✅ **Supported:**
- `--image-path` - directory path to saved container image
- `--leave-running` - restart container after checkpointing
- `--work-path` - work files and logs (marked as "ignored")
- `--compression` - compress checkpoint image
- `--direct` - use O_DIRECT for writing

❌ **NOT Supported:**
- `--work-dir` (use `--work-path` instead)
- `--allow-open-tcp` / `--tcp-established` (CRIU-specific)
- `--skip-in-flight` / `--tcp-skip-in-flight` (CRIU-specific)
- `--link-remap` (CRIU-specific)
- All other CRIU-specific flags

## What Was Fixed

### 1. Corrected Flag Names
```go
// Before
if opts.WorkDir != "" {
    args = append(args, "--work-dir", opts.WorkDir)  // ❌ Wrong flag
}

// After
if opts.WorkDir != "" {
    args = append(args, "--work-path", opts.WorkDir)  // ✅ Correct flag
}
```

### 2. Removed Unsupported Flags
```go
// Removed these (not supported by gVisor):
if opts.AllowOpenTCP {
    args = append(args, "--allow-open-tcp")  // ❌ Removed
}
if opts.SkipInFlight {
    args = append(args, "--skip-in-flight")  // ❌ Removed
}
```

### 3. Added Better Error Reporting
- Now captures stderr separately
- Error messages include actual runsc output
- Shows exactly why checkpoint failed

### 4. Ensured Directories Exist
- Creates `ImagePath` if it doesn't exist
- Creates `WorkDir` if it doesn't exist
- Prevents "directory not found" errors

### 5. Made CUDA Checkpoint Non-Fatal
- CUDA checkpoint failures now warn but don't fail entire checkpoint
- Allows containers without GPU to checkpoint normally

## Testing

```bash
# Build
✅ go build ./cmd/worker/...

# Test
✅ go test ./pkg/runtime/... (all pass)
```

## Summary

The checkpoint should now work correctly. The command being executed is:

```bash
runsc checkpoint \
  --image-path <path> \
  --work-path <workdir> \
  --leave-running \
  <container-id>
```

Instead of the previous (incorrect) command with unsupported flags.

## Next Steps

Try checkpointing again - it should now:
1. ✅ Use correct gVisor-supported flags
2. ✅ Create directories if they don't exist
3. ✅ Show detailed error messages if something fails
4. ✅ Not fail on CUDA checkpoint issues (warnings only)
