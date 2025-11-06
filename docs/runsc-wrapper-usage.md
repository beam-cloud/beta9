# runsc Wrapper - Easy Container Listing

## Problem Solved

Previously, running `runsc list` on the worker pod showed no containers because it used the wrong `--root` directory:

```bash
# Before (didn't work)
root@worker:/# runsc list
ID          PID         STATUS      BUNDLE      CREATED     OWNER
# Empty!
```

Now it works automatically:

```bash
# After (works!)
root@worker:/# runsc list
ID                                           PID    STATUS    BUNDLE
taskqueue-0c9ab734-01b2-4fae-aaaa-82c2c7...  123    running   /tmp/...
```

## How It Works

The worker image includes a wrapper script at `/usr/local/bin/runsc` that automatically adds Beta9's default flags:

```bash
# When you run:
runsc list

# The wrapper executes:
runsc.real --root /run/gvisor --platform systrap list
```

## Commands Available

### List Containers

```bash
# All of these work now:
runsc list
runsc-list      # Alias
runsc-ps        # Alias
```

### Get Container State

```bash
runsc state <container-id>
```

### Get Container Events

```bash
runsc events <container-id>
```

### Kill Container

```bash
runsc kill <container-id> SIGTERM
```

### Delete Container

```bash
runsc delete <container-id>
```

### Get Container Processes

```bash
runsc ps <container-id>
```

### Debug Mode

```bash
# Add --debug flag for verbose output
runsc --debug list
```

## Configuration

The wrapper uses environment variables (set in the Dockerfile):

```bash
GVISOR_ROOT=/run/gvisor        # Where runsc stores container state
GVISOR_PLATFORM=systrap        # Platform to use (systrap/kvm/ptrace)
```

You can override these at runtime if needed:

```bash
# Use different root directory
GVISOR_ROOT=/tmp/runsc runsc list

# Use different platform
GVISOR_PLATFORM=kvm runsc list
```

## Bypassing the Wrapper

If you need to use runsc without the wrapper (e.g., for testing):

```bash
# Use the real binary directly
runsc.real list

# Or use the alias
runsc-raw list

# Or specify the full path
/usr/local/bin/runsc.real list
```

## Examples

### List all containers with details

```bash
root@worker:/# runsc list
ID                                           PID    STATUS    BUNDLE
taskqueue-abc123                             456    running   /tmp/taskqueue-abc123/layer-0/merged
endpoint-xyz789                              789    running   /tmp/endpoint-xyz789/layer-0/merged
```

### Get detailed state of a container

```bash
root@worker:/# runsc state taskqueue-abc123
{
  "id": "taskqueue-abc123",
  "pid": 456,
  "status": "running",
  "bundle": "/tmp/taskqueue-abc123/layer-0/merged",
  "created": "2025-11-05T15:30:45.123456789Z"
}
```

### Monitor container events

```bash
root@worker:/# runsc events taskqueue-abc123
{"type":"stats","id":"taskqueue-abc123","data":{"cpu":{"usage":{"total":1234567890}}}}
```

### List with JSON output

```bash
root@worker:/# runsc list --format=json | jq
[
  {
    "id": "taskqueue-abc123",
    "pid": 456,
    "status": "running",
    "bundle": "/tmp/taskqueue-abc123/layer-0/merged"
  }
]
```

### Debug logging

```bash
root@worker:/# runsc --debug list
# Shows verbose debug output about internal operations
```

## Verification

To verify the wrapper is working:

```bash
# Check which runsc you're using
root@worker:/# which runsc
/usr/local/bin/runsc

# Check it's the wrapper (not the real binary)
root@worker:/# file /usr/local/bin/runsc
/usr/local/bin/runsc: Bourne-Again shell script, ASCII text executable

# Check the real binary exists
root@worker:/# file /usr/local/bin/runsc.real
/usr/local/bin/runsc.real: ELF 64-bit LSB executable

# Test listing (should work without flags)
root@worker:/# runsc list
```

## Troubleshooting

### "No containers" even with wrapper

**Check if containers are actually running:**
```bash
ps aux | grep runsc
```

If you see runsc processes but `runsc list` shows nothing, check the root directory:

```bash
# List what's in the gVisor root
ls -la /run/gvisor/
```

### Wrapper not working

**Check if wrapper is executable:**
```bash
ls -la /usr/local/bin/runsc
# Should show: -rwxr-xr-x ... /usr/local/bin/runsc
```

**Check environment variables:**
```bash
echo $GVISOR_ROOT
echo $GVISOR_PLATFORM
```

**Test the real binary directly:**
```bash
runsc.real --root /run/gvisor list
```

### Want to use different root/platform

**Override via environment:**
```bash
GVISOR_ROOT=/custom/path runsc list
```

**Or bypass wrapper entirely:**
```bash
runsc.real --root /custom/path --platform ptrace list
```

## Integration with Beta9

The wrapper is configured to match Beta9's runtime configuration:

| Setting | Value | Why |
|---------|-------|-----|
| `GVISOR_ROOT` | `/run/gvisor` | Beta9's configured root directory |
| `GVISOR_PLATFORM` | `systrap` | Best performance for most workloads |

This means `runsc` commands will automatically work with Beta9-managed containers.

## Bash Aliases

The worker image includes helpful aliases:

```bash
# List containers (same as 'runsc list')
runsc-list
runsc-ps

# Access real runsc binary (bypass wrapper)
runsc-raw
```

Add these to your `.bashrc` locally:

```bash
# Add to ~/.bashrc on your local machine
alias k-runsc='kubectl exec -it <worker-pod> -- runsc'
alias k-runsc-list='kubectl exec -it <worker-pod> -- runsc list'
```

## Summary

✅ **Before**: Had to run `runsc --root /run/gvisor --platform systrap list`  
✅ **After**: Just run `runsc list`

The wrapper automatically adds Beta9's default flags, making it easy to inspect gVisor containers on the worker pod.
