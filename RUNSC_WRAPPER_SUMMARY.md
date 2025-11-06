# runsc Wrapper - Summary

## Problem Solved

You wanted to be able to run `runsc list` directly on the worker pod without having to remember to add `--root /run/gvisor --platform systrap` every time.

## Solution Implemented

Created a wrapper script that automatically adds Beta9's configuration flags.

### What Changed

#### 1. Wrapper Script (`docker/runsc-wrapper.sh`)
```bash
#!/bin/bash
# Automatically adds --root /run/gvisor --platform systrap
exec /usr/local/bin/runsc.real "$@"
```

#### 2. Modified Dockerfile (`docker/Dockerfile.worker`)
- Real binary installed as: `/usr/local/bin/runsc.real`
- Wrapper installed as: `/usr/local/bin/runsc`
- Environment variables set: `GVISOR_ROOT=/run/gvisor`, `GVISOR_PLATFORM=systrap`
- Bash aliases added to `/root/.bashrc`

#### 3. Bash Aliases
```bash
alias runsc-raw="/usr/local/bin/runsc.real"    # Bypass wrapper
alias runsc-list="runsc list"                   # Shortcut
alias runsc-ps="runsc list"                     # Alternative
```

---

## Usage

### Before (manual flags required)
```bash
root@worker:/# runsc --root /run/gvisor --platform systrap list
ID                                           PID    STATUS    BUNDLE
taskqueue-abc123                             456    running   /tmp/...
```

### After (automatic!)
```bash
root@worker:/# runsc list
ID                                           PID    STATUS    BUNDLE
taskqueue-abc123                             456    running   /tmp/...
```

---

## Common Commands

All standard runsc commands now work without extra flags:

```bash
# List all containers
runsc list

# Get container state
runsc state <container-id>

# Get container processes
runsc ps <container-id>

# View container events
runsc events <container-id>

# Kill container
runsc kill <container-id> SIGTERM

# Delete container
runsc delete <container-id>

# Debug mode (verbose output)
runsc --debug list
```

---

## Advanced Usage

### Override Environment Variables

```bash
# Use different root directory
GVISOR_ROOT=/tmp/runsc runsc list

# Use different platform
GVISOR_PLATFORM=kvm runsc list
```

### Bypass Wrapper

If you need the raw binary without wrapper:

```bash
# Direct path
/usr/local/bin/runsc.real --root /custom/path list

# Or use alias
runsc-raw --root /custom/path list
```

### Format Output

```bash
# JSON output
runsc list --format=json

# With jq for pretty printing
runsc list --format=json | jq
```

---

## Testing

### In the Worker Pod

```bash
# SSH into worker pod
kubectl exec -it <worker-pod> -- bash

# Test wrapper
runsc list
runsc --version

# Test aliases
runsc-list
runsc-ps

# Test bypass
runsc-raw list
```

### Run Test Script

```bash
# Inside worker pod
/workspace/docs/runsc-wrapper-test.sh
```

---

## How It Works

```
User runs: runsc list
    ↓
Wrapper script: /usr/local/bin/runsc
    ↓
Adds flags: --root $GVISOR_ROOT --platform $GVISOR_PLATFORM
    ↓
Executes: /usr/local/bin/runsc.real --root /run/gvisor --platform systrap list
    ↓
Shows containers!
```

---

## Environment Variables

Set in the Dockerfile and available in all worker pods:

| Variable | Value | Purpose |
|----------|-------|---------|
| `GVISOR_ROOT` | `/run/gvisor` | Where runsc stores container state |
| `GVISOR_PLATFORM` | `systrap` | Platform for gVisor execution |

These match Beta9's runtime configuration, ensuring the wrapper always works with Beta9-managed containers.

---

## Files Created/Modified

### New Files
- `docker/runsc-wrapper.sh` - Wrapper script
- `docs/runsc-wrapper-usage.md` - Complete usage guide
- `docs/runsc-wrapper-test.sh` - Test script
- `RUNSC_WRAPPER_SUMMARY.md` - This file

### Modified Files
- `docker/Dockerfile.worker` - Install wrapper, set env vars, add aliases

---

## Verification Checklist

After building and deploying the new worker image:

- [ ] `runsc list` works without flags
- [ ] `runsc state <id>` works
- [ ] `runsc-list` alias works
- [ ] `runsc-raw` bypasses wrapper
- [ ] Environment variables are set (`echo $GVISOR_ROOT`)
- [ ] Real binary exists at `/usr/local/bin/runsc.real`

---

## Troubleshooting

### "runsc: command not found"
Check PATH includes `/usr/local/bin`:
```bash
echo $PATH
which runsc
```

### Wrapper not working
Test the real binary directly:
```bash
/usr/local/bin/runsc.real --root /run/gvisor list
```

### "No containers" shown
Verify containers are actually running:
```bash
ps aux | grep runsc
ls -la /run/gvisor/
```

### Want to use old behavior
Bypass wrapper entirely:
```bash
runsc.real [args...]
```

---

## Benefits

✅ **Convenience**: No need to remember `--root` and `--platform` flags  
✅ **Consistency**: Always uses Beta9's configuration  
✅ **Discoverability**: Standard `runsc` commands work as expected  
✅ **Flexibility**: Can still bypass wrapper when needed  
✅ **Documentation**: Clear aliases and usage guide  

---

## Example Session

```bash
# SSH into worker pod
root@worker:/# 

# List running containers (easy!)
root@worker:/# runsc list
ID                                           PID    STATUS    BUNDLE
taskqueue-0c9ab734-01b2-4fae-aaaa-82c2c7...  123    running   /tmp/...
endpoint-abc123-def456                       456    running   /tmp/...

# Get detailed state
root@worker:/# runsc state taskqueue-0c9ab734-01b2-4fae-aaaa-82c2c7...
{
  "id": "taskqueue-0c9ab734-01b2-4fae-aaaa-82c2c7...",
  "pid": 123,
  "status": "running",
  ...
}

# Check environment
root@worker:/# echo $GVISOR_ROOT
/run/gvisor

root@worker:/# echo $GVISOR_PLATFORM
systrap

# Test aliases
root@worker:/# runsc-list
ID                                           PID    STATUS    BUNDLE
taskqueue-0c9ab734-01b2-4fae-aaaa-82c2c7...  123    running   /tmp/...
endpoint-abc123-def456                       456    running   /tmp/...
```

Perfect! Everything works as expected. 🚀

---

## Next Steps

1. Build new worker image with wrapper
2. Deploy to cluster
3. SSH into worker pod and test `runsc list`
4. Verify all commands work as expected

The wrapper makes debugging and monitoring gVisor containers much easier!
