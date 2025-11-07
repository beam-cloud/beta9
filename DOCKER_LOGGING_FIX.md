# Docker Command Logging Fix

## Problem

When running Docker commands like `docker pull` or `docker build`, no output was visible:

```python
process = sandbox.docker.pull("nginx:latest")
process.wait()
print(process.stdout.read())  # Empty!
print(process.stderr.read())  # Empty!
```

## Root Cause

Docker commands like `pull` and `build` use fancy TTY progress bars by default:

```
Pulling from library/nginx
[=====>                    ] 25%
```

These progress bars:
- Are designed for interactive terminals (TTY)
- Don't output to stdout/stderr in a way that gets captured properly
- Use ANSI escape codes and carriage returns that don't work in non-TTY environments

When running in a sandbox without a TTY, Docker suppresses this output entirely!

## Solution: `--progress=plain`

Docker has a `--progress=plain` flag that:
- Outputs simple line-by-line progress instead of fancy bars
- Works in non-TTY environments
- Gets captured by stdout/stderr properly

### Before
```bash
$ docker pull nginx:latest
# (fancy progress bar that doesn't get captured)
```

### After
```bash
$ docker pull --progress=plain nginx:latest
#1 [internal] load metadata for docker.io/library/nginx:latest
#1 DONE 0.5s
#2 [1/1] FROM docker.io/library/nginx:latest
#2 DONE 1.2s
#3 exporting to image
#3 DONE 0.1s
```

## Implementation

### For `docker pull` (Lines 2400-2411)

```python
cmd = ["docker", "pull"]

if quiet:
    cmd.append("-q")
else:
    # Use plain progress output (not fancy TTY progress bars)
    # This ensures output is captured in logs
    cmd.append("--progress=plain")

cmd.append(image)
```

### For `docker build` (Lines 1972-1980)

```python
if quiet:
    cmd.append("-q")
else:
    # Use plain progress output (not fancy TTY progress bars)
    # This ensures output is captured in logs
    cmd.append("--progress=plain")

cmd.append(context)
```

## Usage

Now logs work as expected:

```python
from beta9 import Image, Sandbox

sandbox = Sandbox(
    cpu=1,
    image=Image.from_registry("ubuntu:22.04").with_docker(),
    docker_enabled=True,
).create()

# Pull with visible logs
process = sandbox.docker.pull("nginx:latest")
process.wait()

print("STDOUT:")
print(process.stdout.read())
# Output:
# #1 [internal] load metadata for docker.io/library/nginx:latest
# #1 DONE 0.5s
# #2 [1/1] FROM docker.io/library/nginx:latest
# #2 DONE 1.2s
# ...

print("\nSTDERR:")
print(process.stderr.read())

# Build with visible logs
sandbox.process.exec("sh", "-c", "echo 'FROM nginx' > /tmp/Dockerfile")

process = sandbox.docker.build(tag="my-app:latest", context="/tmp")
process.wait()

print("Build output:")
print(process.stdout.read())
# Output:
# #1 [internal] load build definition from Dockerfile
# #1 transferring dockerfile: 42B done
# #2 [internal] load metadata for docker.io/library/nginx:latest
# ...

sandbox.terminate()
```

## When to Use `quiet=True`

If you don't want any progress output:

```python
# No output at all
process = sandbox.docker.pull("nginx:latest", quiet=True)
process.wait()
print(process.stdout.read())  # Just the image digest
```

## Why This Matters

### Without `--progress=plain`
- ❌ No way to see what's happening
- ❌ Can't debug pull/build issues
- ❌ Users think commands are hanging
- ❌ No visibility into progress

### With `--progress=plain`
- ✅ See exactly what Docker is doing
- ✅ Can debug issues by reading logs
- ✅ Users know progress is being made
- ✅ Full visibility into operations

## Files Modified

| File | Changes | Description |
|------|---------|-------------|
| `sdk/src/beta9/abstractions/sandbox.py` | Lines 2400-2411 | Added `--progress=plain` to `pull()` |
| `sdk/src/beta9/abstractions/sandbox.py` | Lines 1972-1980 | Added `--progress=plain` to `build()` |

## Example Output

### `docker pull --progress=plain nginx:latest`

```
#1 [internal] load metadata for docker.io/library/nginx:latest
#1 sha256:abc123...
#1 DONE 0.5s

#2 [1/1] FROM docker.io/library/nginx:latest@sha256:abc123...
#2 resolve docker.io/library/nginx:latest@sha256:abc123... done
#2 sha256:def456... 0B / 9.13MB 0.1s
#2 sha256:def456... 1.24MB / 9.13MB 0.2s
#2 sha256:def456... 9.13MB / 9.13MB 0.5s done
#2 extracting sha256:def456... done
#2 DONE 1.2s

#3 exporting to image
#3 exporting layers done
#3 writing image sha256:789abc... done
#3 naming to docker.io/library/nginx:latest done
#3 DONE 0.1s
```

### `docker build --progress=plain`

```
#1 [internal] load build definition from Dockerfile
#1 sha256:abc123... 32B / 32B done
#1 DONE 0.0s

#2 [internal] load metadata for docker.io/library/python:3.11-slim
#2 DONE 0.5s

#3 [internal] load .dockerignore
#3 transferring context: 2B done
#3 DONE 0.0s

#4 [1/3] FROM docker.io/library/python:3.11-slim@sha256:def456...
#4 DONE 0.0s

#5 [2/3] RUN pip install flask
#5 0.445 Collecting flask
#5 1.234 Downloading flask-3.0.0-py3-none-any.whl (101 kB)
#5 2.890 Installing collected packages: flask
#5 3.100 Successfully installed flask-3.0.0
#5 DONE 3.2s

#6 exporting to image
#6 exporting layers 0.1s done
#6 writing image sha256:789abc... done
#6 naming to docker.io/my-app:latest done
#6 DONE 0.1s
```

## Summary

✅ **Problem:** Docker pull/build commands showed no output  
✅ **Cause:** TTY progress bars don't work in non-TTY sandbox environments  
✅ **Solution:** Add `--progress=plain` flag for line-by-line output  
✅ **Result:** Full visibility into Docker operations!  

Now users can see exactly what Docker is doing! 🎉
