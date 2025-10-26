# Clip v2 OCI Reference Format Troubleshooting

## Current Issue

The clip library is still trying to reach Docker Hub registry instead of reading from the local OCI layout directory.

### Error Pattern
```
GET https://index.docker.io/v2/tmp/ubuntu/manifests/24.04: UNAUTHORIZED
```

This indicates the library is parsing the path as a registry reference, not a local filesystem path.

## What We've Tried

### Attempt 1: Plain path with oci: prefix ❌
```go
ImageRef: "oci:" + ociPath + ":" + imageRef
// Result: "oci:/tmp/ubuntu:24.04"
// Problem: Treated "oci" as hostname, tried DNS lookup
```

### Attempt 2: Absolute path without scheme ❌
```go
ImageRef: absOciPath + ":" + imageRef
// Result: "/tmp/ubuntu:24.04"
// Problem: Still treated as registry path "tmp/ubuntu:24.04"
```

### Attempt 3: oci-layout:// scheme (CURRENT TEST)
```go
ImageRef: fmt.Sprintf("oci-layout://%s:%s", absOciPath, imageRef)
// Result: "oci-layout:///tmp/ubuntu:24.04"
// Status: Testing...
```

## OCI Layout Directory Structure

When skopeo does `oci:ubuntu:24.04`, it creates:
```
/tmp/ubuntu/
├── index.json          (contains tag → manifest mapping)
├── oci-layout          (OCI layout version file)
└── blobs/
    └── sha256/
        ├── <manifest-hash>
        ├── <config-hash>
        └── <layer-hashes>...
```

The tag `24.04` is stored **inside** `index.json`, not in the filesystem path.

## Possible Solutions to Test

### Option A: Use index descriptor directly
Instead of specifying a tag, read from the index.json and use the descriptor:
```go
// Just the directory path, let clip read index.json
ImageRef: absOciPath
```

### Option B: Different scheme formats
Try other standard OCI reference schemes:
```go
"dir://" + absOciPath + ":" + imageRef
"file://" + absOciPath + ":" + imageRef  
"oci-archive://" + absOciPath + ":" + imageRef
```

### Option C: Use different clip API
Maybe `CreateFromOCI` isn't meant for local OCI layouts. Check if there's:
- `CreateFromOCILayout()`
- `CreateFromDirectory()`
- A different method specifically for local filesystems

### Option D: Pre-process the OCI layout
Convert the OCI layout to a format clip expects:
- Read the manifest/index directly
- Extract layer information
- Build the archive metadata ourselves

## How to Debug

### 1. Check Clip Library Tests
Look at the clip repository tests to see examples of:
- How they test local OCI layout indexing
- What ImageRef formats they use
- Any special handling for filesystem vs registry

### 2. Enable Maximum Verbosity
```go
Verbose: true  // Already enabled
```

Look for clip log output showing:
- How it parses the ImageRef
- What transport it selects (registry vs filesystem)
- Where it tries to fetch from

### 3. Check OCI Layout Files
Verify the OCI layout is valid:
```bash
# Check OCI layout marker
cat /tmp/ubuntu/oci-layout

# Check index.json structure
cat /tmp/ubuntu/index.json | jq .

# Verify blobs exist
ls -la /tmp/ubuntu/blobs/sha256/
```

### 4. Test with go-containerregistry directly
Try using the underlying library directly:
```go
import "github.com/google/go-containerregistry/pkg/v1/layout"

path, err := layout.FromPath("/tmp/ubuntu")
index, err := path.ImageIndex()
// See if this works...
```

## Questions for Clip Library Maintainers

1. **What is the correct ImageRef format for local OCI layouts?**
   - Should we use `oci-layout://`?
   - Or a different scheme?
   - Or no scheme at all?

2. **Is `ClipArchiver.CreateFromOCI()` meant to work with local filesystem OCI layouts?**
   - Or is it only for remote registries?
   - If not, what method should we use?

3. **Are there any examples of indexing from local OCI layouts?**
   - Test files?
   - Documentation?
   - Example code?

## Temporary Workarounds

### Workaround 1: Stay on V1 (Current Fallback)
```yaml
imageService:
  clipVersion: 1  # Use legacy extraction
```

### Workaround 2: Two-step process
1. Use v1 to extract and create archive
2. Later convert v1 archives to v2 format
3. Migrate gradually

### Workaround 3: Direct manifest reading
Bypass the image reference parsing:
1. Read OCI index.json directly
2. Parse manifest and layer information
3. Call clip with explicit layer references
4. Skip the ImageRef parsing entirely

## Next Steps

1. **Test with `oci-layout://` scheme** (current code)
   - Deploy and check verbose logs
   - See if it recognizes the scheme

2. **If still fails, try removing tag from reference**
   ```go
   ImageRef: fmt.Sprintf("oci-layout://%s", absOciPath)
   ```

3. **Check clip library source code**
   - Look at how ImageRef is parsed
   - Find the transport selection logic
   - Understand what schemes are supported

4. **Consider contributing to clip**
   - If local OCI layout support is missing
   - Add it or document the correct approach
   - Share findings with team

## Status

**Current**: Testing `oci-layout://` scheme with verbose logging  
**Verbose**: Enabled to see clip's internal behavior  
**Fallback**: V1 still works if v2 fails  

The fix is safe to deploy - worst case it falls back to v1 and we get detailed logs to debug further.
