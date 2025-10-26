# Clip v2 Registry-Based Approach

## The Fundamental Issue

Clip v2's `CreateFromOCI`/`CreateFromOCIImage` APIs are designed to index images from **remote registries** (Docker Hub, GHCR, private registries), NOT local OCI layout directories on the filesystem.

## Why Local OCI Layouts Don't Work

When we do:
```go
imageRef := "oci-layout:///tmp/ubuntu:24.04"
clip.CreateFromOCI(imageRef, ...)
```

The clip library's image reference parser treats ANY format with a scheme as a registry URL:
- `oci://` → tries to connect to host "oci"
- `oci-layout://` → tries to connect to host "oci-layout"  
- `/tmp/ubuntu` → treats as registry path "tmp/ubuntu"

## How Clip v2 Is Meant to Work

```
Build/Pull Image
    ↓
Push to Registry (docker.io, ghcr.io, private registry)
    ↓
Clip indexes FROM registry
    ↓
Creates small .clip with:
    - TOC (file paths, modes, xattrs)
    - Decompression indexes
    - Registry references (URL + layer digests)
    ↓
At runtime:
    - Mount .clip
    - Fetch layers on-demand from registry via HTTP
```

## Solution Options

### Option A: Registry-Based V2 (True V2 Design)

**For built images:**
```go
// 1. Build with buildah
buildah bud → image

// 2. Push to ACTUAL registry
buildah push image docker://registry.company.com/images/myimage:latest

// 3. Index from registry
clip.CreateFromOCIImage(ctx, CreateFromOCIImageOptions{
    ImageRef: "registry.company.com/images/myimage:latest",
    OutputPath: "myimage.clip",
    AuthConfig: base64EncodedAuth,
})

// 4. The .clip now references layers at registry.company.com
// 5. At runtime, layers are fetched on-demand via HTTPS
```

**Benefits:**
- ✓ True v2 lazy loading
- ✓ No duplicate storage (layers stay in registry)
- ✓ Works as clip v2 is designed
- ✓ Massive performance gains

**Challenges:**
- Need a container registry (can be private)
- Need to manage registry credentials
- Network dependency for layer fetching

### Option B: Hybrid Approach (V1 for Builds, V2 for Pulls)

**For BuildAndArchiveImage:** Keep V1
```go
// Local builds use v1 (extract + archive)
// Small, self-contained .clip files
```

**For PullAndArchiveImage:** Use V2
```go
// When pulling from external registries, use v2
clip.CreateFromOCIImage(ctx, CreateFromOCIImageOptions{
    ImageRef: sourceImage, // Already a registry reference
    ...
})
```

**Benefits:**
- ✓ V2 works for pulls (already from registry)
- ✓ V1 works for local builds
- ✓ No additional infrastructure needed
- ✓ Each method uses optimal approach

**Tradeoffs:**
- Built images still slow (v1)
- Pulled images fast (v2)
- Mixed behavior

### Option C: Local Registry for Builds

Set up a local/internal registry:

```go
// 1. Build image
buildah bud → image

// 2. Push to local registry
buildah push image docker://localhost:5000/myimage:latest

// 3. Index from local registry  
clip.CreateFromOCIImage(ctx, CreateFromOCIImageOptions{
    ImageRef: "localhost:5000/myimage:latest",
    ...
})
```

**Benefits:**
- ✓ V2 works for everything
- ✓ Registry is local/fast
- ✓ Can be ephemeral (no long-term storage needed)

**Challenges:**
- Need to run a local registry
- Additional complexity
- Registry authentication

### Option D: Stay on V1 (Safe Fallback)

```yaml
imageService:
  clipVersion: 1  # Use proven v1 extraction
```

**Benefits:**
- ✓ Works today
- ✓ Self-contained archives
- ✓ No dependencies

**Tradeoffs:**
- Slower builds (full extraction)
- Larger archives (full rootfs)
- Higher storage costs

## Recommended Approach

### Short Term: Option D (V1)
Keep using v1 until registry infrastructure is in place.

### Long Term: Option A or C (Registry-Based V2)

**If you have a registry:**
Use Option A - push builds to registry, index from there.

**If you don't:**
Set up Option C - local registry for build outputs.

## Implementation for Option A (Registry-Based)

```go
func (c *ImageClient) BuildAndArchiveImage(...) error {
    // Build image
    buildah bud...
    
    // Push to registry
    registryRef := fmt.Sprintf("%s/%s:%s", 
        c.config.Registry, request.ImageId, "latest")
    buildah push image docker://${registryRef}
    
    // Create v2 index from registry
    if c.config.ImageService.ClipVersion == 2 {
        archivePath := "/tmp/" + request.ImageId + ".clip.tmp"
        err = clip.CreateFromOCIImage(ctx, CreateFromOCIImageOptions{
            ImageRef: registryRef,  // Registry reference, not local path
            OutputPath: archivePath,
            AuthConfig: c.getRegistryAuth(),
        })
        // This .clip now references layers in the registry
    }
}
```

## Why Clip V2 Works This Way

The design makes sense for the intended use case:
1. **Deduplication**: Multiple images share layers in the registry
2. **No double storage**: Layers aren't copied to S3
3. **Lazy loading**: Only fetch layers actually needed
4. **Registry caching**: Leverage existing registry infrastructure

For LOCAL OCI layouts, you'd need either:
- Clip to add local layout support (new feature)
- Or use the registry-based workflow (as designed)

## Next Steps

1. **Decide on registry approach:**
   - Use existing container registry?
   - Set up local registry?
   - Stay on v1 for now?

2. **If using registry:**
   - Update build flow to push to registry
   - Configure clip with registry credentials
   - Test v2 indexing from registry

3. **If staying on v1:**
   - Set `clipVersion: 1`
   - V1 is proven and works well
   - Revisit v2 when registry is available
