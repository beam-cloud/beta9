# Image Build Optimization Summary

## Problem Statement

The build process was exhibiting two performance issues:

1. **Inefficient CLIP Indexing**: Even though images were built locally with buildah, the CLIP indexing step was pulling from the remote registry, causing unnecessary network traffic and significantly increasing indexing time.

2. **Duplicate Image Pulls**: The same image blobs were being copied multiple times during the build process - once locally, then again when pushing to remote, and yet again during indexing.

## Root Cause

In the original flow (`pkg/worker/image.go:BuildAndArchiveImage`):
1. Build image with buildah (local)
2. Push to local OCI layout (local)
3. Tag for build registry (local)
4. **Push to remote build registry** (uploads all blobs)
5. **Index from remote build registry** (downloads all blobs again!)
6. Upload CLIP archive

The indexing step was using the remote registry reference, causing CLIP to pull the image that was just pushed, resulting in duplicate network transfers.

## Solution

The optimization uses CLIP's **`StorageImageRef`** feature to:
1. **Index from local OCI layout** (fast, no network)
2. **Store remote registry reference** in metadata (for runtime)

### Optimized Flow

1. Build image with buildah (local)
2. Push to local OCI layout (local)
3. Tag for build registry (local)
4. **Index from local OCI layout with StorageImageRef** (reads locally, embeds remote ref)
5. **Push to remote build registry** (uploads all blobs once)
6. Upload CLIP archive

### Key Changes

#### 1. New Function: `createOCIImageWithProgressAndStorageRef` (lines 783-875)
- **Purpose**: Wraps CLIP's `CreateFromOCIImage` with `StorageImageRef` support
- **Key Feature**: Accepts separate `sourceRef` (for reading) and `storageRef` (for metadata)
- **Result**: Indexes locally but embeds remote reference for runtime

#### 2. `BuildAndArchiveImage` (lines 1003-1018)
- **Changed**: Calls new function with:
  - `sourceRef`: `oci:ociPath:latest` (local OCI layout)
  - `storageRef`: `imageTag` (remote registry)
- **Result**: Fast local indexing with correct remote reference in metadata

#### 3. `cacheOCIMetadata` (lines 393-435)
- **Simplified**: No complex reconstruction logic needed
- **Reason**: CLIP metadata now contains correct remote reference via `StorageImageRef`
- **Result**: Clean, straightforward metadata handling

## Benefits

1. **Faster Builds**: Indexing now reads from local filesystem instead of pulling from remote registry
   - Eliminates one complete image download during build
   - Reduces build time proportional to image size
   - **Example**: For a 2GB image, saves ~2-5 minutes depending on network speed

2. **Reduced Network Traffic**: Only one push to remote registry instead of push + pull
   - Cuts network bandwidth usage in half for the indexing phase
   - Reduces load on remote registry
   - **Example**: For a 2GB image, saves ~2GB of network transfer

3. **Correct Runtime Behavior**: CLIP metadata contains correct remote reference
   - Runtime workers can fetch layers from remote registry
   - Works consistently across all workers in the cluster
   - No need for complex reconstruction logic

4. **Simpler Code**: Using CLIP's native `StorageImageRef` feature
   - Cleaner than metadata manipulation approaches
   - More maintainable and less error-prone
   - Follows CLIP's intended usage pattern

## Technical Details

### CLIP StorageImageRef Feature

CLIP's `StorageImageRef` allows separate source and storage references:

```go
clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
    ImageRef:        "oci:/local/path:latest",        // Read from here (fast)
    StorageImageRef: "registry.example.com/app:v1",   // Store this in metadata
    OutputPath:      outputPath,
    // ...
})
```

This is exactly what we need:
- **ImageRef**: Local OCI layout (no network required for indexing)
- **StorageImageRef**: Remote registry (embedded in metadata for runtime)

### How It Works

1. **Build Time** (worker that builds the image):
   - Buildah creates OCI layout locally
   - CLIP indexes from local OCI layout (fast)
   - CLIP embeds remote registry reference in metadata
   - Image pushed to remote registry

2. **Runtime** (any worker running the image):
   - CLIP archive downloaded
   - Metadata contains correct remote registry reference
   - CLIP mounts filesystem and fetches layers on-demand from remote
   - Works seamlessly across all workers

### Backwards Compatibility

- V1 builds (non-CLIP v2) are unchanged
- `createOCIImageWithProgress` still exists for non-build scenarios
- Existing CLIP archives continue to work

## Testing Recommendations

1. **Build Performance**: Measure build time before/after, especially for large images
   ```bash
   # Look for this log message:
   # "indexing from local OCI layout with remote storage reference"
   ```

2. **Cross-Worker**: Verify that images built on worker A can run on worker B
   ```bash
   # Check that metadata contains correct remote reference:
   # "cached source image reference from metadata"
   ```

3. **Registry Types**: Test with different registry types (ECR, GCR, Docker Hub, localhost)

4. **Workspace Isolation**: Verify workspace-specific tags work correctly
   ```bash
   # Tag format should be: registry/repo:workspace_id-image_id
   ```

## Monitoring

Watch for these log messages:

**Build Phase:**
- ✅ `"indexing from local OCI layout with remote storage reference"` 
  - Indicates optimization is active
  - Shows both source_ref (local) and storage_ref (remote)

**Runtime Phase:**
- ✅ `"cached source image reference from metadata"`
  - Indicates metadata correctly contains remote reference
  - Runtime worker can fetch layers

**Error Cases:**
- ⚠️ If you see multiple "Copying blob" during indexing → optimization not working
- ⚠️ If runtime fails with "layer not found" → metadata has wrong reference

## Performance Impact

Expected improvements for a typical 2GB image:

| Phase | Before | After | Savings |
|-------|--------|-------|---------|
| Build (local) | ~2 min | ~2 min | 0 |
| Push to registry | ~3 min | ~3 min | 0 |
| **Index** | **~5 min** | **~30 sec** | **~4.5 min** |
| Upload CLIP | ~30 sec | ~30 sec | 0 |
| **Total** | **~10.5 min** | **~6 min** | **~43% faster** |

Network savings: ~2GB (no index pull from remote)

## Future Improvements

1. ~~Use CLIP's StorageImageRef feature~~ ✅ **DONE**
2. Add metrics to track indexing time savings
3. Consider parallel push and index operations
