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

The optimization changes the flow to:

1. Build image with buildah (local)
2. Push to local OCI layout (local)
3. Tag for build registry (local)
4. **Index from local OCI layout** (reads locally, fast!)
5. **Push to remote build registry** (uploads all blobs once)
6. Cache remote registry reference for runtime
7. Upload CLIP archive

### Key Changes

#### 1. `BuildAndArchiveImage` (lines 973-1049)
- **Changed**: Index from `oci:ociPath:latest` (local OCI layout) instead of remote registry
- **Added**: Explicit caching of remote registry reference in `v2ImageRefs`
- **Result**: Indexing now reads from local filesystem instead of pulling from remote

#### 2. `cacheOCIMetadata` (lines 393-478)
- **Changed**: Don't overwrite existing cached references
- **Added**: Detect local/incomplete references in CLIP metadata
- **Added**: Reconstruct correct remote registry reference from config when needed
- **Result**: Runtime workers can correctly resolve the remote registry even when CLIP metadata contains local references

#### 3. `processPulledArchive` (line 347)
- **Changed**: Accept `workspaceId` parameter to enable proper registry reference reconstruction
- **Result**: Runtime can reconstruct the correct registry tag format (`workspace_id-image_id`)

## Benefits

1. **Faster Builds**: Indexing now reads from local filesystem instead of pulling from remote registry
   - Eliminates one complete image download during build
   - Reduces build time proportional to image size

2. **Reduced Network Traffic**: Only one push to remote registry instead of push + pull
   - Cuts network bandwidth usage in half for the indexing phase
   - Reduces load on remote registry

3. **Cross-Worker Compatibility**: Runtime workers can correctly resolve registry references
   - CLIP metadata may have local references, but runtime reconstructs remote reference
   - Works consistently across all workers in the cluster

## Technical Details

### CLIP Metadata Handling

When indexing from a local OCI layout (`oci:path:latest`), the CLIP metadata will contain a local reference without full registry information. To handle this:

1. **Build Time**: We explicitly cache the remote registry reference in `v2ImageRefs`
2. **Runtime**: When pulling on a different worker:
   - Check if cached reference exists (from build worker) → use it
   - If not cached, check CLIP metadata
   - If metadata has local reference, reconstruct from config using `buildRegistry/buildRepository:workspaceId-imageId` format

### Backwards Compatibility

- V1 builds (non-CLIP v2) are unchanged
- Images built with old code will still work with new code (metadata-based resolution)
- Images built with new code will work with old code (cached reference fallback)

## Testing Recommendations

1. **Build Performance**: Measure build time before/after, especially for large images
2. **Cross-Worker**: Verify that images built on worker A can run on worker B
3. **Registry Types**: Test with different registry types (ECR, GCR, Docker Hub, localhost)
4. **Workspace Isolation**: Verify workspace-specific tags work correctly

## Monitoring

Watch for these log messages:
- `"indexing from local OCI layout to avoid remote pull"` - indicates optimization is active
- `"reconstructed build registry reference from config"` - indicates runtime fallback
- `"keeping existing cached reference"` - indicates cached reference preserved

## Future Improvements

1. Persist `v2ImageRefs` cache to Redis for cross-worker persistence
2. Add metrics to track indexing time savings
3. Consider using `containers-storage:` transport to avoid intermediate OCI layout
