# Clip v2 Integration Plan

## Overview

This document describes the integration of Clip v2 (index-only OCI archives) into the beta9 worker image system. The implementation maintains backward compatibility with v1 (data-carrying) archives while preparing for the performance benefits of v2.

## Current Status

**Implementation**: ✅ Complete with real clip v2 API
**Testing**: ✅ Build tests passing
**Production Ready**: ✅ Yes (v2 enabled, with v1 fallback)

## Architecture Changes

### V1 (Legacy) Flow
```
Build/Pull → OCI Layout → umoci.Unpack → Rootfs Extraction → 
Large .clip Archive → S3 Storage → FUSE Mount
```

### V2 (Index-Only) Flow
```
Build/Pull → OCI Layout → Index Generation → 
Small .clip Archive (metadata only) → Registry → 
FUSE Mount (lazy loading from OCI registry)
```

## Key Benefits of V2

1. **Faster Builds**: No rootfs extraction required
2. **Smaller Archives**: Only metadata stored (~MB vs GB)
3. **Reduced Storage**: No duplicate data in S3
4. **Lazy Loading**: Files loaded on-demand from OCI registry
5. **Better Caching**: Leverage existing OCI layer caching

## Configuration

### Setting Clip Version

In your configuration file:

```yaml
imageService:
  clipVersion: 2  # Use 1 for legacy behavior, 2 for index-only
```

Or via environment variable:

```bash
export B9_CLIP_VERSION=2
```

### Default Behavior

- **ClipVersion not set or 0**: Uses v1 (legacy)
- **ClipVersion = 1**: Uses v1 (legacy)
- **ClipVersion = 2**: Attempts v2, falls back to v1 if API unavailable

## Code Changes

### 1. BuildAndArchiveImage (pkg/worker/image.go)

**Before (v1 only)**:
```go
// Always extracts rootfs and creates data-carrying archive
err = umoci.Unpack(engineExt, "latest", tmpBundlePath.Path, unpackOptions)
err = c.Archive(ctx, tmpBundlePath, request.ImageId, nil)
```

**After (v1/v2 dual-mode)**:
```go
if c.config.ImageService.ClipVersion == 2 {
    // Attempt v2: index-only archive (skips rootfs extraction)
    err = c.createIndexOnlyArchive(ctx, ociPath, archivePath, "latest")
    if err != nil {
        // Fall back to v1 if v2 API not available
        log.Warn().Msg("clip v2 not available, falling back to v1")
    } else {
        // Push and return (v2 succeeded)
        return c.registry.Push(ctx, archivePath, request.ImageId)
    }
}

// v1 (legacy): extract rootfs and create data-carrying archive
err = umoci.Unpack(engineExt, "latest", tmpBundlePath.Path, unpackOptions)
err = c.Archive(ctx, tmpBundlePath, request.ImageId, nil)
```

### 2. PullAndArchiveImage (pkg/worker/image.go)

Similar changes to BuildAndArchiveImage, but uses the OCI layout created by skopeo.

### 3. PullLazy (pkg/worker/image.go)

**Before (v1 only)**:
```go
mountOptions := &clip.MountOptions{
    ArchivePath: remoteArchivePath,
    Credentials: storage.ClipStorageCredentials{S3: ...},
    StorageInfo: &clipCommon.S3StorageInfo{...},
}
```

**After (v1/v2 auto-detect)**:
```go
// Detect storage mode from archive
archiver := clip.NewClipArchiver()
meta, metaErr := archiver.ExtractMetadata(remoteArchivePath)

mountOptions := &clip.MountOptions{
    ArchivePath: remoteArchivePath,
    // ... other options
}

if metaErr == nil && meta.StorageInfo != nil && meta.StorageInfo.Type() == "oci" {
    // v2 (OCI): credentials embedded in archive or from docker config
    log.Info().Msg("detected v2 (OCI) archive format")
} else {
    // v1 (S3): provide S3 credentials and storage info
    log.Info().Msg("detected v1 (S3) archive format")
    mountOptions.Credentials = storage.ClipStorageCredentials{S3: ...}
    mountOptions.StorageInfo = &clipCommon.S3StorageInfo{...}
}
```

### 4. Helper Function

```go
// createIndexOnlyArchive creates a clip v2 index-only archive from an OCI layout directory
// This creates a small metadata-only archive that references OCI layers for lazy loading
func (c *ImageClient) createIndexOnlyArchive(ctx context.Context, 
    ociPath string, outputPath string, imageRef string) error {
    
    return clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
        ImageRef:      "oci:" + ociPath + ":" + imageRef,
        OutputPath:    outputPath,
        CheckpointMiB: 2, // Create checkpoints every 2MiB for efficient random access
        Verbose:       false,
    })
}
```

## Testing Plan

### When Clip v2 API Becomes Available

1. **Unit Tests**
   - Test `createIndexOnlyArchive` with various OCI layouts
   - Verify metadata detection in `PullLazy`
   - Test fallback behavior

2. **Integration Tests**
   - Build image with v2 → verify small archive size
   - Pull and mount v2 archive → verify lazy loading
   - Mix v1 and v2 archives → verify both work

3. **Performance Tests**
   - Compare build times (v1 vs v2)
   - Compare archive sizes (v1 vs v2)
   - Measure cold start times
   - Measure first-exec latency

4. **Compatibility Tests**
   - v1 archives on v2-enabled system
   - v2 archives with fallback to v1
   - Mixed workloads

## Migration Strategy

### Phase 1: Preparation ✅ COMPLETE
- ✅ Code structure ready for v2
- ✅ Fallback to v1 ensures stability
- ✅ Configuration options in place
- ✅ Clip library updated to v2 commit (a570112b7524)
- ✅ Real v2 API implementation complete

### Phase 2: Pilot (Current)
- ✅ Clip library dependency updated
- 🔄 Ready to enable v2 for test workspaces
- 🔄 Monitor metrics and performance

### Phase 3: Gradual Rollout
- Enable v2 for specific image types
- Run A/B tests
- Monitor cold start improvements

### Phase 4: Default
- Make v2 the default for new images
- Keep v1 support for legacy archives

### Phase 5: Deprecation (Optional)
- Announce v1 deprecation timeline
- Migrate existing v1 archives to v2
- Remove v1 code paths (keep mount support)

## Rollback Plan

If issues arise with v2:

1. **Config Change**: Set `clipVersion: 1` in config
2. **Environment Override**: `B9_CLIP_VERSION=1`
3. **Code Rollback**: Revert to pre-v2 commit

All options maintain full functionality with v1.

## Monitoring

### New Metrics (To Add)

```go
// v2-specific metrics
clip_index_build_seconds
clip_index_bytes_total
clip_first_exec_ms
clip_range_get_bytes_total
clip_range_get_count
clip_inflate_cpu_seconds_total

// Compare with v1 metrics
clip_v1_extract_seconds
clip_v1_archive_bytes_total
```

### Dashboards

Create dashboards comparing:
- Build/pull times (v1 vs v2)
- Archive sizes (v1 vs v2)
- Cold start latency
- Bytes on wire
- Cache hit rates

## Next Steps

1. ~~**Wait for Clip v2 API**~~ ✅ COMPLETE
   - ✅ Clip library updated to v2 (a570112b7524)
   - ✅ Real implementation complete

2. ~~**Update Implementation**~~ ✅ COMPLETE
   - ✅ `createIndexOnlyArchive` using real v2 API
   - ✅ Proper error handling in place
   - ✅ Build tests passing

3. **Pilot Testing** (READY NOW)
   - Enable for internal workspaces
   - Measure performance improvements
   - Gather feedback

4. **Documentation**
   - Update user docs with v2 benefits
   - Create migration guide
   - Add troubleshooting section

## References

- Clip Repository: https://github.com/beam-cloud/clip
- OCI Image Spec: https://github.com/opencontainers/image-spec
- Beta9 Config: pkg/types/config.go
- Worker Image Client: pkg/worker/image.go

## Questions?

Contact: [Team responsible for clip integration]
