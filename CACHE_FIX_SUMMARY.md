# OCI Indexing Cache Lookup Order Fix - Summary

## Problem Statement

When running in OCI indexing mode with the CLIP custom FUSE filesystem, the cache lookup order was incorrect:

1. **Wrong Order**: Disk cache → Content cache → OCI decompression
2. **Expected Order**: Content cache → Disk cache → OCI decompression

This caused:
- Unnecessary disk I/O when content was already available in distributed cache
- Missed opportunities to leverage content-addressed storage
- Lookups failing with "content not found" for hashes that should have been in the content cache

## Root Causes Identified

### 1. ReadFile Priority Issue
In `pkg/storage/oci.go`, the `ReadFile` function was checking disk cache before content cache for range reads:
```go
// OLD ORDER (WRONG)
1. Try disk cache first (fastest - local range read)
2. Try ContentCache (range read) 
3. Decompress from OCI
```

### 2. Missing Full Layer Check in ensureLayerCached
The `ensureLayerCached` function was NOT checking ContentCache for the full decompressed layer before attempting OCI decompression:
```go
// OLD FLOW (INCOMPLETE)
1. Check disk cache
2. If miss → Decompress from OCI directly (MISSING ContentCache check!)
```

### 3. Incorrect Routing Keys
Content cache operations were not consistently using routing keys, which could cause cache misses.

## Changes Made

### File: `/home/ubuntu/go/pkg/mod/github.com/beam-cloud/clip@v0.0.0-20251029193105-898214dda2cd/pkg/storage/oci.go`

#### Change 1: Fixed ReadFile Cache Priority
```go
// NEW ORDER (CORRECT)
// 1. Check ContentCache (range read) - fast, network but only what we need, and most reliable
// 2. Check disk cache (range read) - fastest if available, local
// 3. Decompress from OCI - slow, but cache entire layer for future reads
```

**Lines Changed**: 130-183

**Rationale**: ContentCache is the distributed cache shared across all workers and is the primary source of truth. It should be checked first for range reads before falling back to local disk cache.

#### Change 2: Added Full Layer Load from ContentCache
New function `tryLoadFullLayerFromContentCache` (lines 418-449):
- Attempts to load entire decompressed layer from ContentCache using offset=0, length=0
- Writes atomically to disk for local caching
- Returns error if not found, allowing fallback to OCI decompression

#### Change 3: Updated ensureLayerCached Priority
```go
// NEW FLOW (COMPLETE)
1. Try ContentCache for full decompressed layer (most reliable, distributed)
2. Check disk cache (local, fast)
3. Check for in-progress decompression by other goroutines
4. Decompress from OCI registry (slowest, last resort)
```

**Lines Changed**: 185-242

**Rationale**: Before decompressing from OCI (which is slow and expensive), we should check if another worker has already decompressed and cached this layer in ContentCache.

#### Change 4: Fixed Routing Keys
Updated all ContentCache operations to use routing keys consistently:
- `tryRangeReadFromContentCache` (line 460)
- `storeDecompressedInRemoteCache` (lines 497-499)
- `tryLoadFullLayerFromContentCache` (lines 425-427)

**Rationale**: Routing keys help the distributed cache route requests to the correct nodes that have the data.

#### Change 5: Enhanced Content Hash Logging
Added debug logging in `getContentHash` (lines 262-264):
```go
log.Debug().Str("digest", digest).Str("cache_key", hash).Msg("extracted content hash for caching")
```

**Rationale**: Helps debug cache key issues by logging the exact keys being used.

#### Change 6: Updated Documentation
Updated comments in `decompressAndCacheLayer` (lines 283-287) to reflect the new cache checking order.

### File: `/home/ubuntu/go/pkg/mod/github.com/beam-cloud/clip@v0.0.0-20251029193105-898214dda2cd/pkg/storage/oci_test.go`

#### Change 1: Fixed Mock Cache for Full Layer Reads
Updated `mockCache.GetContent` (lines 56-59) to properly handle full layer reads (offset=0, length=0):
```go
// Full read: offset=0, length=0 means return entire content
if offset == 0 && length == 0 {
    return fullData, nil
}
```

**Rationale**: The mock was incorrectly returning empty data for full layer reads, causing tests to fail.

#### Change 2: Added Cache Priority Tests
New test `TestCachePriority_ContentCacheBeforeDisk` (lines 866-932):
- Verifies ContentCache is checked before disk cache
- Ensures layer is NOT written to disk when ContentCache provides it via range read
- Validates cache hit counts

New test `TestCachePriority_FullLayerFromContentCache` (lines 934-1052):
- Verifies full layer load from ContentCache works correctly
- Ensures OCI registry is NOT contacted when ContentCache has the layer
- Validates ContentCache → Disk → OCI priority

## Performance Improvements

1. **Reduced OCI Registry Load**: Layers cached in ContentCache are loaded directly, avoiding expensive registry fetches
2. **Better Cache Utilization**: Distributed cache (ContentCache) is prioritized over local disk cache
3. **Cross-Worker Efficiency**: Workers can share decompressed layers via ContentCache instead of each decompressing independently
4. **Proper Content-Addressing**: Using correct hash keys ensures cache hits across different images sharing the same layers

## Testing

All existing tests pass, plus two new tests specifically validate the cache priority:

```bash
cd /home/ubuntu/go/pkg/mod/github.com/beam-cloud/clip@v0.0.0-20251029193105-898214dda2cd
go test -v ./pkg/storage -run "TestCachePriority"
```

Results:
- ✅ TestCachePriority_ContentCacheBeforeDisk - PASS
- ✅ TestCachePriority_FullLayerFromContentCache - PASS

## Cache Lookup Flow (After Fix)

### Scenario 1: Range Read from File
```
ReadFile called
  ↓
1. Try ContentCache.GetContent(hash, offset, length) with routing key
   ↓ HIT → Return data ✓
   ↓ MISS
   ↓
2. Try disk cache range read
   ↓ HIT → Return data ✓
   ↓ MISS
   ↓
3. Call ensureLayerCached
   ↓
   a. Try ContentCache.GetContent(hash, 0, 0) for full layer
      ↓ HIT → Write to disk → Read range from disk ✓
      ↓ MISS
      ↓
   b. Check disk cache
      ↓ HIT → Read range from disk ✓
      ↓ MISS
      ↓
   c. Decompress from OCI registry
      ↓ Write to disk
      ↓ Async store in ContentCache for other workers
      ↓ Read range from disk ✓
```

### Scenario 2: Full Layer Needed (e.g., large file access)
```
ensureLayerCached called
  ↓
1. Try ContentCache.GetContent(hash, 0, 0) for full layer with routing key
   ↓ HIT → Write to disk atomically → Return disk path ✓
   ↓ MISS
   ↓
2. Check disk cache
   ↓ HIT → Return disk path ✓
   ↓ MISS
   ↓
3. Decompress from OCI registry
   ↓ Write to disk atomically
   ↓ Async store in ContentCache (with routing key) for other workers
   ↓ Return disk path ✓
```

## Verification

To verify the fix is working correctly, check logs for:

1. **ContentCache hits for range reads**:
   ```
   DBG ContentCache range read hit - digest=sha256:... offset=... length=...
   ```

2. **ContentCache hits for full layer loads**:
   ```
   INF loaded full layer from ContentCache - digest=sha256:... bytes=...
   ```

3. **Proper cache key usage**:
   ```
   DBG extracted content hash for caching - digest=sha256:abc123... cache_key=abc123...
   ```

4. **Reduced "content not found" errors**: The hashes in "content not found" should now be valid layer digest hashes (hex part), and these should decrease significantly as ContentCache is properly utilized.

## Compatibility

These changes are **backward compatible**:
- If ContentCache is not configured (nil), behavior falls back to disk cache → OCI decompression
- Existing v1 (S3 data-carrying) images are unaffected
- Only v2 (OCI index-only) images benefit from these optimizations

## Related Files Modified

1. `/home/ubuntu/go/pkg/mod/github.com/beam-cloud/clip@v0.0.0-20251029193105-898214dda2cd/pkg/storage/oci.go` - Main implementation
2. `/home/ubuntu/go/pkg/mod/github.com/beam-cloud/clip@v0.0.0-20251029193105-898214dda2cd/pkg/storage/oci_test.go` - Tests

## Next Steps

1. Rebuild beta9 workspace to pick up changes: `cd /workspace && go build ./...`
2. Deploy to test environment
3. Monitor logs for ContentCache hit rates
4. Verify "content not found" errors decrease
5. Measure performance improvements in OCI indexing mode
