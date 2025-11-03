# Build Registry Credentials - Final Architecture

## What Changed

### The Problem
1. `BuildRegistryCreds` was in `BuildOptions`, which is build-specific
2. Credential generation was in `pkg/abstractions/image/build.go`, only for build containers
3. Runtime containers never got build registry credentials for CLIP layer mounting

### The Solution  
Moved credential injection to the **scheduler** so ALL containers (build + runtime) get credentials.

## Final Architecture

```
┌────────────────────────────────────────────────────────────────┐
│ SCHEDULER (pkg/scheduler/scheduler.go)                        │
│                                                                │
│ scheduleRequest() {                                            │
│   ...                                                          │
│   attachImageCredentials(request)        // Runtime secrets   │
│   attachBuildRegistryCredentials(request) // Build registry!  │
│   ...                                                          │
│   workerRepo.ScheduleContainerRequest(worker, request)        │
│ }                                                              │
│                                                                │
│ attachBuildRegistryCredentials() {                             │
│   1. Read config.ImageService.BuildRegistryCredentials        │
│   2. Generate fresh token via GetRegistryTokenForImage()      │
│   3. Set request.BuildRegistryCreds = token                   │
│ }                                                              │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ WORKER (pkg/worker/image.go)                                  │
│                                                                │
│ BuildAndArchiveImage() {                                      │
│   buildah push --creds=request.BuildRegistryCreds ...         │
│   v2ImageRefs.Set(imageId, pushedImageRef)                   │
│   createOCIImageWithProgress(request)  // CLIP indexing      │
│ }                                                              │
│                                                                │
│ createOCIImageWithProgress() {                                 │
│   credProvider := getCredentialProviderForImage(request)     │
│   // Uses request.BuildRegistryCreds for indexing            │
│ }                                                              │
│                                                                │
│ PullLazy() {  // Runtime mounting                             │
│   credProvider := getCredentialProviderForImage(request)     │
│   // Uses request.BuildRegistryCreds for layer pulls         │
│ }                                                              │
│                                                                │
│ getCredentialProviderForImage() {                             │
│   if image from build registry:                               │
│     return request.BuildRegistryCreds  // ✅ SAME TOKEN!     │
│ }                                                              │
└────────────────────────────────────────────────────────────────┘
```

## Key Changes

### 1. Proto (`pkg/types/types.proto`)
```protobuf
message BuildOptions {
  // NO BuildRegistryCreds here!
}

message ContainerRequest {
  ...
  string image_credentials = 26;
  string build_registry_creds = 27;  // Top-level!
}
```

### 2. Scheduler (`pkg/scheduler/scheduler.go`)
- **Added**: `attachBuildRegistryCredentials()` function
- **Added**: Call to `attachBuildRegistryCredentials()` in `scheduleRequest()`
- **Added**: `config` field to `Scheduler` struct
- **Added**: Import for `registry` package

This runs for **EVERY** container request, not just build containers.

### 3. Build (`pkg/abstractions/image/build.go`)
- **Removed**: `generateBuildRegistryCredentials()` function
- **Removed**: Credential generation logic
- **Removed**: `reg` import
- **Added**: Comment explaining credentials come from scheduler

### 4. Worker (`pkg/worker/image.go`)
- **Updated**: `getCredentialProviderForImage()` to use `request.BuildRegistryCreds`
- **Updated**: `BuildAndArchiveImage()` to use `request.BuildRegistryCreds`
- **No other changes** - the logic was already correct, just reading from the right place now

## Why This Is Better

### Before (Wrong):
- Build containers: Got credentials from `build.go:generateBuildRegistryCredentials()`
- Runtime containers: No build registry credentials ❌
- Inconsistent: Different code paths for build vs runtime

### After (Correct):
- ALL containers: Get credentials from `scheduler:attachBuildRegistryCredentials()`
- Single source of truth ✅
- Consistent: Same code path for all containers ✅
- Proper separation: Scheduler injects, worker consumes ✅

## Credential Flow

```
1. Config: Store long-term creds (AWS keys, etc.)
   ↓
2. Scheduler: Generate fresh token per request
   ↓
3. ContainerRequest.BuildRegistryCreds = token
   ↓
4. Worker: Use for push + CLIP indexing
   ↓
5. Runtime: Use for CLIP layer mounting
```

## Testing

All tests pass:
```bash
go test ./pkg/abstractions/image/... ./pkg/scheduler/... ./pkg/worker/...
```

Build succeeds:
```bash
go build ./...
```

## Files Modified

1. `pkg/types/types.proto` - Moved field to ContainerRequest
2. `proto/types.pb.go` - Updated generated code
3. `pkg/types/scheduler.go` - Updated Go types
4. `pkg/scheduler/scheduler.go` - **Added credential injection logic**
5. `pkg/abstractions/image/build.go` - **Removed credential generation**
6. `pkg/worker/image.go` - Updated to use top-level field
7. `pkg/worker/image_credentials_flow_test.go` - Updated tests

## Summary

**Before**: Credentials generated in build-specific code, only for build containers.

**After**: Credentials generated in scheduler, for ALL containers.

This ensures runtime containers get the same credentials used to push the image, enabling CLIP V2 to pull layers on-demand from the build registry.
