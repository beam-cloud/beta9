# BuildRegistryCreds Refactoring - Summary

## What Changed

### Problem
`BuildRegistryCreds` was incorrectly placed in `BuildOptions` struct, even though it's needed for both build-time operations (push) AND runtime operations (CLIP layer mounting). This was architecturally inconsistent with other runtime credentials like `ImageCredentials`.

### Solution
Moved `BuildRegistryCreds` from `BuildOptions` to top-level `ContainerRequest`.

## Files Modified

### 1. Proto Definition
**File**: `pkg/types/types.proto`
- Removed `build_registry_creds` from `BuildOptions` message
- Added `build_registry_creds` to `ContainerRequest` message (field 27)

### 2. Generated Proto
**File**: `proto/types.pb.go`
- Removed `BuildRegistryCreds` field from `BuildOptions` struct
- Added `BuildRegistryCreds` field to `ContainerRequest` struct  
- Removed `GetBuildRegistryCreds()` from `BuildOptions`
- Added `GetBuildRegistryCreds()` to `ContainerRequest`

### 3. Scheduler Types
**File**: `pkg/types/scheduler.go`
- Removed `BuildRegistryCreds` from `BuildOptions` struct
- Added `BuildRegistryCreds` to `ContainerRequest` struct

### 4. Scheduler/Build
**File**: `pkg/abstractions/image/build.go`
- Updated `generateContainerRequest()` to place credentials at top-level:
```go
req := &types.ContainerRequest{
    BuildOptions: types.BuildOptions{
        SourceImage:      sourceImagePtr,
        SourceImageCreds: b.opts.BaseImageCreds,
        // ... other build-specific fields
    },
    BuildRegistryCreds: buildRegistryCreds,  // TOP-LEVEL!
    // ... other runtime fields
}
```

### 5. Worker
**File**: `pkg/worker/image.go`
- Updated `getCredentialProviderForImage()` to read from `request.BuildRegistryCreds` (was `request.BuildOptions.BuildRegistryCreds`)
- Updated `BuildAndArchiveImage()` to use `request.BuildRegistryCreds` for buildah push
- Updated comments to reflect the architectural change

### 6. Tests
**Files**: 
- `pkg/abstractions/image/build_registry_creds_test.go`
- `pkg/worker/image_credentials_flow_test.go`

Updated all test cases to use top-level `BuildRegistryCreds`.

## Architecture After Refactoring

```go
ContainerRequest {
    // Runtime-level credentials (available throughout lifecycle)
    ImageCredentials   string  // For runtime-only image access
    BuildRegistryCreds string  // For build registry access (push + runtime)
    
    // Build-specific options
    BuildOptions {
        SourceImage      string  // Base image to build FROM
        SourceImageCreds string  // Credentials for custom base images
        Dockerfile       string
        BuildCtxObject   string
        BuildSecrets     []string
    }
}
```

## Why This Is Better

### Before (Wrong):
```
BuildOptions = Build-time fields + BuildRegistryCreds
```
- Confusing: BuildRegistryCreds is needed at runtime too!
- Inconsistent: ImageCredentials is top-level, but BuildRegistryCreds is nested

### After (Correct):
```
ContainerRequest = Runtime fields (ImageCredentials, BuildRegistryCreds)
BuildOptions = Build-only fields (SourceImage, Dockerfile, etc.)
```
- Clear separation: Build-only vs. Lifecycle-wide credentials
- Consistent: All runtime credentials are peers at the top level
- Intuitive: BuildRegistryCreds sits next to ImageCredentials

## Testing

All tests pass:
```bash
go test ./pkg/abstractions/image/... ./pkg/worker/... ./pkg/registry/...
```

? Token generation tests
? Full credential flow tests  
? Credential priority tests
? Build registry usage tests

## No Functional Changes

This is a pure refactoring:
- ? Same credential generation logic
- ? Same credential usage patterns
- ? Same priority system
- ? Just moved to a more logical location

The behavior is identical, but the code is now more maintainable and easier to understand.
