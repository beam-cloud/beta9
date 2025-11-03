# CLIP V2 Credential Flow

## Your Questions Answered

### 1. How does CLIP work with credentials?

**Yes, we push a completed image to our build registry.** Here's the complete flow:

```
???????????????????????????????????????????????????????????????????
? SCHEDULE TIME (pkg/abstractions/image/build.go)                ?
?                                                                 ?
? 1. Read config.ImageService.BuildRegistryCredentials           ?
?    - Stores long-term creds (AWS keys, GCP service account)   ?
?                                                                 ?
? 2. Generate fresh, short-lived token                           ?
?    - ECR: 12-hour token                                        ?
?    - GCR: 1-hour token                                         ?
?    - Basic auth: username:password                             ?
?                                                                 ?
? 3. Place token in BuildOptions.BuildRegistryCreds              ?
?                                                                 ?
? 4. Send entire ContainerRequest to worker                      ?
???????????????????????????????????????????????????????????????????
                              ?
???????????????????????????????????????????????????????????????????
? BUILD TIME - PUSH PHASE (pkg/worker/image.go)                  ?
?                                                                 ?
? 1. Worker builds image with buildah                            ?
?                                                                 ?
? 2. Worker pushes to build registry                             ?
?    - Uses BuildRegistryCreds for authentication               ?
?    - Image: registry.example.com/userimages:workspace-image   ?
?                                                                 ?
? 3. Worker caches image reference                               ?
?    - v2ImageRefs[imageId] = "registry.example.com/..."        ?
???????????????????????????????????????????????????????????????????
                              ?
???????????????????????????????????????????????????????????????????
? BUILD TIME - CLIP INDEXING (pkg/worker/image.go)               ?
?                                                                 ?
? 1. CLIP reads OCI manifest from registry                       ?
?    - getCredentialProviderForImage()                           ?
?    - Checks: Is this from build registry?                     ?
?    - Uses: BuildRegistryCreds (SAME token from step 1)        ?
?                                                                 ?
? 2. CLIP creates index-only archive                             ?
?    - No layer data, just metadata                              ?
?    - Stores registry location in archive                       ?
???????????????????????????????????????????????????????????????????
                              ?
???????????????????????????????????????????????????????????????????
? RUNTIME - LAYER MOUNTING (pkg/worker/image.go)                 ?
?                                                                 ?
? 1. Container starts                                             ?
?                                                                 ?
? 2. CLIP mounts image filesystem                                ?
?    - getCredentialProviderForImage()                           ?
?    - Gets image ref from v2ImageRefs cache                     ?
?    - Checks: Is this from build registry?                     ?
?    - Uses: BuildRegistryCreds (SAME token from step 1!)       ?
?                                                                 ?
? 3. On-demand layer pulling                                     ?
?    - Container reads /app/file.py                              ?
?    - CLIP fetches layer from registry                          ?
?    - Uses BuildRegistryCreds for authentication               ?
???????????????????????????????????????????????????????????????????
```

### 2. Credentials for build vs runtime - Aren't they the same?

**YES! The same credentials are used throughout.** This is the key insight:

- **At schedule time**: We generate ONE token (e.g., ECR 12h token)
- **At build time**: We use this token to push the image
- **At CLIP indexing**: We use this SAME token to read the manifest
- **At runtime**: We use this SAME token for layer pulls

The credentials **are not** just for the build process. They persist through the entire lifecycle because:

1. The token is stored in `BuildOptions.BuildRegistryCreds`
2. The `ContainerRequest` (with credentials) is kept throughout the container's lifetime
3. Every time CLIP needs to access the registry, it uses `getCredentialProviderForImage()`, which checks the request's `BuildRegistryCreds`

### 3. The Unified Credential System

The credential system IS unified. Here's how it works:

#### Credential Priority (in `getCredentialProviderForImage`)

```
1. Runtime Secrets (ImageCredentials)
   ? if not found
2. Custom Base Image Creds (SourceImageCreds)
   ? if not found
3. Build Registry Creds (BuildRegistryCreds) ? THIS IS THE KEY
   ? if not found
4. Ambient Auth (IAM roles, docker config)
```

#### When BuildRegistryCreds is Used

```go
// From pkg/worker/image.go
func (c *ImageClient) getCredentialProviderForImage(...) {
    // Get cached image reference
    sourceRef := v2ImageRefs.Get(imageId)  // e.g., "registry.example.com/userimages:ws-img"
    
    // Check if this image is from our build registry
    buildRegistry := c.getBuildRegistry()  // e.g., "registry.example.com"
    
    if strings.Contains(sourceRef, buildRegistry) {
        // USE BUILD REGISTRY CREDENTIALS!
        return parseAndCreateProvider(request.BuildOptions.BuildRegistryCreds)
    }
}
```

**This means:**
- ? Images WE built ? use BuildRegistryCreds
- ? Custom base images ? use SourceImageCreds
- ? Public images ? use ambient auth

## Key Code Locations

### 1. Token Generation (Schedule Time)
**File**: `pkg/abstractions/image/build.go`
**Function**: `generateBuildRegistryCredentials()`

```go
// Reads config, generates fresh token
token := reg.GetRegistryTokenForImage(imageRef, config.Credentials)
return token // Placed in BuildOptions.BuildRegistryCreds
```

### 2. Token Usage (Build Time - Push)
**File**: `pkg/worker/image.go`
**Function**: `BuildAndArchiveImage()` ? `getBuildRegistryAuthArgs()`

```go
// Uses BuildRegistryCreds for buildah push
authArgs := getBuildRegistryAuthArgs(request.BuildOptions.BuildRegistryCreds)
// buildah push --authfile=... image registry
```

### 3. Token Usage (Build Time - CLIP Indexing)
**File**: `pkg/worker/image.go`
**Function**: `createOCIImageWithProgress()`

```go
// Gets credentials for CLIP to read manifest
credProvider := c.getCredentialProviderForImage(ctx, imageId, request)
clip.CreateFromOCIImage(..., CredProvider: credProvider)
```

### 4. Token Usage (Runtime - Layer Mounting)
**File**: `pkg/worker/image.go`
**Function**: `PullLazy()`

```go
// Gets credentials for CLIP to pull layers
credProvider := c.getCredentialProviderForImage(ctx, imageId, request)
mountOptions.RegistryCredProvider = credProvider
```

## Important Implementation Details

### Image Reference Caching

The `v2ImageRefs` cache is critical:

```go
// After push:
v2ImageRefs.Set(imageId, "registry.example.com/userimages:workspace-image")

// At runtime:
sourceRef := v2ImageRefs.Get(imageId)  // Get the registry location
// Then check if sourceRef contains buildRegistry
```

This is how we know to use BuildRegistryCreds at runtime!

### Credential Flow Verification

The logs will show:

```
Build time:
  "using build registry credentials for image access (image was built and pushed by us)"
  
Runtime:
  "using build registry credentials for image access (image was built and pushed by us)"
  "credentials provided for runtime CLIP layer mounting"
```

If you see these logs at BOTH build and runtime, the credential flow is working correctly.

## Token Lifetime Considerations

### Current Approach
- ECR tokens: 12 hours
- GCP tokens: 1 hour (refreshable)
- Tokens generated once at schedule time

### Limitations
- Containers running > 12 hours may fail to pull new layers (if using ECR)
- No automatic token refresh

### Workarounds
1. **Use IAM roles on worker nodes** (recommended)
   - Workers authenticate via IAM role
   - Falls back to ambient auth after token expiry
   
2. **Short-lived containers** (< 12h)
   - Most use cases

3. **Future: Token refresh mechanism**
   - Could regenerate tokens periodically
   - Would need to update the credential provider dynamically

## Testing

Comprehensive tests in:
- `pkg/abstractions/image/build_registry_creds_test.go` - Token generation
- `pkg/worker/image_credentials_flow_test.go` - Full flow verification
- `pkg/registry/credentials_dockerhub_test.go` - Registry credential handling

Run tests:
```bash
go test ./pkg/abstractions/image/... ./pkg/worker/... ./pkg/registry/...
```

## Summary

**The credential system is already unified and works correctly!**

1. ? ONE token generated at schedule time
2. ? Token used for push, CLIP indexing, AND runtime layer pulls
3. ? Same `getCredentialProviderForImage()` function used everywhere
4. ? Credentials automatically selected based on image location
5. ? Works with ECR, GCR, private registries, and basic auth

The confusion may have been because the flow isn't immediately obvious, but the implementation is sound. The key insight is that `BuildRegistryCreds` is NOT just for building - it's for accessing ANY image that lives in the build registry, both at build time and runtime.
