# CLIP V2 Build Registry Credentials - Summary

## What Changed

1. **Config Structure** (`pkg/types/config.go`):
   - Added `BuildRegistryCredentialsConfig` to store raw credentials (AWS keys, GCP service account, etc.)
   - Credentials stored in config, tokens generated dynamically

2. **Proto/Types** (`pkg/types/types.proto`, `proto/types.pb.go`, `pkg/types/scheduler.go`):
   - Added `BuildRegistryCreds` field to `BuildOptions`
   - Carries dynamically-generated tokens from scheduler to worker

3. **Token Generation** (`pkg/abstractions/image/build.go`):
   - `generateBuildRegistryCredentials()` uses `GetRegistryTokenForImage()`
   - Generates fresh ECR/GCR/basic auth tokens at schedule time
   - Token placed in `BuildOptions.BuildRegistryCreds`

4. **Credential Usage** (`pkg/worker/image.go`):
   - Enhanced `getCredentialProviderForImage()` with comprehensive comments
   - Used for: buildah push, CLIP indexing, runtime layer mounting
   - SAME token used throughout the entire lifecycle

5. **Registry Support** (`pkg/registry/credentials.go`):
   - Enhanced `GetDockerHubToken()` to support generic registries
   - Falls back to `USERNAME`/`PASSWORD` for non-Docker Hub registries

## Key Insight

**The SAME credentials are used for build AND runtime!**

- Schedule time: Generate token → put in `BuildOptions.BuildRegistryCreds`
- Build time: Use token for push + CLIP indexing
- Runtime: Use SAME token for layer pulls

The `v2ImageRefs` cache maps `imageId` → `registry.example.com/userimages:workspace-image`

At runtime, `getCredentialProviderForImage()` checks:
- Is this image from the build registry?
- If yes → use `BuildRegistryCreds`

## Documentation

See `docs/CLIP_V2_CREDENTIAL_FLOW.md` for complete details.

## Tests

- `pkg/abstractions/image/build_registry_creds_test.go` - Token generation
- `pkg/worker/image_credentials_flow_test.go` - Full flow
- `pkg/registry/credentials_dockerhub_test.go` - Registry support

All tests pass ✅
