# CLIP Image Credential Management - Fixed

## Summary of Changes

This fix addresses the credential management issues for CLIP images (both V1 and V2) by establishing a consistent, structured credential flow that works with CLIP's credential provider interface.

## Problem Statement

The original implementation had several issues:
1. **Inconsistent format**: Credentials were passed as `username:password` strings during build but CLIP needs structured credentials
2. **Missing secrets**: Secrets weren't being created consistently for all registry types
3. **Runtime failures**: Runtime containers couldn't access credentials because secrets weren't properly attached
4. **Limited provider support**: Not all registry types (ECR, GAR, NGC, GHCR, Docker Hub) were properly supported

## Solution

### 1. Structured Credential Functions

Created new functions in `pkg/abstractions/image/credentials.go`:

```go
// GetRegistryCredentials returns structured credentials for any registry
func GetRegistryCredentials(opts *BuildOpts) (registry string, creds map[string]string, err error)

// GetECRCredentials returns structured AWS ECR credentials
func GetECRCredentials(opts *BuildOpts) (map[string]string, error)

// GetGARCredentials returns structured GCP GAR credentials
func GetGARCredentials(opts *BuildOpts) (map[string]string, error)

// GetNGCCredentials returns structured NGC credentials
func GetNGCCredentials(opts *BuildOpts) (map[string]string, error)

// GetGHCRCredentials returns structured GHCR credentials
func GetGHCRCredentials(opts *BuildOpts) (map[string]string, error)

// GetDockerHubCredentials returns structured Docker Hub credentials
func GetDockerHubCredentials(opts *BuildOpts) (map[string]string, error)

// ParseCredentialsFromJSON parses credentials from JSON or legacy format
func ParseCredentialsFromJSON(credStr string) (map[string]string, error)

// MarshalCredentialsToJSON converts credentials to JSON for CLIP
func MarshalCredentialsToJSON(registry string, creds map[string]string) (string, error)
```

### 2. Updated Build Process

Modified `pkg/abstractions/image/build.go`:
- Parse credentials in both JSON and legacy `username:password` formats
- Convert to JSON format for `ImageCredentials` field
- Ensures CLIP can use credentials during build

### 3. Enhanced Secret Creation

Updated `pkg/abstractions/image/image.go`:
- Create secrets for **both V1 and V2** images (not just V2)
- Support all credential types: ECR, GAR, NGC, GHCR, Docker Hub
- Handle both `ExistingImageCreds` (from_registry) and `BaseImageCreds` (base_image)
- Link secrets to images for runtime access

### 4. Runtime Credential Attachment

The scheduler (already working) now:
- Retrieves credential secrets for images
- Attaches them to runtime container requests in JSON format
- CLIP credential provider in worker parses and uses them

## Credential Flow

### Build Time

```
SDK:
  credentials=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]
       ?
  Reads from environment ? {"AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "...", "AWS_REGION": "..."}
       ?
Gateway/Build:
  GetRegistryCredentials() ? Returns structured credentials
       ?
  MarshalCredentialsToJSON() ? JSON format for CLIP
       ?
  Container ImageCredentials field ? CLIP uses during build
       ?
Secret Creation:
  After successful build ? Create/update workspace secret
       ?
  Link secret to image ID in database
```

### Runtime

```
Scheduler:
  Retrieves image credential secret by image ID
       ?
  Attaches secret (JSON format) to container request
       ?
Worker:
  createCredentialProvider() parses JSON
       ?
  Creates CLIP credential provider
       ?
  CLIP uses provider to access OCI registry layers
```

## Supported Registries

All registry types from the original spec are now fully supported:

### 1. Amazon ECR
```python
Image.from_registry(
    "123456789.dkr.ecr.us-east-1.amazonaws.com/my-repo:latest",
    credentials=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]
)
```

### 2. Google Artifact Registry / GCR
```python
Image.from_registry(
    "us-docker.pkg.dev/my-project/my-repo/my-image:latest",
    credentials=["GCP_ACCESS_TOKEN"]
)
```

### 3. NVIDIA GPU Cloud
```python
Image.from_registry(
    "nvcr.io/nvidia/pytorch:latest",
    credentials=["NGC_API_KEY"]
)
```

### 4. GitHub Container Registry
```python
Image.from_registry(
    "ghcr.io/my-org/my-repo:latest",
    credentials=["GITHUB_USERNAME", "GITHUB_TOKEN"]
)
```

### 5. Docker Hub
```python
Image.from_registry(
    "docker.io/my-org/my-image:latest",
    credentials=["DOCKERHUB_USERNAME", "DOCKERHUB_PASSWORD"]
)
```

## Tests

Comprehensive tests were added in `pkg/abstractions/image/credentials_test.go`:

- ? `TestGetECRCredentials` - AWS ECR credential handling
- ? `TestGetGARCredentials` - GCP GAR credential handling
- ? `TestGetNGCCredentials` - NGC credential handling
- ? `TestGetGHCRCredentials` - GHCR credential handling (including public access)
- ? `TestGetDockerHubCredentials` - Docker Hub credential handling (including public access)
- ? `TestGetRegistryCredentials` - Registry-specific routing
- ? `TestParseCredentialsFromJSON` - JSON and legacy format parsing
- ? `TestMarshalCredentialsToJSON` - JSON credential marshaling
- ? `TestValidateRequiredCredential` - Required credential validation
- ? `TestValidateOptionalCredentials` - Optional credential validation

All tests pass successfully.

## Backward Compatibility

The implementation maintains backward compatibility:

1. **Legacy format support**: Still accepts `username:password` format via `ParseCredentialsFromJSON()`
2. **V1 images**: Continue to work with legacy credential handling
3. **V2 images**: Now work correctly with CLIP credential providers
4. **SDK**: No changes required - existing SDK code works as-is

## Key Benefits

1. **Consistent format**: All credentials flow through the same JSON format
2. **Complete provider support**: All 5 registry types fully supported
3. **Secret persistence**: Credentials stored as secrets for runtime reuse
4. **CLIP compatibility**: Credentials in format CLIP expects
5. **Both build and runtime**: Works consistently at both stages
6. **Type-safe**: Structured credentials with proper validation

## Files Modified

1. `pkg/abstractions/image/credentials.go` - New structured credential functions
2. `pkg/abstractions/image/build.go` - Updated to use JSON credentials
3. `pkg/abstractions/image/image.go` - Enhanced secret creation for all registry types
4. `pkg/abstractions/image/credentials_test.go` - Comprehensive test coverage (new file)

## Migration Path

No migration required! The changes are backward compatible:

- Existing images continue to work
- New builds automatically use the new credential system
- Secrets are created/updated automatically on build
- Runtime containers automatically receive credentials via secrets
