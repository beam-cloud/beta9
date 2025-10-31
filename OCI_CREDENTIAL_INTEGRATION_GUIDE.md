# OCI Credential Integration Guide

## Overview

This guide explains how to integrate Beta9's automatic OCI credential management with CLIP's credential provider system.

## Architecture

### Component Flow

```
Python SDK → Backend → Image Build → Secret Creation
                              ↓
                        Image Record
                              ↓
                        Worker Mount
                              ↓
                    Secret Retrieval
                              ↓
                    CLIP Provider Factory
                              ↓
                    OCI Layer Fetching
```

### Implementation Status

✅ **Completed Components:**
1. `pkg/oci/credentials.go` - OCI credential helper package
2. `pkg/repository` - Database schema and methods for credential secrets
3. `pkg/abstractions/image/image.go` - Automatic secret creation during image build
4. `pkg/worker/image.go` - Secret retrieval and provider creation during mount
5. Database migration for credential secret fields

⚠️ **Dependencies:**
1. CLIP must be updated with `pkg/registryauth` package (credential provider interface)
2. Database migration `036_add_image_credential_fields` must be run

## Prerequisites

### 1. Update CLIP Dependency

The CLIP package must include the `registryauth` package with the following interface:

```go
package registryauth

import (
    "context"
    "github.com/google/go-containerregistry/pkg/authn"
)

type RegistryCredentialProvider interface {
    GetCredentials(ctx context.Context, registry string) (*authn.AuthConfig, error)
}
```

Update `go.mod`:
```bash
go get github.com/beam-cloud/clip@latest
```

### 2. Run Database Migration

```bash
# The migration adds credential_secret_name and credential_secret_id columns to the image table
goose -dir pkg/repository/backend_postgres_migrations postgres "your-connection-string" up
```

## Usage Examples

### Python SDK (No Changes Required)

Users continue to use the same API:

```python
from beta9 import Image, endpoint

# Example 1: AWS ECR
image = Image(
    python_version="python3.12",
    base_image="111111111111.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
    base_image_creds=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"],
)

@endpoint(image=image)
def handler():
    return {"status": "success"}
```

```python
# Example 2: GitHub Container Registry
image = Image(
    python_version="python3.12",
    base_image="ghcr.io/myorg/private-image:latest",
    base_image_creds=["GITHUB_USERNAME", "GITHUB_TOKEN"],
)

@endpoint(image=image)
def handler():
    return {"status": "success"}
```

```python
# Example 3: Docker Hub
image = Image(
    python_version="python3.12",
    base_image="myusername/private-image:latest",
    base_image_creds=["DOCKERHUB_USERNAME", "DOCKERHUB_PASSWORD"],
)

@endpoint(image=image)
def handler():
    return {"status": "success"}
```

### Backend Flow

1. **Image Build Request Received**
   - User specifies `base_image` and `base_image_creds`
   - Backend receives credentials as environment variable names or JSON

2. **Automatic Secret Creation** (`pkg/abstractions/image/image.go`)
   ```go
   // After successful build
   func (is *RuncImageService) BuildImage(...) error {
       // ... build completes ...
       
       // Automatically create credential secret
       if err := is.createCredentialSecretIfNeeded(ctx, imageId, buildOptions); err != nil {
           log.Error().Err(err).Msg("failed to create credential secret")
       }
       
       return nil
   }
   ```

3. **Secret Storage**
   - Secret name: `oci-creds-{sha256(registry)[:16]}`
   - Secret value (JSON):
     ```json
     {
       "version": 1,
       "registry": "111111111111.dkr.ecr.us-east-1.amazonaws.com",
       "type": "aws",
       "credentials": {
         "AWS_ACCESS_KEY_ID": "AKIA...",
         "AWS_SECRET_ACCESS_KEY": "wJal...",
         "AWS_REGION": "us-east-1"
       }
     }
     ```

4. **Image Record Update**
   ```sql
   UPDATE image 
   SET credential_secret_name = 'oci-creds-abc123',
       credential_secret_id = 'uuid-here'
   WHERE image_id = 'image-id';
   ```

5. **Worker Mount** (`pkg/worker/image.go`)
   ```go
   // During OCI image mount
   if strings.ToLower(storageType) == string(clipCommon.StorageModeOCI) {
       // Retrieve credential provider
       if provider, err := c.getCredentialProviderForImage(ctx, imageId, &request.Workspace); err == nil {
           mountOptions.CredProvider = provider
       }
   }
   ```

6. **Credential Provider Creation** (`pkg/oci/credentials.go`)
   ```go
   func (c *ImageClient) getCredentialProviderForImage(ctx context.Context, imageId string, workspace *types.Workspace) (registryauth.RegistryCredentialProvider, error) {
       // 1. Retrieve credential secret name from image record
       secretName, _, err := c.backendRepo.GetImageCredentialSecret(ctx, imageId)
       
       // 2. Fetch and decrypt secret
       secret, err := c.backendRepo.GetSecretByNameDecrypted(ctx, workspace, secretName)
       
       // 3. Unmarshal credentials
       credSecret, err := oci.UnmarshalCredentials(secret.Value)
       
       // 4. Create CLIP provider
       provider, err := oci.CreateCLIPProvider(ctx, credSecret)
       
       return provider, nil
   }
   ```

## Supported Registries

| Registry | Credential Type | Environment Variables | Notes |
|----------|----------------|----------------------|-------|
| Amazon ECR | AWS | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_SESSION_TOKEN` | Uses callback provider with token refresh (11hr cache) |
| Google GCR/Artifact Registry | GCP | `GCP_ACCESS_TOKEN` | Uses short-lived access token |
| Docker Hub | DockerHub | `DOCKERHUB_USERNAME`, `DOCKERHUB_PASSWORD` | Static username/password |
| GitHub Container Registry | GHCR | `GITHUB_USERNAME`, `GITHUB_TOKEN` | Static username/token |
| NVIDIA GPU Cloud | NGC | `NGC_API_KEY` | Username: `$oauthtoken`, Password: API key |
| Generic Registry | Generic | `USERNAME`, `PASSWORD` | Any private registry with basic auth |

## Credential Type Detection

The system automatically detects the credential type based on:

1. **Environment variable names** (highest priority)
   - Presence of `AWS_ACCESS_KEY_ID` → AWS
   - Presence of `GCP_ACCESS_TOKEN` → GCP
   - Presence of `NGC_API_KEY` → NGC
   - etc.

2. **Registry pattern matching**
   - `*.dkr.ecr.*.amazonaws.com` → AWS
   - `gcr.io`, `*-docker.pkg.dev` → GCP
   - `ghcr.io` → GHCR
   - `nvcr.io` → NGC
   - `docker.io` → DockerHub

## Security Features

✅ **Credentials encrypted at rest** - Uses workspace secret encryption
✅ **Credentials never logged** - Sanitized by CLIP providers
✅ **Workspace-scoped** - Secrets tied to specific workspaces
✅ **Not in archives** - Credentials only used at mount time
✅ **Short-lived token support** - ECR/GCP tokens cached with TTL

## Testing

### Unit Tests

```bash
# Test credential helpers
go test ./pkg/oci/... -v

# Test image integration
go test ./pkg/abstractions/image/... -v

# Test worker integration
go test ./pkg/worker/... -v
```

### Integration Test

```bash
# 1. Set up test environment
export AWS_ACCESS_KEY_ID="test-key"
export AWS_SECRET_ACCESS_KEY="test-secret"
export AWS_REGION="us-east-1"

# 2. Build image with private base image
python test_integration.py
```

## Troubleshooting

### Build Fails: "no required module provides package github.com/beam-cloud/clip/pkg/registryauth"

**Solution**: Update CLIP dependency to version that includes registryauth:
```bash
go get github.com/beam-cloud/clip@latest
go mod tidy
```

### Secret Not Created

**Symptoms**: Image builds successfully but no credential secret created

**Debugging**:
```bash
# Check logs for secret creation
grep "creating credential secret" /var/log/beta9/gateway.log

# Verify image record
psql -c "SELECT credential_secret_name FROM image WHERE image_id='your-image-id'"
```

**Common Causes**:
1. Not using ClipVersion 2 (OCI images)
2. Base image credentials empty or invalid format
3. Public registry detected (no credentials needed)

### Mount Fails: "failed to retrieve credential secret"

**Symptoms**: Worker cannot mount OCI image

**Debugging**:
```bash
# Check if secret exists
psql -c "SELECT * FROM workspace_secret WHERE name LIKE 'oci-creds-%'"

# Check worker logs
grep "failed to get credential provider" /var/log/beta9/worker.log
```

**Solution**: Rebuild image to create credential secret

### ECR Token Expiry

**Symptoms**: Mount works initially but fails after 12 hours

**Status**: Should not occur - ECR tokens cached for 11 hours and refreshed on next mount

**If it occurs**: Check CachingProvider TTL in `pkg/oci/credentials.go`

## Migration from Legacy System

### Old System (Deprecated)
- Credentials passed once during build
- Stored in archive metadata (security risk)
- No support for token refresh

### New System
- Credentials stored as workspace secrets
- Fetched at mount time (runtime credentials)
- Supports token refresh for short-lived credentials

### Migration Steps

1. **No action required for existing archives** - Old archives continue to work
2. **Rebuild images to use new system** - Next build will create credential secrets
3. **Monitor logs** - Verify secret creation during builds

## API Reference

### `pkg/oci/credentials.go`

```go
// ParseRegistry extracts registry hostname from image reference
func ParseRegistry(imageRef string) string

// DetectCredentialType determines credential type
func DetectCredentialType(registry string, creds map[string]string) CredentialType

// CreateSecretName generates consistent secret name
func CreateSecretName(registry string) string

// MarshalCredentials serializes credentials to JSON
func MarshalCredentials(registry string, credType CredentialType, creds map[string]string) (string, error)

// UnmarshalCredentials deserializes credentials from JSON
func UnmarshalCredentials(data string) (*CredentialSecret, error)

// CreateCLIPProvider creates CLIP provider from stored credentials
func CreateCLIPProvider(ctx context.Context, secret *CredentialSecret) (registryauth.RegistryCredentialProvider, error)
```

### Repository Methods

```go
// SetImageCredentialSecret associates a secret with an image
func (r *PostgresBackendRepository) SetImageCredentialSecret(ctx context.Context, imageId string, secretName string, secretExternalId string) error

// GetImageCredentialSecret retrieves secret info for an image
func (r *PostgresBackendRepository) GetImageCredentialSecret(ctx context.Context, imageId string) (string, string, error)
```

## Performance Considerations

### Secret Lookup
- **First mount**: ~50ms (database lookup + decryption)
- **Subsequent mounts**: Same (no caching - secrets may change)

### Provider Creation
- **AWS ECR**: ~200ms (ECR API call for token)
- **Static providers**: ~1ms (no API calls)
- **Caching**: ECR tokens cached for 11 hours

### Recommendations
- Use static providers (DockerHub, GHCR) for faster mounts
- For ECR, token refresh only on first use after cache expiry

## Future Enhancements

1. **Provider caching** - Cache providers across mounts for same image
2. **Credential rotation** - Auto-update secrets on rotation
3. **Multi-registry support** - Single image from multiple registries
4. **Credential validation** - Test credentials during build
5. **Secret TTL** - Expire unused credential secrets
