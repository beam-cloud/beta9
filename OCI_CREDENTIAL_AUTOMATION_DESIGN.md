# OCI Credential Automation Design

## Problem Statement

Beta9's Python SDK allows users to specify private registry credentials via `base_image_creds` (a list of environment variable names). Currently, these credentials are:
1. Passed once during image build
2. Not persisted for later use (mounting, OCI lazy loading)
3. Not integrated with CLIP's new credential provider system

For OCI images (index-only archives), we need credentials at runtime for lazy layer fetching, not just at build time.

## Solution Architecture

### High-Level Flow

```
1. User creates Image with base_image_creds → ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", ...]
2. SDK sends to backend with credentials as JSON
3. Backend automatically:
   a. Parses registry from base_image
   b. Detects credential type (AWS/GCP/DockerHub/GHCR/NGC)
   c. Creates workspace secret: "oci-creds-{registry-hash}"
   d. Stores secret reference with Image record
4. When mounting OCI image:
   a. Retrieve credential secret
   b. Create appropriate CLIP provider (ECR callback, static, etc.)
   c. Pass provider to CLIP mount operations
```

### Components

#### 1. OCI Credential Helper Package (`pkg/oci/credentials.go`)

**Purpose**: Bridge between Beta9 credential format and CLIP providers

**Key Functions**:
- `ParseRegistry(imageRef string) (string, error)` - Extract registry from image ref
- `DetectCredentialType(registry string, creds map[string]string) CredentialType` - Detect AWS/GCP/etc.
- `CreateSecretName(registry string) string` - Generate consistent secret names
- `MarshalCredentials(creds map[string]string) (string, error)` - Serialize to JSON
- `UnmarshalCredentials(data string) (map[string]string, error)` - Deserialize from JSON
- `CreateCLIPProvider(ctx context.Context, credType CredentialType, creds map[string]string) (registryauth.RegistryCredentialProvider, error)` - Factory for CLIP providers

#### 2. Credential Type Detection

```go
type CredentialType string

const (
    CredTypeAWS        CredentialType = "aws"       // ECR
    CredTypeGCP        CredentialType = "gcp"       // GCR/Artifact Registry
    CredTypeDockerHub  CredentialType = "dockerhub" // Docker Hub
    CredTypeGHCR       CredentialType = "ghcr"      // GitHub Container Registry
    CredTypeNGC        CredentialType = "ngc"       // NVIDIA GPU Cloud
    CredTypeGeneric    CredentialType = "generic"   // Username/password
)
```

**Detection Rules**:
- `*.dkr.ecr.*.amazonaws.com` → AWS (use ECR token callback)
- `gcr.io`, `*.gcr.io`, `*-docker.pkg.dev` → GCP (use GCP token)
- `docker.io`, `registry-1.docker.io` → DockerHub (use username/password)
- `ghcr.io` → GHCR (use username/token)
- `nvcr.io` → NGC (use $oauthtoken/api_key)
- Otherwise → Generic (use username/password if available)

#### 3. Secret Storage Schema

**Secret Name**: `oci-creds-{sha256(registry)[:16]}`

**Secret Value** (JSON):
```json
{
  "version": 1,
  "registry": "111111111111.dkr.ecr.us-east-1.amazonaws.com",
  "type": "aws",
  "credentials": {
    "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
    "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "AWS_REGION": "us-east-1"
  }
}
```

#### 4. Database Schema Addition

Add to `image` table:
```sql
ALTER TABLE image ADD COLUMN credential_secret_name VARCHAR(255);
ALTER TABLE image ADD COLUMN credential_secret_id VARCHAR(36);
```

#### 5. CLIP Provider Factory

```go
func CreateCLIPProvider(ctx context.Context, credType CredentialType, creds map[string]string) (registryauth.RegistryCredentialProvider, error) {
    switch credType {
    case CredTypeAWS:
        // Use callback provider with ECR token refresh
        callback := func(ctx context.Context, registry string) (*authn.AuthConfig, error) {
            return getECRToken(ctx, creds["AWS_ACCESS_KEY_ID"], creds["AWS_SECRET_ACCESS_KEY"], creds["AWS_REGION"])
        }
        provider := registryauth.NewCallbackProvider(callback)
        return registryauth.NewCachingProvider(provider, 11*time.Hour), nil
        
    case CredTypeGCP:
        // Use GCP token provider
        callback := func(ctx context.Context, registry string) (*authn.AuthConfig, error) {
            return &authn.AuthConfig{
                Username: "oauth2accesstoken",
                Password: creds["GCP_ACCESS_TOKEN"],
            }, nil
        }
        return registryauth.NewCallbackProvider(callback), nil
        
    case CredTypeDockerHub:
        // Use static provider
        return registryauth.NewStaticProvider(map[string]*authn.AuthConfig{
            "docker.io": {
                Username: creds["DOCKERHUB_USERNAME"],
                Password: creds["DOCKERHUB_PASSWORD"],
            },
        }), nil
        
    case CredTypeGHCR:
        return registryauth.NewStaticProvider(map[string]*authn.AuthConfig{
            "ghcr.io": {
                Username: creds["GITHUB_USERNAME"],
                Password: creds["GITHUB_TOKEN"],
            },
        }), nil
        
    case CredTypeNGC:
        return registryauth.NewStaticProvider(map[string]*authn.AuthConfig{
            "nvcr.io": {
                Username: "$oauthtoken",
                Password: creds["NGC_API_KEY"],
            },
        }), nil
        
    default:
        // Try generic username/password or fallback to default
        if username, ok := creds["USERNAME"]; ok {
            if password, ok := creds["PASSWORD"]; ok {
                return registryauth.NewStaticProvider(map[string]*authn.AuthConfig{
                    registry: {Username: username, Password: password},
                }), nil
            }
        }
        return registryauth.DefaultProvider, nil
    }
}
```

### Implementation Steps

#### Phase 1: Infrastructure
1. Create `pkg/oci/credentials.go` with helper functions
2. Add database migration for credential_secret fields
3. Update `types.Image` struct with credential fields

#### Phase 2: Secret Creation
4. Update image build flow to auto-create secrets:
   - In `pkg/abstractions/image/build.go`: After container request is created
   - Parse registry from `BaseImage`
   - Detect credential type
   - Create workspace secret
   - Store secret reference in Image record

#### Phase 3: Secret Retrieval & Usage
5. Update `pkg/worker/image.go`:
   - In `createOCIImageWithProgress`: Retrieve credential secret, create CLIP provider
   - In `MountImage` (for OCI images): Retrieve credential secret for lazy loading
6. Update CLIP mount options to include provider

#### Phase 4: Testing
7. Add unit tests for credential helpers
8. Add integration tests for secret creation
9. Test with each credential type (AWS/GCP/DockerHub/GHCR/NGC)

### Backward Compatibility

- **Legacy AuthConfig**: Continue to support for non-OCI images or direct AuthConfig usage
- **Existing Images**: Images without credential secrets will fall back to default provider
- **Non-private Images**: Public images work without credentials (PublicOnlyProvider)

### Security Considerations

1. ✅ Credentials encrypted at rest (existing workspace secret encryption)
2. ✅ Credentials never in logs (sanitized by CLIP providers)
3. ✅ Credentials scoped to workspace
4. ✅ Credentials not in .clip archives (only at mount time)
5. ✅ Short-lived tokens (ECR/GCP tokens cached with TTL)

### Example Usage Flow

```python
# Python SDK (no change to user API)
image = Image(
    base_image="111111111111.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
    base_image_creds=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"],
)

@endpoint(image=image)
def handler():
    pass
```

**Backend Flow**:
1. Image build request received
2. System creates secret `oci-creds-abc123`:
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
3. Image record stores: `credential_secret_name = "oci-creds-abc123"`
4. When container runs:
   - Worker retrieves secret
   - Creates ECR callback provider with token refresh
   - Passes to CLIP mount operations
   - Layers fetched lazily with fresh ECR tokens

### Benefits

1. **Automatic**: No manual secret creation required
2. **Secure**: Credentials encrypted, never in archives
3. **Flexible**: Supports all major registries
4. **Efficient**: Token refresh for short-lived credentials
5. **Compatible**: Works with existing SDK interface
6. **Clean**: Credentials separate from archive artifacts
