# Hybrid Approach: Store Secrets, Pass at Runtime

## Summary

This implementation keeps your original request: **create secrets at build time, pass them at runtime** - but simplified to avoid gRPC complexity.

## What's Complete

### ✅ 1. Secret Creation at Build Time
- `pkg/abstractions/image/image.go` - `createCredentialSecretIfNeeded()` creates workspace secrets
- Secrets stored in database with reference in image table

### ✅ 2. Database Schema
- Migration: `pkg/repository/backend_postgres_migrations/036_add_image_credential_fields.go`
- Adds `credential_secret_name` and `credential_secret_id` to `image` table
- Repository methods: `SetImageCredentialSecret()`, `GetImageCredentialSecret()`

### ✅ 3. Helper Functions
- `pkg/oci/credentials.go` has all functions:
  - `ParseRegistry()` - Extract registry from image ref
  - `CreateProviderFromEnv()` - Runtime provider creation
  - `ParseCredentialsFromEnv()` - Filter credential keys
  - `DetectCredentialType()` - Detect registry type
  - `MarshalCredentials()` - JSON serialization
  - `CreateSecretName()` - Generate secret name

### ✅ 4. Proto/Types Updates
- `pkg/types/types.proto` - Added `image_credentials` field
- `pkg/types/scheduler.go` - Added `ImageCredentials` field to `ContainerRequest`
- `proto/types.pb.go` - Updated protobuf (manual for now, regenerate with `make proto`)

## What Needs to be Completed

### 🔧 5. Scheduler: Fetch & Pass Credentials

When scheduler creates a `ContainerRequest` for runtime (not build), it should:

```go
// In scheduler where ContainerRequest is created:
// 1. Get image credential secret name
secretName, _, err := backendRepo.GetImageCredentialSecret(ctx, imageId)
if err == nil && secretName != "" {
    // 2. Retrieve the secret value
    secret, err := backendRepo.GetSecretByNameDecrypted(ctx, workspace, secretName)
    if err == nil {
        // 3. Add to request
        request.ImageCredentials = secret.Value  // This is JSON string
    }
}
```

### 🔧 6. Worker: Use Credentials from Request

Update `pkg/worker/image.go`:

```go
// In PullLazy(), for OCI images:
if strings.ToLower(storageType) == string(clipCommon.StorageModeOCI) {
    mountOptions.StorageInfo = nil
    
    // Create provider from credentials in request
    if request.ImageCredentials != "" {
        var credData map[string]interface{}
        if err := json.Unmarshal([]byte(request.ImageCredentials), &credData); err == nil {
            registry := credData["registry"].(string)
            creds := credData["credentials"].(map[string]interface{})
            
            // Convert to string map
            credMap := make(map[string]string)
            for k, v := range creds {
                credMap[k] = v.(string)
            }
            
            // Create provider
            if provider, err := oci.CreateProviderFromEnv(ctx, registry, credKeys(credMap)); err == nil {
                mountOptions.RegistryCredProvider = provider
            }
        }
    }
}

// Helper to extract keys
func credKeys(creds map[string]string) []string {
    keys := make([]string, 0, len(creds))
    for k := range creds {
        keys = append(keys, k)
    }
    return keys
}
```

### 🗑️ 7. Remove gRPC Method

Delete `getCredentialProviderForImage()` from `pkg/worker/image.go` - no longer needed since credentials come in the request.

## Flow Diagram

```
Build Time:
  User provides base_image_creds
    ↓
  createCredentialSecretIfNeeded() 
    ↓
  Secret stored in workspace
    ↓
  Reference saved in image table

Runtime:
  Scheduler creates ContainerRequest
    ↓
  Fetches secret by image_id
    ↓
  Adds secret VALUE to request.ImageCredentials
    ↓
  Worker receives request
    ↓
  Parses ImageCredentials JSON
    ↓
  Creates CLIP provider
    ↓
  Passes to CLIP mount
    ↓
  Fresh credentials every time!
```

## Benefits

✅ **Secrets stored** - Created once at build time
✅ **No gRPC complexity** - Credentials in the request
✅ **Fresh at runtime** - Retrieved from DB each time
✅ **Credential rotation** - Update secret, next run uses new value
✅ **Simpler than original** - No worker-to-gateway gRPC calls
✅ **Modal-style** - Similar to their approach

## Testing

1. Build image with `base_image_creds` → Secret created
2. Check database → `image` table has `credential_secret_name`
3. Run container → Scheduler fetches secret, adds to request
4. Worker logs → "using credential provider for OCI image mount"
5. CLIP fetches layers successfully

## Files Modified

- ✅ `pkg/oci/credentials.go` - Helper functions
- ✅ `pkg/abstractions/image/image.go` - Secret creation
- ✅ `pkg/repository/backend_postgres.go` - Repository methods
- ✅ `pkg/repository/backend_postgres_migrations/036_*.go` - Migration
- ✅ `pkg/types/scheduler.go` - ContainerRequest field
- ✅ `pkg/types/types.proto` - Proto definition
- ✅ `proto/types.pb.go` - Generated code
- 🔧 `pkg/scheduler/*.go` - Need to fetch & pass secrets (TODO)
- 🔧 `pkg/worker/image.go` - Need to use request credentials (TODO)

