# OCI Credentials - Hybrid Approach Implementation

## ✅ What's Implemented (Commit: 52419832)

### 1. Secret Creation at Build Time
**File:** `pkg/abstractions/image/image.go`
- `createCredentialSecretIfNeeded()` automatically creates workspace secrets
- Parses `base_image_creds` from Python SDK
- Stores credentials in database with reference to image

### 2. Database Schema
**File:** `pkg/repository/backend_postgres_migrations/036_add_image_credential_fields.go`
- Adds `credential_secret_name` and `credential_secret_id` columns to `image` table
- Adds index for faster lookups
- Migration ready to run

### 3. Repository Methods
**Files:** `pkg/repository/base.go`, `pkg/repository/backend_postgres.go`
- `SetImageCredentialSecret(imageId, secretName, secretId)` 
- `GetImageCredentialSecret(imageId) (secretName, secretId, error)`

### 4. OCI Helper Functions
**File:** `pkg/oci/credentials.go`
- `ParseRegistry(imageRef)` - Extract registry hostname
- `DetectCredentialType(registry, creds)` - Auto-detect credential type
- `MarshalCredentials(registry, type, creds)` - JSON serialization
- `CreateSecretName(registry)` - Generate consistent secret name
- `ParseCredentialsFromEnv(envVars)` - Filter to known keys
- `CreateProviderFromEnv(ctx, imageRef, credKeys)` - Create CLIP provider

### 5. ContainerRequest Update
**Files:** `pkg/types/scheduler.go`, `pkg/types/types.proto`, `proto/types.pb.go`
- Added `ImageCredentials string` field to `ContainerRequest`
- Proto updated (regenerate with `make proto` when protoc available)

## 🔧 What's Left To Do

### 6. Scheduler Integration
**Where:** Find where `ContainerRequest` is created for runtime (not build)

**Add this code:**
```go
// After creating ContainerRequest, before sending to worker:
secretName, _, err := backendRepo.GetImageCredentialSecret(ctx, request.ImageId)
if err == nil && secretName != "" {
    secret, err := backendRepo.GetSecretByNameDecrypted(ctx, &request.Workspace, secretName)
    if err == nil {
        request.ImageCredentials = secret.Value  // JSON string with credentials
    }
}
```

### 7. Worker Integration
**File:** `pkg/worker/image.go` in `PullLazy()` method

**Replace the gRPC provider code with:**
```go
if strings.ToLower(storageType) == string(clipCommon.StorageModeOCI) {
    mountOptions.StorageInfo = nil
    
    // Use credentials from request
    if request.ImageCredentials != "" {
        var credData map[string]interface{}
        if err := json.Unmarshal([]byte(request.ImageCredentials), &credData); err == nil {
            registry := credData["registry"].(string)
            creds := credData["credentials"].(map[string]interface{})
            
            // Convert to string map and get keys
            credMap := make(map[string]string)
            credKeys := []string{}
            for k, v := range creds {
                credMap[k] = v.(string)
                credKeys = append(credKeys, k)
                os.Setenv(k, v.(string))  // Set for CreateProviderFromEnv
            }
            
            // Create provider
            if provider, err := oci.CreateProviderFromEnv(ctx, registry, credKeys); err == nil && provider != nil {
                mountOptions.RegistryCredProvider = provider
                log.Info().Str("image_id", imageId).Msg("using credential provider from request")
            }
        }
    }
}
```

### 8. Cleanup
**File:** `pkg/worker/image.go`
- Delete `getCredentialProviderForImage()` method - no longer needed

## 🎯 Benefits

✅ **Secrets stored** - Created once at build time, reused
✅ **No gRPC calls** - Credentials passed in ContainerRequest  
✅ **Fresh at runtime** - Retrieved from DB each time
✅ **Credential rotation** - Update secret, next run uses new value
✅ **Modal-style** - Simple and proven pattern

## 📊 Flow

```
BUILD TIME:
  User: base_image_creds=["AWS_..."]
    ↓
  createCredentialSecretIfNeeded()
    ↓
  Secret stored in workspace
    ↓
  Reference in image table

RUNTIME:
  Scheduler creates ContainerRequest
    ↓
  Fetches secret by imageId
    ↓
  Adds to request.ImageCredentials
    ↓
  Worker parses JSON
    ↓
  Creates CLIP provider
    ↓
  Mounts image with credentials
```

## 🧪 Testing

1. **Build image** with `base_image_creds`
2. **Check DB:** `SELECT credential_secret_name FROM image WHERE image_id = '...'`
3. **Run container:** Scheduler should fetch secret
4. **Worker logs:** "using credential provider from request"
5. **CLIP:** Successfully fetches layers

## 📁 Files

### Modified
- `pkg/oci/credentials.go` (new)
- `pkg/abstractions/image/image.go` (createCredentialSecretIfNeeded)
- `pkg/repository/backend_postgres_migrations/036_*.go` (new)
- `pkg/repository/backend_postgres.go` (new methods)
- `pkg/types/scheduler.go` (ImageCredentials field)
- `pkg/types/types.proto` (ImageCredentials field)
- `proto/types.pb.go` (generated)

### TODO
- `pkg/scheduler/*.go` (fetch & pass secrets)
- `pkg/worker/image.go` (use request credentials, remove gRPC method)

---

**Branch:** `feat/oci-credentials-hybrid-approach`
**Commit:** `52419832`
**Status:** Core infrastructure complete, integration pending
**Docs:** See `HYBRID_APPROACH_IMPLEMENTATION.md` for details
