# Summary of Changes for Credential Management Debugging

## Problem
Secrets weren't being created after builds, causing runtime failures when trying to pull private images from ECR and other registries.

## Root Cause Analysis
The secret creation logic was in place but lacked visibility into what was happening. Without proper logging, it was impossible to determine:
1. Whether secrets were being created after builds
2. Whether secrets were being retrieved by the scheduler
3. Whether credentials were being attached to container requests
4. Whether the worker was receiving and using the credentials

## Changes Made

### 1. Enhanced Gateway Logging (`pkg/abstractions/image/image.go`)

Added comprehensive logging in `createCredentialSecretIfNeeded()`:
- Log when checking for credentials to save
- Log when processing ExistingImageCreds (from_registry)
- Log when processing BaseImageCreds (base_image)
- Log credential type detection
- Log credential marshaling
- Log when proceeding with secret creation
- Log when skipping secret creation (and why)
- Log successful secret creation with full details

**Key Debug Points:**
- See if `existing_image_creds_count > 0` 
- See if credentials are being marshaled
- See if secret creation succeeds

### 2. Enhanced Scheduler Logging (`pkg/scheduler/scheduler.go`)

Added detailed logging in `attachImageCredentials()`:
- Log when checking for image credentials
- Log when skipping (no image ID or build container)
- Log when secret is found in database
- Log when retrieving secret value
- Log successful credential attachment with length
- Log errors when secret retrieval fails

**Key Debug Points:**
- See if secrets are found for images
- See if secrets are successfully retrieved
- See credential string length (should be >0)

### 3. Enhanced Worker Logging (`pkg/worker/image.go`)

Added detailed logging in credential provider creation:
- Log when checking for OCI image credentials
- Log credential string presence and length
- Log JSON parsing attempts
- Log successful JSON parsing with registry and cred count
- Log provider creation success
- Log when using default provider chain (no credentials)
- Log warnings for parsing failures

**Key Debug Points:**
- See if `has_image_credentials=true`
- See if JSON parsing succeeds
- See if custom provider is attached

## Expected Behavior

### Successful Build with Private Registry

1. **Gateway logs after build:**
```
INF credential secret saved 
    image_id=bc68a8dce91b341f 
    secret_name=oci-registry-187248174200-dkr-ecr-us-east-1-amazonaws-com 
    registry=187248174200.dkr.ecr.us-east-1.amazonaws.com 
    cred_type=aws
```

2. **Scheduler logs at runtime:**
```
INF attached OCI credentials 
    container_id=sandbox-xxx 
    image_id=bc68a8dce91b341f 
    secret_name=oci-registry-187248174200-dkr-ecr-us-east-1-amazonaws-com 
    credentials_length=256
```

3. **Worker logs at runtime:**
```
INF attached custom credential provider for OCI image 
    image_id=bc68a8dce91b341f 
    container_id=sandbox-xxx 
    provider_name=creds-187248174200.dkr.ecr.us-east-1.amazonaws.com
```

## Files Modified

1. `pkg/abstractions/image/image.go` - Enhanced secret creation logging
2. `pkg/scheduler/scheduler.go` - Enhanced credential attachment logging
3. `pkg/worker/image.go` - Enhanced credential provider logging

## How to Debug

1. **Trigger a new build** with your private registry image
2. **Check gateway logs** for "credential secret saved" message
3. **Try to run the image** 
4. **Check scheduler logs** for "attached OCI credentials" message
5. **Check worker logs** for "attached custom credential provider" message

If any of these steps fail, the debug logs will show exactly where and why.

## Common Issues and Solutions

### Issue: "no credentials provided, skipping secret creation"
**Cause:** ExistingImageCreds is empty
**Solution:** Verify SDK is passing credentials in the build request

### Issue: "no credential secret found for image"
**Cause:** Secret wasn't created during build
**Solution:** Check gateway build logs to see why secret creation was skipped

### Issue: "no image credentials provided, using default provider chain"
**Cause:** Scheduler didn't attach credentials
**Solution:** Check if secret exists in database and scheduler logs

### Issue: "failed to parse image credentials JSON"
**Cause:** Credential format is wrong
**Solution:** Check how credentials are being marshaled in gateway

## Verification Commands

```bash
# Watch gateway logs during build
kubectl logs -f <gateway-pod> | grep -E "credential|secret"

# Watch scheduler logs when scheduling
kubectl logs -f <scheduler-pod> | grep -E "credential|secret|image_id"

# Watch worker logs when starting container
kubectl logs -f <worker-pod> | grep -E "credential|provider|ImageCredentials"
```

## Next Steps

Run a new build and collect the logs. Based on what you see, we can:
1. Identify exactly where the flow is breaking
2. Determine if it's a creation, storage, retrieval, or usage issue
3. Fix the specific problem with targeted changes

The extensive logging will make it clear what's happening at each step.
