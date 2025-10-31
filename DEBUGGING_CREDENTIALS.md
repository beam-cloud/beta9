# Debugging Credential Management Issues

## Changes Made

I've added comprehensive logging throughout the credential management flow to help identify where secrets aren't being created or attached. Here's what to look for in the logs:

## 1. Build Time - Gateway Logs

After a build completes, look for these log messages:

### Secret Creation Attempt
```
DBG checking if credentials should be saved as secret 
    image_id=<id> 
    existing_image_uri=<uri> 
    existing_image_creds_count=<count>
```

### If Processing ExistingImageCreds (from_registry)
```
DBG processing ExistingImageCreds image_id=<id>
DBG got registry credentials registry=<registry> creds_count=<count>
DBG detected credential type cred_type=<type>
DBG marshaled credentials has_cred_str=true
DBG proceeding with secret creation base_image=<image>
```

### Secret Creation Success
```
INF credential secret saved 
    image_id=<id> 
    secret_name=oci-registry-<registry-name> 
    registry=<registry> 
    cred_type=aws|gcp|basic|token
```

### Common Issues

**If you see:**
```
DBG no credentials provided, skipping secret creation
```
**Problem:** ExistingImageCreds is empty or BaseImageCreds is empty
**Solution:** Verify SDK is passing credentials correctly

**If you see:**
```
DBG missing base image or credentials, skipping secret creation
```
**Problem:** Either baseImage or credStr is empty after processing
**Check:** Look at earlier logs to see which branch was taken

**If you see:**
```
WRN failed to get registry credentials, skipping secret creation
```
**Problem:** GetRegistryCredentials() failed
**Check:** Verify credential format matches registry type (ECR needs AWS keys, etc.)

## 2. Runtime - Scheduler Logs

When a container is scheduled, look for:

### Credential Attachment Attempt
```
DBG checking for image credentials 
    container_id=<id> 
    image_id=<id>
```

### Secret Found
```
DBG found credential secret, retrieving secret value 
    secret_name=oci-registry-<registry-name> 
    secret_id=<id>
```

### Credentials Attached
```
INF attached OCI credentials 
    container_id=<id> 
    image_id=<id> 
    secret_name=<name> 
    credentials_length=<length>
```

### Common Issues

**If you see:**
```
DBG no credential secret found for image
```
**Problem:** Secret wasn't created or not linked to image
**Solution:** Check build logs to see if secret was created

**If you see:**
```
DBG build container, skipping credential attachment
```
**This is normal** - Build containers get credentials from BuildOptions, not secrets

**If you see:**
```
WRN failed to get secret by name
```
**Problem:** Secret exists in image table but not in secrets table
**Solution:** Check database consistency

## 3. Runtime - Worker Logs

When the worker mounts the image, look for:

### Credential Check
```
DBG checking for OCI image credentials 
    image_id=<id> 
    container_id=<id> 
    has_image_credentials=true 
    credentials_length=<length>
```

### Credential Provider Creation
```
DBG attempting to create credential provider from credentials 
    image_id=<id> 
    cred_str_len=<length>
```

```
DBG parsed credential JSON successfully 
    image_id=<id> 
    registry=<registry> 
    creds_count=<count>
```

### Provider Attached
```
INF attached custom credential provider for OCI image 
    image_id=<id> 
    container_id=<id> 
    provider_name=creds-<registry>
```

### Common Issues

**If you see:**
```
DBG no image credentials provided, using default provider chain
```
**Problem:** Scheduler didn't attach credentials
**Solution:** Check scheduler logs to see if secret was found

**If you see:**
```
WRN failed to create credential provider despite having credentials
```
**Problem:** Credentials are present but malformed
**Check:** Look for earlier warning about JSON parsing

**If you see:**
```
WRN failed to parse image credentials JSON
```
**Problem:** Credentials are not in valid JSON format
**Solution:** Check secret creation logic in gateway

**If you see:**
```
WRN missing registry or credentials in JSON
```
**Problem:** JSON is valid but missing required fields
**Solution:** Check credential marshaling in gateway

## Expected Flow for Working System

For a successful build and runtime with private registry:

### Build (Gateway)
```
DBG checking if credentials should be saved as secret (existing_image_creds_count=3)
DBG processing ExistingImageCreds
DBG got registry credentials (registry=187248174200.dkr.ecr.us-east-1.amazonaws.com, creds_count=3)
DBG detected credential type (cred_type=aws)
DBG marshaled credentials (has_cred_str=true)
DBG proceeding with secret creation
INF credential secret saved (secret_name=oci-registry-187248174200-dkr-ecr-us-east-1-amazonaws-com)
```

### Runtime (Scheduler)
```
DBG checking for image credentials (image_id=bc68a8dce91b341f)
DBG found credential secret, retrieving secret value (secret_name=oci-registry-187248174200-dkr-ecr-us-east-1-amazonaws-com)
INF attached OCI credentials (credentials_length=256)
```

### Runtime (Worker)
```
DBG checking for OCI image credentials (has_image_credentials=true, credentials_length=256)
DBG attempting to create credential provider from credentials (cred_str_len=256)
DBG parsed credential JSON successfully (registry=187248174200.dkr.ecr.us-east-1.amazonaws.com, creds_count=3)
INF attached custom credential provider for OCI image (provider_name=creds-187248174200.dkr.ecr.us-east-1.amazonaws.com)
INF initialized OCI storage with disk cache (cred_provider=creds-187248174200.dkr.ecr.us-east-1.amazonaws.com)
```

## Next Steps

1. **Trigger a new build** with your ECR image and credentials
2. **Collect gateway logs** during and after the build - look for secret creation
3. **Collect scheduler logs** when running the container - look for credential attachment
4. **Collect worker logs** when the container starts - look for provider creation

## Quick Debug Checklist

- [ ] Gateway: Is `existing_image_creds_count > 0`?
- [ ] Gateway: Did secret creation succeed? (INF credential secret saved)
- [ ] Scheduler: Was secret found? (DBG found credential secret)
- [ ] Scheduler: Were credentials attached? (INF attached OCI credentials)
- [ ] Worker: Were credentials received? (has_image_credentials=true)
- [ ] Worker: Was provider created? (INF attached custom credential provider)

## If Still Not Working

If you see all the success messages but still get 401 Unauthorized:

1. **Verify credential values** - The credentials might be incorrect/expired
2. **Check registry URL** - Make sure it matches exactly
3. **Test credentials manually** - Try docker login with the same credentials
4. **Check CLIP version** - Must be v2 for this to work

## Database Verification

You can also verify secrets are being created in the database:

```sql
-- Check if image has credential secret
SELECT image_id, credential_secret_name, credential_secret_id 
FROM image 
WHERE image_id = 'bc68a8dce91b341f';

-- Check if secret exists
SELECT name, external_id 
FROM secret 
WHERE name LIKE 'oci-registry-%';
```

If `credential_secret_name` is NULL, the secret wasn't created.
If the secret name exists but the secret doesn't exist in the secrets table, there's a data inconsistency.
