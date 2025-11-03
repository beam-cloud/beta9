# How to Verify the Optimization is Working

## Expected Log Flow (Correct Behavior)

### Phase 1: Buildah Build (EXPECTED copying)
```
COMMIT 54051e496b5c2463:latest
Getting image source signatures
Copying blob sha256:...      <-- This is EXPECTED (buildah push to OCI)
Copying blob sha256:...      <-- This is EXPECTED (buildah push to OCI)
Writing manifest to image destination
Successfully tagged localhost/54051e496b5c2463:latest
```
**Status**: ✅ This is normal - buildah is copying the image to local OCI layout

### Phase 2: CLIP Indexing Start (Our optimization)
```
indexing from local OCI layout with remote storage reference
source_ref=oci:/tmp/1282758051/oci:latest    <-- LOCAL reference (good!)
storage_ref=187248174200.dkr.ecr...          <-- REMOTE reference (for runtime)
Indexing from local: oci:/tmp/1282758051/oci:latest
```
**Status**: ✅ CLIP is configured to read from local

### Phase 3: CLIP Indexing Progress (CHECK THIS!)
```
Indexing layer 1/5 (20%)
Completed layer 1/5 (20%, 1234 files indexed)
Indexing layer 2/5 (40%)
...
```

**CRITICAL**: Look for "Copying blob" or "Getting image source signatures" messages here.

- ✅ **GOOD**: Only see "Indexing layer" and "Completed layer" messages
- ❌ **BAD**: See "Copying blob" messages during this phase

### Phase 4: Push to Remote Registry
```
Pushing image to build registry: 187248174200.dkr.ecr...
Getting image source signatures
Copying blob sha256:...      <-- This is EXPECTED (push to remote)
Copying blob sha256:...      <-- This is EXPECTED (push to remote)
Writing manifest to image destination
```
**Status**: ✅ This is normal - pushing to remote registry for runtime

## Summary

The optimization is working correctly if:

1. ✅ You see `source_ref=oci:/tmp/.../oci:latest` in the logs
2. ✅ CLIP indexing progress shows only "Indexing layer" messages
3. ✅ NO "Copying blob" during CLIP indexing phase
4. ✅ "Copying blob" messages only appear:
   - Before indexing (buildah → OCI layout)
   - After indexing (buildah → remote registry)

## What You Previously Saw

From your logs:
```
Successfully tagged localhost/54051e496b5c2463:latest
Getting image source signatures        <-- buildah push to OCI
Copying blob sha256:222d3cf7...        <-- buildah push to OCI
Copying blob sha256:ab34259f...        <-- buildah push to OCI
Writing manifest to image destination  <-- buildah push to OCI
indexing from local OCI layout...      <-- Our optimization starts HERE
```

This is **CORRECT**! The "Copying blob" happened BEFORE indexing as part of buildah's push to OCI.

## Next Steps

Please check your full logs and confirm:
- Are there any "Copying blob" messages AFTER the "Indexing image from local OCI layout..." line?
- What messages appear during the actual indexing progress?

If you only see "Indexing layer" / "Completed layer" messages during indexing, the optimization is working!
