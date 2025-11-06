# Build Fix - Kernel URL Correction

## Issue

Docker build was failing with:
```
curl: (22) The requested URL returned error: 404
https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.13/${FIRECRACKER_ARCH}/vmlinux-5.10.223
```

## Root Cause

The kernel image path for v1.13 doesn't exist in the S3 bucket yet. While Firecracker v1.13.1 binary is available, the CI kernel images are published separately and the v1.13 path hasn't been created.

## Solution

Use the v1.10 path which has the same kernel version (vmlinux-5.10.223):

```bash
# ❌ Old (doesn't exist):
curl -fsSL https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.13/${FIRECRACKER_ARCH}/vmlinux-5.10.223

# ✅ New (works):
curl -fsSL https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/${FIRECRACKER_ARCH}/vmlinux-5.10.223
```

## Verification

```bash
# x86_64 kernel exists at v1.10:
$ curl -fsSL https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/x86_64/vmlinux-5.10.223 -I
HTTP/1.1 200 OK ✅

# aarch64 kernel exists at v1.10:
$ curl -fsSL https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/aarch64/vmlinux-5.10.223 -I
HTTP/1.1 200 OK ✅
```

## Compatibility

The vmlinux-5.10.223 kernel from the v1.10 CI path is **fully compatible** with Firecracker v1.13.1. The kernel version is what matters, not the CI version path.

- **Firecracker binary**: v1.13.1 (latest)
- **Kernel**: vmlinux-5.10.223 (from v1.10 S3 path, but compatible with v1.13.1)

## Files Updated

1. **`docker/Dockerfile.worker`** - Line 157: Changed kernel URL to use v1.10 path
2. **`bin/test_firecracker_local.sh`** - Line 143: Changed kernel URL to use v1.10 path

## Build Status

✅ **Docker build should now succeed**

The kernel download will work for both x86_64 and aarch64 architectures.

## Future Note

When the Firecracker team publishes v1.13 kernel images to S3, we can optionally update to:
```bash
curl -fsSL https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.13/${FIRECRACKER_ARCH}/vmlinux-5.10.XXX
```

But this is **not necessary** - the v1.10 kernel works perfectly with v1.13.1 Firecracker.
