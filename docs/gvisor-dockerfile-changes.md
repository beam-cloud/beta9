# gVisor Dockerfile Changes

## Overview

The worker Dockerfile has been updated to include gVisor (runsc) alongside runc, enabling multi-runtime support.

## Changes Made

### 1. New Build Stage: gVisor

Added a new build stage that downloads and installs the runsc binary:

```dockerfile
# gVisor (runsc)
# ========================
FROM debian:bookworm AS gvisor

ARG TARGETARCH
ARG GVISOR_VERSION=20250407.0

WORKDIR /tmp

RUN apt-get update && apt-get install -y curl

RUN <<EOT
set -eux

# Determine the correct architecture for gVisor
if [ "$TARGETARCH" = "amd64" ]; then
    GVISOR_ARCH="x86_64"
elif [ "$TARGETARCH" = "arm64" ]; then
    GVISOR_ARCH="aarch64"
else
    echo "Unsupported architecture: $TARGETARCH" && exit 1
fi

# Download runsc and its checksum
curl -fsSL https://storage.googleapis.com/gvisor/releases/release/${GVISOR_VERSION}/${GVISOR_ARCH}/runsc -o /usr/local/bin/runsc
curl -fsSL https://storage.googleapis.com/gvisor/releases/release/${GVISOR_VERSION}/${GVISOR_ARCH}/runsc.sha512 -o /tmp/runsc.sha512

# Verify checksum
sha512sum -c /tmp/runsc.sha512

# Make runsc executable
chmod +x /usr/local/bin/runsc

# Verify installation
/usr/local/bin/runsc --version
EOT
```

### 2. Copy runsc to Final Image

Added the runsc binary to the final image:

```dockerfile
COPY --from=gvisor /usr/local/bin/runsc /usr/local/bin/runsc
```

### 3. Create Runtime Directories

Created the default gVisor state directory:

```dockerfile
# Create gVisor runtime directories
RUN mkdir -p /run/gvisor
```

## Architecture Support

The installation script supports both architectures:
- **amd64** (x86_64) - Standard Intel/AMD servers
- **arm64** (aarch64) - ARM-based servers (e.g., AWS Graviton)

## Version Management

The gVisor version is controlled by the `GVISOR_VERSION` build argument:

```bash
# Build with specific gVisor version
docker build --build-arg GVISOR_VERSION=20250407.0 -f docker/Dockerfile.worker .

# Build with latest nightly (not recommended for production)
docker build --build-arg GVISOR_VERSION=latest -f docker/Dockerfile.worker .
```

### Recommended Versions

- **Production**: Use a specific release version (e.g., `20250407.0`)
- **Testing**: Can use `latest` but pin before production deployment

To find available versions, check: https://gvisor.dev/docs/user_guide/install/

## Security

The installation includes checksum verification:
1. Downloads runsc binary
2. Downloads corresponding SHA512 checksum
3. Verifies the binary against the checksum
4. Fails build if verification fails

## Verification

After building, verify gVisor is installed correctly:

```bash
# Check runsc is available
docker run --rm <worker-image> runsc --version

# Expected output:
# runsc version release-20250407.0
# spec: 1.0.0
```

## Image Size Impact

Adding gVisor increases the image size by approximately:
- **runsc binary**: ~60-80 MB (varies by architecture and version)
- **Total impact**: Minimal compared to overall worker image size

## Runtime Selection

With both runc and runsc installed, runtime selection happens at the worker pool level:

```yaml
worker:
  pools:
    runc-pool:
      runtime: "runc"
    
    gvisor-pool:
      runtime: "gvisor"
      runtimeConfig:
        gvisorPlatform: "ptrace"  # or "kvm"
```

## Troubleshooting

### Build Fails with Architecture Error

**Error**: `Unsupported architecture: <arch>`

**Solution**: Ensure you're building for amd64 or arm64:
```bash
docker buildx build --platform linux/amd64 -f docker/Dockerfile.worker .
# or
docker buildx build --platform linux/arm64 -f docker/Dockerfile.worker .
```

### Checksum Verification Fails

**Error**: `sha512sum: WARNING: X computed checksums did NOT match`

**Solutions**:
1. Check internet connectivity
2. Verify the GVISOR_VERSION exists: https://storage.googleapis.com/gvisor/releases/release/
3. Try rebuilding with `--no-cache`

### runsc Not Found at Runtime

**Error**: `failed to create gvisor runtime: runsc binary not found in PATH`

**Solution**: Verify the COPY command in the Dockerfile is present:
```dockerfile
COPY --from=gvisor /usr/local/bin/runsc /usr/local/bin/runsc
```

### KVM Platform Not Available

**Error**: Platform initialization failed for KVM

**Note**: This is expected if:
- Running in a container without KVM access
- Host doesn't have KVM enabled
- Not running with proper privileges

**Solution**: Use ptrace platform instead:
```yaml
runtimeConfig:
  gvisorPlatform: "ptrace"
```

## Multi-Architecture Builds

To build for multiple architectures:

```bash
docker buildx create --use
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f docker/Dockerfile.worker \
  -t myregistry/worker:latest \
  --push \
  .
```

## Updating gVisor Version

To update to a newer gVisor version:

1. Check available versions: https://github.com/google/gvisor/releases
2. Update the `GVISOR_VERSION` in the Dockerfile
3. Test the new version in a non-production environment
4. Rebuild and deploy

```dockerfile
# Update this line in Dockerfile.worker
ARG GVISOR_VERSION=20250407.0  # Change to new version
```

## Related Documentation

- [Container Runtime Configuration](container-runtime-configuration.md)
- [gVisor Installation Guide](https://gvisor.dev/docs/user_guide/install/)
- [gVisor Architecture](https://gvisor.dev/docs/architecture_guide/)
