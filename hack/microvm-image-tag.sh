#!/bin/sh
# Prints the beta9-microvm tag for the current docker/Dockerfile.microvm inputs:
# a hash of the Dockerfile, kernel inputs and virtiofsd patches, so changes need a
# new image and a new pin in the worker Dockerfiles.
set -eu
cd "$(dirname "$0")/.."
if command -v sha256sum >/dev/null 2>&1; then
  sum() { sha256sum "$@"; }
else
  sum() { shasum -a 256 "$@"; }
fi
find docker/Dockerfile.microvm docker/microvm docker/virtiofsd -type f | LC_ALL=C sort | while read -r f; do sum "$f"; done | sum | cut -c1-12
