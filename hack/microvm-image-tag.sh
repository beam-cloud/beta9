#!/bin/sh
# Prints the beta9-microvm tag for the current microvm inputs: a hash of the
# microvm section of docker/Dockerfile.worker and of docker/microvm/, so a
# change to either publishes a new image.
set -eu
cd "$(dirname "$0")/.."
if command -v sha256sum >/dev/null 2>&1; then
  sum() { sha256sum "$@"; }
else
  sum() { shasum -a 256 "$@"; }
fi
{
  awk '/^# Cloud Hypervisor, virtiofsd and the guest kernel \(microvm sandboxes\)$/,/^# beta9 worker$/' docker/Dockerfile.worker
  find docker/microvm -type f | LC_ALL=C sort | while read -r f; do sum "$f"; done
} | sum | cut -c1-12
