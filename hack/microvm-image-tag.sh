#!/bin/sh
# Content-derived tags for the runtime (default) or NVIDIA guest image.
set -eu
cd "$(dirname "$0")/.."
variant="${1:-runtime}"
[ "$#" -le 1 ] || { echo "usage: $0 [runtime|nvidia]" >&2; exit 1; }
set -- docker/Dockerfile.microvm docker/microvm
suffix=""
case "$variant" in
  runtime) ;;
  nvidia) set -- "$@" docker/Dockerfile.vm-gpu; suffix="-nvidia" ;;
  *) echo "usage: $0 [runtime|nvidia]" >&2; exit 1 ;;
esac
if command -v sha256sum >/dev/null 2>&1; then
  sum() { sha256sum "$@"; }
else
  sum() { shasum -a 256 "$@"; }
fi
files="$(find "$@" -type f)"
tag="$(printf '%s\n' "$files" | LC_ALL=C sort | while read -r f; do sum "$f"; done | sum | cut -c1-12)"
printf '%s%s\n' "$tag" "$suffix"
