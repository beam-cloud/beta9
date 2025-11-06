#!/bin/bash
# runsc-wrapper.sh
# Wrapper script to invoke runsc with Beta9-specific defaults
# This allows running "runsc list" without manually specifying --root and --platform

# Beta9 gVisor configuration
GVISOR_ROOT="${GVISOR_ROOT:-/run/gvisor}"
GVISOR_PLATFORM="${GVISOR_PLATFORM:-systrap}"

# Build args with Beta9 defaults
args=("--root" "$GVISOR_ROOT")

# Add platform if set
if [ -n "$GVISOR_PLATFORM" ]; then
    args+=("--platform" "$GVISOR_PLATFORM")
fi

# Append user's arguments
args+=("$@")

# Execute real runsc binary
exec /usr/local/bin/runsc.real "${args[@]}"
