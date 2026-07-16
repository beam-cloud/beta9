#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"

registry="${BEAM_REGISTRY:-}"
if [ -z "$registry" ]; then
  log_warn "BEAM_REGISTRY is not set; skipping registry pull checks"
  exit 0
fi

record_fact beam_registry "$registry"

if have_cmd docker && docker info >/dev/null 2>&1; then
  image="$registry/beta9-worker:latest"
  if is_master; then
    image="$registry/beta9-gateway:latest"
  fi
  if docker manifest inspect "$image" >/dev/null 2>&1 || docker pull "$image" >/dev/null 2>&1; then
    log_pass "registry image reachable: $image"
  else
    log_warn "could not verify registry image: $image"
  fi
else
  log_skip "docker unavailable; registry checks should be performed through Kubernetes image pulls"
fi

