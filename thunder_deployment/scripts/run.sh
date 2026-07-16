#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

parse_common_args "$@"
init_fact_file

log_info "starting beam node preflight role=$ROLE mode=$MODE host=$(hostname -f 2>/dev/null || hostname)"

checks=(
  "00-host-baseline.sh"
  "01-tailscale.sh"
  "02-k3s-server.sh"
  "03-k3s-agent.sh"
  "04-container-runtime.sh"
  "05-nvidia.sh"
  "06-k8s-gpu-device-plugin.sh"
  "07-registry-access.sh"
  "08-beta9-readiness.sh"
)

for check in "${checks[@]}"; do
  path="$SCRIPT_DIR/$check"
  if [ ! -x "$path" ]; then
    log_fail "script is missing or not executable: $path"
    continue
  fi
  log_info "running $check"
  "$path" --role "$ROLE" --mode "$MODE"
  status=$?
  if [ "$status" -ne 0 ]; then
    log_fail "$check failed with exit code $status"
  fi
done

summary_and_exit

