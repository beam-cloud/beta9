#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"

if ! have_cmd tailscale; then
  if require_install_mode "tailscale"; then
    if have_cmd curl; then
      curl -fsSL https://tailscale.com/install.sh | sh
    else
      log_fail "curl is required to install tailscale"
    fi
  fi
fi

if ! have_cmd tailscale; then
  log_fail "tailscale command is unavailable"
  exit 1
fi

log_pass "tailscale command available"

if systemctl is-active --quiet tailscaled 2>/dev/null; then
  log_pass "tailscaled service is active"
else
  if is_install; then
    run_sudo systemctl enable --now tailscaled || log_fail "failed to start tailscaled"
  else
    log_fail "tailscaled service is not active"
  fi
fi

ip="$(tailscale ip -4 2>/dev/null | head -n1 || true)"
if [ -z "$ip" ] && is_install && [ -n "${TS_AUTHKEY:-}" ]; then
  run_sudo tailscale up --auth-key "$TS_AUTHKEY" --ssh || log_fail "tailscale authentication failed"
  ip="$(tailscale ip -4 2>/dev/null | head -n1 || true)"
fi

if [ -n "$ip" ]; then
  record_fact tailscale_ip "$ip"
  log_pass "tailscale IPv4 detected: $ip"
else
  log_warn "tailscale is installed but not authenticated or no IPv4 address is assigned"
fi

status="$(tailscale status --json 2>/dev/null || true)"
if [ -n "$status" ] && have_cmd jq; then
  dns="$(printf '%s' "$status" | jq -r '.Self.DNSName // empty')"
  if [ -n "$dns" ]; then
    record_fact tailscale_dns "$dns"
    log_pass "tailscale DNS detected: $dns"
  fi
fi

