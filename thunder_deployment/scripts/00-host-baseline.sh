#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"

record_fact role "$ROLE"
record_fact hostname "$(hostname -s)"
record_fact fqdn "$(hostname -f 2>/dev/null || hostname)"
record_fact kernel "$(uname -r)"
record_fact arch "$(uname -m)"

if [ -r /etc/os-release ]; then
  . /etc/os-release
  record_fact os_id "${ID:-unknown}"
  record_fact os_version "${VERSION_ID:-unknown}"
  case "${ID:-}" in
    ubuntu|debian) log_pass "supported OS family: ${ID:-unknown} ${VERSION_ID:-}" ;;
    *) log_warn "untested OS family: ${ID:-unknown} ${VERSION_ID:-}" ;;
  esac
else
  log_warn "/etc/os-release not readable"
fi

for cmd in bash sh awk sed grep cut sort uniq tr date hostname uname df free id; do
  need_cmd "$cmd" || true
done

for cmd in curl jq tar systemctl; do
  if ! have_cmd "$cmd"; then
    if require_install_mode "$cmd"; then
      if have_cmd apt-get; then
        run_sudo apt-get update
        run_sudo apt-get install -y "$cmd"
      else
        log_fail "cannot install $cmd: apt-get not available"
      fi
    fi
  else
    log_pass "command available: $cmd"
  fi
done

root_avail_kb="$(df -Pk / | awk 'NR==2 {print $4}')"
record_fact root_available_kb "$root_avail_kb"
if [ "${root_avail_kb:-0}" -lt 10485760 ]; then
  log_warn "root filesystem has less than 10GiB available"
else
  log_pass "root filesystem free space is acceptable"
fi

mem_mb="$(free -m | awk '/^Mem:/ {print $2}')"
record_fact memory_mb "${mem_mb:-0}"
if is_gpu && [ "${mem_mb:-0}" -lt 32768 ]; then
  log_warn "GPU node has less than 32GiB RAM"
else
  log_pass "memory check complete"
fi

if timedatectl show -p NTPSynchronized --value >/tmp/beam-ntp 2>/dev/null; then
  ntp="$(cat /tmp/beam-ntp)"
  record_fact ntp_synchronized "$ntp"
  [ "$ntp" = "yes" ] && log_pass "time synchronization enabled" || log_warn "time synchronization is not confirmed"
else
  log_warn "timedatectl unavailable or not reporting NTP state"
fi

