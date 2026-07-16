#!/usr/bin/env bash
set -uo pipefail

ROLE=""
MODE="check"
VERBOSE=0
FAILURES=0
WARNINGS=0
FACT_DIR="${FACT_DIR:-/tmp/beam-deployment-state}"
FACT_FILE=""

usage_common() {
  cat <<'EOF'
Usage: run.sh --role master|gpu|cpu [--mode check|install] [--verbose]

Modes:
  check    Verify prerequisites only. This is the default.
  install  Install or configure missing prerequisites where the script supports it.
EOF
}

parse_common_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --role)
        ROLE="${2:-}"
        shift 2
        ;;
      --mode)
        MODE="${2:-}"
        shift 2
        ;;
      --check)
        MODE="check"
        shift
        ;;
      --install)
        MODE="install"
        shift
        ;;
      --verbose|-v)
        VERBOSE=1
        shift
        ;;
      --help|-h)
        usage_common
        exit 0
        ;;
      *)
        log_fail "unknown argument: $1"
        usage_common
        exit 2
        ;;
    esac
  done

  case "$ROLE" in
    master|gpu|cpu) ;;
    "")
      log_fail "--role is required"
      exit 2
      ;;
    *)
      log_fail "unsupported role: $ROLE"
      exit 2
      ;;
  esac

  case "$MODE" in
    check|install) ;;
    *)
      log_fail "unsupported mode: $MODE"
      exit 2
      ;;
  esac

  mkdir -p "$FACT_DIR"
  FACT_FILE="$FACT_DIR/$(hostname -s).facts.env"
}

init_fact_file() {
  mkdir -p "$FACT_DIR"
  : > "$FACT_FILE"
}

ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
  printf '%s [%s] %s\n' "$(ts)" "$1" "$2"
}

log_info() { log INFO "$*"; }
log_pass() { log PASS "$*"; }
log_skip() { log SKIP "$*"; }
log_warn() { WARNINGS=$((WARNINGS + 1)); log WARN "$*"; }
log_fail() { FAILURES=$((FAILURES + 1)); log FAIL "$*"; }

debug() {
  if [ "$VERBOSE" -eq 1 ]; then
    log DEBUG "$*"
  fi
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

need_cmd() {
  if have_cmd "$1"; then
    log_pass "command available: $1"
    return 0
  fi
  log_fail "missing command: $1"
  return 1
}

run_sudo() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  else
    sudo "$@"
  fi
}

is_master() { [ "$ROLE" = "master" ]; }
is_gpu() { [ "$ROLE" = "gpu" ]; }
is_cpu() { [ "$ROLE" = "cpu" ]; }
is_install() { [ "$MODE" = "install" ]; }

require_install_mode() {
  if ! is_install; then
    log_warn "$1 missing; rerun with --mode install to let scripts attempt installation"
    return 1
  fi
  return 0
}

use_k3s_kubeconfig() {
  if [ -z "${KUBECONFIG:-}" ] && [ -r /etc/rancher/k3s/k3s.yaml ]; then
    export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
    debug "using KUBECONFIG=$KUBECONFIG"
  fi
}

record_fact() {
  key="$1"
  value="$2"
  printf '%s=%q\n' "$key" "$value" >> "$FACT_FILE"
}

summary_and_exit() {
  log_info "facts written to $FACT_FILE"
  if [ "$WARNINGS" -gt 0 ]; then
    log_warn "warnings: $WARNINGS"
  fi
  if [ "$FAILURES" -gt 0 ]; then
    log_fail "failed checks: $FAILURES"
    exit 1
  fi
  log_pass "all required checks passed"
}

